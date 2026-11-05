/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

//! Native I/O JNI library for Apache BookKeeper
//!
//! This library provides POSIX file I/O operations via JNI for high-performance direct I/O.
//! It is a Rust implementation of the original C native_io_jni.c

use jni::objects::{JByteArray, JClass, JObject, JString, JThrowable};
use jni::signature::{Primitive, ReturnType};
use jni::sys::{jint, jlong, jlongArray, jobjectArray, jstring};
use jni::JNIEnv;
use std::ffi::{c_void, CString};
use std::ptr;

// Java flags (from NativeIO.java)
const O_CREAT: jint = 0x01;
const O_RDONLY: jint = 0x02;
const O_WRONLY: jint = 0x04;
const O_TRUNC: jint = 0x08;
const O_DIRECT: jint = 0x10;
const O_DSYNC: jint = 0x20;

// ============================================================================
// Helper functions for exception throwing (equivalent to C functions)
// ============================================================================

/// Throws a NativeIOException with the current errno
fn throw_exception_with_errno(env: &mut JNIEnv, message: &str) {
    let errno = std::io::Error::last_os_error().raw_os_error().unwrap_or(0);

    throw_exception_with_error_code(env, message, errno);
}

fn throw_exception_with_error_code(env: &mut JNIEnv, message: &str, error_code: jint) {
    let err_msg = std::io::Error::from_raw_os_error(error_code).to_string();
    let full_msg = format!("{}: {}", message, err_msg);

    if let Ok(class) =
        env.find_class("org/apache/bookkeeper/common/util/nativeio/NativeIOException")
    {
        if let Ok(msg) = env.new_string(&full_msg) {
            let result = env.new_object(
                class,
                "(Ljava/lang/String;I)V",
                &[(&msg).into(), error_code.into()],
            );
            if let Ok(exception) = result {
                let _ = env.throw(JThrowable::from(exception));
            }
        }
    }
}

/// Throws a NativeIOException without errno
fn throw_exception(env: &mut JNIEnv, message: &str) {
    if let Ok(class) =
        env.find_class("org/apache/bookkeeper/common/util/nativeio/NativeIOException")
    {
        let _ = env.throw_new(class, message);
    }
}

// ============================================================================
// JNI exported functions
// ============================================================================

/// Fixed bootstrap data only. Static field lookup may initialize classes; no process or property mutation.
#[cfg(any(target_os = "linux", target_os = "macos"))]
fn jdk_process_launch<'local>(env: &mut JNIEnv<'local>) -> jni::errors::Result<JObject<'local>> {
    let process = env.find_class("java/lang/ProcessImpl")?;
    let enum_class = env.find_class("java/lang/ProcessImpl$LaunchMechanism")?;
    let object = env.find_class("java/lang/Object")?;
    let base = env
        .call_method(&object, "getModule", "()Ljava/lang/Module;", &[])?
        .l()?;
    for class in [&process, &enum_class] {
        let loader = env
            .call_method(class, "getClassLoader", "()Ljava/lang/ClassLoader;", &[])?
            .l()?;
        let module = env
            .call_method(class, "getModule", "()Ljava/lang/Module;", &[])?
            .l()?;
        if !loader.is_null() || !env.is_same_object(&module, &base)? {
            throw_exception_with_error_code(
                env,
                "Unsupported ProcessImpl bootstrap source",
                libc::ENOTSUP,
            );
            return Ok(JObject::null());
        }
    }
    let mechanism = env
        .get_static_field(
            &process,
            "launchMechanism",
            "Ljava/lang/ProcessImpl$LaunchMechanism;",
        )?
        .l()?;
    if mechanism.is_null() {
        throw_exception_with_error_code(env, "Missing cached ProcessImpl mechanism", libc::ENOTSUP);
        return Ok(JObject::null());
    }
    let actual = env.get_object_class(&mechanism)?;
    if !env.is_same_object(&actual, &enum_class)? {
        throw_exception_with_error_code(
            env,
            "Unsupported cached ProcessImpl enum class",
            libc::ENOTSUP,
        );
        return Ok(JObject::null());
    }
    let ordinal = env.call_method(&mechanism, "ordinal", "()I", &[])?.i()?;
    let name = JString::from(
        env.call_method(&mechanism, "name", "()Ljava/lang/String;", &[])?
            .l()?,
    );
    if name.is_null() || env.call_method(&name, "length", "()I", &[])?.i()? > 11 {
        throw_exception_with_error_code(env, "Invalid cached ProcessImpl enum name", libc::ENOTSUP);
        return Ok(JObject::null());
    }
    let expected: &[u8] = match ordinal {
        0 => b"FORK",
        1 => b"POSIX_SPAWN",
        2 => b"VFORK",
        _ => {
            throw_exception_with_error_code(
                env,
                "Unknown cached ProcessImpl ordinal",
                libc::ENOTSUP,
            );
            return Ok(JObject::null());
        }
    };
    if env.get_string(&name)?.to_bytes() != expected {
        throw_exception_with_error_code(
            env,
            "Conflicting cached ProcessImpl mechanism",
            libc::ENOTSUP,
        );
        return Ok(JObject::null());
    }
    let helper = env.get_static_field(&process, "helperpath", "[B")?.l()?;
    if helper.is_null() {
        throw_exception_with_error_code(
            env,
            "Missing cached ProcessImpl helper bytes",
            libc::ENOTSUP,
        );
        return Ok(JObject::null());
    }
    let helper = JByteArray::from(helper);
    let length = env.get_array_length(&helper)?;
    if !(2..=4097).contains(&length) {
        throw_exception_with_error_code(
            env,
            "Excessive or empty cached ProcessImpl helper bytes",
            libc::ENOTSUP,
        );
        return Ok(JObject::null());
    }
    let bytes = env.convert_byte_array(&helper)?;
    if bytes.last() != Some(&0) || bytes[..bytes.len() - 1].contains(&0) {
        throw_exception_with_error_code(
            env,
            "Invalid cached ProcessImpl helper terminator",
            libc::ENOTSUP,
        );
        return Ok(JObject::null());
    }
    let copy = env.byte_array_from_slice(&bytes)?;
    env.new_object(
        "org/apache/bookkeeper/common/util/nativeio/NativeIO$JdkProcessLaunchObservation",
        "(Ljava/lang/String;I[B)V",
        &[(&name).into(), ordinal.into(), (&copy).into()],
    )
}

#[no_mangle]
pub extern "system" fn Java_org_apache_bookkeeper_common_util_nativeio_NativeIOJni_jdkProcessLaunch(
    mut env: JNIEnv,
    _class: JClass,
) -> jni::sys::jobject {
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    {
        match jdk_process_launch(&mut env) {
            Ok(value) => value.into_raw(),
            Err(_) => {
                if !env.exception_check().unwrap_or(true) {
                    throw_exception_with_error_code(
                        &mut env,
                        "Unsupported cached ProcessImpl layout or read failure",
                        libc::ENOTSUP,
                    );
                }
                ptr::null_mut()
            }
        }
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        throw_exception_with_error_code(
            &mut env,
            "ProcessImpl cache observation unsupported",
            libc::ENOSYS,
        );
        ptr::null_mut()
    }
}

/// This process only. Saved credentials and exec/dropper authority remain external obligations.
#[no_mangle]
pub extern "system" fn Java_org_apache_bookkeeper_common_util_nativeio_NativeIOJni_currentProcessCredentials(
    mut env: JNIEnv,
    _class: JClass,
) -> jlongArray {
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    {
        let before = unsafe { [libc::getuid(), libc::geteuid(), libc::getgid(), libc::getegid()] };
        let mut groups = [0 as libc::gid_t; 64];
        let Some(count) = credential_group_count(&mut env, &mut groups) else {
            return ptr::null_mut();
        };
        let after = unsafe { [libc::getuid(), libc::geteuid(), libc::getgid(), libc::getegid()] };
        if before != after || count > groups.len() {
            throw_exception_with_error_code(&mut env, "Current process credentials changed", libc::EAGAIN);
            return ptr::null_mut();
        }
        let mut values: Vec<jlong> = before.iter().map(|value| *value as jlong).collect();
        values.extend(groups[..count].iter().map(|value| *value as jlong));
        let array = match env.new_long_array(values.len() as jint) {
            Ok(array) => array,
            Err(_) => return ptr::null_mut(),
        };
        if env.set_long_array_region(&array, 0, &values).is_err() {
            return ptr::null_mut();
        }
        array.into_raw()
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        throw_exception(&mut env, "Current process credential query is unsupported");
        ptr::null_mut()
    }
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn credential_group_count(env: &mut JNIEnv, groups: &mut [libc::gid_t; 64]) -> Option<usize> {
    let count = unsafe { libc::getgroups(groups.len() as libc::c_int, groups.as_mut_ptr()) };
    if count < 0 {
        throw_exception_with_errno(env, "Current process supplementary group query failed");
        return None;
    }
    Some(count as usize)
}

#[cfg(target_os = "linux")]
fn linux_thread_ids(env: &mut JNIEnv) -> Option<[jlong; 6]> {
    let (mut ruid, mut euid, mut suid) = (0, 0, 0);
    if unsafe { libc::getresuid(&mut ruid, &mut euid, &mut suid) } != 0 {
        throw_exception_with_errno(env, "Current Linux thread getresuid failed");
        return None;
    }
    let (mut rgid, mut egid, mut sgid) = (0, 0, 0);
    if unsafe { libc::getresgid(&mut rgid, &mut egid, &mut sgid) } != 0 {
        throw_exception_with_errno(env, "Current Linux thread getresgid failed");
        return None;
    }
    Some([ruid, euid, suid, rgid, egid, sgid].map(jlong::from))
}

/// Current Linux calling thread only. The separate ID/group calls do not form an atomic snapshot.
#[no_mangle]
pub extern "system" fn Java_org_apache_bookkeeper_common_util_nativeio_NativeIOJni_currentLinuxThreadCredentials(
    mut env: JNIEnv,
    _class: JClass,
) -> jlongArray {
    #[cfg(target_os = "linux")]
    {
        let Some(before) = linux_thread_ids(&mut env) else {
            return ptr::null_mut();
        };
        let mut groups = [0 as libc::gid_t; 64];
        let Some(count) = credential_group_count(&mut env, &mut groups) else {
            return ptr::null_mut();
        };
        let Some(after) = linux_thread_ids(&mut env) else {
            return ptr::null_mut();
        };
        if before != after || count > groups.len() {
            throw_exception_with_error_code(
                &mut env,
                "Current Linux thread credentials changed",
                libc::EAGAIN,
            );
            return ptr::null_mut();
        }
        let mut values = [0; 70];
        values[..6].copy_from_slice(&before);
        for (value, group) in values[6..].iter_mut().zip(&groups[..count]) {
            *value = jlong::from(*group);
        }
        let values = &values[..6 + count];
        let array = match env.new_long_array(values.len() as jint) {
            Ok(array) => array,
            Err(_) => return ptr::null_mut(),
        };
        if env.set_long_array_region(&array, 0, values).is_err() {
            return ptr::null_mut();
        }
        array.into_raw()
    }
    #[cfg(not(target_os = "linux"))]
    {
        throw_exception_with_error_code(
            &mut env,
            "Current Linux thread credential query unsupported",
            libc::ENOSYS,
        );
        ptr::null_mut()
    }
}

/// The type at this literal directory in the caller's current mount view, not cgroup identity or membership.
#[no_mangle]
pub extern "system" fn Java_org_apache_bookkeeper_common_util_nativeio_NativeIOJni_currentCgroupRootFileSystemType(
    mut env: JNIEnv,
    _class: JClass,
) -> jlong {
    #[cfg(target_os = "linux")]
    {
        let path = b"/sys/fs/cgroup/\0";
        let mut value: libc::statfs = unsafe { std::mem::zeroed() };
        if unsafe { libc::statfs(path.as_ptr().cast::<libc::c_char>(), &mut value) } != 0 {
            let error = std::io::Error::last_os_error()
                .raw_os_error()
                .unwrap_or(libc::EIO);
            throw_exception_with_error_code(&mut env, "Current cgroup root statfs failed", error);
            return 0;
        }
        value.f_type as jlong
    }
    #[cfg(not(target_os = "linux"))]
    {
        throw_exception_with_error_code(
            &mut env,
            "Current cgroup root filesystem type query unsupported",
            libc::ENOSYS,
        );
        0
    }
}

#[cfg(any(target_os = "linux", test))]
fn parse_boot_id(bytes: &[u8]) -> Result<String, libc::c_int> {
    if bytes.len() != 37 || bytes[36] != b'\n' {
        return Err(libc::EINVAL);
    }
    let mut nonzero = false;
    for (i, byte) in bytes[..36].iter().copied().enumerate() {
        if matches!(i, 8 | 13 | 18 | 23) {
            if byte != b'-' {
                return Err(libc::EINVAL);
            }
        } else if byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte) {
            nonzero |= byte != b'0';
        } else {
            return Err(libc::EINVAL);
        }
    }
    if !nonzero {
        return Err(libc::EINVAL);
    }
    String::from_utf8(bytes[..36].to_vec()).map_err(|_| libc::EINVAL)
}

#[cfg(target_os = "linux")]
type ProcIdentityResult<T> = Result<T, (&'static str, libc::c_int)>;

#[cfg(target_os = "linux")]
fn proc_identity_errno(operation: &'static str) -> (&'static str, libc::c_int) {
    (
        operation,
        std::io::Error::last_os_error()
            .raw_os_error()
            .unwrap_or(libc::EIO),
    )
}

// Each opened fd has exactly one explicit close, even on read/validation failure. Never retry close.
#[cfg(target_os = "linux")]
fn close_proc_identity_fd<T>(
    fd: libc::c_int,
    outcome: ProcIdentityResult<T>,
) -> ProcIdentityResult<T> {
    let closed = unsafe { libc::close(fd) };
    let close_error = if closed == 0 {
        None
    } else {
        Some(proc_identity_errno("proc identity close failed"))
    };
    match outcome {
        Err(first) => Err(first),
        Ok(value) => match close_error {
            None => Ok(value),
            Some(error) => Err(error),
        },
    }
}

#[cfg(target_os = "linux")]
fn proc_identity_stat(fd: libc::c_int) -> Result<libc::stat, (&'static str, libc::c_int)> {
    let mut fs: libc::statfs = unsafe { std::mem::zeroed() };
    if unsafe { libc::fstatfs(fd, &mut fs) } != 0 {
        return Err(proc_identity_errno("proc identity held-fd fstatfs failed"));
    }
    if fs.f_type != 0x9fa0 {
        // Linux UAPI PROC_SUPER_MAGIC, full f_type.
        return Err(("proc identity source is not procfs", libc::EINVAL));
    }
    let mut stat: libc::stat = unsafe { std::mem::zeroed() };
    if unsafe { libc::fstat(fd, &mut stat) } != 0 {
        return Err(proc_identity_errno("proc identity held-fd fstat failed"));
    }
    Ok(stat)
}

#[cfg(target_os = "linux")]
fn proc_identity_root(fd: libc::c_int) -> Result<libc::stat, (&'static str, libc::c_int)> {
    let stat = proc_identity_stat(fd)?;
    if stat.st_mode & libc::S_IFMT != libc::S_IFDIR || stat.st_ino != 1 {
        // PROCFS_ROOT_INO
        return Err(("proc identity source is not the procfs root", libc::EINVAL));
    }
    Ok(stat)
}

#[cfg(target_os = "linux")]
fn linux_boot_id() -> ProcIdentityResult<String> {
    let root = unsafe {
        libc::open(
            b"/proc\0".as_ptr().cast(),
            libc::O_RDONLY | libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC,
        )
    };
    if root < 0 {
        return Err(proc_identity_errno("boot ID fixed proc root open failed"));
    }
    let outcome = (|| {
        let root_stat = proc_identity_root(root)?;
        // Linux UAPI open_how: exactly three u64 fields, zero mode. No open/realpath fallback.
        let how = [
            (libc::O_RDONLY | libc::O_NOFOLLOW | libc::O_CLOEXEC | libc::O_NONBLOCK) as u64,
            0,
            0x08 | 0x01 | 0x04 | 0x02, // BENEATH | NO_XDEV | NO_SYMLINKS | NO_MAGICLINKS
        ];
        let leaf = unsafe {
            libc::syscall(
                libc::SYS_openat2,
                root,
                b"sys/kernel/random/boot_id\0"
                    .as_ptr()
                    .cast::<libc::c_char>(),
                how.as_ptr(),
                std::mem::size_of_val(&how),
            )
        };
        if leaf < 0 {
            return Err(proc_identity_errno("boot ID fixed openat2 failed"));
        }
        let leaf = leaf as libc::c_int;
        let outcome = (|| {
            let before = proc_identity_stat(leaf)?;
            if before.st_mode & libc::S_IFMT != libc::S_IFREG || before.st_dev != root_stat.st_dev {
                return Err(("boot ID leaf differs from its held procfs", libc::EINVAL));
            }
            // Proc sysctl st_size may be zero. Read at most the canonical line plus one byte, then require EOF.
            let mut bytes = [0_u8; 38];
            let mut used = 0;
            loop {
                let count = unsafe {
                    libc::read(leaf, bytes[used..].as_mut_ptr().cast(), bytes.len() - used)
                };
                if count < 0 {
                    return Err(proc_identity_errno("boot ID bounded read failed"));
                }
                if count == 0 {
                    break;
                }
                used += count as usize;
                if used > 37 {
                    return Err(("boot ID output exceeds its canonical line", libc::EOVERFLOW));
                }
            }
            let value = parse_boot_id(&bytes[..used])
                .map_err(|error| ("Invalid kernel boot ID line", error))?;
            let after = proc_identity_stat(leaf)?;
            let root_after = proc_identity_root(root)?;
            if before.st_dev != after.st_dev
                || before.st_ino != after.st_ino
                || before.st_mode != after.st_mode
                || root_stat.st_dev != root_after.st_dev
                || root_stat.st_ino != root_after.st_ino
            {
                return Err(("boot ID held source changed during read", libc::EAGAIN));
            }
            Ok(value)
        })();
        close_proc_identity_fd(leaf, outcome)
    })();
    close_proc_identity_fd(root, outcome)
}

#[cfg(any(target_os = "linux", test))]
fn proc_unsigned_decimal(token: &[u8]) -> Result<u64, libc::c_int> {
    if token.is_empty() || !token.iter().all(u8::is_ascii_digit) {
        return Err(libc::EINVAL);
    }
    std::str::from_utf8(token)
        .map_err(|_| libc::EINVAL)?
        .parse()
        .map_err(|_| libc::EINVAL)
}

// The complete bounded file is read first. Only the prefix through field 22 is interpreted.
#[cfg(any(target_os = "linux", test))]
fn parse_proc_start_ticks(bytes: &[u8], expected_pid: libc::pid_t) -> Result<u64, libc::c_int> {
    if bytes.is_empty() || bytes.len() > 4096 || bytes.last() != Some(&b'\n') || expected_pid <= 0 {
        return Err(libc::EINVAL);
    }
    let pid_end = bytes.iter().position(|b| *b == b' ').ok_or(libc::EINVAL)?;
    if bytes.get(pid_end + 1) != Some(&b'(')
        || proc_unsigned_decimal(&bytes[..pid_end])? != expected_pid as u64
    {
        return Err(libc::EINVAL);
    }
    // comm is raw kernel text and can itself contain spaces, ')' and newlines, even non-UTF8 bytes.
    let comm_end = bytes.iter().rposition(|b| *b == b')').ok_or(libc::EINVAL)?;
    if comm_end < pid_end + 2 || bytes.get(comm_end + 1) != Some(&b' ') {
        return Err(libc::EINVAL);
    }
    let mut fields = bytes[comm_end + 2..]
        .split(u8::is_ascii_whitespace)
        .filter(|f| !f.is_empty());
    let state = fields.next().ok_or(libc::EINVAL)?;
    if state.len() != 1 || !state[0].is_ascii_alphabetic() {
        return Err(libc::EINVAL);
    }
    for number in 4..=22 {
        let token = fields.next().ok_or(libc::EINVAL)?;
        if (4..=8).contains(&number) || (16..=20).contains(&number) {
            let digits = token.strip_prefix(b"-").unwrap_or(token);
            proc_unsigned_decimal(digits)?;
            std::str::from_utf8(token)
                .map_err(|_| libc::EINVAL)?
                .parse::<i64>()
                .map_err(|_| libc::EINVAL)?;
        } else {
            let value = proc_unsigned_decimal(token)?;
            if number == 22 {
                return Ok(value);
            }
        }
    }
    Err(libc::EINVAL)
}

#[cfg(target_os = "linux")]
fn linux_process_birth() -> ProcIdentityResult<[u64; 3]> {
    let pid = unsafe { libc::getpid() };
    if pid <= 0 {
        return Err(("Invalid direct current PID", libc::EINVAL));
    }
    let root = unsafe {
        libc::open(
            b"/proc\0".as_ptr().cast(),
            libc::O_RDONLY | libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC,
        )
    };
    if root < 0 {
        return Err(proc_identity_errno(
            "process birth fixed proc root open failed",
        ));
    }
    let outcome = (|| {
        let root_stat = proc_identity_root(root)?;
        let mut starts = [0_u64; 2];
        // self is the kernel's ordinary proc symlink, interpreted in this proc superblock's PID view.
        // Only that fixed path omits NO_SYMLINKS. Both paths keep NO_XDEV/BENEATH/NO_MAGICLINKS.
        for (index, (path, resolve, expected)) in [
            (b"self/stat\0".as_slice(), 0x08 | 0x01 | 0x02, pid),
            (b"1/stat\0".as_slice(), 0x08 | 0x01 | 0x02 | 0x04, 1),
        ]
        .into_iter()
        .enumerate()
        {
            let how = [
                (libc::O_RDONLY | libc::O_NOFOLLOW | libc::O_CLOEXEC | libc::O_NONBLOCK) as u64,
                0,
                resolve,
            ];
            let leaf = unsafe {
                libc::syscall(
                    libc::SYS_openat2,
                    root,
                    path.as_ptr().cast::<libc::c_char>(),
                    how.as_ptr(),
                    std::mem::size_of_val(&how),
                )
            };
            if leaf < 0 {
                return Err(proc_identity_errno("process birth fixed openat2 failed"));
            }
            let leaf = leaf as libc::c_int;
            let outcome = (|| {
                let before = proc_identity_stat(leaf)?;
                if before.st_mode & libc::S_IFMT != libc::S_IFREG
                    || before.st_dev != root_stat.st_dev
                {
                    return Err((
                        "process birth leaf differs from its held procfs",
                        libc::EINVAL,
                    ));
                }
                let mut bytes = [0_u8; 4097];
                let mut used = 0;
                loop {
                    let count = unsafe {
                        libc::read(leaf, bytes[used..].as_mut_ptr().cast(), bytes.len() - used)
                    };
                    if count < 0 {
                        return Err(proc_identity_errno("process birth bounded read failed"));
                    }
                    if count == 0 {
                        break;
                    }
                    used += count as usize;
                    if used > 4096 {
                        return Err((
                            "process birth stat exceeds its fixed bound",
                            libc::EOVERFLOW,
                        ));
                    }
                }
                let ticks = parse_proc_start_ticks(&bytes[..used], expected).map_err(|error| {
                    ("Invalid process birth stat or mismatched PID view", error)
                })?;
                let after = proc_identity_stat(leaf)?;
                if before.st_dev != after.st_dev
                    || before.st_ino != after.st_ino
                    || before.st_mode != after.st_mode
                {
                    return Err(("process birth held leaf changed during read", libc::EAGAIN));
                }
                Ok(ticks)
            })();
            starts[index] = close_proc_identity_fd(leaf, outcome)?;
        }
        let after = proc_identity_root(root)?;
        if root_stat.st_dev != after.st_dev || root_stat.st_ino != after.st_ino {
            return Err((
                "process birth held proc root changed during read",
                libc::EAGAIN,
            ));
        }
        Ok([pid as u64, starts[0], starts[1]])
    })();
    close_proc_identity_fd(root, outcome)
}

/// Cold process identities from one verified proc root, with raw unsigned ticks, never wall-clock instants.
#[no_mangle]
pub extern "system" fn Java_org_apache_bookkeeper_common_util_nativeio_NativeIOJni_currentLinuxProcessBirth(
    mut env: JNIEnv,
    _class: JClass,
) -> jlongArray {
    #[cfg(target_os = "linux")]
    {
        match linux_process_birth() {
            Ok(values) => {
                let array = match env.new_long_array(3) {
                    Ok(array) => array,
                    Err(_) => return ptr::null_mut(),
                };
                // Rust u64 -> Java long is deliberately a bit-preserving conversion.
                let bits = values.map(|value| value as jlong);
                if env.set_long_array_region(&array, 0, &bits).is_err() {
                    return ptr::null_mut();
                }
                array.into_raw()
            }
            Err((operation, error)) => {
                throw_exception_with_error_code(&mut env, operation, error);
                ptr::null_mut()
            }
        }
    }
    #[cfg(not(target_os = "linux"))]
    {
        throw_exception_with_error_code(
            &mut env,
            "Current Linux process birth query unsupported",
            libc::ENOSYS,
        );
        ptr::null_mut()
    }
}

#[cfg(any(target_os = "linux", test))]
fn parse_namespace_inode(target: &[u8], prefix: &[u8]) -> Result<u32, libc::c_int> {
    if target.len() > 17 {
        // user:[4294967295] is the longest canonical value.
        return Err(libc::EINVAL);
    }
    let digits = target
        .strip_prefix(prefix)
        .and_then(|s| s.strip_suffix(b"]"))
        .ok_or(libc::EINVAL)?;
    if digits.first() == Some(&b'0') {
        return Err(libc::EINVAL);
    }
    u32::try_from(proc_unsigned_decimal(digits)?).map_err(|_| libc::EINVAL)
}

#[cfg(target_os = "linux")]
fn linux_thread_namespaces() -> ProcIdentityResult<[u32; 2]> {
    let root = unsafe {
        libc::open(
            b"/proc\0".as_ptr().cast(),
            libc::O_RDONLY | libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC,
        )
    };
    if root < 0 {
        return Err(proc_identity_errno(
            "thread namespaces fixed proc root open failed",
        ));
    }
    let outcome = (|| {
        let root_stat = proc_identity_root(root)?;
        let mut inodes = [0_u32; 2];
        for (index, (path, prefix)) in [
            (b"thread-self/ns/pid\0".as_slice(), b"pid:[".as_slice()),
            (b"thread-self/ns/user\0".as_slice(), b"user:[".as_slice()),
        ]
        .into_iter()
        .enumerate()
        {
            // thread-self is the ordinary kernel link. O_PATH|O_NOFOLLOW holds the final magic link itself.
            let how = [
                (libc::O_PATH | libc::O_NOFOLLOW | libc::O_CLOEXEC) as u64,
                0,
                0x08 | 0x01 | 0x02,
            ]; // BENEATH | NO_XDEV | NO_MAGICLINKS
            let link = unsafe {
                libc::syscall(
                    libc::SYS_openat2,
                    root,
                    path.as_ptr().cast::<libc::c_char>(),
                    how.as_ptr(),
                    std::mem::size_of_val(&how),
                )
            };
            if link < 0 {
                return Err(proc_identity_errno(
                    "thread namespaces fixed openat2 failed",
                ));
            }
            let link = link as libc::c_int;
            let outcome = (|| {
                let before = proc_identity_stat(link)?;
                if before.st_mode & libc::S_IFMT != libc::S_IFLNK
                    || before.st_dev != root_stat.st_dev
                {
                    return Err((
                        "thread namespace link differs from its held procfs",
                        libc::EINVAL,
                    ));
                }
                let mut bytes = [0_u8; 18];
                // Use the syscall directly: an unavailable readlinkat must not fall back to a pathname.
                let count = unsafe {
                    libc::syscall(
                        libc::SYS_readlinkat,
                        link,
                        b"\0".as_ptr().cast::<libc::c_char>(),
                        bytes.as_mut_ptr().cast::<libc::c_char>(),
                        bytes.len(),
                    )
                };
                if count < 0 {
                    return Err(proc_identity_errno(
                        "thread namespace held readlinkat failed",
                    ));
                }
                if count as usize >= bytes.len() {
                    return Err((
                        "thread namespace link output exceeds its bound",
                        libc::EOVERFLOW,
                    ));
                }
                let inode = parse_namespace_inode(&bytes[..count as usize], prefix)
                    .map_err(|error| ("Invalid canonical thread namespace link target", error))?;
                let after = proc_identity_stat(link)?;
                if before.st_dev != after.st_dev
                    || before.st_ino != after.st_ino
                    || before.st_mode != after.st_mode
                {
                    return Err((
                        "thread namespace held link changed during read",
                        libc::EAGAIN,
                    ));
                }
                Ok(inode)
            })();
            inodes[index] = close_proc_identity_fd(link, outcome)?;
        }
        let after = proc_identity_root(root)?;
        if root_stat.st_dev != after.st_dev || root_stat.st_ino != after.st_ino {
            return Err((
                "thread namespaces held proc root changed during read",
                libc::EAGAIN,
            ));
        }
        Ok(inodes)
    })();
    close_proc_identity_fd(root, outcome)
}

/// Read the calling thread's two fixed proc link targets, never follow a magic link into nsfs.
#[no_mangle]
pub extern "system" fn Java_org_apache_bookkeeper_common_util_nativeio_NativeIOJni_currentLinuxThreadNamespaces(
    mut env: JNIEnv,
    _class: JClass,
) -> jlongArray {
    #[cfg(target_os = "linux")]
    {
        match linux_thread_namespaces() {
            Ok(values) => {
                let array = match env.new_long_array(2) {
                    Ok(array) => array,
                    Err(_) => return ptr::null_mut(),
                };
                let unsigned = values.map(|value| value as jlong);
                if env.set_long_array_region(&array, 0, &unsigned).is_err() {
                    return ptr::null_mut();
                }
                array.into_raw()
            }
            Err((operation, error)) => {
                throw_exception_with_error_code(&mut env, operation, error);
                ptr::null_mut()
            }
        }
    }
    #[cfg(not(target_os = "linux"))]
    {
        throw_exception_with_error_code(
            &mut env,
            "Current Linux thread namespace query unsupported",
            libc::ENOSYS,
        );
        ptr::null_mut()
    }
}

#[cfg(any(target_os = "linux", test))]
fn complete_environment_word(
    copied: libc::c_long,
    word: usize,
) -> Result<usize, (&'static str, libc::c_int)> {
    const OPERATION: &str = "Current Linux environ pointer copy failed";
    if copied < 0 {
        return Err((
            OPERATION,
            std::io::Error::last_os_error()
                .raw_os_error()
                .unwrap_or(libc::EIO),
        ));
    }
    if copied as usize != std::mem::size_of::<usize>() {
        return Err(("Current Linux environ pointer copy was short", libc::EIO));
    }
    Ok(word)
}

#[cfg(any(target_os = "linux", test))]
fn sample_environment(
    symbol: usize,
    mut read_word: impl FnMut(usize) -> Result<usize, (&'static str, libc::c_int)>,
) -> Result<bool, (&'static str, libc::c_int)> {
    let table = read_word(symbol)?;
    let first = if table == 0 { 0 } else { read_word(table)? };
    if read_word(symbol)? != table {
        return Err((
            "Current Linux environ table changed during sampling",
            libc::EAGAIN,
        ));
    }
    Ok(first == 0)
}

#[cfg(target_os = "linux")]
fn copy_linux_environment_word(
    pid: libc::pid_t,
    address: usize,
) -> Result<usize, (&'static str, libc::c_int)> {
    let mut word = 0usize;
    let size = std::mem::size_of::<usize>();
    let local = libc::iovec {
        iov_base: ptr::addr_of_mut!(word).cast(),
        iov_len: size,
    };
    let remote = libc::iovec {
        iov_base: address as *mut c_void,
        iov_len: size,
    };
    // Both iovecs live through the syscall. Only the kernel reads the sampled address.
    let copied = unsafe {
        libc::syscall(
            libc::SYS_process_vm_readv,
            pid,
            &local as *const libc::iovec,
            1 as libc::c_ulong,
            &remote as *const libc::iovec,
            1 as libc::c_ulong,
            0 as libc::c_ulong,
        )
    };
    complete_environment_word(copied, word)
}

/// Samples only the current process's libc environ pointers. This is not an atomic environment snapshot.
#[no_mangle]
pub extern "system" fn Java_org_apache_bookkeeper_common_util_nativeio_NativeIOJni_currentLinuxNativeEnvironment(
    mut env: JNIEnv,
    _class: JClass,
) -> jint {
    #[cfg(target_os = "linux")]
    {
        extern "C" {
            static mut environ: *mut *mut libc::c_char;
        }
        let pid = unsafe { libc::getpid() };
        // Take only the address of the foreign symbol, without a Rust reference or load.
        let symbol = unsafe { ptr::addr_of!(environ) as usize };
        match sample_environment(symbol, |address| copy_linux_environment_word(pid, address)) {
            Ok(true) => 1,
            Ok(false) => 2,
            Err((operation, error)) => {
                throw_exception_with_error_code(&mut env, operation, error);
                0
            }
        }
    }
    #[cfg(not(target_os = "linux"))]
    {
        throw_exception_with_error_code(
            &mut env,
            "Current Linux native environment sampling unsupported",
            libc::ENOSYS,
        );
        0
    }
}

#[cfg(test)]
mod native_environment_tests {
    use super::{complete_environment_word, sample_environment};

    #[test]
    fn null_table_is_rechecked_and_present_table_reads_only_its_first_slot() {
        for (reads, empty) in [
            (vec![(1, 0), (1, 0)], true),
            (vec![(1, 2), (2, 0), (1, 2)], true),
            (vec![(1, 2), (2, 3), (1, 2)], false),
        ] {
            let mut reads = reads.into_iter();
            assert_eq!(
                sample_environment(1, |address| {
                    let (expected, value) = reads.next().unwrap();
                    assert_eq!(expected, address);
                    Ok(value)
                }),
                Ok(empty),
            );
            assert!(reads.next().is_none());
        }
    }

    #[test]
    fn short_unreadable_or_changed_words_cannot_yield_an_empty_sample() {
        let width = std::mem::size_of::<usize>() as libc::c_long;
        for short in 0..width {
            assert_eq!(
                complete_environment_word(short, 0).unwrap_err().1,
                libc::EIO,
            );
        }
        for results in [vec![Ok(0), Ok(2)], vec![Ok(2), Ok(0), Ok(3)]] {
            let mut results = results.into_iter();
            assert_eq!(
                sample_environment(1, |_| results.next().unwrap())
                    .unwrap_err()
                    .1,
                libc::EAGAIN,
            );
        }
        let unreadable = ("unreadable first slot", libc::EFAULT);
        let mut results = [Ok(2), Err(unreadable)].into_iter();
        assert_eq!(
            sample_environment(1, |_| results.next().unwrap()),
            Err(unreadable),
        );
    }
}

/// Fixed Linux procfs boot identity only. No caller pathname/fd, cached value or namespace qualification.
#[no_mangle]
pub extern "system" fn Java_org_apache_bookkeeper_common_util_nativeio_NativeIOJni_currentLinuxBootId(
    mut env: JNIEnv,
    _class: JClass,
) -> jstring {
    #[cfg(target_os = "linux")]
    {
        match linux_boot_id() {
            Ok(value) => env
                .new_string(value)
                .map_or(ptr::null_mut(), |value| value.into_raw()),
            Err((operation, error)) => {
                throw_exception_with_error_code(&mut env, operation, error);
                ptr::null_mut()
            }
        }
    }
    #[cfg(not(target_os = "linux"))]
    {
        throw_exception_with_error_code(
            &mut env,
            "Current Linux boot ID query unsupported",
            libc::ENOSYS,
        );
        ptr::null_mut()
    }
}

#[cfg(test)]
mod boot_id_tests {
    use super::parse_boot_id;

    #[test]
    fn canonical_boot_line_preserves_its_complete_uuid() {
        assert_eq!(
            parse_boot_id(b"12345678-9abc-def0-1234-56789abcdef0\n").unwrap(),
            "12345678-9abc-def0-1234-56789abcdef0"
        );
    }

    #[test]
    fn boot_line_rejects_truncation_overflow_noncanonical_and_zero() {
        let valid = b"12345678-9abc-def0-1234-56789abcdef0\n";
        for length in 0..valid.len() {
            assert!(parse_boot_id(&valid[..length]).is_err());
        }
        for invalid in [
            b"12345678-9abc-def0-1234-56789abcdef0\nX".as_slice(),
            b"12345678-9ABC-def0-1234-56789abcdef0\n",
            b"00000000-0000-0000-0000-000000000000\n",
            b"12345678_9abc-def0-1234-56789abcdef0\n",
            b"12345678-9abc-def0-1234-56789abcdef0\r\n",
        ] {
            assert!(parse_boot_id(invalid).is_err());
        }
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn held_ordinary_directory_and_real_proc_subdirectory_are_not_proc_roots() {
        use std::os::fd::AsRawFd;
        let ordinary = std::fs::File::open(std::env::temp_dir()).unwrap();
        assert!(super::proc_identity_root(ordinary.as_raw_fd()).is_err());
        let subdirectory = std::fs::File::open("/proc/sys").unwrap();
        assert!(super::proc_identity_stat(subdirectory.as_raw_fd()).is_ok());
        assert!(super::proc_identity_root(subdirectory.as_raw_fd()).is_err());
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn close_failure_cannot_replace_an_earlier_read_failure_or_publish_success() {
        let first = ("earlier read failure", libc::EIO);
        assert_eq!(
            super::close_proc_identity_fd::<String>(-1, Err(first)),
            Err(first)
        );
        assert_eq!(
            super::close_proc_identity_fd(-1, Ok("unpublished".to_string())),
            Err(("proc identity close failed", libc::EBADF))
        );
    }
}

#[cfg(test)]
mod process_birth_tests {
    use super::parse_proc_start_ticks;

    fn stat(pid: i32, comm: &[u8], start: &str) -> Vec<u8> {
        let mut bytes = format!("{pid} (").into_bytes();
        bytes.extend_from_slice(comm);
        bytes.extend_from_slice(b") S");
        for field in 4..=52 {
            bytes.push(b' ');
            bytes.extend_from_slice(if field == 22 { start.as_bytes() } else { b"0" });
        }
        bytes.push(b'\n');
        bytes
    }

    #[test]
    fn raw_comm_with_spaces_parentheses_newlines_and_non_utf8_does_not_shift_birth() {
        for comm in [
            b"plain".as_slice(),
            b"with space ) tail))\n",
            b"\xff ) (\n) name",
            b"",
        ] {
            assert_eq!(
                parse_proc_start_ticks(&stat(42, comm, "1234"), 42).unwrap(),
                1234
            );
        }
    }

    #[test]
    fn start_ticks_preserve_zero_high_bit_and_complete_u64_without_hz_conversion() {
        for value in [0_u64, 1, 1_u64 << 63, u64::MAX] {
            assert_eq!(
                parse_proc_start_ticks(&stat(1, b"init", &value.to_string()), 1).unwrap(),
                value
            );
        }
    }

    #[test]
    fn stat_rejects_pid_view_mismatch_missing_fields_bad_numbers_and_overflow() {
        assert!(parse_proc_start_ticks(&stat(43, b"x", "123"), 42).is_err());
        assert!(parse_proc_start_ticks(&stat(0, b"x", "0"), 0).is_err());
        for start in ["18446744073709551616", "-1", "+1", "NaN"] {
            assert!(parse_proc_start_ticks(&stat(42, b"x", start), 42).is_err());
        }
        let short = format!("42 (x) S{}\n", " 0".repeat(18));
        assert!(parse_proc_start_ticks(short.as_bytes(), 42).is_err());
        let bad = String::from_utf8(stat(42, b"x", "123"))
            .unwrap()
            .replace(") S 0", ") S 9223372036854775808");
        assert!(parse_proc_start_ticks(bad.as_bytes(), 42).is_err());
        let mut no_newline = stat(42, b"x", "123");
        no_newline.pop();
        assert!(parse_proc_start_ticks(&no_newline, 42).is_err());
        assert!(parse_proc_start_ticks(&stat(42, &vec![b'x'; 4096], "123"), 42).is_err());
    }
}

#[cfg(test)]
mod thread_namespace_tests {
    use super::parse_namespace_inode;

    #[test]
    fn canonical_pid_user_and_full_unsigned_inodes_keep_their_exact_values() {
        assert_eq!(
            parse_namespace_inode(b"pid:[4026531836]", b"pid:[").unwrap(),
            0xefff_fffc
        );
        assert_eq!(
            parse_namespace_inode(b"user:[4026531837]", b"user:[").unwrap(),
            0xefff_fffd
        );
        assert_eq!(
            parse_namespace_inode(b"user:[4294967295]", b"user:[").unwrap(),
            u32::MAX
        );
    }

    #[test]
    fn swapped_types_truncation_overflow_and_noncanonical_target_bytes_are_rejected() {
        assert!(parse_namespace_inode(b"user:[4026531837]", b"pid:[").is_err());
        assert!(parse_namespace_inode(b"pid:[4026531836]", b"user:[").is_err());
        let valid = b"user:[4294967295]";
        for length in 0..valid.len() {
            assert!(parse_namespace_inode(&valid[..length], b"user:[").is_err());
        }
        for bad in [
            b"user:[4294967296]".as_slice(),
            b"user:[4294967295]X",
            b"user:[1]\0",
            b"user:[1]\n",
            b"user:[0]",
            b"user:[01]",
            b"user:[+1]",
            b"user:[-1]",
            b"user:[ 1]",
            b"user:[]",
        ] {
            assert!(parse_namespace_inode(bad, b"user:[").is_err());
        }
    }
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn kernel_field(field: &[libc::c_char]) -> Result<String, libc::c_int> {
    let end = field
        .iter()
        .position(|byte| *byte == 0)
        .ok_or(libc::EINVAL)?;
    if end == 0 {
        return Err(libc::EINVAL);
    }
    let bytes: Vec<u8> = field[..end].iter().map(|byte| *byte as u8).collect();
    String::from_utf8(bytes).map_err(|_| libc::EINVAL)
}

/// One libc uname call in this process's view, without hostname or domain-name output or qualification.
#[no_mangle]
pub extern "system" fn Java_org_apache_bookkeeper_common_util_nativeio_NativeIOJni_currentKernelIdentity(
    mut env: JNIEnv,
    _class: JClass,
) -> jobjectArray {
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    {
        let mut value: libc::utsname = unsafe { std::mem::zeroed() };
        if unsafe { libc::uname(&mut value) } != 0 {
            let error = std::io::Error::last_os_error()
                .raw_os_error()
                .unwrap_or(libc::EIO);
            throw_exception_with_error_code(&mut env, "Current kernel uname failed", error);
            return ptr::null_mut();
        }
        let fields: [&[libc::c_char]; 4] = [
            &value.sysname,
            &value.machine,
            &value.release,
            &value.version,
        ];
        let mut texts = Vec::with_capacity(fields.len());
        for field in fields {
            match kernel_field(field) {
                Ok(text) => texts.push(text),
                Err(error) => {
                    throw_exception_with_error_code(
                        &mut env,
                        "Invalid bounded UTF8 uname field",
                        error,
                    );
                    return ptr::null_mut();
                }
            }
        }
        let array = match env.new_object_array(4, "java/lang/String", JObject::null()) {
            Ok(array) => array,
            Err(_) => return ptr::null_mut(),
        };
        for (index, text) in texts.iter().enumerate() {
            let value = match env.new_string(text) {
                Ok(value) => value,
                Err(_) => return ptr::null_mut(),
            };
            if env
                .set_object_array_element(&array, index as jint, value)
                .is_err()
            {
                return ptr::null_mut();
            }
        }
        array.into_raw()
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        throw_exception_with_error_code(
            &mut env,
            "Current kernel identity query unsupported",
            libc::ENOSYS,
        );
        ptr::null_mut()
    }
}

#[cfg(all(test, any(target_os = "linux", target_os = "macos")))]
mod kernel_field_tests {
    use super::kernel_field;

    #[test]
    fn preserves_first_nul_bounded_full_utf8_text() {
        let text = "#1 SMP  雪; release:build ";
        let mut field: Vec<libc::c_char> = text.bytes().map(|b| b as libc::c_char).collect();
        field.extend([0, b'x' as libc::c_char]);
        assert_eq!(kernel_field(&field).unwrap(), text);
    }

    #[test]
    fn rejects_empty_unterminated_and_invalid_utf8_fields() {
        for bytes in [vec![], vec![0], vec![b'x'], vec![0xff, 0]] {
            let field: Vec<libc::c_char> = bytes.into_iter().map(|b| b as libc::c_char).collect();
            assert_eq!(kernel_field(&field), Err(libc::EINVAL));
        }
    }
}

/// Original descriptor identity only; never opens, duplicates, repositions or closes a descriptor.
#[no_mangle]
pub extern "system" fn Java_org_apache_bookkeeper_common_util_nativeio_NativeIOJni_fileIdentity(
    mut env: JNIEnv,
    _class: JClass,
    channel: JObject,
) -> jlongArray {
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    {
        let values = match original_file_identity(&mut env, &channel) {
            Ok(Some(value)) => value,
            Ok(None) => return ptr::null_mut(),
            Err(error) => {
                if !env.exception_check().unwrap_or(true) {
                    throw_exception(
                        &mut env,
                        &format!("Original file-channel identity failed: {error}"),
                    );
                }
                return ptr::null_mut();
            }
        };
        let array = match env.new_long_array(2) {
            Ok(value) => value,
            Err(_) => return ptr::null_mut(),
        };
        if env.set_long_array_region(&array, 0, &values).is_err() {
            return ptr::null_mut();
        }
        array.into_raw()
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        throw_exception_with_error_code(
            &mut env,
            "File-channel identity is unsupported",
            libc::ENOTSUP,
        );
        ptr::null_mut()
    }
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn original_file_identity(
    env: &mut JNIEnv,
    channel: &JObject,
) -> jni::errors::Result<Option<[jlong; 2]>> {
    if channel.is_null() {
        throw_exception_with_error_code(env, "Original file channel is null", libc::EINVAL);
        return Ok(None);
    }
    let actual = env.get_object_class(channel)?;
    let expected = env.find_class("sun/nio/ch/FileChannelImpl")?;
    if !env.is_same_object(&actual, &expected)? {
        throw_exception_with_error_code(
            env,
            "Unsupported original file-channel provider",
            libc::ENOTSUP,
        );
        return Ok(None);
    }
    let channel_fd = env.get_field_id(&expected, "fd", "Ljava/io/FileDescriptor;")?;
    let descriptor = env
        .get_field_unchecked(channel, channel_fd, ReturnType::Object)?
        .l()?;
    if descriptor.is_null() {
        throw_exception_with_error_code(
            env,
            "Original file descriptor is unavailable",
            libc::EBADF,
        );
        return Ok(None);
    }
    let descriptor_class = env.get_object_class(&descriptor)?;
    let expected_descriptor = env.find_class("java/io/FileDescriptor")?;
    if !env.is_same_object(&descriptor_class, &expected_descriptor)? {
        throw_exception_with_error_code(
            env,
            "Unsupported original file-descriptor type",
            libc::ENOTSUP,
        );
        return Ok(None);
    }
    let object = env.find_class("java/lang/Object")?;
    let base = env
        .call_method(&object, "getModule", "()Ljava/lang/Module;", &[])?
        .l()?;
    for class in [&actual, &descriptor_class] {
        let loader = env
            .call_method(class, "getClassLoader", "()Ljava/lang/ClassLoader;", &[])?
            .l()?;
        let module = env
            .call_method(class, "getModule", "()Ljava/lang/Module;", &[])?
            .l()?;
        if !loader.is_null() || !env.is_same_object(&module, &base)? {
            throw_exception_with_error_code(
                env,
                "File identity requires bootstrap java.base",
                libc::ENOTSUP,
            );
            return Ok(None);
        }
    }
    // Resolve fixed layout and error types before taking the descriptor monitor. No caller callback or close.
    let fd_field = env.get_field_id(&expected_descriptor, "fd", "I")?;
    let closed_field = env.get_field_id(&expected_descriptor, "closed", "Z")?;
    let open_method = env.get_method_id(&expected, "isOpen", "()Z")?;
    env.find_class("org/apache/bookkeeper/common/util/nativeio/NativeIOException")?;
    let raw = env.get_raw();
    let entered = unsafe { ((**raw).MonitorEnter.unwrap())(raw, descriptor.as_raw()) };
    if entered != 0 {
        if !env.exception_check().unwrap_or(true) {
            throw_exception(env, "Original file-descriptor monitor acquisition failed");
        }
        return Ok(None);
    }
    let observed = (|| -> jni::errors::Result<Option<[jlong; 2]>> {
        if env.exception_check()? {
            return Ok(None);
        }
        // isOpen is final in the fixed bootstrap superclass; it only reads the logical closed flag.
        let open = unsafe {
            env.call_method_unchecked(
                channel,
                open_method,
                ReturnType::Primitive(Primitive::Boolean),
                &[],
            )?
        }
        .z()?;
        let fd = env
            .get_field_unchecked(&descriptor, fd_field, ReturnType::Primitive(Primitive::Int))?
            .i()?;
        let closed = env
            .get_field_unchecked(
                &descriptor,
                closed_field,
                ReturnType::Primitive(Primitive::Boolean),
            )?
            .z()?;
        if !open || closed || fd < 0 {
            throw_exception_with_error_code(
                env,
                "Original file channel is closed or closing",
                libc::EBADF,
            );
            return Ok(None);
        }
        let mut status = std::mem::MaybeUninit::<libc::stat>::uninit();
        if unsafe { libc::fstat(fd, status.as_mut_ptr()) } != 0 {
            throw_exception_with_errno(env, "Original file-descriptor fstat failed");
            return Ok(None);
        }
        let status = unsafe { status.assume_init() };
        let final_fd = env
            .get_field_unchecked(&descriptor, fd_field, ReturnType::Primitive(Primitive::Int))?
            .i()?;
        let final_closed = env
            .get_field_unchecked(
                &descriptor,
                closed_field,
                ReturnType::Primitive(Primitive::Boolean),
            )?
            .z()?;
        let final_open = unsafe {
            env.call_method_unchecked(
                channel,
                open_method,
                ReturnType::Primitive(Primitive::Boolean),
                &[],
            )?
        }
        .z()?;
        if !final_open || final_closed || final_fd != fd {
            throw_exception_with_error_code(
                env,
                "Original file channel closed during fstat",
                libc::EBADF,
            );
            return Ok(None);
        }
        Ok(Some([status.st_dev as jlong, status.st_ino as jlong]))
    })();
    // MonitorExit is permitted with a pending exception; never strand a descriptor on any failure branch.
    let exited = unsafe { ((**raw).MonitorExit.unwrap())(raw, descriptor.as_raw()) };
    if exited != 0 || env.exception_check().unwrap_or(true) {
        if !env.exception_check().unwrap_or(true) {
            throw_exception(env, "Original file-descriptor monitor release failed");
        }
        return Ok(None);
    }
    observed
}

/// Numeric Linux peer data only. The original channel owner excludes concurrent close and retains the channel.
#[no_mangle]
pub extern "system" fn Java_org_apache_bookkeeper_common_util_nativeio_NativeIOJni_unixPeerCredentials(
    mut env: JNIEnv,
    _class: JClass,
    _channel: JObject,
) -> jlongArray {
    #[cfg(target_os = "linux")]
    {
        if _channel.is_null() {
            throw_exception_with_error_code(
                &mut env,
                "Original Unix channel is null",
                libc::EINVAL,
            );
            return ptr::null_mut();
        }
        // This bridge is deliberately restricted to the approved JDK provider. No reflection override or fd input.
        let actual = match env.get_object_class(&_channel) {
            Ok(value) => value,
            Err(_) => return ptr::null_mut(),
        };
        let expected = match env.find_class("sun/nio/ch/SocketChannelImpl") {
            Ok(value) => value,
            Err(_) => return ptr::null_mut(),
        };
        if !env.is_same_object(&actual, &expected).unwrap_or(false) {
            throw_exception_with_error_code(
                &mut env,
                "Unsupported Unix channel provider",
                libc::ENOTSUP,
            );
            return ptr::null_mut();
        }
        let loader = match env
            .call_method(&actual, "getClassLoader", "()Ljava/lang/ClassLoader;", &[])
            .and_then(|v| v.l())
        {
            Ok(value) => value,
            Err(_) => return ptr::null_mut(),
        };
        let module = match env
            .call_method(&actual, "getModule", "()Ljava/lang/Module;", &[])
            .and_then(|v| v.l())
        {
            Ok(value) => value,
            Err(_) => return ptr::null_mut(),
        };
        let module_name = match env
            .call_method(&module, "getName", "()Ljava/lang/String;", &[])
            .and_then(|v| v.l())
        {
            Ok(value) => JString::from(value),
            Err(_) => return ptr::null_mut(),
        };
        let java_base = match env.get_string(&module_name) {
            Ok(value) => value.to_bytes() == b"java.base",
            Err(_) => return ptr::null_mut(),
        };
        if !loader.is_null() || !java_base {
            throw_exception_with_error_code(
                &mut env,
                "Unix channel must come from bootstrap java.base",
                libc::ENOTSUP,
            );
            return ptr::null_mut();
        }
        let lock = match env
            .get_field(&_channel, "stateLock", "Ljava/lang/Object;")
            .and_then(|v| v.l())
        {
            Ok(value) => value,
            Err(_) => return ptr::null_mut(),
        };
        if lock.is_null() {
            throw_exception_with_error_code(
                &mut env,
                "Approved Unix channel state lock is unavailable",
                libc::ENOTSUP,
            );
            return ptr::null_mut();
        }
        // Load the error class before taking the socket state lock; no business callback or I/O is performed inside.
        if env
            .find_class("org/apache/bookkeeper/common/util/nativeio/NativeIOException")
            .is_err()
        {
            return ptr::null_mut();
        }
        let raw = env.get_raw();
        // jni 0.21 lock_obj ignores MonitorEnter's status. Check it explicitly instead of assuming ownership.
        let entered = unsafe { ((**raw).MonitorEnter.unwrap())(raw, lock.as_raw()) };
        if entered != 0 {
            if !env.exception_check().unwrap_or(true) {
                throw_exception(&mut env, "Approved Unix channel monitor acquisition failed");
            }
            return ptr::null_mut();
        }
        let observed = if env.exception_check().unwrap_or(true) {
            None
        } else {
            linux_unix_peer_locked(&mut env, &_channel)
        };
        // JNI permits MonitorExit with a pending exception; release even when a field or syscall read failed.
        let exited = unsafe { ((**raw).MonitorExit.unwrap())(raw, lock.as_raw()) };
        if exited != 0 || env.exception_check().unwrap_or(true) {
            if !env.exception_check().unwrap_or(true) {
                throw_exception(&mut env, "Approved Unix channel monitor release failed");
            }
            return ptr::null_mut();
        }
        let values = match observed {
            Some(value) => value,
            None => return ptr::null_mut(),
        };
        let array = match env.new_long_array(values.len() as jint) {
            Ok(value) => value,
            Err(_) => return ptr::null_mut(),
        };
        if env.set_long_array_region(&array, 0, &values).is_err() {
            return ptr::null_mut();
        }
        array.into_raw()
    }
    #[cfg(not(target_os = "linux"))]
    {
        throw_exception_with_error_code(
            &mut env,
            "Linux Unix peer credential query is unsupported",
            libc::ENOTSUP,
        );
        ptr::null_mut()
    }
}

#[cfg(target_os = "linux")]
fn linux_unix_peer_locked(env: &mut JNIEnv, channel: &JObject) -> Option<[jlong; 3]> {
    let open = match env
        .call_method(channel, "isOpen", "()Z", &[])
        .and_then(|v| v.z())
    {
        Ok(value) => value,
        Err(_) => return None,
    };
    if !open {
        throw_exception_with_error_code(env, "Original Unix channel is closed", libc::EBADF);
        return None;
    }
    let connected = match env
        .call_method(channel, "isConnected", "()Z", &[])
        .and_then(|v| v.z())
    {
        Ok(value) => value,
        Err(_) => return None,
    };
    if !connected {
        throw_exception_with_error_code(
            env,
            "Original Unix channel is not connected",
            libc::ENOTCONN,
        );
        return None;
    }
    let descriptor = match env
        .get_field(channel, "fd", "Ljava/io/FileDescriptor;")
        .and_then(|v| v.l())
    {
        Ok(value) => value,
        Err(_) => return None,
    };
    if descriptor.is_null() {
        throw_exception_with_error_code(
            env,
            "Original Unix descriptor object is unavailable",
            libc::EBADF,
        );
        return None;
    }
    let fd = match env.get_field(&descriptor, "fd", "I").and_then(|v| v.i()) {
        Ok(value) => value,
        Err(_) => return None,
    };
    if fd < 0 {
        throw_exception_with_error_code(
            env,
            "Original Unix descriptor is unavailable",
            libc::EBADF,
        );
        return None;
    }
    let mut domain: libc::c_int = 0;
    let mut domain_length = std::mem::size_of_val(&domain) as libc::socklen_t;
    let domain_result = unsafe {
        libc::getsockopt(
            fd,
            libc::SOL_SOCKET,
            libc::SO_DOMAIN,
            &mut domain as *mut _ as *mut c_void,
            &mut domain_length,
        )
    };
    if domain_result != 0 {
        throw_exception_with_errno(env, "Original Unix socket domain query failed");
        return None;
    }
    if domain_length as usize != std::mem::size_of_val(&domain) || domain != libc::AF_UNIX {
        throw_exception_with_error_code(
            env,
            "Only Unix domain sockets have supported peer data",
            libc::EAFNOSUPPORT,
        );
        return None;
    }
    let mut socket_type: libc::c_int = 0;
    let mut type_length = std::mem::size_of_val(&socket_type) as libc::socklen_t;
    let type_result = unsafe {
        libc::getsockopt(
            fd,
            libc::SOL_SOCKET,
            libc::SO_TYPE,
            &mut socket_type as *mut _ as *mut c_void,
            &mut type_length,
        )
    };
    if type_result != 0 {
        throw_exception_with_errno(env, "Original Unix socket type query failed");
        return None;
    }
    if type_length as usize != std::mem::size_of_val(&socket_type)
        || socket_type != libc::SOCK_STREAM
    {
        throw_exception_with_error_code(
            env,
            "Only Unix stream sockets have supported peer data",
            libc::EAFNOSUPPORT,
        );
        return None;
    }
    let mut peer: libc::ucred = unsafe { std::mem::zeroed() };
    let mut length = std::mem::size_of_val(&peer) as libc::socklen_t;
    let result = unsafe {
        libc::getsockopt(
            fd,
            libc::SOL_SOCKET,
            libc::SO_PEERCRED,
            &mut peer as *mut _ as *mut c_void,
            &mut length,
        )
    };
    if result != 0 {
        throw_exception_with_errno(env, "Linux Unix peer credential query failed");
        return None;
    }
    let open_after = match env
        .call_method(channel, "isOpen", "()Z", &[])
        .and_then(|v| v.z())
    {
        Ok(value) => value,
        Err(_) => return None,
    };
    let fd_after = match env.get_field(&descriptor, "fd", "I").and_then(|v| v.i()) {
        Ok(value) => value,
        Err(_) => return None,
    };
    if !open_after
        || fd != fd_after
        || length as usize != std::mem::size_of_val(&peer)
        || peer.pid <= 0
        || peer.uid == u32::MAX
        || peer.gid == u32::MAX
    {
        throw_exception_with_error_code(
            env,
            "Original Unix peer observation is unavailable or changed",
            libc::EINVAL,
        );
        return None;
    }
    Some([peer.pid as jlong, peer.uid as jlong, peer.gid as jlong])
}

// Read UTF-16 directly: native operations must never act on a lossy or NUL-truncated replacement path.
fn native_path(env: &mut JNIEnv, path: &JString) -> Option<CString> {
    if path.is_null() {
        throw_exception_with_error_code(env, "Null native path", libc::EINVAL);
        return None;
    }
    let units = unsafe {
        // These functions are mandatory in the JNI 1.1 interface supplied by the current JVM.
        let raw = env.get_raw();
        let length = (**raw).GetStringLength.unwrap()(raw, path.as_raw());
        if env.exception_check().unwrap_or(true) {
            return None;
        }
        let mut units = vec![0_u16; length as usize];
        (**raw).GetStringRegion.unwrap()(raw, path.as_raw(), 0, length, units.as_mut_ptr());
        if env.exception_check().unwrap_or(true) {
            return None;
        }
        units
    };
    let value = match String::from_utf16(&units) {
        Ok(value) => value,
        Err(_) => {
            throw_exception_with_error_code(env, "Invalid native path encoding", libc::EINVAL);
            return None;
        }
    };
    match CString::new(value) {
        Ok(value) => Some(value),
        Err(_) => {
            throw_exception_with_error_code(env, "NUL in native path", libc::EINVAL);
            None
        }
    }
}

/// One atomic no-replace rename only. The caller owns file close and directory durability.
#[no_mangle]
pub extern "system" fn Java_org_apache_bookkeeper_common_util_nativeio_NativeIOJni_renameNoReplace(
    mut env: JNIEnv,
    _class: JClass,
    source: JString,
    target: JString,
) -> jint {
    let source = match native_path(&mut env, &source) {
        Some(path) => path,
        None => return -1,
    };
    let target = match native_path(&mut env, &target) {
        Some(path) => path,
        None => return -1,
    };
    #[cfg(target_os = "linux")]
    let result = unsafe {
        // Do not add a newer glibc symbol requirement to the existing JNI library.
        libc::syscall(
            libc::SYS_renameat2,
            libc::AT_FDCWD,
            source.as_ptr(),
            libc::AT_FDCWD,
            target.as_ptr(),
            libc::RENAME_NOREPLACE,
        )
    };
    #[cfg(target_os = "macos")]
    let result = unsafe { libc::renamex_np(source.as_ptr(), target.as_ptr(), libc::RENAME_EXCL) };
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        throw_exception_with_error_code(
            &mut env,
            "Atomic no-replace rename unsupported",
            libc::ENOSYS,
        );
        return -1;
    }
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    {
        if result != 0 {
            throw_exception_with_errno(&mut env, "Atomic no-replace rename failed");
            return -1;
        }
        0
    }
}

#[cfg(target_os = "macos")]
extern "C" {
    fn filesec_init() -> *mut c_void;
    fn filesec_free(security: *mut c_void);
    fn filesec_query_property(
        security: *mut c_void,
        property: libc::c_int,
        valid: *mut libc::c_int,
    ) -> libc::c_int;
    // Match the SDK's inode64 struct stat ABI, also used by libc::stat on macOS.
    #[cfg_attr(target_arch = "x86_64", link_name = "lstatx_np$INODE64")]
    #[cfg_attr(not(target_arch = "x86_64"), link_name = "lstatx_np")]
    fn acl_lstat(
        path: *const libc::c_char,
        stat: *mut libc::stat,
        security: *mut c_void,
    ) -> libc::c_int;
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn acl_presence(path: &CString) -> Result<bool, libc::c_int> {
    let errno = || {
        std::io::Error::last_os_error()
            .raw_os_error()
            .unwrap_or(libc::EIO)
    };
    let mut before: libc::stat = unsafe { std::mem::zeroed() };
    if unsafe { libc::lstat(path.as_ptr(), &mut before) } != 0 {
        return Err(errno());
    }
    let kind = before.st_mode & libc::S_IFMT;
    if kind != libc::S_IFREG && kind != libc::S_IFDIR {
        return Err(libc::EINVAL);
    }
    let mut filesystem: libc::statfs = unsafe { std::mem::zeroed() };
    if unsafe { libc::statfs(path.as_ptr(), &mut filesystem) } != 0 {
        return Err(errno());
    }
    #[cfg(target_os = "linux")]
    let present = {
        // These local filesystems expose access/default POSIX ACLs through the named xattrs.
        // In particular, never infer NFSv4/SMB/FUSE permissions from absent POSIX ACL attributes.
        if !matches!(
            filesystem.f_type as u64,
            0xef53 | 0x58465342 | 0x9123683e | 0x01021994
        ) {
            return Err(libc::EOPNOTSUPP);
        }
        let mut present = false;
        for name in [
            b"system.posix_acl_access\0".as_slice(),
            b"system.posix_acl_default\0".as_slice(),
        ] {
            let result =
                unsafe { libc::lgetxattr(path.as_ptr(), name.as_ptr().cast(), ptr::null_mut(), 0) };
            if result >= 0 {
                present = true;
            } else if errno() != libc::ENODATA {
                return Err(errno());
            }
        }
        present
    };
    #[cfg(target_os = "macos")]
    let present = {
        let name = unsafe { std::ffi::CStr::from_ptr(filesystem.f_fstypename.as_ptr()) }.to_bytes();
        if filesystem.f_flags & libc::MNT_LOCAL as u32 == 0 || (name != b"apfs" && name != b"hfs") {
            return Err(libc::EOPNOTSUPP);
        }
        let security = unsafe { filesec_init() };
        if security.is_null() {
            return Err(libc::ENOMEM);
        }
        let mut observed: libc::stat = unsafe { std::mem::zeroed() };
        let mut present = -1;
        let result = if unsafe { acl_lstat(path.as_ptr(), &mut observed, security) } != 0 {
            Err(errno())
        } else if observed.st_dev != before.st_dev
            || observed.st_ino != before.st_ino
            || observed.st_mode != before.st_mode
        {
            Err(libc::EAGAIN)
        } else if unsafe { filesec_query_property(security, 5, &mut present) } != 0 {
            // FILESEC_ACL
            Err(errno())
        } else if present < 0 {
            Err(libc::EIO)
        } else {
            Ok(present != 0)
        };
        // filesec owns its ACL allocation. No descriptor is opened or handed to Java.
        unsafe { filesec_free(security) };
        result?
    };
    let mut after: libc::stat = unsafe { std::mem::zeroed() };
    if unsafe { libc::lstat(path.as_ptr(), &mut after) } != 0 {
        return Err(errno());
    }
    if before.st_dev != after.st_dev
        || before.st_ino != after.st_ino
        || before.st_mode != after.st_mode
        || before.st_uid != after.st_uid
        || before.st_gid != after.st_gid
        || before.st_ctime != after.st_ctime
        || before.st_ctime_nsec != after.st_ctime_nsec
    {
        return Err(libc::EAGAIN);
    }
    Ok(present)
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn ordinary_symbolic_link(path: &CString) -> Result<(libc::stat, Vec<u8>), libc::c_int> {
    let errno = || {
        std::io::Error::last_os_error()
            .raw_os_error()
            .unwrap_or(libc::EIO)
    };
    let bytes = path.as_bytes();
    if !bytes.starts_with(b"/") {
        return Err(libc::EINVAL);
    }
    let mut before: libc::stat = unsafe { std::mem::zeroed() };
    if unsafe { libc::lstat(path.as_ptr(), &mut before) } != 0 {
        return Err(errno());
    }
    if before.st_mode & libc::S_IFMT != libc::S_IFLNK {
        return Err(libc::EINVAL);
    }
    let slash = bytes.iter().rposition(|&b| b == b'/').unwrap();
    let parent = CString::new(&bytes[..slash.max(1)]).map_err(|_| libc::EINVAL)?;
    let mut filesystem: libc::statfs = unsafe { std::mem::zeroed() };
    if unsafe { libc::statfs(parent.as_ptr(), &mut filesystem) } != 0 {
        return Err(errno());
    }
    #[cfg(target_os = "linux")]
    if !matches!(
        filesystem.f_type as u64,
        0xef53 | 0x58465342 | 0x9123683e | 0x01021994
    ) {
        // In particular, do not treat procfs magic links as ordinary filesystem aliases.
        return Err(libc::EOPNOTSUPP);
    }
    #[cfg(target_os = "macos")]
    {
        let name = unsafe { std::ffi::CStr::from_ptr(filesystem.f_fstypename.as_ptr()) }.to_bytes();
        if filesystem.f_flags & libc::MNT_LOCAL as u32 == 0 || (name != b"apfs" && name != b"hfs") {
            return Err(libc::EOPNOTSUPP);
        }
    }
    let mut target = [0_u8; 4097];
    let count = unsafe { libc::readlink(path.as_ptr(), target.as_mut_ptr().cast(), target.len()) };
    if count < 0 {
        return Err(errno());
    }
    if count == 0 || count > 4096 {
        return Err(libc::ENAMETOOLONG);
    }
    let mut after: libc::stat = unsafe { std::mem::zeroed() };
    if unsafe { libc::lstat(path.as_ptr(), &mut after) } != 0 {
        return Err(errno());
    }
    if before.st_dev != after.st_dev
        || before.st_ino != after.st_ino
        || before.st_mode != after.st_mode
        || before.st_uid != after.st_uid
        || before.st_gid != after.st_gid
        || before.st_nlink != after.st_nlink
        || before.st_size != after.st_size
        || before.st_mtime != after.st_mtime
        || before.st_mtime_nsec != after.st_mtime_nsec
        || before.st_ctime != after.st_ctime
        || before.st_ctime_nsec != after.st_ctime_nsec
    {
        return Err(libc::EAGAIN);
    }
    if before.st_size != count as libc::off_t {
        return Err(libc::EINVAL);
    }
    Ok((before, target[..count as usize].to_vec()))
}

/// Raw ordinary symlink bytes and the compared no-follow identity; no handles, target lookup or retry.
#[no_mangle]
pub extern "system" fn Java_org_apache_bookkeeper_common_util_nativeio_NativeIOJni_observeSymbolicLink(
    mut env: JNIEnv,
    _class: JClass,
    path: JString,
) -> jni::sys::jobject {
    let path = match native_path(&mut env, &path) {
        Some(path) => path,
        None => return ptr::null_mut(),
    };
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    {
        let (identity, target) = match ordinary_symbolic_link(&path) {
            Ok(observed) => observed,
            Err(errno) => {
                throw_exception_with_error_code(
                    &mut env,
                    "Ordinary symbolic-link observation failed",
                    errno,
                );
                return ptr::null_mut();
            }
        };
        let bytes = match env.byte_array_from_slice(&target) {
            Ok(value) => value,
            Err(_) => return ptr::null_mut(),
        };
        match env.new_object(
            "org/apache/bookkeeper/common/util/nativeio/NativeIO$SymbolicLinkObservation",
            "(JJJJIJJJJJJ[B)V",
            &[
                (identity.st_dev as jlong).into(),
                (identity.st_ino as jlong).into(),
                (identity.st_uid as jlong).into(),
                (identity.st_gid as jlong).into(),
                (identity.st_mode as jint).into(),
                (identity.st_nlink as jlong).into(),
                (identity.st_size as jlong).into(),
                (identity.st_mtime as jlong).into(),
                (identity.st_mtime_nsec as jlong).into(),
                (identity.st_ctime as jlong).into(),
                (identity.st_ctime_nsec as jlong).into(),
                (&bytes).into(),
            ],
        ) {
            Ok(value) => value.into_raw(),
            Err(_) => ptr::null_mut(),
        }
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        throw_exception_with_error_code(
            &mut env,
            "Ordinary symbolic-link observation unsupported",
            libc::ENOSYS,
        );
        ptr::null_mut()
    }
}

/// Read-only ACL presence, not Unix trust. Unsupported or unreadable is never converted to absence.
#[no_mangle]
pub extern "system" fn Java_org_apache_bookkeeper_common_util_nativeio_NativeIOJni_hasExtendedAcl(
    mut env: JNIEnv,
    _class: JClass,
    path: JString,
) -> jint {
    let path = match native_path(&mut env, &path) {
        Some(path) => path,
        None => return -1,
    };
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    match acl_presence(&path) {
        Ok(present) => {
            if present {
                1
            } else {
                0
            }
        }
        Err(errno) => {
            throw_exception_with_error_code(&mut env, "Native ACL presence query failed", errno);
            -1
        }
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        throw_exception_with_error_code(
            &mut env,
            "Native ACL presence query unsupported",
            libc::ENOSYS,
        );
        -1
    }
}

/// Class: org.apache.bookkeeper.common.util.nativeio.NativeIOJni
/// Method: open
/// Signature: (Ljava/lang/String;II)I
#[no_mangle]
pub extern "system" fn Java_org_apache_bookkeeper_common_util_nativeio_NativeIOJni_open(
    mut env: JNIEnv,
    _class: JClass,
    path: JString,
    java_flags: jint,
    mode: jint,
) -> jint {
    // Convert Java flags to POSIX flags (same logic as C code)
    let mut flags: i32 = 0;

    if java_flags & O_CREAT != 0 {
        flags |= libc::O_CREAT;
    }
    if java_flags & O_RDONLY != 0 {
        flags |= libc::O_RDONLY;
    }
    if java_flags & O_WRONLY != 0 {
        flags |= libc::O_WRONLY;
    }
    if java_flags & O_TRUNC != 0 {
        flags |= libc::O_TRUNC;
    }

    // O_DIRECT is Linux only
    #[cfg(target_os = "linux")]
    if java_flags & O_DIRECT != 0 {
        flags |= libc::O_DIRECT;
    }

    // O_DSYNC is not available on Windows
    #[cfg(not(target_os = "windows"))]
    if java_flags & O_DSYNC != 0 {
        flags |= libc::O_DSYNC;
    }

    // Get the path string
    let path_str: String = match env.get_string(&path) {
        Ok(s) => s.into(),
        Err(_) => {
            throw_exception(&mut env, "Failed to get path string");
            return -1;
        }
    };

    let path_cstr = match CString::new(path_str) {
        Ok(path) => path,
        Err(_) => {
            throw_exception(&mut env, "Path contains interior NUL byte");
            return -1;
        }
    };

    let fd = unsafe { libc::open(path_cstr.as_ptr(), flags, mode as libc::c_uint) };

    if fd == -1 {
        throw_exception_with_errno(&mut env, "Failed to open file");
    }

    fd
}

/// Class: org.apache.bookkeeper.common.util.nativeio.NativeIOJni
/// Method: fsync
/// Signature: (I)I
#[no_mangle]
pub extern "system" fn Java_org_apache_bookkeeper_common_util_nativeio_NativeIOJni_fsync(
    mut env: JNIEnv,
    _class: JClass,
    fd: jint,
) -> jint {
    let res = unsafe { libc::fsync(fd) };

    if res == -1 {
        throw_exception_with_errno(&mut env, "Failed to fsync");
    }

    res as jint
}

/// Class: org.apache.bookkeeper.common.util.nativeio.NativeIOJni
/// Method: fallocate
/// Signature: (IIJJ)I
#[no_mangle]
pub extern "system" fn Java_org_apache_bookkeeper_common_util_nativeio_NativeIOJni_fallocate(
    mut env: JNIEnv,
    _class: JClass,
    fd: jint,
    mode: jint,
    offset: jlong,
    len: jlong,
) -> jint {
    // fallocate is Linux only
    #[cfg(target_os = "linux")]
    {
        let res = unsafe { libc::fallocate(fd, mode, offset, len) };

        if res == -1 {
            throw_exception_with_errno(&mut env, "Failed to fallocate");
        }

        res
    }

    #[cfg(not(target_os = "linux"))]
    {
        throw_exception(&mut env, "fallocate is not available");
        -1
    }
}

/// Class: org.apache.bookkeeper.common.util.nativeio.NativeIOJni
/// Method: posix_fadvise
/// Signature: (IJJI)I
/// Note: In JNI, underscores in method names are escaped as _1
#[no_mangle]
pub extern "system" fn Java_org_apache_bookkeeper_common_util_nativeio_NativeIOJni_posix_1fadvise(
    mut env: JNIEnv,
    _class: JClass,
    fd: jint,
    offset: jlong,
    len: jlong,
    flag: jint,
) -> jint {
    // posix_fadvise is Linux only
    #[cfg(target_os = "linux")]
    {
        let res = unsafe { libc::posix_fadvise(fd, offset, len, flag) };

        if res != 0 {
            throw_exception_with_error_code(&mut env, "Failed to posix_fadvise", res);
        }

        res
    }

    #[cfg(not(target_os = "linux"))]
    {
        throw_exception(&mut env, "posix_fadvise is not available");
        -1
    }
}

/// Class: org.apache.bookkeeper.common.util.nativeio.NativeIOJni
/// Method: lseek
/// Signature: (IJI)J
#[no_mangle]
pub extern "system" fn Java_org_apache_bookkeeper_common_util_nativeio_NativeIOJni_lseek(
    mut env: JNIEnv,
    _class: JClass,
    fd: jint,
    offset: jlong,
    whence: jint,
) -> jlong {
    let res = unsafe { libc::lseek(fd, offset, whence) };

    if res == -1 {
        throw_exception_with_errno(&mut env, "Failed to lseek");
    }

    res as jlong
}

/// Class: org.apache.bookkeeper.common.util.nativeio.NativeIOJni
/// Method: close
/// Signature: (I)I
#[no_mangle]
pub extern "system" fn Java_org_apache_bookkeeper_common_util_nativeio_NativeIOJni_close(
    mut env: JNIEnv,
    _class: JClass,
    fd: jint,
) -> jint {
    let res = unsafe { libc::close(fd) };

    if res == -1 {
        throw_exception_with_errno(&mut env, "Failed to close file");
    }

    res as jint
}

/// Class: org.apache.bookkeeper.common.util.nativeio.NativeIOJni
/// Method: pwrite
/// Signature: (IJIJ)I
#[no_mangle]
pub extern "system" fn Java_org_apache_bookkeeper_common_util_nativeio_NativeIOJni_pwrite(
    mut env: JNIEnv,
    _class: JClass,
    fd: jint,
    pointer: jlong,
    count: jint,
    offset: jlong,
) -> jint {
    let res = unsafe { libc::pwrite(fd, pointer as *const c_void, count as usize, offset) };

    if res == -1 {
        throw_exception_with_errno(&mut env, "Failed to write on file");
    }

    res as jint
}

/// Class: org.apache.bookkeeper.common.util.nativeio.NativeIOJni
/// Method: posix_memalign
/// Signature: (II)J
/// Note: In JNI, underscores in method names are escaped as _1
#[no_mangle]
pub extern "system" fn Java_org_apache_bookkeeper_common_util_nativeio_NativeIOJni_posix_1memalign(
    mut env: JNIEnv,
    _class: JClass,
    alignment: jint,
    size: jint,
) -> jlong {
    let mut ptr: *mut c_void = ptr::null_mut();

    let res = unsafe { libc::posix_memalign(&mut ptr, alignment as usize, size as usize) };

    if res != 0 {
        throw_exception_with_error_code(&mut env, "Failed to allocate aligned memory", res);
        return 0;
    }

    ptr as jlong
}

/// Class: org.apache.bookkeeper.common.util.nativeio.NativeIOJni
/// Method: pread
/// Signature: (IJJJ)J
#[no_mangle]
pub extern "system" fn Java_org_apache_bookkeeper_common_util_nativeio_NativeIOJni_pread(
    mut env: JNIEnv,
    _class: JClass,
    fd: jint,
    pointer: jlong,
    size: jlong,
    offset: jlong,
) -> jlong {
    let res = unsafe { libc::pread(fd, pointer as *mut c_void, size as usize, offset) };

    if res == -1 {
        throw_exception_with_errno(&mut env, "Failed to read from file");
    }

    res as jlong
}

/// Class: org.apache.bookkeeper.common.util.nativeio.NativeIOJni
/// Method: free
/// Signature: (J)V
#[no_mangle]
pub extern "system" fn Java_org_apache_bookkeeper_common_util_nativeio_NativeIOJni_free(
    _env: JNIEnv,
    _class: JClass,
    pointer: jlong,
) {
    unsafe {
        libc::free(pointer as *mut c_void);
    }
}
