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
use jni::sys::{jint, jlong, jlongArray};
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
        let count = unsafe { libc::getgroups(groups.len() as libc::c_int, groups.as_mut_ptr()) };
        if count < 0 {
            throw_exception_with_errno(&mut env, "Current process supplementary group query failed");
            return ptr::null_mut();
        }
        let after = unsafe { [libc::getuid(), libc::geteuid(), libc::getgid(), libc::getegid()] };
        if before != after || count as usize > groups.len() {
            throw_exception_with_error_code(&mut env, "Current process credentials changed", libc::EAGAIN);
            return ptr::null_mut();
        }
        let mut values: Vec<jlong> = before.iter().map(|value| *value as jlong).collect();
        values.extend(groups[..count as usize].iter().map(|value| *value as jlong));
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
