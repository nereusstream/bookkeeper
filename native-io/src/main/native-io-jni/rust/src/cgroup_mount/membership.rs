/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#[cfg(target_os = "linux")]
use super::{
    close_proc_identity_fd, proc_identity_errno, proc_identity_stat, query_statx, Member,
    MountDirectory, ProcIdentityResult,
};
#[cfg(target_os = "linux")]
use crate::close_identity_fd;

const MAX_MEMBERSHIP_PATH: usize = 4095;
const MAX_MEMBERSHIP_BYTES: usize = b"0::\n".len() + MAX_MEMBERSHIP_PATH;

fn parse_membership(bytes: &[u8]) -> Result<&str, libc::c_int> {
    if bytes.len() > MAX_MEMBERSHIP_BYTES {
        return Err(libc::EOVERFLOW);
    }
    let path = bytes
        .strip_prefix(b"0::")
        .and_then(|line| line.strip_suffix(b"\n"))
        .ok_or(libc::EINVAL)?;
    let relative = path.strip_prefix(b"/").ok_or(libc::EINVAL)?;
    if !relative.is_empty() && !relative.split(|byte| *byte == b'/').all(valid_component) {
        return Err(libc::EINVAL);
    }
    std::str::from_utf8(path).map_err(|_| libc::EINVAL)
}

fn valid_component(component: &[u8]) -> bool {
    if component.is_empty() || component.len() > 255 || component == b"." || component == b".." {
        return false;
    }
    let mut remaining = component;
    while let Some((&byte, tail)) = remaining.split_first() {
        if byte.is_ascii_alphanumeric() || b":_.@-".contains(&byte) {
            remaining = tail;
        } else if byte == b'\\'
            && tail.len() >= 3
            && tail[0] == b'x'
            && tail[1].is_ascii_hexdigit()
            && tail[2].is_ascii_hexdigit()
        {
            remaining = &tail[3..];
        } else {
            return false;
        }
    }
    true
}

#[cfg(target_os = "linux")]
fn read_membership(root: libc::c_int, proc_root: &libc::stat) -> ProcIdentityResult<String> {
    let how = [
        (libc::O_RDONLY | libc::O_NOFOLLOW | libc::O_CLOEXEC | libc::O_NONBLOCK) as u64,
        0,
        0x08 | 0x01 | 0x02, // BENEATH | NO_XDEV | NO_MAGICLINKS; thread-self is an ordinary link.
    ];
    let leaf = unsafe {
        libc::syscall(
            libc::SYS_openat2,
            root,
            b"thread-self/cgroup\0".as_ptr().cast::<libc::c_char>(),
            how.as_ptr(),
            std::mem::size_of_val(&how),
        )
    };
    if leaf < 0 {
        return Err(proc_identity_errno("thread cgroup fixed openat2 failed"));
    }
    let leaf = leaf as libc::c_int;
    let outcome = (|| {
        let before = proc_identity_stat(leaf)?;
        if before.st_mode & libc::S_IFMT != libc::S_IFREG || before.st_dev != proc_root.st_dev {
            return Err((
                "thread cgroup leaf differs from its held procfs",
                libc::EINVAL,
            ));
        }
        let mut bytes = [0_u8; MAX_MEMBERSHIP_BYTES + 1];
        let mut used = 0;
        loop {
            let count =
                unsafe { libc::read(leaf, bytes[used..].as_mut_ptr().cast(), bytes.len() - used) };
            if count < 0 {
                return Err(proc_identity_errno("thread cgroup bounded read failed"));
            }
            if count == 0 {
                break;
            }
            used += count as usize;
            if used > MAX_MEMBERSHIP_BYTES {
                return Err(("thread cgroup output exceeds its bound", libc::EOVERFLOW));
            }
        }
        let after = proc_identity_stat(leaf)?;
        if before.st_dev != after.st_dev
            || before.st_ino != after.st_ino
            || before.st_mode != after.st_mode
        {
            return Err((
                "thread cgroup held source changed during read",
                libc::EAGAIN,
            ));
        }
        parse_membership(&bytes[..used])
            .map(str::to_owned)
            .map_err(|error| ("Invalid unified thread cgroup membership", error))
    })();
    close_proc_identity_fd(leaf, outcome)
}

#[cfg(target_os = "linux")]
fn open_member(root: libc::c_int, path: &str) -> ProcIdentityResult<Option<libc::c_int>> {
    if path == "/" {
        return Ok(None); // Borrow the original held root for the root membership case.
    }
    let mut relative = path.as_bytes()[1..].to_vec();
    relative.push(0);
    let how = [
        (libc::O_PATH | libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC) as u64,
        0,
        0x08 | 0x01 | 0x04 | 0x02, // BENEATH | NO_XDEV | NO_SYMLINKS | NO_MAGICLINKS
    ];
    let fd = unsafe {
        libc::syscall(
            libc::SYS_openat2,
            root,
            relative.as_ptr().cast::<libc::c_char>(),
            how.as_ptr(),
            std::mem::size_of_val(&how),
        )
    };
    if fd < 0 {
        return Err(proc_identity_errno(
            "held-root cgroup member openat2 failed",
        ));
    }
    Ok(Some(fd as libc::c_int))
}

#[cfg(target_os = "linux")]
fn member_identity(fd: libc::c_int, mount: MountDirectory) -> ProcIdentityResult<[u64; 2]> {
    let mut fs = std::mem::MaybeUninit::<libc::statfs>::zeroed();
    if unsafe { libc::fstatfs(fd, fs.as_mut_ptr()) } != 0 {
        return Err(proc_identity_errno("held cgroup member fstatfs failed"));
    }
    if unsafe { fs.assume_init() }.f_type != 0x63677270 {
        return Err(("held cgroup member is not cgroup2", libc::EINVAL));
    }
    let mut stat = std::mem::MaybeUninit::<libc::stat>::zeroed();
    if unsafe { libc::fstat(fd, stat.as_mut_ptr()) } != 0 {
        return Err(proc_identity_errno("held cgroup member fstat failed"));
    }
    let stat = unsafe { stat.assume_init() };
    let identity = query_statx(fd, b"\0", libc::AT_EMPTY_PATH)?;
    let (major, minor) = unsafe { (libc::major(stat.st_dev), libc::minor(stat.st_dev)) };
    if stat.st_mode & libc::S_IFMT != libc::S_IFDIR
        || identity.inode != stat.st_ino
        || identity.major != major
        || identity.minor != minor
        || identity.mount_id != mount.mount_id
        || major != mount.major
        || minor != mount.minor
    {
        return Err((
            "held cgroup member identity differs from the original mount",
            libc::ESTALE,
        ));
    }
    Ok([stat.st_dev as u64, stat.st_ino as u64])
}

#[cfg(target_os = "linux")]
fn close_member<T>(
    fd: Option<libc::c_int>,
    outcome: ProcIdentityResult<T>,
) -> ProcIdentityResult<T> {
    match fd {
        Some(fd) => close_identity_fd(fd, outcome, "held cgroup member close failed"),
        None => outcome,
    }
}

#[cfg(target_os = "linux")]
pub(super) fn observe(
    cgroup_root: libc::c_int,
    mount: MountDirectory,
    proc_root: libc::c_int,
    proc_stat: &libc::stat,
) -> ProcIdentityResult<Member> {
    let path = read_membership(proc_root, proc_stat)?;
    let member = open_member(cgroup_root, &path)?;
    let outcome = (|| {
        let before = member_identity(member.unwrap_or(cgroup_root), mount)?;
        if read_membership(proc_root, proc_stat)? != path {
            return Err((
                "calling thread cgroup membership changed during observation",
                libc::EAGAIN,
            ));
        }
        let recheck = open_member(cgroup_root, &path)?;
        let after = close_member(
            recheck,
            member_identity(recheck.unwrap_or(cgroup_root), mount),
        )?;
        if before != after {
            return Err((
                "held cgroup member directory changed during observation",
                libc::ESTALE,
            ));
        }
        Ok(Member {
            path,
            device: before[0],
            inode: before[1],
        })
    })();
    close_member(member, outcome)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn preserves_root_colons_and_literal_systemd_escape_bytes() {
        assert_eq!(parse_membership(b"0::/\n"), Ok("/"));
        assert_eq!(
            parse_membership(b"0::/system.slice/bookie:old\\x2D1@a.service\n"),
            Ok("/system.slice/bookie:old\\x2D1@a.service")
        );
    }

    #[test]
    fn rejects_other_hierarchies_truncation_and_unsupported_paths() {
        for invalid in [
            b"0::/system.slice\n1:cpu:/system.slice\n".as_slice(),
            b"0::/\n0::/\n",
            b"1:cpu:/\n",
            b"0::/system.slice",
            b"0::/gone (deleted)\n",
            b"0::/embedded\nnewline\n",
            b"0::/dot/../group\n",
            b"0::/dot/./group\n",
            b"0::/double//group\n",
            b"0::/trailing/\n",
            b"0::relative\n",
            b"0::/bad\\x2Z\n",
            b"0::/bad\\040escape\n",
            b"0::/bad\0byte\n",
            b"0::/bad\xffbyte\n",
        ] {
            assert_eq!(parse_membership(invalid), Err(libc::EINVAL));
        }
        let oversized_component = format!("0::/{}\n", "a".repeat(256));
        assert_eq!(
            parse_membership(oversized_component.as_bytes()),
            Err(libc::EINVAL)
        );
    }

    #[test]
    fn accepts_the_exact_path_limit_and_rejects_one_byte_more() {
        let prefix = format!("0::{}", format!("/{}", "a".repeat(255)).repeat(15));
        let maximum = format!("{prefix}/{}\n", "b".repeat(254));
        assert_eq!(maximum.len(), MAX_MEMBERSHIP_BYTES);
        assert_eq!(
            parse_membership(maximum.as_bytes()).unwrap().len(),
            MAX_MEMBERSHIP_PATH
        );
        let too_long = format!("{prefix}/{}\n", "b".repeat(255));
        assert_eq!(parse_membership(too_long.as_bytes()), Err(libc::EOVERFLOW));
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn root_case_borrows_and_member_close_failure_preserves_the_first_error() {
        assert_eq!(open_member(-1, "/"), Ok(None));
        let first = ("earlier member error", libc::ESTALE);
        assert_eq!(close_member(Some(-1), Err::<(), _>(first)), Err(first));
        assert_eq!(
            close_member(Some(-1), Ok(())),
            Err(("held cgroup member close failed", libc::EBADF))
        );
        assert_eq!(close_member(None, Ok(())), Ok(()));
    }
}
