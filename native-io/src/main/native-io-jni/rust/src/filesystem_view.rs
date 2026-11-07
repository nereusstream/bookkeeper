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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct MountIdentity {
    device: u64,
    inode: u64,
    mount_id: u32,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct View {
    namespace: u32,
    root: MountIdentity,
}

fn require_same_view(before: View, after: View) -> Result<(), libc::c_int> {
    if before != after {
        return Err(libc::ESTALE);
    }
    Ok(())
}

#[cfg(target_os = "linux")]
mod linux {
    use super::{require_same_view, MountIdentity, View};
    use crate::cgroup_mount::{mount_statx, require_mount_statx};
    use crate::{
        close_identity_fd, proc_identity_errno, proc_identity_root, proc_identity_stat,
        read_proc_start_ticks, read_thread_namespace_links, ProcIdentityResult,
    };

    const FIXED_PROC: u64 = libc::RESOLVE_BENEATH
        | libc::RESOLVE_NO_XDEV
        | libc::RESOLVE_NO_SYMLINKS
        | libc::RESOLVE_NO_MAGICLINKS;
    const DIRECTORY: libc::c_int = libc::O_PATH | libc::O_DIRECTORY | libc::O_CLOEXEC;

    fn with_fd<T>(
        fd: libc::c_int,
        opening: &'static str,
        read: impl FnOnce(libc::c_int) -> ProcIdentityResult<T>,
    ) -> ProcIdentityResult<T> {
        if fd < 0 {
            return Err(proc_identity_errno(opening));
        }
        close_identity_fd(fd, read(fd), "Filesystem view temporary fd close failed")
    }

    fn proc_open(parent: libc::c_int, path: &[u8], flags: libc::c_int) -> libc::c_int {
        let how = [flags as u64, 0, FIXED_PROC];
        unsafe {
            libc::syscall(
                libc::SYS_openat2,
                parent,
                path.as_ptr().cast::<libc::c_char>(),
                how.as_ptr(),
                std::mem::size_of_val(&how),
            ) as libc::c_int
        }
    }

    fn identity(
        fd: libc::c_int,
        original: &libc::stat,
        kind: libc::mode_t,
    ) -> ProcIdentityResult<MountIdentity> {
        let stat = mount_statx(fd, b"\0", libc::AT_EMPTY_PATH | libc::AT_SYMLINK_NOFOLLOW)?;
        require_mount_statx(&stat)?;
        let (major, minor) =
            unsafe { (libc::major(original.st_dev), libc::minor(original.st_dev)) };
        if original.st_mode & libc::S_IFMT != kind
            || stat.stx_mode as libc::mode_t & libc::S_IFMT != kind
            || (stat.stx_dev_major, stat.stx_dev_minor, stat.stx_ino)
                != (major, minor, original.st_ino)
        {
            return Err((
                "Filesystem view fstat and statx differ or have an unexpected type",
                libc::ESTALE,
            ));
        }
        Ok(MountIdentity {
            device: original.st_dev,
            inode: original.st_ino,
            mount_id: stat.stx_mnt_id as u32,
        })
    }

    fn root_identity(fd: libc::c_int) -> ProcIdentityResult<MountIdentity> {
        let mut stat = std::mem::MaybeUninit::<libc::stat>::uninit();
        if unsafe { libc::fstat(fd, stat.as_mut_ptr()) } != 0 {
            return Err(proc_identity_errno("Filesystem view root fstat failed"));
        }
        identity(fd, &unsafe { stat.assume_init() }, libc::S_IFDIR)
    }

    fn require_identity(
        original: MountIdentity,
        current: MountIdentity,
        operation: &'static str,
    ) -> ProcIdentityResult<()> {
        if original != current {
            return Err((operation, libc::ESTALE));
        }
        Ok(())
    }

    fn open_proc() -> libc::c_int {
        unsafe { libc::open(b"/proc\0".as_ptr().cast(), DIRECTORY | libc::O_NOFOLLOW) }
    }

    fn with_proc<T>(
        read: impl FnOnce(libc::c_int, &libc::stat, MountIdentity) -> ProcIdentityResult<T>,
    ) -> ProcIdentityResult<T> {
        with_fd(
            open_proc(),
            "Filesystem view fixed proc root open failed",
            |proc| {
                let status = proc_identity_root(proc)?;
                let original = identity(proc, &status, libc::S_IFDIR)?;
                let result = read(proc, &status, original)?;
                require_identity(
                    original,
                    identity(proc, &proc_identity_root(proc)?, libc::S_IFDIR)?,
                    "Filesystem view held proc root changed",
                )?;
                let current = with_fd(
                    open_proc(),
                    "Filesystem view fixed proc root reopen failed",
                    |again| identity(again, &proc_identity_root(again)?, libc::S_IFDIR),
                )?;
                require_identity(original, current, "Filesystem view fixed proc root changed")?;
                Ok(result)
            },
        )
    }

    fn with_current_root<T>(
        read: impl FnOnce(libc::c_int) -> ProcIdentityResult<T>,
    ) -> ProcIdentityResult<T> {
        let fd = unsafe { libc::open(b"/\0".as_ptr().cast(), DIRECTORY | libc::O_NOFOLLOW) };
        with_fd(fd, "Filesystem view current root open failed", read)
    }

    fn current_namespace(proc: libc::c_int, status: &libc::stat) -> ProcIdentityResult<u32> {
        let [namespace] =
            read_thread_namespace_links(proc, status, [(b"thread-self/ns/mnt\0", b"mnt:[")])?;
        Ok(namespace)
    }

    fn view_values(view: View) -> [u64; 4] {
        [
            u64::from(view.namespace),
            view.root.device,
            view.root.inode,
            u64::from(view.root.mount_id),
        ]
    }

    pub(crate) fn current_view() -> ProcIdentityResult<[u64; 4]> {
        with_proc(|proc, status, _| {
            let namespace = current_namespace(proc, status)?;
            with_current_root(|fd| {
                let first = View {
                    namespace,
                    root: root_identity(fd)?,
                };
                let again = with_current_root(root_identity)?;
                let current = View {
                    namespace: current_namespace(proc, status)?,
                    root: again,
                };
                require_same_view(first, current)
                    .map_err(|error| ("Current Linux filesystem view changed", error))?;
                require_identity(
                    first.root,
                    root_identity(fd)?,
                    "Filesystem view held current root changed",
                )?;
                Ok(view_values(first))
            })
        })
    }

    fn pid1_proc_directory(proc: libc::c_int) -> libc::c_int {
        proc_open(proc, b"1\0", DIRECTORY | libc::O_NOFOLLOW)
    }

    fn proc_member(
        fd: libc::c_int,
        proc: MountIdentity,
        kind: libc::mode_t,
    ) -> ProcIdentityResult<MountIdentity> {
        let actual = identity(fd, &proc_identity_stat(fd)?, kind)?;
        if (actual.device, actual.mount_id) != (proc.device, proc.mount_id) {
            return Err((
                "PID 1 filesystem view member differs from the original procfs",
                libc::ESTALE,
            ));
        }
        Ok(actual)
    }

    fn pid1_root_link(pid1: libc::c_int) -> libc::c_int {
        proc_open(
            pid1,
            b"root\0",
            libc::O_PATH | libc::O_NOFOLLOW | libc::O_CLOEXEC,
        )
    }

    fn with_pid1_root<T>(
        pid1: libc::c_int,
        proc: MountIdentity,
        read: impl FnOnce(libc::c_int) -> ProcIdentityResult<T>,
    ) -> ProcIdentityResult<T> {
        with_fd(
            pid1_root_link(pid1),
            "Filesystem view fixed PID 1 root link open failed",
            |link| {
                let original = proc_member(link, proc, libc::S_IFLNK)?;
                // The only magic-link follow. The verified PID 1 proc directory is held and this is one fixed leaf.
                let target = unsafe { libc::openat(pid1, b"root\0".as_ptr().cast(), DIRECTORY) };
                let result = with_fd(
                    target,
                    "Filesystem view fixed PID 1 root follow failed",
                    read,
                )?;
                require_identity(
                    original,
                    proc_member(link, proc, libc::S_IFLNK)?,
                    "Filesystem view held PID 1 root link changed",
                )?;
                let again = with_fd(
                    pid1_root_link(pid1),
                    "Filesystem view fixed PID 1 root link reopen failed",
                    |fd| proc_member(fd, proc, libc::S_IFLNK),
                )?;
                require_identity(
                    original,
                    again,
                    "Filesystem view fixed PID 1 root link changed",
                )?;
                Ok(result)
            },
        )
    }

    fn pid1_namespace(pid1: libc::c_int, status: &libc::stat) -> ProcIdentityResult<u32> {
        let [namespace] = read_thread_namespace_links(pid1, status, [(b"ns/mnt\0", b"mnt:[")])?;
        Ok(namespace)
    }

    fn require_root_caller() -> ProcIdentityResult<()> {
        if unsafe { libc::getuid() != 0 || libc::geteuid() != 0 } {
            return Err((
                "PID 1 filesystem view requires actual real/effective UID zero",
                libc::EPERM,
            ));
        }
        Ok(())
    }

    pub(crate) fn pid1_view() -> ProcIdentityResult<[u64; 5]> {
        require_root_caller()?;
        let result = with_proc(|proc, status, proc_mount| {
            with_fd(
                pid1_proc_directory(proc),
                "Filesystem view fixed PID 1 directory open failed",
                |pid1| {
                    let original = proc_member(pid1, proc_mount, libc::S_IFDIR)?;
                    let start = read_proc_start_ticks(pid1, status, b"stat\0", FIXED_PROC, 1)?;
                    let current_namespace_first = current_namespace(proc, status)?;
                    let pid1_namespace_first = pid1_namespace(pid1, status)?;
                    let view = with_current_root(|current| {
                        with_pid1_root(pid1, proc_mount, |peer| {
                            let first = View {
                                namespace: current_namespace_first,
                                root: root_identity(current)?,
                            };
                            let peer_first = View {
                                namespace: pid1_namespace_first,
                                root: root_identity(peer)?,
                            };
                            require_same_view(first, peer_first).map_err(|error| {
                                ("Current Linux filesystem view differs from PID 1", error)
                            })?;
                            let current_again = with_current_root(root_identity)?;
                            let peer_again = with_pid1_root(pid1, proc_mount, root_identity)?;
                            let own_after = View {
                                namespace: current_namespace(proc, status)?,
                                root: current_again,
                            };
                            let peer_after = View {
                                namespace: pid1_namespace(pid1, status)?,
                                root: peer_again,
                            };
                            for after in [own_after, peer_after] {
                                require_same_view(first, after)
                                    .map_err(|error| ("PID 1 filesystem view changed", error))?;
                            }
                            require_identity(
                                first.root,
                                root_identity(current)?,
                                "Held current root changed",
                            )?;
                            require_identity(
                                first.root,
                                root_identity(peer)?,
                                "Held PID 1 root changed",
                            )?;
                            Ok(first)
                        })
                    })?;
                    if read_proc_start_ticks(pid1, status, b"stat\0", FIXED_PROC, 1)? != start {
                        return Err(("Filesystem view PID 1 birth changed", libc::ESTALE));
                    }
                    require_identity(
                        original,
                        proc_member(pid1, proc_mount, libc::S_IFDIR)?,
                        "Filesystem view held PID 1 directory changed",
                    )?;
                    let again = with_fd(
                        pid1_proc_directory(proc),
                        "Filesystem view fixed PID 1 directory reopen failed",
                        |fd| proc_member(fd, proc_mount, libc::S_IFDIR),
                    )?;
                    require_identity(
                        original,
                        again,
                        "Filesystem view fixed PID 1 directory changed",
                    )?;
                    let [namespace, device, inode, mount] = view_values(view);
                    Ok([namespace, device, inode, mount, start])
                },
            )
        })?;
        require_root_caller()?;
        Ok(result)
    }
}

#[cfg(target_os = "linux")]
pub(crate) use linux::{current_view, pid1_view};

#[cfg(test)]
mod tests {
    use super::*;

    const ORIGINAL: View = View {
        namespace: 0xffff_ffff,
        root: MountIdentity {
            device: u64::MAX,
            inode: 0,
            mount_id: 17,
        },
    };

    #[test]
    fn view_comparison_requires_namespace_and_every_held_root_field() {
        assert_eq!(require_same_view(ORIGINAL, ORIGINAL), Ok(()));
        for changed in [
            View {
                namespace: 19,
                ..ORIGINAL
            },
            View {
                root: MountIdentity {
                    device: 1,
                    ..ORIGINAL.root
                },
                ..ORIGINAL
            },
            View {
                root: MountIdentity {
                    inode: 2,
                    ..ORIGINAL.root
                },
                ..ORIGINAL
            },
            View {
                root: MountIdentity {
                    mount_id: 18,
                    ..ORIGINAL.root
                },
                ..ORIGINAL
            },
        ] {
            assert_eq!(require_same_view(ORIGINAL, changed), Err(libc::ESTALE));
        }
    }

    #[cfg(target_os = "linux")]
    #[test]
    #[ignore = "requires approved Linux root and a prepared BOOKKEEPER_TEST_CHROOT with authentic procfs"]
    fn rejects_another_namespace_and_chroot_in_the_same_namespace() {
        use std::ffi::CString;
        use std::os::unix::ffi::OsStrExt;

        let original = current_view().unwrap();
        assert_eq!(&pid1_view().unwrap()[..4], &original);
        let jail =
            std::fs::canonicalize(std::env::var_os("BOOKKEEPER_TEST_CHROOT").unwrap()).unwrap();
        assert_ne!(jail, std::path::Path::new("/"));
        let jail = CString::new(jail.as_os_str().as_bytes()).unwrap();
        std::thread::spawn(move || {
            // Change only this fixture thread's root. The prepared jail exposes the same authentic procfs.
            assert_eq!(
                unsafe { libc::unshare(libc::CLONE_FS) },
                0,
                "{}",
                std::io::Error::last_os_error()
            );
            assert_eq!(
                unsafe { libc::chroot(jail.as_ptr()) },
                0,
                "{}",
                std::io::Error::last_os_error()
            );
            assert_eq!(unsafe { libc::chdir(b"/\0".as_ptr().cast()) }, 0);
            let changed = current_view().unwrap();
            assert_eq!(changed[0], original[0]);
            assert_ne!(&changed[1..3], &original[1..3]);
            assert_eq!(pid1_view().unwrap_err().1, libc::ESTALE);
        })
        .join()
        .unwrap();
        std::thread::spawn(move || {
            // A separate namespace is sufficient to differ without modifying any mounts.
            assert_eq!(
                unsafe { libc::unshare(libc::CLONE_NEWNS) },
                0,
                "{}",
                std::io::Error::last_os_error()
            );
            let changed = current_view().unwrap();
            assert_ne!(changed[0], original[0]);
            assert_eq!(&changed[1..3], &original[1..3]);
            assert_eq!(pid1_view().unwrap_err().1, libc::ESTALE);
        })
        .join()
        .unwrap();
        assert_eq!(current_view().unwrap(), original);
    }
}
