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

use crate::proc_unsigned_decimal;
use std::collections::HashSet;

const MAX_EVENTS_BYTES: usize = 4096;
const MAX_EVENT_LINE_BYTES: usize = 256;

fn parse_events(bytes: &[u8]) -> Result<bool, libc::c_int> {
    if bytes.len() > MAX_EVENTS_BYTES {
        return Err(libc::EOVERFLOW);
    }
    let body = bytes.strip_suffix(b"\n").ok_or(libc::EINVAL)?;
    let mut keys = HashSet::new();
    let mut populated = None;
    for line in body.split(|byte| *byte == b'\n') {
        if line.len() > MAX_EVENT_LINE_BYTES {
            return Err(libc::EOVERFLOW);
        }
        let space = line
            .iter()
            .position(|byte| *byte == b' ')
            .ok_or(libc::EINVAL)?;
        let key = &line[..space];
        let value = &line[space + 1..];
        if !key.first().is_some_and(u8::is_ascii_lowercase)
            || !key
                .iter()
                .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || *byte == b'_')
            || !keys.insert(key)
            || value.len() > 1 && value.starts_with(b"0")
        {
            return Err(libc::EINVAL);
        }
        let number = proc_unsigned_decimal(value)?;
        if key == b"populated" || key == b"frozen" {
            let set = match number {
                0 => false,
                1 => true,
                _ => return Err(libc::EINVAL),
            };
            if key == b"populated" {
                populated = Some(set);
            }
        }
    }
    populated.ok_or(libc::EINVAL)
}

#[cfg(target_os = "linux")]
mod linux {
    use super::{parse_events, MAX_EVENTS_BYTES};
    use crate::cgroup_mount::{mount_statx, query_statx, require_mount_statx, MountDirectory};
    use crate::{close_identity_fd, proc_identity_errno, ProcIdentityResult};

    fn require_cgroup2(fd: libc::c_int) -> ProcIdentityResult<()> {
        let mut fs = std::mem::MaybeUninit::<libc::statfs>::uninit();
        if unsafe { libc::fstatfs(fd, fs.as_mut_ptr()) } != 0 {
            return Err(proc_identity_errno("cgroup.events fstatfs failed"));
        }
        if unsafe { fs.assume_init() }.f_type != libc::CGROUP2_SUPER_MAGIC {
            return Err((
                "cgroup.events requires an actual cgroup2 filesystem",
                libc::EINVAL,
            ));
        }
        Ok(())
    }

    fn fstat(fd: libc::c_int) -> ProcIdentityResult<libc::stat> {
        let mut stat = std::mem::MaybeUninit::<libc::stat>::uninit();
        if unsafe { libc::fstat(fd, stat.as_mut_ptr()) } != 0 {
            return Err(proc_identity_errno("cgroup.events fstat failed"));
        }
        Ok(unsafe { stat.assume_init() })
    }

    fn directory_identity(
        fd: libc::c_int,
        original: &libc::stat,
    ) -> ProcIdentityResult<MountDirectory> {
        if original.st_mode & libc::S_IFMT != libc::S_IFDIR {
            return Err((
                "cgroup.events requires an original directory",
                libc::ENOTDIR,
            ));
        }
        require_cgroup2(fd)?;
        let facts = query_statx(fd, b"\0", libc::AT_EMPTY_PATH)?;
        let (major, minor) =
            unsafe { (libc::major(original.st_dev), libc::minor(original.st_dev)) };
        if (facts.major, facts.minor, facts.inode) != (major, minor, original.st_ino) {
            return Err((
                "cgroup.events directory differs from the original fstat",
                libc::ESTALE,
            ));
        }
        Ok(facts)
    }

    fn with_events<T>(
        directory: libc::c_int,
        read: impl FnOnce(libc::c_int) -> ProcIdentityResult<T>,
    ) -> ProcIdentityResult<T> {
        let how = [
            (libc::O_RDONLY | libc::O_NONBLOCK | libc::O_CLOEXEC | libc::O_NOFOLLOW) as u64,
            0,
            libc::RESOLVE_BENEATH
                | libc::RESOLVE_NO_XDEV
                | libc::RESOLVE_NO_SYMLINKS
                | libc::RESOLVE_NO_MAGICLINKS,
        ];
        let leaf = unsafe {
            libc::syscall(
                libc::SYS_openat2,
                directory,
                b"cgroup.events\0".as_ptr().cast::<libc::c_char>(),
                how.as_ptr(),
                std::mem::size_of_val(&how),
            )
        };
        if leaf < 0 {
            return Err(proc_identity_errno("Fixed cgroup.events openat2 failed"));
        }
        let leaf = leaf as libc::c_int;
        close_identity_fd(leaf, read(leaf), "cgroup.events temporary fd close failed")
    }

    fn event_identity(fd: libc::c_int, directory: MountDirectory) -> ProcIdentityResult<u64> {
        require_cgroup2(fd)?;
        let file = fstat(fd)?;
        let observed = mount_statx(fd, b"\0", libc::AT_EMPTY_PATH)?;
        require_mount_statx(&observed)?;
        let (major, minor) = unsafe { (libc::major(file.st_dev), libc::minor(file.st_dev)) };
        if file.st_mode & libc::S_IFMT != libc::S_IFREG
            || observed.stx_mode as libc::mode_t & libc::S_IFMT != libc::S_IFREG
            || (
                observed.stx_dev_major,
                observed.stx_dev_minor,
                observed.stx_ino,
            ) != (major, minor, file.st_ino)
            || (major, minor, observed.stx_mnt_id)
                != (
                    directory.major,
                    directory.minor,
                    u64::from(directory.mount_id),
                )
        {
            return Err((
                "Fixed cgroup.events leaf differs from its held cgroup2 directory",
                libc::ESTALE,
            ));
        }
        Ok(file.st_ino)
    }

    fn read_events(fd: libc::c_int) -> ProcIdentityResult<bool> {
        let mut bytes = [0_u8; MAX_EVENTS_BYTES + 1];
        let mut used = 0;
        loop {
            let count =
                unsafe { libc::read(fd, bytes[used..].as_mut_ptr().cast(), bytes.len() - used) };
            if count < 0 {
                return Err(proc_identity_errno("cgroup.events bounded read failed"));
            }
            if count == 0 {
                break;
            }
            used += count as usize;
            if used > MAX_EVENTS_BYTES {
                return Err(("cgroup.events exceeds the read bound", libc::EOVERFLOW));
            }
        }
        parse_events(&bytes[..used])
            .map_err(|error| ("Invalid or incomplete cgroup.events data", error))
    }

    pub(crate) fn observe(fd: libc::c_int, original: &libc::stat) -> ProcIdentityResult<[u64; 4]> {
        let directory = directory_identity(fd, original)?;
        let populated = with_events(fd, |leaf| {
            let before = event_identity(leaf, directory)?;
            let sample = read_events(leaf)?;
            if before != event_identity(leaf, directory)?
                || before != with_events(fd, |current| event_identity(current, directory))?
            {
                return Err((
                    "Fixed cgroup.events leaf changed during observation",
                    libc::ESTALE,
                ));
            }
            if directory != directory_identity(fd, &fstat(fd)?)? {
                return Err((
                    "Original cgroup.events directory changed during observation",
                    libc::ESTALE,
                ));
            }
            Ok(sample)
        })?;
        Ok([
            original.st_dev,
            original.st_ino,
            u64::from(directory.mount_id),
            u64::from(populated),
        ])
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        use std::fs::{self, File};
        use std::os::fd::AsRawFd;
        use std::os::unix::fs::symlink;
        use std::path::{Path, PathBuf};
        use std::process::{Child, Command, Stdio};

        fn sample(directory: &File) -> ProcIdentityResult<[u64; 4]> {
            let fd = directory.as_raw_fd();
            observe(fd, &fstat(fd)?)
        }

        #[test]
        fn ordinary_filesystem_cannot_supply_events_and_fixed_lookup_rejects_symlinks() {
            let root =
                std::env::temp_dir().join(format!("bk-events-symlink-{}", std::process::id()));
            fs::create_dir(&root).unwrap();
            fs::write(root.join("forged"), b"populated 0\nfrozen 0\n").unwrap();
            symlink("forged", root.join("cgroup.events")).unwrap();
            let directory = File::open(&root).unwrap();
            let wrong_fs = sample(&directory);
            let forbidden = with_events(directory.as_raw_fd(), |_| Ok(()));
            drop(directory);
            fs::remove_dir_all(root).unwrap();
            assert_eq!(wrong_fs.unwrap_err().1, libc::EINVAL);
            assert_eq!(forbidden.unwrap_err().1, libc::ELOOP);
        }

        struct Fixture(PathBuf);

        impl Fixture {
            fn create(suffix: &str) -> Self {
                let parent =
                    fs::canonicalize(std::env::var_os("BOOKKEEPER_TEST_CGROUP_PARENT").unwrap())
                        .unwrap();
                let root = parent.join(format!("bk-events-{}-{suffix}", std::process::id()));
                fs::create_dir(&root).unwrap();
                let fixture = Self(root);
                fs::create_dir(fixture.descendant()).unwrap();
                fixture
            }

            fn descendant(&self) -> PathBuf {
                self.0.join("descendant")
            }
        }

        impl Drop for Fixture {
            fn drop(&mut self) {
                for path in [self.descendant(), self.0.clone()] {
                    if let Err(error) = fs::remove_dir(&path) {
                        if error.kind() != std::io::ErrorKind::NotFound {
                            eprintln!("Could not remove test cgroup {}: {error}", path.display());
                        }
                    }
                }
            }
        }

        struct ChildWithInput(Child);

        impl Drop for ChildWithInput {
            fn drop(&mut self) {
                drop(self.0.stdin.take());
                if !matches!(self.0.try_wait(), Ok(Some(_))) {
                    let _ = self.0.kill();
                    let _ = self.0.wait();
                }
            }
        }

        #[test]
        #[ignore = "requires an approved writable test-only BOOKKEEPER_TEST_CGROUP_PARENT"]
        fn fresh_samples_observe_descendant_exit_and_reject_a_replaced_directory() {
            let fixture = Fixture::create("population");
            let original = File::open(&fixture.0).unwrap();
            let empty = sample(&original).unwrap();
            assert_eq!(empty[3], 0);
            let mut child = ChildWithInput(
                Command::new("/bin/cat")
                    .stdin(Stdio::piped())
                    .stdout(Stdio::null())
                    .spawn()
                    .unwrap(),
            );
            fs::write(
                fixture.descendant().join("cgroup.procs"),
                format!("{}\n", child.0.id()),
            )
            .unwrap();
            assert!(child.0.try_wait().unwrap().is_none());
            let populated = sample(&original).unwrap();
            assert_eq!(&populated[..3], &empty[..3]);
            assert_eq!(populated[3], 1);
            drop(child.0.stdin.take());
            assert!(child.0.wait().unwrap().success());
            assert_eq!(sample(&original).unwrap(), empty);
            fs::remove_dir(fixture.descendant()).unwrap();
            fs::remove_dir(&fixture.0).unwrap();
            fs::create_dir(&fixture.0).unwrap();
            assert!(sample(&original).is_err());
            let replacement = sample(&File::open(&fixture.0).unwrap()).unwrap();
            assert_eq!(replacement[3], 0);
            assert_ne!(replacement[1], empty[1]);
        }

        #[test]
        #[ignore = "requires CAP_SYS_ADMIN and an approved test-only BOOKKEEPER_TEST_CGROUP_PARENT"]
        fn same_device_bind_over_the_fixed_events_file_is_rejected() {
            use std::ffi::CString;
            use std::os::unix::ffi::OsStrExt;
            use std::ptr;

            fn c_path(path: &Path) -> CString {
                CString::new(path.as_os_str().as_bytes()).unwrap()
            }

            let fixture = Fixture::create("bind");
            std::thread::scope(|scope| {
                scope
                    .spawn(|| {
                        assert_eq!(
                            unsafe { libc::unshare(libc::CLONE_NEWNS) },
                            0,
                            "{}",
                            std::io::Error::last_os_error()
                        );
                        assert_eq!(
                            unsafe {
                                libc::mount(
                                    ptr::null(),
                                    b"/\0".as_ptr().cast(),
                                    ptr::null(),
                                    libc::MS_REC | libc::MS_PRIVATE,
                                    ptr::null(),
                                )
                            },
                            0,
                            "{}",
                            std::io::Error::last_os_error()
                        );
                        let original = File::open(&fixture.0).unwrap();
                        let before = sample(&original).unwrap();
                        let source = c_path(&fixture.descendant().join("cgroup.events"));
                        let target = c_path(&fixture.0.join("cgroup.events"));
                        assert_eq!(
                            unsafe {
                                libc::mount(
                                    source.as_ptr(),
                                    target.as_ptr(),
                                    ptr::null(),
                                    libc::MS_BIND,
                                    ptr::null(),
                                )
                            },
                            0,
                            "{}",
                            std::io::Error::last_os_error()
                        );
                        assert_eq!(sample(&original).unwrap_err().1, libc::EXDEV);
                        assert_eq!(
                            unsafe { libc::umount2(target.as_ptr(), 0) },
                            0,
                            "{}",
                            std::io::Error::last_os_error()
                        );
                        assert_eq!(sample(&original).unwrap(), before);
                    })
                    .join()
                    .unwrap()
            });
        }
    }
}

#[cfg(target_os = "linux")]
pub(crate) use linux::observe;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reads_one_populated_value_in_either_order_and_validates_other_rows() {
        assert_eq!(parse_events(b"populated 0\n"), Ok(false));
        assert_eq!(parse_events(b"frozen 1\npopulated 0\n"), Ok(false));
        assert_eq!(
            parse_events(b"populated 1\nfuture_2 18446744073709551615\nfrozen 0\n"),
            Ok(true)
        );
    }

    #[test]
    fn missing_duplicate_truncated_or_noncanonical_data_cannot_be_empty() {
        for invalid in [
            b"".as_slice(),
            b"frozen 0\n",
            b"populated 0",
            b"populated 0\n\n",
            b"populated 0\npopulated 0\n",
            b"populated 1\npopulated 0\n",
            b"populated 0\nfrozen 2\n",
            b"populated 2\n",
            b"populated 00\n",
            b"populated -1\n",
            b"populated  0\n",
            b"populated\t0\n",
            b"populated 0\r\n",
            b"populated 0\0\n",
            b"populated 0\nunknown 1 2\n",
            b"populated 0\nunknown 18446744073709551616\n",
            b"populated 0\nunknown 1\nunknown 2\n",
            b"populated 0\n\xff 1\n",
        ] {
            assert_eq!(parse_events(invalid), Err(libc::EINVAL), "{invalid:?}");
        }
    }

    #[test]
    fn enforces_both_bounds_even_after_populated_was_seen() {
        let at_line_limit = format!("populated 0\n{} 1\n", "a".repeat(MAX_EVENT_LINE_BYTES - 2));
        assert_eq!(parse_events(at_line_limit.as_bytes()), Ok(false));
        let long_line = format!("populated 0\n{} 1\n", "a".repeat(MAX_EVENT_LINE_BYTES - 1));
        assert_eq!(parse_events(long_line.as_bytes()), Err(libc::EOVERFLOW));
        let mut at_total_limit = String::from("populated 0\n");
        let mut index = 0;
        while at_total_limit.len() < MAX_EVENTS_BYTES {
            let prefix = format!("f{index:02}");
            let row_len = (MAX_EVENTS_BYTES - at_total_limit.len()).min(200);
            assert!(row_len >= prefix.len() + 3);
            at_total_limit.push_str(&format!(
                "{prefix}{} 1\n",
                "a".repeat(row_len - prefix.len() - 3)
            ));
            index += 1;
        }
        assert_eq!(parse_events(at_total_limit.as_bytes()), Ok(false));
        at_total_limit.push('x');
        assert_eq!(
            parse_events(at_total_limit.as_bytes()),
            Err(libc::EOVERFLOW)
        );
    }
}
