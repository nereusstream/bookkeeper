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

use super::{mount_id, mountinfo_number, MountDirectory};

pub(crate) fn canonical_root(path: &[u8]) -> bool {
    path.len() <= 4095 && std::str::from_utf8(path).is_ok() && absolute_components(path)
}

fn canonical_member(path: &[u8]) -> bool {
    path.len() <= 4095 && std::str::from_utf8(path).is_ok() && canonical_components(path)
}

fn absolute_components(path: &[u8]) -> bool {
    path == b"/" || path.strip_prefix(b"/").is_some_and(canonical_components)
}

fn canonical_components(path: &[u8]) -> bool {
    !path.contains(&0)
        && path
            .split(|byte| *byte == b'/')
            .all(|name| !name.is_empty() && name != b"." && name != b"..")
}

fn mount_path(field: &[u8]) -> Result<Vec<u8>, libc::c_int> {
    let mut path = Vec::with_capacity(field.len());
    let mut remaining = field;
    while let Some((&byte, tail)) = remaining.split_first() {
        if byte != b'\\' {
            if byte == b'\t' {
                return Err(libc::EINVAL);
            }
            path.push(byte);
            remaining = tail;
            continue;
        }
        path.push(match tail.get(..3) {
            Some(b"040") => b' ',
            Some(b"011") => b'\t',
            Some(b"012") => b'\n',
            Some(b"134") => b'\\',
            _ => return Err(libc::EINVAL),
        });
        remaining = &tail[3..];
    }
    if !absolute_components(&path) {
        return Err(libc::EINVAL);
    }
    Ok(path)
}

fn contains_path(directory: &[u8], path: &[u8]) -> bool {
    directory == path
        || directory == b"/"
        || path
            .strip_prefix(directory)
            .is_some_and(|tail| tail.starts_with(b"/"))
}

#[derive(Debug, PartialEq, Eq)]
struct SelectedMount {
    parent: u32,
    point: Vec<u8>,
}

struct Ext4Table<'a> {
    held: MountDirectory,
    root: &'a [u8],
    selected: Option<SelectedMount>,
    ancestors: Vec<Vec<u8>>,
}

impl<'a> Ext4Table<'a> {
    fn new(held: MountDirectory, root: &'a [u8]) -> Self {
        Self {
            held,
            root,
            selected: None,
            ancestors: Vec::new(),
        }
    }

    fn record(&mut self, line: &[u8]) -> Result<(), libc::c_int> {
        let mut fields = line.split(|byte| *byte == b' ');
        let mut field = || {
            fields
                .next()
                .filter(|token| !token.is_empty())
                .ok_or(libc::EINVAL)
        };
        let id = mount_id(field()?)?;
        let parent = mount_id(field()?)?;
        let mut device = field()?.split(|byte| *byte == b':');
        let major = mountinfo_number(device.next().ok_or(libc::EINVAL)?)?;
        let minor = mountinfo_number(device.next().ok_or(libc::EINVAL)?)?;
        if device.next().is_some() {
            return Err(libc::EINVAL);
        }
        let root = mount_path(field()?)?;
        let point = mount_path(field()?)?;
        field()?; // per-mount options
        let mut idmapped = false;
        loop {
            let tag = field()?;
            if tag == b"-" {
                break;
            }
            let name = tag.split(|byte| *byte == b':').next();
            idmapped |= matches!(name, Some(b"idmapped" | b"idmap"));
        }
        let kind = field()?;
        field()?; // source
        field()?; // superblock options
        if fields.next().is_some() || line.contains(&0) {
            return Err(libc::EINVAL);
        }
        let same_device = (major, minor) == (self.held.major, self.held.minor);
        if id == self.held.mount_id {
            if !same_device
                || root != b"/"
                || kind != b"ext4"
                || idmapped
                || !contains_path(&point, self.root)
                || self.selected.is_some()
            {
                return Err(libc::EINVAL);
            }
            self.selected = Some(SelectedMount { parent, point });
        } else {
            if same_device || contains_path(self.root, &point) {
                return Err(libc::EXDEV);
            }
            if contains_path(&point, self.root) {
                self.ancestors.push(point);
            }
        }
        Ok(())
    }

    fn finish(self) -> Result<SelectedMount, libc::c_int> {
        let selected = self.selected.ok_or(libc::ENOENT)?;
        if self.ancestors.contains(&selected.point) {
            return Err(libc::EXDEV);
        }
        Ok(selected)
    }
}

#[cfg(target_os = "linux")]
mod linux {
    use super::{canonical_member, canonical_root, Ext4Table, MountDirectory};
    use crate::cgroup_mount::{
        mount_statx, query_statx, read_mountinfo, require_current_directory, require_mount_statx,
    };
    use crate::{
        close_identity_fd, close_proc_identity_fd, proc_identity_errno, proc_identity_root,
        read_thread_namespace_links, ProcIdentityResult,
    };
    use std::ffi::CString;

    pub(crate) struct Observation {
        pub(crate) mount_id: u32,
        pub(crate) parent_id: u32,
        pub(crate) namespace: u32,
        pub(crate) point: String,
    }

    pub(crate) struct MemberObservation {
        pub(crate) device: libc::dev_t,
        pub(crate) inode: u64,
        pub(crate) mount_id: u64,
        pub(crate) directory: bool,
    }

    fn held_directory(
        fd: libc::c_int,
        original: &libc::stat,
    ) -> ProcIdentityResult<MountDirectory> {
        if original.st_mode & libc::S_IFMT != libc::S_IFDIR {
            return Err(("Original retained root is not a directory", libc::ENOTDIR));
        }
        let held = query_statx(fd, b"\0", libc::AT_EMPTY_PATH)?;
        let (major, minor) =
            unsafe { (libc::major(original.st_dev), libc::minor(original.st_dev)) };
        if (held.major, held.minor, held.inode) != (major, minor, original.st_ino) {
            return Err((
                "Retained root statx differs from original fstat",
                libc::ESTALE,
            ));
        }
        Ok(held)
    }

    pub(crate) fn observe_member(
        fd: libc::c_int,
        original: &libc::stat,
        relative: &[u8],
    ) -> ProcIdentityResult<MemberObservation> {
        if !canonical_member(relative) {
            return Err(("Invalid canonical UTF-8 retained member path", libc::EINVAL));
        }
        let path =
            CString::new(relative).map_err(|_| ("Invalid retained member path", libc::EINVAL))?;
        let held = held_directory(fd, original)?;
        // O_PATH does not open the member for I/O or release POSIX locks when its fd closes.
        let how = [
            (libc::O_PATH | libc::O_CLOEXEC) as u64,
            0,
            libc::RESOLVE_BENEATH
                | libc::RESOLVE_NO_XDEV
                | libc::RESOLVE_NO_SYMLINKS
                | libc::RESOLVE_NO_MAGICLINKS,
        ];
        let member = unsafe {
            libc::syscall(
                libc::SYS_openat2,
                fd,
                path.as_ptr(),
                how.as_ptr(),
                std::mem::size_of_val(&how),
            )
        };
        if member < 0 {
            return Err(proc_identity_errno(
                "Retained member same-mount openat2 failed",
            ));
        }
        let member = member as libc::c_int;
        let observed = (|| {
            let current = mount_statx(member, b"\0", libc::AT_EMPTY_PATH)?;
            require_mount_statx(&current)?;
            if (
                current.stx_dev_major,
                current.stx_dev_minor,
                current.stx_mnt_id,
            ) != (held.major, held.minor, u64::from(held.mount_id))
            {
                return Err(("Retained member crossed the held root mount", libc::EXDEV));
            }
            let kind = current.stx_mode as libc::mode_t & libc::S_IFMT;
            if kind != libc::S_IFREG && kind != libc::S_IFDIR {
                return Err((
                    "Retained member is not a regular file or directory",
                    libc::EINVAL,
                ));
            }
            Ok(MemberObservation {
                device: original.st_dev,
                inode: current.stx_ino,
                mount_id: current.stx_mnt_id,
                directory: kind == libc::S_IFDIR,
            })
        })();
        close_identity_fd(member, observed, "Retained member observation close failed")
    }

    fn require_root_path(path: &CString, held: MountDirectory) -> ProcIdentityResult<()> {
        let how = [
            (libc::O_PATH | libc::O_DIRECTORY | libc::O_CLOEXEC) as u64,
            0,
            0x04 | 0x02, // NO_SYMLINKS | NO_MAGICLINKS. Ancestors may cross into the ext4 mount.
        ];
        let fd = unsafe {
            libc::syscall(
                libc::SYS_openat2,
                libc::AT_FDCWD,
                path.as_ptr(),
                how.as_ptr(),
                std::mem::size_of_val(&how),
            )
        };
        if fd < 0 {
            return Err(proc_identity_errno("ext4 root path openat2 failed"));
        }
        let fd = fd as libc::c_int;
        let observed = query_statx(fd, b"\0", libc::AT_EMPTY_PATH).and_then(|current| {
            require_current_directory(held, current).map_err(|error| {
                (
                    "ext4 root path differs from the original mount or directory",
                    error,
                )
            })
        });
        close_identity_fd(fd, observed, "ext4 root path observation close failed")
    }

    pub(crate) fn observe(
        fd: libc::c_int,
        original: &libc::stat,
        root: &[u8],
    ) -> ProcIdentityResult<Observation> {
        if !canonical_root(root) {
            return Err(("Invalid canonical UTF-8 ext4 root path", libc::EINVAL));
        }
        let path = CString::new(root).map_err(|_| ("Invalid ext4 root path", libc::EINVAL))?;
        let held = held_directory(fd, original)?;
        let mut fs = std::mem::MaybeUninit::<libc::statfs>::uninit();
        if unsafe { libc::fstatfs(fd, fs.as_mut_ptr()) } != 0 {
            return Err(proc_identity_errno(
                "Original ext4 directory fstatfs failed",
            ));
        }
        if unsafe { fs.assume_init() }.f_type != 0xef53 {
            return Err((
                "Original directory does not have ext filesystem magic",
                libc::EINVAL,
            ));
        }
        let proc_root = unsafe {
            libc::open(
                b"/proc\0".as_ptr().cast::<libc::c_char>(),
                libc::O_RDONLY | libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC,
            )
        };
        if proc_root < 0 {
            return Err(proc_identity_errno(
                "ext4 mount fixed proc root open failed",
            ));
        }
        const LINKS: [(&[u8], &[u8]); 1] = [(b"thread-self/ns/mnt\0", b"mnt:[")];
        let observed = (|| {
            let proc_stat = proc_identity_root(proc_root)?;
            let [namespace] = read_thread_namespace_links(proc_root, &proc_stat, LINKS)?;
            require_root_path(&path, held)?;
            let mut table = Ext4Table::new(held, root);
            read_mountinfo(proc_root, &proc_stat, |line| table.record(line))?;
            let selected = table
                .finish()
                .map_err(|error| ("Missing or stacked ext4 mount", error))?;
            let after = query_statx(fd, b"\0", libc::AT_EMPTY_PATH)?;
            require_current_directory(held, after)
                .map_err(|error| ("Original ext4 directory mount changed", error))?;
            require_root_path(&path, held)?;
            let [namespace_after] = read_thread_namespace_links(proc_root, &proc_stat, LINKS)?;
            let proc_after = proc_identity_root(proc_root)?;
            if namespace != namespace_after
                || (proc_stat.st_dev, proc_stat.st_ino) != (proc_after.st_dev, proc_after.st_ino)
            {
                return Err((
                    "ext4 mount namespace or held procfs root changed",
                    libc::EAGAIN,
                ));
            }
            // An ancestor of the validated UTF-8 root can only end at a component boundary.
            let point = String::from_utf8(selected.point)
                .map_err(|_| ("Invalid ext4 mount point encoding", libc::EINVAL))?;
            Ok(Observation {
                mount_id: held.mount_id,
                parent_id: selected.parent,
                namespace,
                point,
            })
        })();
        close_proc_identity_fd(proc_root, observed)
    }
}

#[cfg(target_os = "linux")]
pub(crate) use linux::{observe, observe_member};

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cgroup_mount::{MountInfoLines, MAX_MOUNTINFO_BYTES, MAX_MOUNTINFO_LINE};

    const HELD: MountDirectory = MountDirectory {
        mount_id: 29,
        major: 8,
        minor: 1,
        inode: 1234,
    };
    const ROW: &[u8] = b"29 23 8:1 / /data/a rw future:tag - ext4 /dev/example rw\n";

    fn scan(root: &[u8], input: &[&[u8]]) -> Result<SelectedMount, libc::c_int> {
        let mut table = Ext4Table::new(HELD, root);
        let mut lines = MountInfoLines::default();
        for piece in input {
            for chunk in piece.chunks(7) {
                lines.push(chunk, |line| table.record(line))?;
            }
        }
        lines.finish()?;
        table.finish()
    }

    #[test]
    fn single_attachment_with_root_at_or_inside_mount_and_component_boundary() {
        let unrelated = b"30 23 0:2 / /data/ab rw - tmpfs tmpfs rw\n";
        for root in [b"/data/a".as_slice(), b"/data/a/retained"] {
            assert_eq!(
                scan(root, &[ROW, unrelated]),
                Ok(SelectedMount {
                    parent: 23,
                    point: b"/data/a".to_vec()
                })
            );
        }
        assert_eq!(scan(b"/data/ab", &[ROW, unrelated]), Err(libc::EINVAL));
    }

    #[test]
    fn rejects_same_device_aliases_everywhere_and_different_device_mounts_under_root() {
        for other in [
            b"30 23 8:1 / /elsewhere rw - ext4 /dev/example rw\n".as_slice(),
            b"30 23 8:1 /retained /elsewhere rw - ext4 /dev/example rw\n",
            b"30 29 8:1 /retained /data/a/retained/nested rw - ext4 /dev/example rw\n",
            b"30 29 8:1 / /data/a/retained rw - ext4 /dev/example rw\n",
            b"30 29 8:1 /retained /elsewhere rw - other other rw\n",
            b"30 29 0:3 / /data/a/retained/nested rw - tmpfs tmpfs rw\n",
            b"30 29 0:3 / /data/a/retained rw - tmpfs tmpfs rw\n",
            b"30 29 0:3 / /data/a rw - tmpfs tmpfs rw\n",
        ] {
            assert_eq!(scan(b"/data/a/retained", &[ROW, other]), Err(libc::EXDEV));
            assert_eq!(scan(b"/data/a/retained", &[other, ROW]), Err(libc::EXDEV));
        }
    }

    #[test]
    fn decodes_kernel_escapes_as_bytes_without_changing_unrelated_non_utf8() {
        let row = b"29 23 8:1 / /data/a\\040b\\011c\\012d\\134e rw - ext4 /dev/example rw\n";
        let other =
            b"30 23 0:2 /bad\\012path /data/a\\040b\\011c\\012d\\134ef\xff rw - tmpfs tmpfs rw\n";
        assert_eq!(
            scan(b"/data/a b\tc\nd\\e/retained", &[other, row]),
            Ok(SelectedMount {
                parent: 23,
                point: b"/data/a b\tc\nd\\e".to_vec(),
            })
        );
        assert_eq!(
            scan(
                b"/data/a b\tc\nd\\e",
                &[
                    row,
                    b"30 29 0:2 / /data/a\\040b\\011c\\012d\\134e/\xff rw - tmpfs tmpfs rw\n",
                ]
            ),
            Err(libc::EXDEV)
        );
    }

    #[test]
    fn requires_complete_well_formed_table_and_one_matching_record() {
        assert_eq!(scan(b"/data/a", &[]), Err(libc::ENOENT));
        assert_eq!(scan(b"/data/a", &[ROW, ROW]), Err(libc::EINVAL));
        assert_eq!(
            scan(b"/data/a", &[ROW, &ROW[..ROW.len() - 1]]),
            Err(libc::EINVAL)
        );
        for wrong in [
            b"29 23 8:2 / /data/a rw - ext4 /dev/example rw\n".as_slice(),
            b"29 23 8:1 /subtree /data/a rw - ext4 /dev/example rw\n",
            b"29 23 8:1 / /data/a rw - ext3 /dev/example rw\n",
            b"29 23 8:1 / /data/a rw idmapped - ext4 /dev/example rw\n",
            b"30 23 0:2 / /other\\057dir rw - tmpfs tmpfs rw\n",
            b"30 23 0:2 / /other rw missing-separator tmpfs tmpfs rw\n",
            b"30 23 0:2:1 / /other rw - tmpfs tmpfs rw\n",
            b"30 23 0:2 / /other rw - tmpfs tmpfs\n",
            b"30 23 0:2 / /other rw - tmpfs tmpfs rw extra\n",
        ] {
            assert_eq!(scan(b"/data/a", &[wrong]), Err(libc::EINVAL));
        }
    }

    #[test]
    fn selected_record_does_not_waive_either_size_limit() {
        let too_long = vec![b'x'; MAX_MOUNTINFO_LINE + 1];
        assert_eq!(scan(b"/data/a", &[ROW, &too_long]), Err(libc::EOVERFLOW));
        let unrelated = b"30 23 0:2 / /other rw - tmpfs tmpfs rw\n";
        let oversized = unrelated.repeat(MAX_MOUNTINFO_BYTES / unrelated.len() + 1);
        assert_eq!(scan(b"/data/a", &[ROW, &oversized]), Err(libc::EOVERFLOW));
    }

    #[test]
    fn jni_root_input_must_be_bounded_absolute_canonical_utf8() {
        for valid in [b"/".as_slice(), b"/a/b", "/有空 格/é".as_bytes()] {
            assert!(canonical_root(valid));
        }
        for invalid in [
            b"".as_slice(),
            b"data/a",
            b"/a/",
            b"//a",
            b"/a/../b",
            b"/./a",
            b"/a\0b",
            b"/a\xff",
        ] {
            assert!(!canonical_root(invalid));
        }
        let long = format!("/{}", "a".repeat(4095));
        assert!(!canonical_root(long.as_bytes()));
    }

    #[test]
    fn member_input_must_be_bounded_canonical_relative_utf8() {
        assert!(canonical_member("current/旧 file".as_bytes()));
        for invalid in [
            b"".as_slice(),
            b"/current",
            b"current/",
            b"current//file",
            b"./file",
            b"../file",
            b"current/../file",
            b"current/.",
            b"a\0b",
            b"a\xff",
        ] {
            assert!(!canonical_member(invalid));
        }
        assert!(canonical_member(&[b'a'; 4095]));
        assert!(!canonical_member(&[b'a'; 4096]));
    }

    #[cfg(target_os = "linux")]
    #[test]
    #[ignore = "requires CAP_SYS_ADMIN and BOOKKEEPER_TEST_EXT4_SCRATCH on an approved ext4 fixture"]
    fn self_bind_preserves_dev_and_inode_but_member_resolution_rejects_it() {
        use std::ffi::CString;
        use std::fs::{self, File};
        use std::os::{fd::AsRawFd, unix::ffi::OsStrExt, unix::fs::MetadataExt};
        use std::ptr;

        let scratch =
            fs::canonicalize(std::env::var_os("BOOKKEEPER_TEST_EXT4_SCRATCH").unwrap()).unwrap();
        let root = scratch.join(format!("bk-ext4-member-test-{}", std::process::id()));
        fs::create_dir(&root).unwrap();
        let run = std::thread::scope(|scope| {
            scope
                .spawn(|| {
                    // Only the isolated test thread's mount namespace changes. Disable outward propagation first.
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
                    let child = root.join("current");
                    fs::create_dir(&child).unwrap();
                    let original = File::open(&root).unwrap();
                    let fd = original.as_raw_fd();
                    let mut status = std::mem::MaybeUninit::<libc::stat>::uninit();
                    assert_eq!(unsafe { libc::fstat(fd, status.as_mut_ptr()) }, 0);
                    let status = unsafe { status.assume_init() };
                    let initial = observe(fd, &status, root.as_os_str().as_bytes()).unwrap();
                    let before = observe_member(fd, &status, b"current").unwrap();
                    let path = CString::new(child.as_os_str().as_bytes()).unwrap();
                    assert_eq!(
                        unsafe {
                            libc::mount(
                                path.as_ptr(),
                                path.as_ptr(),
                                ptr::null(),
                                libc::MS_BIND,
                                ptr::null(),
                            )
                        },
                        0,
                        "{}",
                        std::io::Error::last_os_error()
                    );
                    let mounted = fs::metadata(&child).unwrap();
                    assert_eq!(
                        (mounted.dev(), mounted.ino()),
                        (before.device, before.inode)
                    );
                    let mounted_id = crate::cgroup_mount::query_statx(
                        libc::AT_FDCWD,
                        path.as_bytes_with_nul(),
                        libc::AT_SYMLINK_NOFOLLOW,
                    )
                    .unwrap()
                    .mount_id;
                    assert_ne!(u64::from(mounted_id), before.mount_id);
                    assert_eq!(
                        observe_member(fd, &status, b"current").err().unwrap().1,
                        libc::EXDEV
                    );
                    assert_eq!(
                        observe(fd, &status, root.as_os_str().as_bytes())
                            .err()
                            .unwrap()
                            .1,
                        libc::EXDEV
                    );
                    assert_eq!(
                        unsafe { libc::umount2(path.as_ptr(), 0) },
                        0,
                        "{}",
                        std::io::Error::last_os_error()
                    );
                    let after = observe_member(fd, &status, b"current").unwrap();
                    assert_eq!(after.mount_id, u64::from(initial.mount_id));
                })
                .join()
        });
        fs::remove_dir_all(root).unwrap();
        run.unwrap();
    }
}
