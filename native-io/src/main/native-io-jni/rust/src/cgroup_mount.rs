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

use super::proc_unsigned_decimal;
#[cfg(target_os = "linux")]
use super::{
    close_proc_identity_fd, proc_identity_errno, proc_identity_root, proc_identity_stat,
    read_thread_namespace_links, ProcIdentityResult,
};

mod membership;

const CGROUP_PATH: &[u8] = b"/sys/fs/cgroup";
const MAX_MOUNTINFO_BYTES: usize = 1024 * 1024;
const MAX_MOUNTINFO_LINE: usize = 64 * 1024;
const INITIAL_CGROUP_NAMESPACE: u32 = 0xefff_fffb;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct MountDirectory {
    mount_id: u32,
    major: u32,
    minor: u32,
    inode: u64,
}

fn require_current_directory(
    held: MountDirectory,
    path: MountDirectory,
) -> Result<(), libc::c_int> {
    if held == path {
        Ok(())
    } else {
        Err(libc::ESTALE)
    }
}

fn require_initial_cgroup_namespace(namespaces: [u32; 2]) -> Result<(), libc::c_int> {
    if namespaces[0] == INITIAL_CGROUP_NAMESPACE {
        Ok(())
    } else {
        Err(libc::EINVAL)
    }
}

fn mountinfo_number(token: &[u8]) -> Result<u32, libc::c_int> {
    if token.len() > 1 && token.first() == Some(&b'0') {
        return Err(libc::EINVAL);
    }
    u32::try_from(proc_unsigned_decimal(token)?).map_err(|_| libc::EINVAL)
}

fn mount_id(token: &[u8]) -> Result<u32, libc::c_int> {
    let id = mountinfo_number(token)?;
    if id == 0 || id > i32::MAX as u32 {
        return Err(libc::EINVAL);
    }
    Ok(id)
}

fn selected_mount_parent(
    line: &[u8],
    directory: MountDirectory,
) -> Result<Option<u32>, libc::c_int> {
    let mut fields = line.split(|byte| *byte == b' ');
    let mut field = || {
        fields
            .next()
            .filter(|token| !token.is_empty())
            .ok_or(libc::EINVAL)
    };
    if mount_id(field()?)? != directory.mount_id {
        // Unrelated records can contain kernel-escaped and non-UTF-8 names.
        return Ok(None);
    }
    let parent = mount_id(field()?)?;
    let mut device = field()?.split(|byte| *byte == b':');
    let major = mountinfo_number(device.next().ok_or(libc::EINVAL)?)?;
    let minor = mountinfo_number(device.next().ok_or(libc::EINVAL)?)?;
    if device.next().is_some() || major != directory.major || minor != directory.minor {
        return Err(libc::EINVAL);
    }
    // Neither expected path contains a character that the kernel escapes. An escaped name cannot equal either.
    if field()? != b"/" || field()? != CGROUP_PATH {
        return Err(libc::EINVAL);
    }
    field()?; // per-mount options
    while field()? != b"-" {
        // Optional fields are extensible. The mount is selected by the held fd, not by these tags.
    }
    if field()? != b"cgroup2" {
        return Err(libc::EINVAL);
    }
    field()?; // source
    field()?; // superblock options
    if fields.next().is_some() || line.contains(&0) {
        return Err(libc::EINVAL);
    }
    Ok(Some(parent))
}

struct MountInfo {
    directory: MountDirectory,
    bytes: usize,
    line: Vec<u8>,
    parent: Option<u32>,
}

impl MountInfo {
    fn new(directory: MountDirectory) -> Self {
        Self {
            directory,
            bytes: 0,
            line: Vec::new(),
            parent: None,
        }
    }

    fn push(&mut self, bytes: &[u8]) -> Result<(), libc::c_int> {
        if bytes.len() > MAX_MOUNTINFO_BYTES - self.bytes {
            return Err(libc::EOVERFLOW);
        }
        self.bytes += bytes.len();
        for part in bytes.split_inclusive(|byte| *byte == b'\n') {
            let complete = part.ends_with(b"\n");
            let body = if complete {
                &part[..part.len() - 1]
            } else {
                part
            };
            if body.len() > MAX_MOUNTINFO_LINE - self.line.len() {
                return Err(libc::EOVERFLOW);
            }
            self.line.extend_from_slice(body);
            if complete {
                if let Some(parent) = selected_mount_parent(&self.line, self.directory)? {
                    if self.parent.replace(parent).is_some() {
                        return Err(libc::EINVAL);
                    }
                }
                self.line.clear();
            }
        }
        Ok(())
    }

    // Called only after the actual source read returns EOF, even when a selected record was already found.
    fn finish(self) -> Result<u32, libc::c_int> {
        if !self.line.is_empty() {
            return Err(libc::EINVAL);
        }
        self.parent.ok_or(libc::ENOENT)
    }
}

#[cfg(target_os = "linux")]
const MOUNT_STATX_MASK: u32 = libc::STATX_TYPE | libc::STATX_INO | libc::STATX_MNT_ID;

#[cfg(target_os = "linux")]
fn statx_directory(stat: &libc::statx) -> ProcIdentityResult<MountDirectory> {
    if stat.stx_mask & MOUNT_STATX_MASK != MOUNT_STATX_MASK {
        return Err((
            "cgroup mount statx is missing required fields",
            libc::EOPNOTSUPP,
        ));
    }
    if stat.stx_mode as libc::mode_t & libc::S_IFMT != libc::S_IFDIR
        || stat.stx_mnt_id == 0
        || stat.stx_mnt_id > i32::MAX as u64
    {
        return Err((
            "cgroup mount statx is not a directory with an ordinary mount ID",
            libc::EINVAL,
        ));
    }
    Ok(MountDirectory {
        mount_id: stat.stx_mnt_id as u32,
        major: stat.stx_dev_major,
        minor: stat.stx_dev_minor,
        inode: stat.stx_ino,
    })
}

#[cfg(target_os = "linux")]
fn query_statx(
    fd: libc::c_int,
    path: &[u8],
    flags: libc::c_int,
) -> ProcIdentityResult<MountDirectory> {
    let mut stat = std::mem::MaybeUninit::<libc::statx>::zeroed();
    let result = unsafe {
        libc::syscall(
            libc::SYS_statx,
            fd,
            path.as_ptr().cast::<libc::c_char>(),
            flags,
            MOUNT_STATX_MASK,
            stat.as_mut_ptr(),
        )
    };
    if result != 0 {
        return Err(proc_identity_errno("cgroup mount statx failed"));
    }
    statx_directory(&unsafe { stat.assume_init() })
}

#[cfg(target_os = "linux")]
fn require_fixed_path(held: MountDirectory) -> ProcIdentityResult<()> {
    let path = query_statx(
        libc::AT_FDCWD,
        b"/sys/fs/cgroup\0",
        libc::AT_SYMLINK_NOFOLLOW | libc::AT_NO_AUTOMOUNT,
    )?;
    require_current_directory(held, path).map_err(|error| {
        (
            "fixed cgroup path differs from its held mount and directory",
            error,
        )
    })
}

#[cfg(target_os = "linux")]
fn read_mountinfo(
    root: libc::c_int,
    root_stat: &libc::stat,
    held: MountDirectory,
) -> ProcIdentityResult<u32> {
    let how = [
        (libc::O_RDONLY | libc::O_NOFOLLOW | libc::O_CLOEXEC | libc::O_NONBLOCK) as u64,
        0,
        0x08 | 0x01 | 0x02, // BENEATH | NO_XDEV | NO_MAGICLINKS; thread-self is an ordinary link.
    ];
    let leaf = unsafe {
        libc::syscall(
            libc::SYS_openat2,
            root,
            b"thread-self/mountinfo\0".as_ptr().cast::<libc::c_char>(),
            how.as_ptr(),
            std::mem::size_of_val(&how),
        )
    };
    if leaf < 0 {
        return Err(proc_identity_errno("cgroup mountinfo fixed openat2 failed"));
    }
    let leaf = leaf as libc::c_int;
    let outcome = (|| {
        let before = proc_identity_stat(leaf)?;
        if before.st_mode & libc::S_IFMT != libc::S_IFREG || before.st_dev != root_stat.st_dev {
            return Err((
                "cgroup mountinfo leaf differs from its held procfs",
                libc::EINVAL,
            ));
        }
        let mut parser = MountInfo::new(held);
        let mut bytes = [0_u8; 8192];
        loop {
            let count = unsafe { libc::read(leaf, bytes.as_mut_ptr().cast(), bytes.len()) };
            if count < 0 {
                return Err(proc_identity_errno("cgroup mountinfo bounded read failed"));
            }
            if count == 0 {
                break;
            }
            parser
                .push(&bytes[..count as usize])
                .map_err(|error| ("Invalid or oversized cgroup mountinfo", error))?;
        }
        let after = proc_identity_stat(leaf)?;
        if before.st_dev != after.st_dev
            || before.st_ino != after.st_ino
            || before.st_mode != after.st_mode
        {
            return Err((
                "cgroup mountinfo held leaf changed during read",
                libc::EAGAIN,
            ));
        }
        parser
            .finish()
            .map_err(|error| ("Missing or truncated cgroup mountinfo record", error))
    })();
    close_proc_identity_fd(leaf, outcome)
}

#[cfg(target_os = "linux")]
pub(super) struct Member {
    pub(super) path: String,
    pub(super) device: u64,
    pub(super) inode: u64,
}

#[cfg(target_os = "linux")]
pub(super) struct Observation {
    pub(super) mount: [u32; 3],
    pub(super) member: Option<Member>,
}

#[cfg(target_os = "linux")]
pub(super) fn observe(
    fd: libc::c_int,
    original: &libc::stat,
    include_membership: bool,
) -> ProcIdentityResult<Observation> {
    let held = query_statx(fd, b"\0", libc::AT_EMPTY_PATH)?;
    let (major, minor) = unsafe { (libc::major(original.st_dev), libc::minor(original.st_dev)) };
    if original.st_mode & libc::S_IFMT != libc::S_IFDIR
        || held.inode != original.st_ino
        || held.major != major
        || held.minor != minor
    {
        return Err((
            "cgroup mount statx differs from the original directory fstat",
            libc::ESTALE,
        ));
    }
    require_fixed_path(held)?;
    let root = unsafe {
        libc::open(
            b"/proc\0".as_ptr().cast::<libc::c_char>(),
            libc::O_RDONLY | libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC,
        )
    };
    if root < 0 {
        return Err(proc_identity_errno(
            "cgroup mount fixed proc root open failed",
        ));
    }
    const LINKS: [(&[u8], &[u8]); 2] = [
        (b"thread-self/ns/cgroup\0", b"cgroup:["),
        (b"thread-self/ns/mnt\0", b"mnt:["),
    ];
    let outcome = (|| {
        let root_stat = proc_identity_root(root)?;
        let namespaces = read_thread_namespace_links(root, &root_stat, LINKS)?;
        require_initial_cgroup_namespace(namespaces)
            .map_err(|error| ("cgroup mount requires the initial cgroup namespace", error))?;
        let parent = read_mountinfo(root, &root_stat, held)?;
        let member = if include_membership {
            Some(membership::observe(fd, held, root, &root_stat)?)
        } else {
            None
        };
        require_fixed_path(held)?;
        let after_namespaces = read_thread_namespace_links(root, &root_stat, LINKS)?;
        let after = proc_identity_root(root)?;
        if namespaces != after_namespaces
            || root_stat.st_dev != after.st_dev
            || root_stat.st_ino != after.st_ino
        {
            return Err((
                "cgroup mount namespaces or held procfs root changed",
                libc::EAGAIN,
            ));
        }
        Ok(Observation {
            mount: [held.mount_id, parent, namespaces[1]],
            member,
        })
    })();
    close_proc_identity_fd(root, outcome)
}

#[cfg(test)]
mod tests {
    use super::*;

    const HELD: MountDirectory = MountDirectory {
        mount_id: 29,
        major: 0,
        minor: 26,
        inode: 1234,
    };
    const ROW: &[u8] =
        b"29 23 0:26 / /sys/fs/cgroup rw,nosuid shared:4 future:tag - cgroup2 cg\\040source rw\n";

    #[test]
    fn exact_mount_record_accepts_chunk_boundaries_escaped_names_and_unknown_tags() {
        let mut parser = MountInfo::new(HELD);
        parser
            .push(b"18 12 0:99 /bad\\012path /any\\040path\xff rw - tmpfs tmpfs rw\n")
            .unwrap();
        for chunk in ROW.chunks(3) {
            parser.push(chunk).unwrap();
        }
        assert_eq!(parser.finish(), Ok(23));
    }

    #[test]
    fn absent_duplicate_mismatched_and_incomplete_records_reject() {
        assert_eq!(MountInfo::new(HELD).finish(), Err(libc::ENOENT));
        let mut parser = MountInfo::new(HELD);
        parser.push(ROW).unwrap();
        assert_eq!(parser.push(ROW), Err(libc::EINVAL));
        for invalid in [
            b"29 23 0:26 /subtree /sys/fs/cgroup rw - cgroup2 cg rw\n".as_slice(),
            b"29 23 0:26 / /sys/fs/cgroup\\040 rw - cgroup2 cg rw\n",
            b"29 23 0:27 / /sys/fs/cgroup rw - cgroup2 cg rw\n",
            b"29 23 0:26 / /sys/fs/cgroup rw - cgroup cg rw\n",
            b"29 23 0:26 / /sys/fs/cgroup rw cgroup2 cg rw\n",
            b"29 23 0:26 / /sys/fs/cgroup rw - cgroup2 cg\n",
        ] {
            assert_eq!(MountInfo::new(HELD).push(invalid), Err(libc::EINVAL));
        }
        let mut parser = MountInfo::new(HELD);
        parser.push(ROW).unwrap();
        parser.push(b"30 29 0:28 / /unfinished").unwrap();
        assert_eq!(parser.finish(), Err(libc::EINVAL));
    }

    #[test]
    fn selected_record_does_not_end_the_scan_or_waive_either_bound() {
        let mut parser = MountInfo::new(HELD);
        parser.push(ROW).unwrap();
        parser.push(&vec![b'a'; MAX_MOUNTINFO_LINE]).unwrap();
        assert_eq!(parser.push(b"a\n"), Err(libc::EOVERFLOW));
        let mut parser = MountInfo::new(HELD);
        parser.push(ROW).unwrap();
        let unrelated = b"18 12 0:99 / /other rw - tmpfs tmpfs rw\n";
        while parser.bytes + unrelated.len() <= MAX_MOUNTINFO_BYTES {
            parser.push(unrelated).unwrap();
        }
        assert_eq!(parser.push(unrelated), Err(libc::EOVERFLOW));
    }

    #[test]
    fn same_inode_on_another_mount_and_nested_cgroup_view_reject() {
        assert_eq!(require_current_directory(HELD, HELD), Ok(()));
        assert_eq!(
            require_current_directory(
                HELD,
                MountDirectory {
                    mount_id: 30,
                    ..HELD
                }
            ),
            Err(libc::ESTALE)
        );
        assert_eq!(require_initial_cgroup_namespace([0xefff_fffb, 1]), Ok(()));
        assert_eq!(
            require_initial_cgroup_namespace([0xf000_0000, 1]),
            Err(libc::EINVAL)
        );
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn statx_requires_the_ordinary_mount_id_mask_and_directory_type() {
        let mut stat: libc::statx = unsafe { std::mem::zeroed() };
        stat.stx_mode = libc::S_IFDIR as u16;
        stat.stx_mnt_id = 29;
        for missing in [libc::STATX_TYPE, libc::STATX_INO, libc::STATX_MNT_ID] {
            stat.stx_mask = MOUNT_STATX_MASK & !missing;
            assert_eq!(statx_directory(&stat).unwrap_err().1, libc::EOPNOTSUPP);
        }
        stat.stx_mask = MOUNT_STATX_MASK;
        assert_eq!(statx_directory(&stat).unwrap().mount_id, 29);
        stat.stx_mode = libc::S_IFREG as u16;
        assert_eq!(statx_directory(&stat).unwrap_err().1, libc::EINVAL);
    }
}
