//! Btrfs filesystem identification and the tree walk that stays within one filesystem

use std::{
    collections::HashMap,
    fs::{self, File},
    io,
    mem::zeroed,
    os::{fd::AsRawFd as _, unix::fs::MetadataExt as _},
    path::{Path, PathBuf},
    sync::Mutex,
};

use anyhow::Context as _;
use linux_raw_sys::btrfs::btrfs_ioctl_fs_info_args;

use crate::size_tracker::wanted_file_size;

/// Identifier of a Btrfs filesystem, the same for all of its subvolumes
type BtrfsFsid = [u8; 16];

nix::ioctl_read!(
    /// Query the Btrfs filesystem holding an open file
    btrfs_fs_info,
    0x94,
    31,
    btrfs_ioctl_fs_info_args
);

/// Return the identifier of the Btrfs filesystem holding `path`, `None` if it is not on Btrfs
pub(crate) fn btrfs_fsid(path: &Path) -> Result<Option<BtrfsFsid>, io::Error> {
    let file = File::open(path)?;
    // SAFETY: every field is an integer, for which zero is a valid value
    let mut args: btrfs_ioctl_fs_info_args = unsafe { zeroed() };
    // SAFETY: the ioctl writes at most `size_of::<btrfs_ioctl_fs_info_args>()` bytes to `args`
    match unsafe { btrfs_fs_info(file.as_raw_fd(), &raw mut args) } {
        Ok(_) => Ok(Some(args.fsid)),
        // Any other filesystem leaves the ioctl unimplemented
        Err(nix::errno::Errno::ENOTTY) => Ok(None),
        Err(e) => Err(e.into()),
    }
}

/// A single Btrfs filesystem, which reflinks can not reach outside of
pub(crate) struct BtrfsFilesystem {
    /// Identifier of the filesystem
    fsid: BtrfsFsid,
    /// Whether a device belongs to the filesystem, cached to keep the ioctl off the per file path
    devices: HashMap<u64, bool>,
}

impl BtrfsFilesystem {
    /// Identify the Btrfs filesystem holding `path`, if any
    pub(crate) fn containing(path: &Path) -> Result<Option<Self>, io::Error> {
        Ok(btrfs_fsid(path)?.map(|fsid| Self {
            fsid,
            devices: HashMap::new(),
        }))
    }

    /// Test whether `path`, sitting on device `device`, is on this filesystem
    ///
    /// Each subvolume has its own device, so the device alone does not settle it.
    pub(crate) fn holds(&mut self, path: &Path, device: u64) -> Result<bool, io::Error> {
        if let Some(&known) = self.devices.get(&device) {
            return Ok(known);
        }
        let holds = btrfs_fsid(path)? == Some(self.fsid);
        self.devices.insert(device, holds);
        Ok(holds)
    }
}

/// Number of directory walker threads
const WALKER_THREADS: usize = 16;

/// Walk a directory tree in parallel, yielding each regular file eligible for analysis along with its size,
/// without leaving the Btrfs filesystem the tree starts on
///
/// Subvolumes of a filesystem each have their own device, so pruning on a device change would skip them, while
/// reflinking across them is perfectly valid.
pub(crate) fn walk_btrfs_dir(
    input_dir: &Path,
    min_size: Option<u64>,
) -> anyhow::Result<impl Iterator<Item = (PathBuf, u64)>> {
    let filesystem = Mutex::new(
        BtrfsFilesystem::containing(input_dir)
            .with_context(|| format!("Failed to identify the filesystem of {input_dir:?}"))?
            .with_context(|| format!("{input_dir:?} is not on a Btrfs filesystem"))?,
    );

    // An eligible file carries its size, a directory carries nothing but is kept to descend into
    let walk = jwalk::WalkDirGeneric::<((), Option<u64>)>::new(input_dir)
        // Dotfiles are ordinary candidates, against jwalk's default of skipping them
        .skip_hidden(false)
        .parallelism(jwalk::Parallelism::RayonNewPool(WALKER_THREADS))
        .process_read_dir(move |depth, _dir, _state, entries| {
            entries.retain_mut(|result| {
                let Ok(entry) = result else {
                    return true;
                };
                let path = entry.path();
                // The root, at depth none, is reached through a symlink it may be named as, while entries below it
                // are stated unfollowed; the stat also drops a regular file that is its own mount point, and sizes it
                let metadata = if depth.is_none() {
                    fs::metadata(&path)
                } else {
                    fs::symlink_metadata(&path)
                };
                let Ok(metadata) = metadata.inspect_err(|e| {
                    log::warn!("Error while reading metadata of {path:?}: {e}");
                }) else {
                    return false;
                };
                if !metadata.is_dir() && !metadata.is_file() {
                    return false;
                }
                // The lock guards only this quick filesystem test, which never panics, so it can not be poisoned
                let holds = match filesystem.lock() {
                    Ok(mut fs) => fs.holds(&path, metadata.dev()),
                    Err(_) => return false,
                };
                let Ok(true) = holds.inspect_err(|e| {
                    log::warn!("Error while identifying the filesystem of {path:?}: {e}");
                }) else {
                    return false;
                };
                entry.client_state = metadata
                    .is_file()
                    .then(|| wanted_file_size(metadata.len(), min_size))
                    .flatten();
                metadata.is_dir() || entry.client_state.is_some()
            });
        })
        .try_into_iter()?;

    Ok(walk.filter_map(|result| {
        let entry = result
            .inspect_err(|e| log::warn!("Error while walking the tree: {e}"))
            .ok()?;
        entry.client_state.map(|size| (entry.path(), size))
    }))
}

#[cfg(test)]
mod tests {
    use std::{
        os::unix::fs::symlink,
        process::{Command, Stdio},
    };

    use super::*;
    use crate::tests::btrfs_test_dir;

    /// Create a Btrfs subvolume named `name` under `parent`, which needs the `btrfs` binary
    fn create_subvolume(parent: &Path, name: &str) -> PathBuf {
        let path = parent.join(name);
        let status = Command::new("btrfs")
            .args(["subvolume", "create"])
            .arg(&path)
            .stdout(Stdio::null())
            .status()
            .unwrap();
        assert!(status.success());
        path
    }

    #[test]
    fn btrfs_fsid_of_other_filesystem_is_none() {
        assert_eq!(btrfs_fsid(Path::new("/proc")).unwrap(), None);
    }

    #[test]
    fn btrfs_fsid_of_regular_file_of_other_filesystem_is_none() {
        assert_eq!(btrfs_fsid(Path::new("/proc/self/status")).unwrap(), None);
    }

    #[test]
    fn btrfs_fsid_is_shared_by_subvolumes_holding_distinct_devices() {
        let Some(dir) = btrfs_test_dir() else { return };
        let subvolume = create_subvolume(dir.path(), "subvolume");

        assert_ne!(
            fs::metadata(&subvolume).unwrap().dev(),
            fs::metadata(dir.path()).unwrap().dev()
        );
        assert_eq!(
            btrfs_fsid(&subvolume).unwrap(),
            btrfs_fsid(dir.path()).unwrap()
        );
    }

    #[test]
    fn walk_btrfs_dir_follows_a_root_symlink() {
        let Some(dir) = btrfs_test_dir() else { return };
        let tree = dir.path().join("tree");
        fs::create_dir(&tree).unwrap();
        fs::write(tree.join("file"), b"content").unwrap();
        let link = dir.path().join("link");
        symlink(&tree, &link).unwrap();

        let walked: Vec<PathBuf> = walk_btrfs_dir(&link, None)
            .unwrap()
            .map(|(path, _size)| path)
            .collect();

        assert_eq!(walked, vec![link.join("file")]);
    }

    #[test]
    fn walk_btrfs_dir_does_not_follow_nested_symlinks() {
        let Some(dir) = btrfs_test_dir() else { return };
        let tree = dir.path().join("tree");
        let outside = dir.path().join("outside");
        fs::create_dir(&tree).unwrap();
        fs::create_dir(&outside).unwrap();
        fs::write(tree.join("inside"), b"content").unwrap();
        fs::write(outside.join("escaped"), b"content").unwrap();
        symlink(&outside, tree.join("link")).unwrap();

        let walked: Vec<PathBuf> = walk_btrfs_dir(&tree, None)
            .unwrap()
            .map(|(path, _size)| path)
            .collect();

        assert_eq!(walked, vec![tree.join("inside")]);
    }

    #[test]
    fn walk_btrfs_dir_includes_hidden_files() {
        let Some(dir) = btrfs_test_dir() else { return };
        let hidden = dir.path().join(".hidden");
        let regular = dir.path().join("regular");
        fs::write(&hidden, b"content").unwrap();
        fs::write(&regular, b"content").unwrap();

        let mut walked: Vec<PathBuf> = walk_btrfs_dir(dir.path(), None)
            .unwrap()
            .map(|(path, _size)| path)
            .collect();
        walked.sort();

        assert_eq!(walked, vec![hidden, regular]);
    }

    #[test]
    fn walk_btrfs_dir_descends_into_subvolumes() {
        let Some(dir) = btrfs_test_dir() else { return };
        let subvolume = create_subvolume(dir.path(), "subvolume");
        let nested = create_subvolume(&subvolume, "nested");
        fs::write(dir.path().join("outside"), b"content").unwrap();
        fs::write(subvolume.join("inside"), b"content").unwrap();
        fs::write(nested.join("deeper"), b"content").unwrap();

        let mut walked: Vec<PathBuf> = walk_btrfs_dir(dir.path(), None)
            .unwrap()
            .map(|(path, _size)| path)
            .collect();
        walked.sort();

        assert_eq!(
            walked,
            vec![
                dir.path().join("outside"),
                subvolume.join("inside"),
                nested.join("deeper"),
            ]
        );
    }
}
