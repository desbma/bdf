//! Withholding files by size until enough share one to settle how to check them

use std::{
    collections::hash_map::{Entry, HashMap},
    mem,
    path::PathBuf,
};

/// Return the size when a file this large should be considered
pub(crate) fn wanted_file_size(file_size: u64, min_size: Option<u64>) -> Option<u64> {
    // Don't bother for empty files
    (file_size != 0 && min_size.is_none_or(|minimum| file_size >= minimum)).then_some(file_size)
}

/// Files seen so far for one size
enum SizeEntry {
    /// A single file, which can not have a duplicate yet
    One(PathBuf),
    /// Exactly two files, withheld for a direct comparison, which reads less than hashing them
    Pair(PathBuf, PathBuf),
    /// Three files or more, whose hashes partition the size into content groups cheaper than comparisons would
    Hashing,
}

/// Tracks the sizes seen so far, withholding files until enough share a size to settle how to check them
///
/// Identical files have identical sizes, so a file whose size no other file shares can not have a duplicate, and
/// hashing it would read it in full for nothing. A size shared by exactly two files is settled by comparing them
/// directly, so hashing only pays once a third file shows up.
#[derive(Default)]
pub(crate) struct SizeTracker(HashMap<u64, SizeEntry>);

impl SizeTracker {
    /// Take in a file, returning the files whose size is now known to need hashing: none while the size holds two
    /// files or fewer, the three withheld files when a third one ends the direct comparison plan, this one alone
    /// afterwards
    pub(crate) fn track(&mut self, path: PathBuf, file_size: u64) -> [Option<PathBuf>; 3] {
        match self.0.entry(file_size) {
            Entry::Vacant(e) => {
                e.insert(SizeEntry::One(path));
                [None, None, None]
            }
            Entry::Occupied(mut e) => match mem::replace(e.get_mut(), SizeEntry::Hashing) {
                SizeEntry::One(first) => {
                    *e.get_mut() = SizeEntry::Pair(first, path);
                    [None, None, None]
                }
                SizeEntry::Pair(first, second) => [Some(first), Some(second), Some(path)],
                SizeEntry::Hashing => [None, None, Some(path)],
            },
        }
    }

    /// Drain the sizes holding exactly two files, which a direct comparison settles without hashing
    pub(crate) fn into_pairs(self) -> impl Iterator<Item = Vec<PathBuf>> {
        self.0.into_values().filter_map(|entry| match entry {
            SizeEntry::Pair(first, second) => Some(vec![first, second]),
            SizeEntry::One(_) | SizeEntry::Hashing => None,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Collect the pairs a tracker still withholds, sorted for stable comparison
    fn tracked_pairs(size_tracker: SizeTracker) -> Vec<Vec<PathBuf>> {
        let mut pairs: Vec<_> = size_tracker.into_pairs().collect();
        pairs.sort();
        pairs
    }

    /// Take in a file named `path` of the given size, as the size iteration does
    fn track(size_tracker: &mut SizeTracker, path: &str, file_size: u64) -> [Option<PathBuf>; 3] {
        size_tracker.track(path.into(), file_size)
    }

    #[test]
    fn size_tracker_withholds_a_pair_for_direct_comparison() {
        let mut size_tracker = SizeTracker::default();

        assert_eq!(track(&mut size_tracker, "a", 1), [None, None, None]);
        assert_eq!(track(&mut size_tracker, "b", 1), [None, None, None]);
        assert_eq!(
            tracked_pairs(size_tracker),
            vec![vec![PathBuf::from("a"), PathBuf::from("b")]]
        );
    }

    #[test]
    fn size_tracker_transitions_from_pair_to_hashing() {
        let mut size_tracker = SizeTracker::default();
        assert_eq!(track(&mut size_tracker, "a", 1), [None, None, None]);
        assert_eq!(track(&mut size_tracker, "b", 1), [None, None, None]);

        // The third file of a size ends the direct comparison plan, releasing the two withheld files with it
        assert_eq!(
            track(&mut size_tracker, "c", 1),
            [Some("a".into()), Some("b".into()), Some("c".into())]
        );
        // Once hashing, each further file of the size is released on its own
        assert_eq!(
            track(&mut size_tracker, "d", 1),
            [None, None, Some("d".into())]
        );
        assert_eq!(tracked_pairs(size_tracker), Vec::<Vec<PathBuf>>::new());
    }

    #[test]
    fn size_tracker_keeps_sizes_independent() {
        let mut size_tracker = SizeTracker::default();
        track(&mut size_tracker, "a", 1);
        track(&mut size_tracker, "b", 2);
        track(&mut size_tracker, "c", 2);

        // The lone file of size 1 stays withheld while size 2 crosses into hashing
        assert_eq!(
            track(&mut size_tracker, "d", 2),
            [Some("b".into()), Some("c".into()), Some("d".into())]
        );
        assert_eq!(tracked_pairs(size_tracker), Vec::<Vec<PathBuf>>::new());
    }

    #[test]
    fn wanted_file_size_keeps_the_minimum_and_drops_empty_files() {
        assert_eq!(wanted_file_size(4, Some(4)), Some(4));
        assert_eq!(wanted_file_size(3, Some(4)), None);
        assert_eq!(wanted_file_size(4, None), Some(4));
        // An empty file has no duplicate worth reflinking, whatever the threshold
        assert_eq!(wanted_file_size(0, None), None);
    }
}
