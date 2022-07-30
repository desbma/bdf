# bdf

[![Build status](https://github.com/desbma/bdf/actions/workflows/ci.yml/badge.svg)](https://github.com/desbma/bdf/actions)
[![AUR version](https://img.shields.io/aur/version/bdf.svg?style=flat)](https://aur.archlinux.org/packages/bdf/)
[![License](https://img.shields.io/github/license/desbma/bdf.svg?style=flat)](https://github.com/desbma/bdf/blob/master/LICENSE)

Btrfs Duplicate Finder

`bdf` is a simple tool to **efficiently** find identical files, candidates for [reflinking](https://btrfs.readthedocs.io/en/latest/Reflink.html), on Btrfs filesystems.

In keeping with the Unix philosophy "_do one thing and do it well_", it finds duplicates not yet reflinked, but does not deduplicate files. It is up to you to decide what to do with the identical files. If you are looking for an easy way to deduplicate that "just works", it is very easy to use `bdf`'s output in a pipeline to deduplicate though, see [Auto deduplication](#auto-deduplication).

Compared to alternative solutions, `bdf` is fast and simple. It does not store any state or database locally, and does not touch your files, you can even run it on a read only mounted filesystem.

## Algorithm summary

1. For all files, get the file size and compute the [XXH3-64](https://github.com/Cyan4973/xxHash) \*
2. For files with similar hashes and size, check the file content (for the extremely unlikely but possible case of a hash collision)
3. For files with similar content, check if some Btrfs file extents are different (using [fiemap](https://www.kernel.org/doc/html/latest/filesystems/fiemap.html))
4. If some extents are not shared, the files are considered duplicates candidate for reflinking

_\* Some optimizations take place: we only compute hashes for files having the same size as at least another file. This avoids computing hashes for files which can not be duplicate anyway (the common case), and leads to a major overall speedup._

## Installation

### From source

You need a Rust build environment for example from [rustup](https://rustup.rs/).

```
cargo build --release
strip --strip-all target/release/bdf
install -Dm 755 -t /usr/local/bin target/release/bdf
```

### From the AUR

Arch Linux users can install the [bdf AUR package](https://aur.archlinux.org/packages/bdf/).

## Usage

`bdf` outputs progress information on _stderr_, and NUL (`'\0'`) terminated pairs of filepaths to deduplicate on _stdout_, for easy and safe usage in shell scripts.

See `bdf -h` for complete command line reference.

### Auto deduplication

To automatically deduplicate files in directory `target_dir`, run `bdf` with `xargs`, and deduplicate with `cp`:

```
bdf target_dir | xargs -0 -r -p -n 2 cp -v --reflink=always --preserve=all
```

You will need to confirm before each deduplication (due to `xargs` `-p` switch). Be careful because if a file is modified during the analysis, it may get deduplicated although the pair of files are not identical anymore.

## License

[GPLv3](./LICENSE)
