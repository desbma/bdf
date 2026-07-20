//! Shared driver for the end to end benchmarks
//!
//! `BDF_BENCH_DIR` names the tree to walk, which has to be on Btrfs.
//!
//! Measurements are warm cache: each minimum size gets a discarded warmup run first, and evicting the page cache
//! between runs would need privileges.

use std::{
    env, iter,
    path::{Path, PathBuf},
    process::Output,
    time::{Duration, Instant},
};

use anyhow::Context as _;

/// Environment variable naming the benched directory tree
const DIR_VAR: &str = "BDF_BENCH_DIR";

/// Minimum file sizes to bench, in bytes
const MIN_SIZES: [u64; 4] = [4 * 1024, 128 * 1024, 512 * 1024, 1024 * 1024];

/// Number of measured runs per minimum size, each following a discarded warmup run
const RUN_COUNT: usize = 3;

/// Path of the benched binary
pub(crate) const BIN: &str = env!("CARGO_BIN_EXE_bdf");

/// Time `run` over the benched tree for every minimum size, printing a row of results per size
///
/// Everything `run` does is timed, so a mode that enumerates the tree itself pays for that walk just as the binary
/// pays for its own.
pub(crate) fn bench<F>(run: F) -> anyhow::Result<()>
where
    F: Fn(&Path, u64) -> anyhow::Result<Output>,
{
    let dir = PathBuf::from(env::var_os(DIR_VAR).with_context(|| format!("{DIR_VAR} is not set"))?);
    println!("{dir:?}, {RUN_COUNT} warm cache runs per minimum size");
    println!(
        "{:>10}  {:>10}  {:>10}  {:>8}",
        "min size", "min", "max", "pairs"
    );

    for min_size in MIN_SIZES {
        let measure = || -> anyhow::Result<(Duration, usize)> {
            let start = Instant::now();
            let output = run(&dir, min_size)?;
            let elapsed = start.elapsed();
            anyhow::ensure!(
                output.status.success(),
                "Run over {dir:?} with minimum size {min_size} failed: {stderr}",
                stderr = String::from_utf8_lossy(&output.stderr)
            );
            // Reported pairs are a flat sequence of NUL terminated paths, two paths per pair
            let pairs = output.stdout.split_inclusive(|byte| *byte == 0).count() / 2;
            Ok((elapsed, pairs))
        };

        let (_, pairs) = measure()?;
        let durations = iter::repeat_with(|| measure().map(|(elapsed, _)| elapsed))
            .take(RUN_COUNT)
            .collect::<anyhow::Result<Vec<_>>>()?;
        let fastest = durations.iter().min().context("No measured run")?;
        let slowest = durations.iter().max().context("No measured run")?;
        println!("{min_size:>10}  {fastest:>10.2?}  {slowest:>10.2?}  {pairs:>8}");
    }

    Ok(())
}
