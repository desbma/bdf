//! End to end benchmark of the whole binary, over a real directory tree
//!
//! `BDF_BENCH_DIR` names the tree to walk, which has to be on Btrfs.
//!
//! Measurements are warm cache: each minimum size gets a discarded warmup run first, and evicting the page cache
//! between runs would need privileges.

// The binary is driven as a subprocess, so none of its dependencies are linked here
#![expect(unused_crate_dependencies)]

use std::{
    env, iter,
    path::{Path, PathBuf},
    process::Command,
    time::{Duration, Instant},
};

use anyhow::Context as _;

/// Environment variable naming the benched directory tree
const DIR_VAR: &str = "BDF_BENCH_DIR";

/// Minimum file sizes to bench, in bytes
const MIN_SIZES: [u64; 4] = [4 * 1024, 128 * 1024, 512 * 1024, 1024 * 1024];

/// Number of measured runs per minimum size, each following a discarded warmup run
const RUN_COUNT: usize = 3;

/// Run the binary over `dir`, returning how long it took and how many duplicate pairs it reported
fn run(dir: &Path, min_size: u64) -> anyhow::Result<(Duration, usize)> {
    let start = Instant::now();
    let output = Command::new(env!("CARGO_BIN_EXE_bdf"))
        .arg("--min-size")
        .arg(min_size.to_string())
        .arg(dir)
        .output()
        .context("Failed to run the binary")?;
    let elapsed = start.elapsed();
    anyhow::ensure!(
        output.status.success(),
        "Run over {dir:?} with minimum size {min_size} failed: {stderr}",
        stderr = String::from_utf8_lossy(&output.stderr)
    );
    // Reported pairs are a flat sequence of NUL terminated paths, two paths per pair
    let pairs = output.stdout.split_inclusive(|byte| *byte == 0).count() / 2;
    Ok((elapsed, pairs))
}

fn main() -> anyhow::Result<()> {
    let dir = PathBuf::from(env::var_os(DIR_VAR).with_context(|| format!("{DIR_VAR} is not set"))?);
    println!("{dir:?}, {RUN_COUNT} warm cache runs per minimum size");
    println!(
        "{:>10}  {:>10}  {:>10}  {:>8}",
        "min size", "min", "max", "pairs"
    );

    for min_size in MIN_SIZES {
        let (_, pairs) = run(&dir, min_size)?;
        let durations = iter::repeat_with(|| run(&dir, min_size).map(|(elapsed, _)| elapsed))
            .take(RUN_COUNT)
            .collect::<anyhow::Result<Vec<_>>>()?;
        let fastest = durations.iter().min().context("No measured run")?;
        let slowest = durations.iter().max().context("No measured run")?;
        println!("{min_size:>10}  {fastest:>10.2?}  {slowest:>10.2?}  {pairs:>8}");
    }

    Ok(())
}
