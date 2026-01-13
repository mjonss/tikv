// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use clap::{Args, Subcommand};

#[derive(Args)]
pub struct TestArgs {
    #[clap(subcommand)]
    command: TestCommands,
}

#[derive(Subcommand)]
enum TestCommands {
    BurnCpus(BurnCpusArgs),
}

#[derive(Args)]
pub struct BurnCpusArgs {
    /// The number of CPUs to burn.
    #[clap(long, default_value_t = 1)]
    pub cpus: usize,
    /// The burn duration in seconds.
    #[clap(long, default_value_t = 1)]
    pub seconds: usize,
}

pub fn execute_test(args: TestArgs) {
    match args.command {
        TestCommands::BurnCpus(args) => {
            execute_burn_cpus(args);
        }
    }
}

// TODO: support number of cpus in decimal.
fn execute_burn_cpus(args: BurnCpusArgs) {
    let run = Arc::new(AtomicBool::new(true));
    let mut handles = Vec::with_capacity(args.cpus);
    for _ in 0..args.cpus {
        let run = run.clone();
        let h = std::thread::spawn(move || {
            let mut count = 0u64;
            while run.load(Ordering::Relaxed) {
                for _ in 0..u16::MAX {
                    count = count.wrapping_add(1u64);
                }
            }
            count
        });
        handles.push(h);
    }

    std::thread::sleep(Duration::from_secs(args.seconds as u64));
    run.store(false, Ordering::Relaxed);

    let total: u64 = handles.into_iter().map(|h| h.join().unwrap()).sum();
    let avg = total as f64 / args.seconds as f64 / args.cpus as f64;
    println!("counter: {total}, avg (per cpu x second): {avg}");
}
