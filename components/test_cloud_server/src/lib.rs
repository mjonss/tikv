// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

#![feature(slice_pattern)]
#![feature(assert_matches)]

pub mod client;
pub mod cluster;
pub mod copr;
pub mod keyspace;
pub mod load_data;
pub mod oss;
pub mod scheduler;
pub mod table;
pub mod tidb;
mod tiflash;
pub mod tpc;
pub mod txn;
pub mod util;
pub use cluster::*;
pub mod sync_diff_inspector;
pub mod ticdc;
pub mod tikv_bin;

#[cfg(test)]
mod tests;

use std::sync::atomic::{AtomicU16, Ordering};

lazy_static::lazy_static! {
    static ref NODE_ALLOCATOR: AtomicU16 = AtomicU16::new(1);

    // Ref: https://nexte.st/docs/configuration/env-vars/?h=env#environment-variables-nextest-sets
    static ref IN_NEXTEST: bool = std::env::var("NEXTEST").is_ok_and(|x| x == "1");
}

pub fn alloc_node_id() -> u16 {
    let node_id = if *IN_NEXTEST {
        random_alloc_node_id()
    } else {
        NODE_ALLOCATOR.fetch_add(1, Ordering::Relaxed)
    };

    tikv_util::info!("allocated node_id {}", node_id);
    node_id
}

pub fn alloc_node_id_vec(count: usize) -> Vec<u16> {
    let mut nodes = vec![];
    nodes.resize_with(count, || alloc_node_id());
    nodes
}

fn random_alloc_node_id() -> u16 {
    use std::{
        collections::HashSet,
        fs::{File, OpenOptions},
        os::unix::io::AsRawFd,
        sync::Mutex,
    };

    use libc::{F_SETLK, F_WRLCK, SEEK_SET, fcntl, flock};
    use rand::Rng;

    lazy_static::lazy_static! {
        static ref LOCK_FILE: File = OpenOptions::new().read(true).create(true).truncate(false).write(true).open(std::env::temp_dir().join("cse-alloc-node-id.lock")).unwrap();
        static ref ALLOCATED_NODE_IDS: Mutex<HashSet<u16>> = Mutex::new(HashSet::new());
    }
    let fd = LOCK_FILE.as_raw_fd();

    loop {
        // tikv-server port base: 21000, status port base: 25000, tikv-worker: 17000.
        // See cluster.rs, `node_addr`, `node_status_addr`, `tikv_worker_addr`.
        // TODO: get free port from OS.
        let node_id = rand::thread_rng().gen_range(0..4000);

        if !ALLOCATED_NODE_IDS.lock().unwrap().insert(node_id) {
            continue;
        }

        #[allow(clippy::unnecessary_cast)]
        let lock = flock {
            l_type: F_WRLCK as i16,
            l_whence: SEEK_SET as i16,
            l_start: node_id as i64,
            l_len: 1,
            l_pid: 0,
        };
        let result = unsafe { fcntl(fd, F_SETLK, &lock) };
        // Ref: https://man7.org/linux/man-pages/man2/fcntl.2.html, RETURN VALUE
        if result == -1 {
            continue;
        }

        return node_id;
    }
}
