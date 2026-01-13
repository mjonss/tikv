// Copyright 2025 TiKV Project Authors. Licensed under Apache-2.0.

#![feature(test)]

use std::time::Duration;

use criterion::{BatchSize, Bencher, Criterion};
use kvengine::{
    FreeMemMsg, UserMeta, WRITE_CF, free_mem,
    table::{
        InnerKey,
        memtable::{CfTable, skl::WriteBatch},
    },
};
use tikv_util::mpsc;

const VALUE_SIZE: usize = 256; // 256 bytes

fn prepare_mem_tables(tbl_cnt: usize, tbl_size_mb: usize) -> Vec<CfTable> {
    let mut tables = Vec::with_capacity(tbl_cnt);
    let um = UserMeta::new(100, 200);
    let value = vec![0u8; VALUE_SIZE];
    let entry_cnt = tbl_size_mb * 1024 * 1024 / VALUE_SIZE;
    for _ in 0..tbl_cnt {
        let mut wb = WriteBatch::default();
        for i in 0..entry_cnt {
            wb.put(
                InnerKey::from_inner_buf(&i_to_key(i as u32)),
                0,
                &um.to_array(),
                200,
                &value,
            );
        }

        let tbl = CfTable::new();
        tbl.get_cf(WRITE_CF).put_batch(&mut wb, None, WRITE_CF);
        tables.push(tbl);
    }
    tables
}

fn baseline(b: &mut Bencher<'_>, options: &(usize, usize)) {
    let (tbl_cnt, tbl_size_mb) = *options;

    b.iter_batched(
        || prepare_mem_tables(tbl_cnt, tbl_size_mb),
        |tables| {
            for tbl in tables {
                std::hint::black_box(tbl);
            }
        },
        BatchSize::LargeInput,
    );
}

fn free_worker(b: &mut Bencher<'_>, options: &(usize, usize)) {
    let (tbl_cnt, tbl_size_mb) = *options;
    let (free_tx, free_rx) = mpsc::bounded(tbl_cnt);
    let handle = std::thread::spawn(move || {
        free_mem(free_rx);
    });

    b.iter_batched(
        || prepare_mem_tables(tbl_cnt, tbl_size_mb),
        |tables| {
            for tbl in tables {
                free_tx.send(FreeMemMsg::FreeMem(tbl)).unwrap();
            }
        },
        BatchSize::LargeInput,
    );

    free_tx.send(FreeMemMsg::Stop).unwrap();
    handle.join().unwrap();
}

fn main() {
    let mut c = Criterion::default()
        .measurement_time(Duration::from_secs(10))
        .configure_from_args();
    let mut group = c.benchmark_group("free_mem");

    let cases = vec![(1, 1), (1, 16), (1, 64), (1, 128), (10, 16)];

    for (tbl_cnt, tbl_size_mb) in cases {
        group.bench_with_input(
            format!("baseline_{tbl_cnt}_{tbl_size_mb}"),
            &(tbl_cnt, tbl_size_mb),
            baseline,
        );
        group.bench_with_input(
            format!("free_worker_{tbl_cnt}_{tbl_size_mb}"),
            &(tbl_cnt, tbl_size_mb),
            free_worker,
        );
    }

    group.finish();
    c.final_summary();
}

fn i_to_key(i: u32) -> Vec<u8> {
    i.to_le_bytes().to_vec()
}
