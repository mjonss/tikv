// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

pub mod blobtable;
pub mod columnar;
pub mod file;
pub mod fts;
pub mod memtable;
pub mod merge_iterator;
pub mod schema_file;
pub mod sstable;
pub mod table;
pub mod tiny_meta;
pub mod txn_file;
pub mod vector_index;

#[cfg(test)]
mod tests;

pub use merge_iterator::*;
pub use table::*;
pub use txn_file::*;

pub struct EncryptionProperty {
    pub data_key_id: u64,
    pub encrypted_data_key: Vec<u8>,
    pub method: u8,
}
