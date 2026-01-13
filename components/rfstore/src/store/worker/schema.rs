// Copyright 2025 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    fmt,
    fmt::{Display, Formatter},
    time::Duration,
};

use kvengine::{
    STORAGE_CLASS_KEY, SchemaFileMeta, Shard, ShardMeta,
    table::{BoundedDataSet, OwnedInnerKey, schema_file::SchemaFile},
};
use kvproto::{metapb, metapb::Region};
use schema::schema::StorageClassSpec;
use tikv_util::{
    Either, codec::bytes::encode_bytes, debug, info, time::Instant, warn, worker::Runnable,
};

use crate::{
    RaftRouter,
    store::{Callback, CasualMessage, PeerMsg, PeerTag, RegionIdVer, StoreMsg},
};

pub enum SchemaTask {
    StorageClass {
        region: metapb::Region,
        schema_meta: SchemaFileMeta,
    },
}

impl Display for SchemaTask {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            SchemaTask::StorageClass {
                ref region,
                ref schema_meta,
            } => {
                write!(
                    f,
                    "check schema of region {} schema_meta {:?}",
                    region.id, schema_meta
                )
            }
        }
    }
}

pub struct SchemaRunner {
    store_id: u64,
    kv: kvengine::Engine,
    router: RaftRouter,
}

impl Runnable for SchemaRunner {
    type Task = SchemaTask;
    fn run(&mut self, task: SchemaTask) {
        match task {
            SchemaTask::StorageClass {
                region,
                schema_meta,
            } => self.handle_storage_class(region, schema_meta),
        }
    }
}

impl SchemaRunner {
    pub fn new(store_id: u64, kv: kvengine::Engine, router: RaftRouter) -> Self {
        Self {
            store_id,
            kv,
            router,
        }
    }

    fn peer_tag(&self, region: &Region) -> PeerTag {
        let id_ver = RegionIdVer::from_region(region);
        PeerTag::new(self.store_id, id_ver)
    }

    fn handle_storage_class(&self, region: metapb::Region, schema_meta: SchemaFileMeta) {
        let tag = self.peer_tag(&region);
        if let Ok(shard) = self
            .kv
            .get_shard_with_ver(region.get_id(), region.get_region_epoch().version)
        {
            let schema_file = shard.get_schema_file();
            let schema_file_id = schema_file.as_ref().map(|x| x.get_file_id());
            if !schema_file_is_matched_with_meta(schema_file.as_ref(), &schema_meta) {
                debug!("{} handle storage class: skip, schema file is changed", tag;
                    "schema_meta" => ?schema_meta, "schema_file" => ?schema_file_id);
                return;
            }
            if shard_is_matched_with_schema(shard.as_ref(), &schema_meta) {
                debug!("{} handle storage class: skip, shard is up-to-date", tag;
                    "schema_meta" => ?schema_meta, "checked_ver" => shard.get_checked_schema_ver());
                return;
            }

            let schema_version = schema_file.as_ref().map_or(0, |x| x.get_version());
            let shard_sc_spec = shard.get_storage_class_spec();
            let mut expect_sc_spec: Option<StorageClassSpec> = None;
            if let Some(schema_file) = schema_file {
                let overlapped = schema_file.overlap_storage_class_tables(shard.range.data_bound());
                info!("{} handle storage class", tag; "schema_version" => schema_version, "overlapped" => ?overlapped);
                match overlapped {
                    Either::Left(table_id) => {
                        // The table fully covers the region.
                        let schema = schema_file.get_table(table_id).unwrap();
                        if &shard_sc_spec != schema.get_storage_class_spec() {
                            expect_sc_spec = Some(schema.get_storage_class_spec().clone());
                        }
                    }
                    Either::Right(table_inner_keys) => {
                        if !table_inner_keys.is_empty() {
                            // The overlapped keys of tables require exclusive region.
                            self.split_regions_for_tables(&tag, &shard, &region, &table_inner_keys);
                            return;
                        } else if shard_sc_spec.is_specified() {
                            // No table with specified storage class covers this region.
                            expect_sc_spec = Some(StorageClassSpec::default());
                        }
                    }
                }
            } else if shard_sc_spec.is_specified() {
                expect_sc_spec = Some(StorageClassSpec::default());
            }

            if let Some(sc_spec) = expect_sc_spec {
                let can_transit_to_other_sc = sc_spec.can_transit_to_other_sc();
                if self.update_storage_class(tag, &shard, sc_spec, schema_version) {
                    shard.set_checked_schema_ver(schema_version);
                    if can_transit_to_other_sc {
                        // `update_storage_class` will reload the snap and set the files to target
                        // storage class.
                        shard.set_last_transit_storage_class_instant_to_now();
                    }
                    info!("{} update shard storage class spec to {:?}", tag, shard.get_storage_class_spec(); "schema_version" => schema_version);
                }
            } else {
                debug!("{} handle storage class: skip, no need to update", tag;
                    "schema_meta" => ?schema_meta, "schema_version" => schema_version, "shard.sc_spec" => ?shard_sc_spec);
                shard.set_checked_schema_ver(schema_version);
            }
        } else {
            info!("{} handle storage class: skip, shard not found/match", tag;
                "region" => ?region, "schema_meta" => ?schema_meta);
        }
    }

    fn split_regions_for_tables(
        &self,
        tag: &PeerTag,
        shard: &Shard,
        region: &metapb::Region,
        table_inner_keys: &[OwnedInnerKey],
    ) {
        info!(
            "{} handle storage class: schedule ask split", tag;
            "table_keys" => ?table_inner_keys,
        );
        let split_keys = table_inner_keys
            .iter()
            .map(|inner_key| encode_bytes(&shard.to_outer_key(inner_key.as_ref())))
            .collect::<Vec<_>>();
        let msg = CasualMessage::SplitRegion {
            region_epoch: region.get_region_epoch().clone(),
            split_keys,
            callback: Callback::None,
            source: "schema".into(),
        };
        self.router.send(shard.id, PeerMsg::CasualMessage(msg));
    }

    fn update_storage_class(
        &self,
        tag: PeerTag,
        shard: &Shard,
        spec: StorageClassSpec,
        schema_version: i64,
    ) -> bool {
        info!("{} update storage class", tag; "spec" => ?spec, "schema_ver" => schema_version);
        let mut cs = kvengine::new_change_set(shard.id, shard.ver);
        cs.set_property_key(STORAGE_CLASS_KEY.to_string());
        cs.set_property_value(spec.marshal());
        let (tx, rx) = tikv_util::mpsc::bounded(1);
        let cb = Callback::write(Box::new(move |res| {
            let _ = tx.send(res);
        }));
        let msg = StoreMsg::GenerateEngineChangeSet(cs, cb);
        if let Err(e) = self.router.store_sender.send(msg) {
            warn!("{} update storage class: send failed: {:?}", tag, e);
            return false;
        }
        let begin = Instant::now_coarse();
        let timeout = Duration::from_secs(10);
        let Ok(res) = rx.recv_timeout(timeout) else {
            warn!("{} update storage class: wait for propose timeout", tag);
            return false;
        };
        if res.response.get_header().has_error() {
            let err = res.response.get_header().get_error();
            warn!("{} update storage class: propose failed: {:?}", tag, err);
            return false;
        }

        // Check whether schema version & storage class is matched.
        // Note that callback with success does not mean the schema version is updated.
        // Changesets are applied async.
        loop {
            if let Some(schema_file) = shard.get_schema_file() {
                if schema_version != schema_file.get_version() {
                    // If the schema version changes, recheck.
                    return false;
                }
            }
            let current = shard.get_storage_class_spec();
            if current == spec {
                return true;
            }
            if begin.saturating_elapsed() < timeout {
                std::thread::sleep(Duration::from_millis(100));
                continue;
            }

            warn!("{} update storage class: wait for storage class updated timeout", tag;
                    "current" => ?current, "expect" => ?spec);
            break false;
        }
    }
}

pub(crate) fn schema_file_is_matched_with_meta(
    schema_file: Option<&SchemaFile>,
    schema_meta: &SchemaFileMeta,
) -> bool {
    match (schema_meta.is_valid(), schema_file) {
        (false, None) => true,
        (true, Some(schema_file)) => schema_meta.file_ver() == schema_file.get_version(),
        _ => false,
    }
}

pub(crate) fn shard_is_matched_with_meta(shard: &Shard, shard_meta: &ShardMeta) -> bool {
    let schema_meta = &shard_meta.schema;
    if schema_meta.is_valid() {
        shard.get_checked_schema_ver() >= schema_meta.file_ver()
    } else {
        !shard_meta.get_storage_class_spec().is_specified()
    }
}

pub(crate) fn shard_is_matched_with_schema(shard: &Shard, schema_meta: &SchemaFileMeta) -> bool {
    if schema_meta.is_valid() {
        shard.get_checked_schema_ver() >= schema_meta.file_ver()
    } else {
        !shard.get_storage_class_spec().is_specified()
    }
}
