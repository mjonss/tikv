// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    collections::hash_map::Entry,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use collections::HashMap;
use crossbeam::atomic::AtomicCell;
use kvproto::cdcpb::Compatibility;
use tikv_util::{error, info};

use crate::{
    channel::Sink,
    delegate::{DownstreamId, DownstreamState},
};

static CONNECTION_ID_ALLOC: AtomicUsize = AtomicUsize::new(0);

/// A unique identifier of a Connection.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub struct ConnId(usize);

impl ConnId {
    pub fn new() -> ConnId {
        ConnId(CONNECTION_ID_ALLOC.fetch_add(1, Ordering::SeqCst))
    }

    pub fn into_inner(self) -> usize {
        self.0
    }
}

impl Default for ConnId {
    fn default() -> Self {
        Self::new()
    }
}

bitflags::bitflags! {
    pub struct FeatureGate: u8 {
        const BATCH_RESOLVED_TS = 0b00000001;
        // Uncomment when its ready.
        // const LargeTxn       = 0b00000010;
    }
}

impl FeatureGate {
    // Returns the first version (v4.0.8) that supports batch resolved ts.
    pub fn batch_resolved_ts() -> semver::Version {
        semver::Version::new(4, 0, 8)
    }
}

pub struct Conn {
    id: ConnId,
    sink: Sink,
    // region id -> DownstreamId
    downstreams: HashMap<u64, (DownstreamId, Arc<AtomicCell<DownstreamState>>)>,
    peer: String,
    version: Option<(semver::Version, FeatureGate)>,
}

impl Conn {
    pub fn new(sink: Sink, peer: String) -> Conn {
        Conn {
            id: ConnId::new(),
            sink,
            downstreams: HashMap::default(),
            version: None,
            peer,
        }
    }

    // TODO refactor into Error::Version.
    pub fn check_version_and_set_feature(&mut self, ver: semver::Version) -> Option<Compatibility> {
        match &self.version {
            Some((version, _)) => {
                if version == &ver {
                    None
                } else {
                    error!("cdc different version on the same connection";
                        "previous version" => ?version, "version" => ?ver,
                        "downstream" => ?self.peer, "conn_id" => ?self.id);
                    Some(Compatibility {
                        required_version: version.to_string(),
                        ..Default::default()
                    })
                }
            }
            None => {
                let mut features = FeatureGate::empty();
                if FeatureGate::batch_resolved_ts() <= ver {
                    features.toggle(FeatureGate::BATCH_RESOLVED_TS);
                }
                info!("cdc connection version";
                    "version" => ver.to_string(), "features" => ?features, "downstream" => ?self.peer);
                self.version = Some((ver, features));
                None
            }
        }
        // Return Err(Compatibility) when TiKV reaches the next major release,
        // so that we can remove feature gates.
    }

    pub fn get_feature(&self) -> Option<&FeatureGate> {
        self.version.as_ref().map(|(_, f)| f)
    }

    pub fn get_peer(&self) -> &str {
        &self.peer
    }

    pub fn get_id(&self) -> ConnId {
        self.id
    }

    pub fn get_downstreams(
        &self,
    ) -> &HashMap<u64, (DownstreamId, Arc<AtomicCell<DownstreamState>>)> {
        &self.downstreams
    }

    pub fn take_downstreams(
        self,
    ) -> HashMap<u64, (DownstreamId, Arc<AtomicCell<DownstreamState>>)> {
        self.downstreams
    }

    pub fn get_sink(&self) -> &Sink {
        &self.sink
    }

    pub fn subscribe(
        &mut self,
        region_id: u64,
        downstream_id: DownstreamId,
        downstream_state: Arc<AtomicCell<DownstreamState>>,
    ) -> bool {
        match self.downstreams.entry(region_id) {
            Entry::Occupied(_) => false,
            Entry::Vacant(v) => {
                v.insert((downstream_id, downstream_state));
                true
            }
        }
    }

    pub fn unsubscribe(&mut self, region_id: u64) {
        self.downstreams.remove(&region_id);
    }

    pub fn downstream_id(&self, region_id: u64) -> Option<DownstreamId> {
        self.downstreams.get(&region_id).map(|x| x.0)
    }
}
