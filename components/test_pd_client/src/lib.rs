// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

#[macro_use]
extern crate tikv_util;

mod pd;
mod real_pd;

use std::time::Duration;

pub use real_pd::PdWrapper;
mod service;

use kvproto::{metapb, pdpb};

pub use crate::{pd::*, service::*};

/// Extension trait of `PdClient` for test purpose.
///
/// Used to provide uniform testing features based on both real & test (mock)
/// PD.
pub trait PdClientExt: pd_client::PdClient {
    fn get_regions_number(&self) -> usize;

    fn get_all_regions(&self) -> Vec<metapb::Region>;

    fn transfer_leader(&self, region_id: u64, peer: metapb::Peer, peers: Vec<metapb::Peer>);

    fn region_leader_must_be(&self, region_id: u64, peer: metapb::Peer);

    fn must_transfer_leader(&self, region_id: u64, peer: metapb::Peer) {
        self.transfer_leader(region_id, peer.clone(), vec![peer.clone()]);
        self.region_leader_must_be(region_id, peer);
    }

    fn must_remove_peer(&self, region_id: u64, peer: metapb::Peer);

    fn must_add_peer(&self, region_id: u64, peer: metapb::Peer);

    fn must_split_region(
        &self,
        region: metapb::Region,
        policy: pdpb::CheckPolicy,
        keys: Vec<Vec<u8>>,
    ) {
        self.must_split_region_opt(region, policy, keys, Duration::from_secs(5))
            .expect("must_split_region");
    }

    fn must_split_region_opt(
        &self,
        region: metapb::Region,
        policy: pdpb::CheckPolicy,
        keys: Vec<Vec<u8>>,
        timeout: Duration,
    ) -> pd_client::Result<()>;

    fn split_region(&self, region: metapb::Region, policy: pdpb::CheckPolicy, keys: Vec<Vec<u8>>);

    fn merge_region(&self, from: u64, target: u64);

    fn try_merge_region(&self, from: u64, target: u64);
}
