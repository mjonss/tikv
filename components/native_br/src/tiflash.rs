// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use std::{collections::HashMap, default::Default, sync::Arc, time::Duration};

use api_version::ApiV2;
use http::Request;
use hyper::Body;
use kvproto::metapb::Store;
use pd_client::{PdClient, pd_control::PdControl, util::get_tiflash_storage_stores};
use security::SecurityManager;
use slog_global::{error, info};
use tikv_util::{box_err, retry::sleep_async, time::Instant};

use crate::{
    common::send_request_to_store_with_retry,
    error::{HttpRequestError, Result},
};

const WAIT_TIFLASH_REMOVE_REPLICA_INTERVAL: Duration = Duration::from_secs(1);
const WAIT_TIFLASH_REMOVE_REPLICA_TIMEOUT: Duration = Duration::from_secs(30);
const GET_TIFLASH_STATUS_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Clone, Serialize, Deserialize, PartialEq, Debug, Default)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct TiFlashSyncRegionResp {
    pub count: u64,
    pub regions: Vec<u64>,
}

async fn get_tiflash_keyspace_status(
    keyspace_id: u32,
    store: Store,
    security_mgr: &SecurityManager,
) -> Result<TiFlashSyncRegionResp> {
    let uri = security_mgr.build_uri(format!(
        "{}/tiflash/sync-region/keyspace/{}",
        &store.status_address, keyspace_id
    ))?;
    let req = || Request::get(uri.clone()).body(Body::empty()).unwrap();
    match send_request_to_store_with_retry(req, &store, security_mgr, GET_TIFLASH_STATUS_TIMEOUT)
        .await
    {
        Ok(resp) => Ok(serde_json::from_slice(&resp).unwrap()),
        Err(e) => Err(box_err!(
            "Fail to get tiflash keyspace {keyspace_id} status {uri}, err {:?}",
            e
        )),
    }
}

async fn wait_tiflash_replica_removed(
    pd_client: Arc<dyn PdClient>,
    keyspace_id: u32,
) -> Result<()> {
    let mut stores: HashMap<u64, Store> = get_tiflash_storage_stores(pd_client.as_ref())?
        .into_iter()
        .map(|s| (s.id, s))
        .collect();
    let mut last_err = None;
    let security_mgr = pd_client.get_security_mgr();
    let start = Instant::now();
    while Instant::now().duration_since(start) < WAIT_TIFLASH_REMOVE_REPLICA_TIMEOUT {
        // wait a while for tiflash replica to be removed.
        sleep_async(WAIT_TIFLASH_REMOVE_REPLICA_INTERVAL).await;

        let mut handles = Vec::with_capacity(stores.len());
        for (id, store) in stores.clone() {
            let security_mgr = security_mgr.clone();
            handles.push(async move {
                get_tiflash_keyspace_status(keyspace_id, store, security_mgr.as_ref())
                    .await
                    .map(|r| (id, r))
            })
        }
        for store_resp in futures::future::join_all(handles).await {
            match store_resp {
                Ok((store_id, resp)) => {
                    if resp.count == 0 {
                        stores.remove(&store_id);
                        info!(
                            "Tiflash store {} removed the keyspace {} replica",
                            store_id, keyspace_id
                        );
                    } else {
                        info!(
                            "Tiflash store {} is removing keyspace {} replica, resp {:?}",
                            store_id, keyspace_id, resp
                        )
                    }
                }
                Err(err) => {
                    error!(
                        "Query tiflash keyspace {keyspace_id} status error, {:?}",
                        err
                    );
                    last_err = Some(err);
                }
            }
        }
        if stores.is_empty() {
            return Ok(());
        }
    }
    error!("Tiflash remove replica timeout err {:?}", last_err);
    Err(HttpRequestError::Timeout(
        "Wait remove tiflash replica".to_string(),
        WAIT_TIFLASH_REMOVE_REPLICA_TIMEOUT,
    )
    .into())
}

pub async fn remove_tiflash_replica_of_keyspace(
    keyspace_id: u32,
    pd_control: &PdControl,
    pd_client: Arc<dyn PdClient>,
) -> Result<()> {
    let (keyspace_start, keyspace_end) = ApiV2::get_txn_keyspace_range(keyspace_id);
    let (hex_start, hex_end) = (hex::encode(keyspace_start), hex::encode(keyspace_end));
    let tiflash_rule_group = pd_control.get_tiflash_placement_rule_group().await?;
    if let Some(rule_group) = tiflash_rule_group {
        for rule in rule_group.rules.unwrap_or_default() {
            if hex_start <= rule.start_key && hex_end >= rule.end_key {
                pd_control
                    .remove_tiflash_placement_rule_by_id(&rule.id)
                    .await?;
                info!("remove tiflash rule {:?} succeed", rule);
            }
        }
    }
    wait_tiflash_replica_removed(pd_client, keyspace_id).await?;
    Ok(())
}
