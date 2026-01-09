// Copyright 2025 TiKV Project Authors. Licensed under Apache-2.0.

use cdc::ConnId;

use crate::{delegate::RequestId, ticdc_util::TiCdcError};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("cdc error {0}")]
    CdcError(#[from] cdc::Error),
    #[error("nix error {0}")]
    NixError(#[from] nix::Error),
    #[error("native_br error {0}")]
    BrError(#[from] native_br::error::Error),
    #[error("merged engine error {0}")]
    MergedEngineError(#[from] merged_engine::Error),
    #[error("kube error {0}")]
    KubeError(#[from] kube::Error),
    #[error("pd client error {0}")]
    PdClientError(#[from] pd_client::Error),
    #[error("hyper error {0}")]
    HyperError(#[from] hyper::Error),
    #[error("rfengine error {0}")]
    RfEngineError(#[from] rfengine::Error),
    #[error("store timeout {0}")]
    StoreTimeout(String),
    #[error(transparent)]
    TiCdcError(#[from] TiCdcError),
    #[error("request TiCDC timeout")]
    TiCdcTimeout,
    #[error("duplicated register, conn: {conn_id:?}, request: {request_id}")]
    DuplicatedRegister {
        conn_id: ConnId,
        request_id: RequestId,
    },
    #[error("register cancelled: {0}")]
    RegisterCancelled(String),
    #[error("no valid backup")]
    NoValidBackup,
    #[error("start_ts before safepoint ({start_ts} < {gc_safe_point})")]
    StartTsBeforeSafepoint { start_ts: u64, gc_safe_point: u64 },
    #[error("store unhealthy: {store_id}")]
    StoreUnhealthy { store_id: u64 },
    #[error("update stores errors: {0:?}")]
    UpdateStores(Vec<Error>),
    #[error("other error {0}")]
    OtherError(#[from] Box<dyn std::error::Error + Sync + Send>),

    #[cfg(feature = "testexport")]
    #[error("replication worker force stopped")]
    ForceStopped,
}

pub type Result<T> = std::result::Result<T, Error>;
