// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

// #[PerformanceCriticalPath]
use std::sync::mpsc;

use crossbeam::channel::TrySendError;

use crate::{
    DiscardReason, Error, Result,
    store::{CasualMessage, RaftCommand, SignificantMsg, StoreMsg},
};

/// Routes message to target region.
///
/// Messages are not guaranteed to be delivered by this trait.
pub trait CasualRouter: Send {
    fn send(&self, region_id: u64, msg: CasualMessage) -> Result<()>;
}

/// Routes message to target region.
///
/// Messages aret guaranteed to be delivered by this trait.
pub trait SignificantRouter: Send {
    fn significant_send(&self, region_id: u64, msg: SignificantMsg) -> Result<()>;
}

/// Routes proposal to target region.
pub trait ProposalRouter {
    fn send(&self, cmd: RaftCommand) -> std::result::Result<(), TrySendError<RaftCommand>>;
}

/// Routes message to store FSM.
///
/// Messages are not guaranteed to be delivered by this trait.
pub trait StoreRouter: Send {
    fn send(&self, msg: StoreMsg) -> Result<()>;
}

impl CasualRouter for mpsc::SyncSender<(u64, CasualMessage)> {
    fn send(&self, region_id: u64, msg: CasualMessage) -> Result<()> {
        match self.try_send((region_id, msg)) {
            Ok(()) => Ok(()),
            Err(mpsc::TrySendError::Disconnected(_)) => {
                Err(Error::Transport(DiscardReason::Disconnected))
            }
            Err(mpsc::TrySendError::Full(_)) => Err(Error::Transport(DiscardReason::Full)),
        }
    }
}

impl ProposalRouter for mpsc::SyncSender<RaftCommand> {
    fn send(&self, cmd: RaftCommand) -> std::result::Result<(), TrySendError<RaftCommand>> {
        match self.try_send(cmd) {
            Ok(()) => Ok(()),
            Err(mpsc::TrySendError::Disconnected(cmd)) => Err(TrySendError::Disconnected(cmd)),
            Err(mpsc::TrySendError::Full(cmd)) => Err(TrySendError::Full(cmd)),
        }
    }
}

impl StoreRouter for mpsc::Sender<StoreMsg> {
    fn send(&self, msg: StoreMsg) -> Result<()> {
        match self.send(msg) {
            Ok(()) => Ok(()),
            Err(mpsc::SendError(_)) => Err(Error::Transport(DiscardReason::Disconnected)),
        }
    }
}
