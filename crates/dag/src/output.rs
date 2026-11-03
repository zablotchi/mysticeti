// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! What a replica streams to its embedder: committed sub-dags, optionally interleaved with the
//! blocks entering its local DAG.

use tokio::sync::mpsc::{self, error::SendError};

use crate::{block::Block, consensus::CommittedSubDag, data::Data};

/// What one step of the core added to the local DAG and committed, each in order.
#[derive(Default)]
pub struct DagOutput {
    /// Received blocks newly connected to the DAG, each after the blocks it includes.
    pub added: Vec<Data<Block>>,
    /// This replica's new proposal, if any.
    pub proposed: Option<Data<Block>>,
    /// Newly committed sub-dags, in commit order.
    pub committed: Vec<CommittedSubDag>,
}

impl DagOutput {
    /// The step as an event sequence: the added blocks, then the proposal, then the commits.
    fn into_events(self) -> impl Iterator<Item = DagEvent> {
        self.added
            .into_iter()
            .chain(self.proposed)
            .map(DagEvent::Block)
            .chain(self.committed.into_iter().map(DagEvent::Commit))
    }
}

/// One entry of the stream fed to [`DagConsumer::Events`].
pub enum DagEvent {
    /// A block entering the local DAG: received from a peer once its causal history is local,
    /// or proposed by this replica.
    Block(Data<Block>),
    /// A sub-dag committed and durably written to the WAL.
    Commit(CommittedSubDag),
}

/// Where a replica streams its output. The channel must be bounded: a slow receiver throttles
/// the whole replica rather than dropping or reordering anything.
#[derive(Clone)]
pub enum DagConsumer {
    /// Every committed sub-dag, in commit order.
    Commits(mpsc::Sender<CommittedSubDag>),
    /// Every block entering the local DAG and every committed sub-dag, in one stream: a block
    /// arrives after the blocks it includes and before any commit containing it. Genesis blocks
    /// and blocks recovered from the WAL on restart are never streamed.
    Events(mpsc::Sender<DagEvent>),
}

impl DagConsumer {
    /// Whether `output` holds anything this consumer streams.
    pub(crate) fn streams(&self, output: &DagOutput) -> bool {
        match self {
            Self::Commits(_) => !output.committed.is_empty(),
            Self::Events(_) => {
                !output.added.is_empty()
                    || output.proposed.is_some()
                    || !output.committed.is_empty()
            }
        }
    }

    /// Streams `output` from a synchronous thread, blocking while the channel is full. Fails
    /// once the receiver is dropped.
    pub(crate) fn blocking_send(&self, output: DagOutput) -> Result<(), SendError<()>> {
        match self {
            Self::Commits(sender) => output
                .committed
                .into_iter()
                .try_for_each(|subdag| sender.blocking_send(subdag).map_err(|_| SendError(()))),
            Self::Events(sender) => output
                .into_events()
                .try_for_each(|event| sender.blocking_send(event).map_err(|_| SendError(()))),
        }
    }

    /// Streams `output`, waiting while the channel is full. Fails once the receiver is dropped.
    pub async fn send(&self, output: DagOutput) -> Result<(), SendError<()>> {
        match self {
            Self::Commits(sender) => {
                for subdag in output.committed {
                    sender.send(subdag).await.map_err(|_| SendError(()))?;
                }
            }
            Self::Events(sender) => {
                for event in output.into_events() {
                    sender.send(event).await.map_err(|_| SendError(()))?;
                }
            }
        }
        Ok(())
    }
}
