// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::collections::HashSet;

use tokio::sync::Mutex;

use dag::{
    authority::Authority,
    block::{Block, BlockReference, RoundNumber},
    consensus::DagConsensus,
    core::{core_thread::CoreDispatch, syncer::Syncer},
    data::Data,
    output::{DagConsumer, DagOutput},
};

use crate::context::SimulatorContext;

pub struct InlineDispatcher<D: DagConsensus> {
    syncer: Mutex<Syncer<SimulatorContext, D>>,
    consumer: Option<DagConsumer>,
}

impl<D: DagConsensus> InlineDispatcher<D> {
    pub fn new(syncer: Syncer<SimulatorContext, D>, consumer: Option<DagConsumer>) -> Self {
        Self {
            syncer: Mutex::new(syncer),
            consumer,
        }
    }

    /// Forward a step's output to the consumer. Awaited while the caller still holds the
    /// syncer lock, so the consumer observes it in order and a full consumer suspends the
    /// producing task (backpressure).
    async fn forward(&self, output: DagOutput) {
        if let Some(consumer) = &self.consumer {
            consumer.send(output).await.ok();
        }
    }
}

impl<D: DagConsensus> CoreDispatch<SimulatorContext, D> for InlineDispatcher<D> {
    async fn add_blocks(&self, blocks: Vec<Data<Block>>) {
        let mut syncer = self.syncer.lock().await;
        let output = syncer.add_blocks(blocks);
        self.forward(output).await;
    }

    async fn force_new_block(&self, round: RoundNumber) {
        let mut syncer = self.syncer.lock().await;
        let output = syncer.force_new_block(round);
        self.forward(output).await;
    }

    async fn cleanup(&self) {
        self.syncer.lock().await.core().cleanup();
    }

    async fn get_missing_blocks(&self) -> Vec<HashSet<BlockReference>> {
        self.syncer
            .lock()
            .await
            .core()
            .block_manager()
            .missing_blocks()
            .to_vec()
    }

    async fn authority_connection(&self, authority: Authority, connected: bool) {
        let mut lock = self.syncer.lock().await;
        if connected {
            lock.connect_authority(authority);
        } else {
            lock.disconnect_authority(authority);
        }
    }

    fn stop(self) -> Syncer<SimulatorContext, D> {
        self.syncer.into_inner()
    }
}
