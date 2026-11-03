// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::time::Duration;

use futures::future::join_all;
use rand::{SeedableRng, rngs::StdRng};

use dag::{
    block::{BlockReference, transaction::Transaction},
    context::Ctx,
    output::{DagConsumer, DagEvent},
    test_util::check_event_stream,
};
use replica::replica::ReplicaHandle;
use simulator::{SimulatedNetwork, SimulatorContext, SimulatorExecutor};
use tokio::sync::mpsc;

/// One event as observed by a consumer: a block, or a commit's anchor and block references.
#[derive(PartialEq, Debug)]
enum ObservedEvent {
    Block(BlockReference),
    Commit(BlockReference, Vec<BlockReference>),
}

/// Run a committee with an event consumer and a submitter task per replica; return each
/// replica's observed stream, after asserting its ordering contract, that it carries the
/// replica's own proposals, and that its commits match the replica's storage.
fn run_once(seed: u64, committee_size: usize) -> Vec<Vec<ObservedEvent>> {
    let rng = StdRng::seed_from_u64(seed);
    SimulatorExecutor::run(rng, async move {
        let mut consumers = Vec::with_capacity(committee_size);
        let mut collectors = Vec::with_capacity(committee_size);
        for _ in 0..committee_size {
            let (sender, mut receiver) = mpsc::channel(1024);
            consumers.push(Some(DagConsumer::Events(sender)));
            collectors.push(SimulatorContext::spawn(async move {
                let mut events = vec![];
                while let Some(event) = receiver.recv().await {
                    events.push(event);
                }
                events
            }));
        }

        let (_network, replicas) = SimulatedNetwork::new_for_test_with_consumers(consumers).await;
        let submitters: Vec<_> = replicas
            .iter()
            .map(|handle| {
                let client = handle.transaction_client();
                SimulatorContext::spawn(async move {
                    while client
                        .submit(vec![Transaction::new_for_test()])
                        .await
                        .is_ok()
                    {
                        SimulatorContext::sleep(Duration::from_millis(100)).await;
                    }
                })
            })
            .collect();

        SimulatorContext::sleep(Duration::from_secs(30)).await;
        for submitter in &submitters {
            SimulatorContext::abort(submitter);
        }

        let syncers = join_all(replicas.into_iter().map(ReplicaHandle::shutdown)).await;
        let mut streams = Vec::with_capacity(committee_size);
        for (collector, syncer) in collectors.into_iter().zip(syncers) {
            let events = collector.await.expect("collector task failed");
            check_event_stream(&events);
            let observed: Vec<ObservedEvent> = events
                .into_iter()
                .map(|event| match event {
                    DagEvent::Block(block) => ObservedEvent::Block(*block.reference()),
                    DagEvent::Commit(subdag) => ObservedEvent::Commit(
                        subdag.anchor,
                        subdag
                            .blocks
                            .iter()
                            .map(|block| *block.reference())
                            .collect(),
                    ),
                })
                .collect();

            let storage = syncer.into_storage();
            let last_proposal = storage
                .block_reader()
                .last_own_block_ref()
                .expect("replica must have proposed");
            assert!(
                observed.contains(&ObservedEvent::Block(last_proposal)),
                "own proposal {last_proposal} was not streamed"
            );
            let persisted: Vec<ObservedEvent> = storage
                .iter_commits()
                .map(|commit| ObservedEvent::Commit(commit.leader, commit.sub_dag))
                .collect();
            let streamed: Vec<&ObservedEvent> = observed
                .iter()
                .filter(|event| matches!(event, ObservedEvent::Commit(..)))
                .collect();
            assert_eq!(
                streamed,
                persisted.iter().collect::<Vec<_>>(),
                "streamed commits must match the WAL"
            );
            assert!(!persisted.is_empty(), "expected at least one commit");
            streams.push(observed);
        }
        streams
    })
}

/// Identical seeds must yield identical consumer-observed event streams.
#[test]
fn event_stream_is_deterministic_per_seed() {
    for seed in [7, 42] {
        let first = run_once(seed, 4);
        let second = run_once(seed, 4);
        assert_eq!(
            first, second,
            "seed {seed} produced diverging event streams"
        );
    }
}
