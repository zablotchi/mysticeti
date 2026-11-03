// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::time::Duration;

mod common;

use common::committee_and_cores;
use dag::{
    context::TokioCtx,
    core::{
        block_handler::CommitHandler,
        syncer::{Syncer, SyncerSignals},
    },
    metrics::Metrics,
    output::{DagConsumer, DagEvent},
    sync::net_sync::NetworkSyncer,
    test_util::{check_commits, check_event_stream, networks_and_addresses},
};
use tokio::sync::{mpsc, oneshot};

#[tokio::test]
async fn test_network_sync() {
    let (_committee, cores) = committee_and_cores::<TokioCtx>(4);
    let metrics: Vec<_> = cores.iter().map(|c| c.metrics.clone()).collect();
    let (networks, _) = networks_and_addresses(&metrics, 5101).await;
    let mut network_syncers = vec![];
    for (network, core) in networks.into_iter().zip(cores.into_iter()) {
        let commit_handler = CommitHandler::new(
            core.block_handler().transaction_time.clone(),
            Metrics::new_for_test(0),
        );
        let network_syncer = NetworkSyncer::start(
            network,
            core,
            Duration::from_secs(1),
            false,
            commit_handler,
            Metrics::new_for_test(0),
            None,
        );
        network_syncers.push(network_syncer);
    }
    println!("Started");
    tokio::time::sleep(Duration::from_secs(3)).await;
    println!("Done");
    let mut syncers = vec![];
    for network_syncer in network_syncers {
        let syncer = network_syncer.shutdown().await;
        syncers.push(syncer);
    }

    check_commits(&syncers);
}

/// Every consumer must receive exactly the anchors its replica committed, in commit order.
#[tokio::test]
async fn test_network_sync_with_commit_consumer() {
    const MIN_COMMITS: usize = 10;
    let (_committee, cores) = committee_and_cores::<TokioCtx>(4);
    let metrics: Vec<_> = cores.iter().map(|c| c.metrics.clone()).collect();
    let (networks, _) = networks_and_addresses(&metrics, 5201).await;
    let mut network_syncers = vec![];
    let mut receivers = vec![];
    for (network, core) in networks.into_iter().zip(cores.into_iter()) {
        let commit_handler = CommitHandler::new(
            core.block_handler().transaction_time.clone(),
            Metrics::new_for_test(0),
        );
        let (sender, receiver) = mpsc::channel(1024);
        receivers.push(receiver);
        let network_syncer = NetworkSyncer::start(
            network,
            core,
            Duration::from_secs(1),
            false,
            commit_handler,
            Metrics::new_for_test(0),
            Some(DagConsumer::Commits(sender)),
        );
        network_syncers.push(network_syncer);
    }

    let mut received = vec![vec![]; receivers.len()];
    tokio::time::timeout(Duration::from_secs(30), async {
        for (receiver, anchors) in receivers.iter_mut().zip(received.iter_mut()) {
            while anchors.len() < MIN_COMMITS {
                anchors.push(receiver.recv().await.expect("channel closed early").anchor);
            }
        }
    })
    .await
    .expect("timed out waiting for commits");

    let mut syncers = vec![];
    for network_syncer in network_syncers {
        syncers.push(network_syncer.shutdown().await);
    }

    // Shutdown dropped the senders; draining yields the complete forwarded sequence.
    for ((mut receiver, mut anchors), syncer) in receivers.into_iter().zip(received).zip(&syncers) {
        while let Some(subdag) = receiver.recv().await {
            anchors.push(subdag.anchor);
        }
        assert_eq!(anchors, syncer.commit_handler().committed_leaders());
    }
    check_commits(&syncers);
}

/// A lagging replica handed a backlog connects old blocks, proposes, and commits leaders made of
/// those blocks all in one step: the step's events must still stream every block before the
/// commit containing it, and the proposal after the blocks it includes.
#[tokio::test]
async fn test_catch_up_streams_blocks_before_their_commits() {
    const ROUNDS: usize = 10;
    let (_committee, mut cores) = committee_and_cores::<TokioCtx>(4);
    let lagging = cores.pop().expect("committee is not empty");
    // The remaining cores hold a quorum, so they advance without the lagging one.
    let mut backlog = vec![];
    for _ in 0..ROUNDS {
        let round: Vec<_> = cores
            .iter_mut()
            .map(|core| core.try_new_block().expect("a quorum must advance"))
            .collect();
        for core in &mut cores {
            core.add_blocks(round.clone());
        }
        backlog.extend(round);
    }

    let commit_handler = CommitHandler::new(
        lagging.block_handler().transaction_time.clone(),
        Metrics::new_for_test(0),
    );
    let mut syncer = Syncer::new(
        lagging,
        SyncerSignals::test(),
        commit_handler,
        Metrics::new_for_test(0),
    );
    let output = syncer.add_blocks(backlog.clone());
    assert_eq!(output.added.len(), backlog.len());
    assert!(
        output.proposed.is_some(),
        "the backlog must unlock a proposal"
    );
    assert!(
        !output.committed.is_empty(),
        "the backlog must commit leaders"
    );

    let (sender, mut receiver) = mpsc::channel(1024);
    DagConsumer::Events(sender)
        .send(output)
        .await
        .expect("receiver is alive");
    let mut events = vec![];
    while let Some(event) = receiver.recv().await {
        events.push(event);
    }
    check_event_stream(&events);
    let blocks = events
        .iter()
        .filter(|event| matches!(event, DagEvent::Block(_)))
        .count();
    assert_eq!(
        blocks,
        backlog.len() + 1,
        "every block and the proposal stream once"
    );
}

/// Every event consumer must receive an ordered stream whose commits are exactly the anchors
/// its replica committed, in commit order.
#[tokio::test]
async fn test_network_sync_with_event_consumer() {
    const MIN_COMMITS: usize = 10;
    let (_committee, cores) = committee_and_cores::<TokioCtx>(4);
    let metrics: Vec<_> = cores.iter().map(|c| c.metrics.clone()).collect();
    let (networks, _) = networks_and_addresses(&metrics, 5301).await;
    let mut network_syncers = vec![];
    let mut collectors = vec![];
    let mut milestones = vec![];
    for (network, core) in networks.into_iter().zip(cores.into_iter()) {
        let commit_handler = CommitHandler::new(
            core.block_handler().transaction_time.clone(),
            Metrics::new_for_test(0),
        );
        let (sender, mut receiver) = mpsc::channel(1024);
        // Drain every stream concurrently: an undrained one would stall its replica.
        let (milestone, reached) = oneshot::channel();
        milestones.push(reached);
        collectors.push(tokio::spawn(async move {
            let mut milestone = Some(milestone);
            let mut events = vec![];
            let mut commits = 0;
            while let Some(event) = receiver.recv().await {
                if matches!(event, DagEvent::Commit(_)) {
                    commits += 1;
                }
                events.push(event);
                if commits >= MIN_COMMITS
                    && let Some(milestone) = milestone.take()
                {
                    milestone.send(()).ok();
                }
            }
            events
        }));
        let network_syncer = NetworkSyncer::start(
            network,
            core,
            Duration::from_secs(1),
            false,
            commit_handler,
            Metrics::new_for_test(0),
            Some(DagConsumer::Events(sender)),
        );
        network_syncers.push(network_syncer);
    }

    tokio::time::timeout(Duration::from_secs(30), async {
        for reached in milestones {
            reached.await.expect("collector stopped early");
        }
    })
    .await
    .expect("timed out waiting for commits");

    let mut syncers = vec![];
    for network_syncer in network_syncers {
        syncers.push(network_syncer.shutdown().await);
    }

    // Shutdown dropped the senders, so every collector holds its complete stream.
    for (collector, syncer) in collectors.into_iter().zip(&syncers) {
        let events = collector.await.expect("collector task failed");
        check_event_stream(&events);
        let anchors: Vec<_> = events
            .iter()
            .filter_map(|event| match event {
                DagEvent::Commit(subdag) => Some(subdag.anchor),
                DagEvent::Block(_) => None,
            })
            .collect();
        assert_eq!(anchors, syncer.commit_handler().committed_leaders());
    }
    check_commits(&syncers);
}
