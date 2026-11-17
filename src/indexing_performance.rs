use std::{
    collections::HashMap,
    ops::Deref,
    time::{Duration, Instant},
};

use parking_lot::RwLock;
use thegraph_core::{DeploymentId, IndexerId, alloy::primitives::BlockNumber};
use tokio::{self, sync::mpsc, time::MissedTickBehavior};

use crate::network::NetworkService;

#[derive(Default)]
pub struct Snapshot {
    pub response: indexer_selection::Performance,
    /// The highest block the indexer has been observed to have indexed.
    pub latest_block: Option<BlockNumber>,
    /// When `latest_block` was last observed, either in a query response or a status poll.
    pub observed_at: Option<Instant>,
}

impl Snapshot {
    /// Record that the indexer reported `block` at `at`. A report at or above the known block
    /// refreshes the observation time, so an indexer that keeps up looks fresh even when it is
    /// rarely queried. A lower report is ignored.
    fn observe(&mut self, block: BlockNumber, at: Instant) {
        if self.latest_block.is_none_or(|latest| block >= latest) {
            self.latest_block = Some(block);
            self.observed_at = Some(at);
        }
    }
}

#[derive(Clone)]
pub struct IndexingPerformance {
    data: &'static DoubleBuffer,
    msgs: mpsc::UnboundedSender<Feedback>,
}

struct Feedback {
    indexer: IndexerId,
    deployment: DeploymentId,
    success: bool,
    latency_ms: u16,
    latest_block: Option<BlockNumber>,
}

impl IndexingPerformance {
    pub fn new(network: NetworkService) -> Self {
        let (tx, rx) = mpsc::unbounded_channel();
        let data: &'static DoubleBuffer = Box::leak(Box::default());
        Actor::spawn(data, rx, network);
        Self { data, msgs: tx }
    }

    pub fn latest(&self) -> impl Deref<Target = HashMap<(IndexerId, DeploymentId), Snapshot>> + '_ {
        loop {
            // This is guaranteed to only move forward in time, and is almost guaranteed to acquire
            // the lock "immediately". These guarantees come from the invariant that there is a
            // single writer, and it can only be in a few possible states.
            for unlocked in &self.data.0 {
                if let Some(data) = unlocked.try_read() {
                    return data;
                }
            }
        }
    }

    pub fn feedback(
        &self,
        indexer: IndexerId,
        deployment: DeploymentId,
        success: bool,
        latency_ms: u16,
        latest_block: Option<BlockNumber>,
    ) {
        self.msgs
            .send(Feedback {
                indexer,
                deployment,
                success,
                latency_ms,
                latest_block,
            })
            .unwrap();
    }
}

#[derive(Default)]
struct DoubleBuffer([RwLock<HashMap<(IndexerId, DeploymentId), Snapshot>>; 2]);

struct Actor {
    data: &'static DoubleBuffer,
}

impl Actor {
    fn spawn(
        data: &'static DoubleBuffer,
        mut messages: mpsc::UnboundedReceiver<Feedback>,
        mut network: NetworkService,
    ) {
        let mut actor = Self { data };
        let mut timer = tokio::time::interval(Duration::from_secs(1));
        timer.set_missed_tick_behavior(MissedTickBehavior::Skip);
        tokio::spawn(async move {
            let batch_limit = 32;
            let mut msg_buf = Vec::with_capacity(batch_limit);
            loop {
                tokio::select! {
                    _ = timer.tick() => actor.decay(),
                    _ = messages.recv_many(&mut msg_buf, batch_limit) => actor.handle_msgs(&mut msg_buf),
                    _ = network.changed() => actor.handle_network(&network),
                }
            }
        });
    }

    fn decay(&mut self) {
        for unlocked in &self.data.0 {
            for snapshot in unlocked.write().values_mut() {
                snapshot.response.decay();
            }
        }
    }

    fn handle_msgs(&mut self, msgs: &mut Vec<Feedback>) {
        let now = Instant::now();
        for unlocked in &self.data.0 {
            let mut locked = unlocked.write();
            for &Feedback {
                indexer,
                deployment,
                success,
                latency_ms,
                latest_block,
            } in msgs.iter()
            {
                let snapshot = locked.entry((indexer, deployment)).or_default();
                snapshot.response.feedback(success, latency_ms);
                if let Some(block) = latest_block {
                    snapshot.observe(block, now);
                }
            }
        }
        msgs.clear();
    }

    fn handle_network(&mut self, network: &NetworkService) {
        let progress = network.indexing_progress();
        let now = Instant::now();
        for unlocked in &self.data.0 {
            let mut locked = unlocked.write();
            for (indexing, latest_block) in progress.iter() {
                let snapshot = locked
                    .entry((indexing.indexer, indexing.deployment))
                    .or_default();
                snapshot.observe(*latest_block, now);
            }
        }
    }
}

#[cfg(test)]
mod feedback_tests {
    use super::*;

    #[test]
    fn completed_feedback_is_visible_through_either_buffer() {
        let data = Box::leak(Box::new(DoubleBuffer::default()));
        let (msgs, _rx) = mpsc::unbounded_channel();
        let performance = IndexingPerformance { data, msgs };
        let mut actor = Actor { data };
        let indexer = thegraph_core::alloy::primitives::Address::ZERO.into();
        let deployment = [0; 32].into();
        let mut expected = indexer_selection::Performance::default();
        for (success, latency_ms, latest_block) in [(true, 250, Some(100)), (false, 900, Some(90))]
        {
            expected.feedback(success, latency_ms);
            let mut batch = vec![Feedback {
                indexer,
                deployment,
                success,
                latency_ms,
                latest_block,
            }];
            actor.handle_msgs(&mut batch);
            assert!(batch.is_empty());
            for blocked in 0..2 {
                let _guard = data.0[blocked].write();
                let snapshot = performance.latest();
                let snapshot = snapshot
                    .get(&(indexer, deployment))
                    .expect("feedback snapshot");
                assert_eq!(snapshot.latest_block, Some(100));
                let actual = snapshot.response.expected_performance();
                let expected = expected.expected_performance();
                assert_eq!(actual.latency_ms, expected.latency_ms);
                assert_eq!(actual.success_rate, expected.success_rate);
            }
        }
    }
}

#[cfg(test)]
mod observation_tests {
    use super::*;

    #[test]
    fn reports_at_or_above_the_known_block_refresh_the_observation_time() {
        let start = Instant::now();
        let mut snapshot = Snapshot::default();
        snapshot.observe(100, start);
        assert_eq!(snapshot.latest_block, Some(100));
        assert_eq!(snapshot.observed_at, Some(start));

        let later = start + Duration::from_secs(30);
        snapshot.observe(100, later);
        assert_eq!(snapshot.latest_block, Some(100));
        assert_eq!(snapshot.observed_at, Some(later));

        let latest = later + Duration::from_secs(30);
        snapshot.observe(105, latest);
        assert_eq!(snapshot.latest_block, Some(105));
        assert_eq!(snapshot.observed_at, Some(latest));
    }

    #[test]
    fn a_lower_report_keeps_the_known_block_and_its_observation_time() {
        let start = Instant::now();
        let mut snapshot = Snapshot::default();
        snapshot.observe(100, start);
        snapshot.observe(90, start + Duration::from_secs(30));
        assert_eq!(snapshot.latest_block, Some(100));
        assert_eq!(snapshot.observed_at, Some(start));
    }
}
