//! Which broker each fetchable partition is fetched from.

use crate::BrokerId;
use crate::consumer::state::{FetchTarget, PartitionKey};

/// Fetch targets grouped by the broker they are sent to.
#[derive(Debug, Default)]
pub(super) struct FetchRoutingPlan {
    /// Partitions per broker, in the order they were given.
    pub(super) by_broker: Vec<(BrokerId, Vec<FetchTarget>)>,
    /// Partitions with neither a live preferred replica nor a known leader.
    pub(super) skipped: Vec<PartitionKey>,
}

/// Where the metadata cache says a partition is led.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct Route {
    /// `None` while no leader is known.
    pub(super) leader: Option<BrokerId>,
    /// Sent as `current_leader_epoch`; `-1` when unknown.
    pub(super) leader_epoch: i32,
}

/// Route each target to its KIP-392 preferred replica when it has a live one,
/// else to its leader, as `route` reads the metadata cache (also where a
/// leader a broker reported in a fetch response, KIP-951, lands). Each
/// target carries its leader epoch from there. Each broker's list is sized
/// once, for every target left.
pub(super) fn build_fetch_routing_plan(
    targets: Vec<FetchTarget>,
    mut route: impl FnMut(&PartitionKey) -> Route,
) -> FetchRoutingPlan {
    let mut plan = FetchRoutingPlan::default();
    let total = targets.len();
    for (index, mut target) in targets.into_iter().enumerate() {
        let Route {
            leader,
            leader_epoch,
        } = route(&target.key);
        target.leader_epoch = leader_epoch;
        let Some(broker) = target.preferred_replica.or(leader) else {
            plan.skipped.push(target.key);
            continue;
        };
        match plan.by_broker.iter_mut().find(|(id, _)| *id == broker) {
            Some((_, targets)) => targets.push(target),
            None => {
                let mut targets = Vec::with_capacity(total - index);
                targets.push(target);
                plan.by_broker.push((broker, targets));
            }
        }
    }
    plan
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    fn target(topic: &str, partition: i32, preferred: Option<BrokerId>) -> FetchTarget {
        FetchTarget {
            key: (std::sync::Arc::from(topic), partition),
            version: 1,
            offset: 0,
            last_fetched_epoch: -1,
            preferred_replica: preferred,
            leader_epoch: -1,
        }
    }

    /// A route for each partition named: led by its broker, at epoch 5.
    fn leaders(entries: &[(&str, i32, BrokerId)]) -> impl FnMut(&PartitionKey) -> Route {
        let entries: Vec<(String, i32, BrokerId)> = entries
            .iter()
            .map(|(t, p, b)| (t.to_string(), *p, *b))
            .collect();
        move |key| {
            let leader = entries
                .iter()
                .find(|(t, p, _)| **t == *key.0 && *p == key.1)
                .map(|(_, _, b)| *b);
            Route {
                leader,
                leader_epoch: if leader.is_some() { 5 } else { -1 },
            }
        }
    }

    fn brokers_of(plan: &FetchRoutingPlan, topic: &str, partition: i32) -> Vec<BrokerId> {
        plan.by_broker
            .iter()
            .filter(|(_, targets)| {
                targets
                    .iter()
                    .any(|t| *t.key.0 == *topic && t.key.1 == partition)
            })
            .map(|(b, _)| *b)
            .collect()
    }

    #[test]
    fn the_leader_is_used_without_a_preferred_replica() {
        let plan = build_fetch_routing_plan(vec![target("t", 0, None)], leaders(&[("t", 0, 1)]));
        assert_eq!(brokers_of(&plan, "t", 0), vec![1]);
        assert!(plan.skipped.is_empty());
    }

    #[test]
    fn a_preferred_replica_wins_over_the_leader() {
        let plan = build_fetch_routing_plan(vec![target("t", 0, Some(3))], leaders(&[("t", 0, 1)]));
        assert_eq!(brokers_of(&plan, "t", 0), vec![3]);
    }

    #[test]
    fn a_partition_without_a_leader_is_skipped() {
        let plan = build_fetch_routing_plan(
            vec![target("t", 0, None), target("t", 1, None)],
            leaders(&[("t", 1, 2)]),
        );
        assert_eq!(plan.skipped, vec![(std::sync::Arc::from("t"), 0)]);
        assert_eq!(brokers_of(&plan, "t", 1), vec![2]);
    }

    #[test]
    fn partitions_of_one_broker_share_a_request_in_order() {
        let plan = build_fetch_routing_plan(
            vec![
                target("t", 2, None),
                target("t", 0, None),
                target("u", 1, Some(2)),
            ],
            leaders(&[("t", 0, 1), ("t", 2, 1), ("u", 1, 1)]),
        );
        assert_eq!(plan.by_broker.len(), 2);
        let (broker, targets) = &plan.by_broker[0];
        assert_eq!(*broker, 1);
        let keys: Vec<i32> = targets.iter().map(|t| t.key.1).collect();
        assert_eq!(keys, vec![2, 0]);
        assert!(
            targets.iter().all(|t| t.leader_epoch == 5),
            "each target carries its leader epoch"
        );
    }
}
