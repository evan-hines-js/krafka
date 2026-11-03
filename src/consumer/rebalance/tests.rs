#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

//! The join a heartbeat starts while a revocation runs.

use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use tokio::sync::Notify;

use crate::consumer::group::GroupState;
use crate::consumer::{ConsumerRebalanceListener, PartitionAssignmentStrategy, TopicPartition};
use crate::error::ErrorCode;
use crate::protocol::{
    ApiKey, ConsumerProtocolSubscription, decode_consumer_protocol_assignment,
    encode_consumer_protocol_subscription,
};
use crate::testing::{Control, FakeBroker};

const GROUP: &str = "held-beside-a-revocation";
const TOPIC: &str = "events";
const STANDING: &str = "standing-member";
/// Only bounds the waits of a test that has already failed.
const LONG: Duration = Duration::from_secs(30);

/// Holds a revocation until the test ends it, and counts finished rounds.
#[derive(Default)]
struct Revocation {
    began: Notify,
    end: Notify,
    held: AtomicBool,
    rounds: AtomicUsize,
    revoked: Mutex<Vec<i32>>,
}

impl ConsumerRebalanceListener for Revocation {
    async fn on_partitions_assigned(&self, _partitions: &[TopicPartition]) {
        self.rounds.fetch_add(1, Ordering::SeqCst);
    }

    async fn on_partitions_revoked(&self, partitions: &[TopicPartition]) {
        self.revoked
            .lock()
            .unwrap()
            .extend(partitions.iter().map(|tp| tp.partition));
        if self.held.load(Ordering::SeqCst) {
            self.began.notify_one();
            self.end.notified().await;
        }
    }
}

/// A subscription to the topic that owns nothing.
fn joining_member() -> Bytes {
    let mut metadata = BytesMut::new();
    let subscription = ConsumerProtocolSubscription::new(vec![TOPIC.to_string()]);
    encode_consumer_protocol_subscription(&subscription, &mut metadata).unwrap();
    metadata.freeze()
}

fn partitions_of(broker: &FakeBroker, member: &str) -> Vec<i32> {
    let assignment = broker
        .with_state(|state| state.groups[GROUP].assignments.get(member).cloned())
        .expect("the leader assigned to the member");
    let mut partitions: Vec<i32> = decode_consumer_protocol_assignment(&assignment)
        .unwrap()
        .assigned_partitions
        .into_iter()
        .flat_map(|tp| tp.partitions)
        .collect();
    partitions.sort_unstable();
    partitions
}

/// A heartbeat answered `REBALANCE_IN_PROGRESS` during a revocation starts a
/// join that reports what is being revoked, so the leader leaves it out for
/// everyone. The revocation's own request for a follow-up round lands while
/// the coordinator holds that join, and the join's success replaces it. The
/// round then gives this member what it has left: nothing is revoked, and only
/// what the join reported says the follow-up round is owed. Without that
/// round the group is Stable with the partitions held by nobody.
#[tokio::test]
async fn a_revocation_ending_while_the_next_join_is_held_still_starts_the_follow_up_round() {
    let broker = FakeBroker::start().await.unwrap();
    broker.create_topic(TOPIC, 4);
    let kafka = broker.kafka().connect().await.unwrap();
    let revocation = Arc::new(Revocation::default());
    let consumer = kafka
        .consumer(GROUP)
        .partition_assignment_strategy(PartitionAssignmentStrategy::CooperativeSticky)
        .heartbeat_interval(Duration::from_millis(10))
        .rebalance_listener(revocation.clone())
        .build()
        .await
        .unwrap();
    consumer.subscribe(&[TOPIC]).await.unwrap();
    while revocation.rounds.load(Ordering::SeqCst) < 1 {
        consumer.poll(Duration::from_millis(10)).await.unwrap();
    }
    let coordinator = consumer.group_coordinator.clone().unwrap();
    assert_eq!(consumer.assignment().await[TOPIC].len(), 4);

    // Round 2: a member arrives, and half the partitions start to move.
    broker.stand_in_group(GROUP, STANDING, joining_member());
    revocation.held.store(true, Ordering::SeqCst);
    broker.on_once(ApiKey::Heartbeat, |_| {
        Control::Error(ErrorCode::RebalanceInProgress)
    });

    let script = async {
        tokio::time::timeout(LONG, revocation.began.notified())
            .await
            .expect("round 2 revokes");
        let joins = broker.request_count(ApiKey::JoinGroup);
        assert_eq!(joins, 2, "round 1 and round 2");

        // Round 3: the heartbeat's join leaves with the revocation running.
        let join_released = Arc::new(Notify::new());
        let held = join_released.clone();
        broker.on_once(ApiKey::JoinGroup, move |_| Control::Hold(held.clone()));
        broker.on_once(ApiKey::Heartbeat, |_| {
            Control::Error(ErrorCode::RebalanceInProgress)
        });
        assert!(
            broker
                .wait_for_requests(ApiKey::JoinGroup, joins + 1, LONG)
                .await,
            "the heartbeat starts a join"
        );

        // The revocation ends inside the held join, and its request for a
        // follow-up round lands.
        revocation.end.notify_one();
        tokio::time::timeout(LONG, async {
            while coordinator.state() != GroupState::PreparingRebalance {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .expect("the revocation asks for a follow-up round");
        join_released.notify_one();
    };
    let polls = async {
        while revocation.rounds.load(Ordering::SeqCst) < 3 {
            consumer.poll(Duration::from_millis(10)).await.unwrap();
        }
    };
    tokio::join!(script, polls);

    // Round 3 is applied. It left out what it reported, and the member no
    // longer holds any of that, so it revoked nothing.
    let revoked = revocation.revoked.lock().unwrap().clone();
    assert_eq!(revoked.len(), 2, "round 2 revoked half: {revoked:?}");
    assert_eq!(
        partitions_of(&broker, STANDING),
        Vec::<i32>::new(),
        "round 3 withholds what the join reported"
    );
    assert!(
        coordinator.needs_rejoin() || coordinator.rejoin_in_flight(),
        "round 3 reported {revoked:?} and left them out, the group is {:?}, and nothing \
         owes the round that hands them over",
        coordinator.state()
    );

    // Round 4 hands them over.
    while revocation.rounds.load(Ordering::SeqCst) < 4 {
        consumer.poll(Duration::from_millis(10)).await.unwrap();
    }
    let mut revoked = revoked;
    revoked.sort_unstable();
    assert_eq!(partitions_of(&broker, STANDING), revoked);
    assert_eq!(
        broker.request_count(ApiKey::JoinGroup),
        4,
        "one join a round"
    );
    assert_eq!(
        broker.request_count(ApiKey::SyncGroup),
        4,
        "one sync a round"
    );
    revocation.held.store(false, Ordering::SeqCst);
    consumer.close().await.unwrap();
}
