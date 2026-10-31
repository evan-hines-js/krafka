//! What fences a consumer group member's offsets, on the fake broker: KIP-447
//! stable offsets (a taker's `OffsetFetch(require_stable)` waits while a
//! transaction holds an offset staged) and KIP-345 static membership (a
//! process joining with a member's `group.instance.id` takes its place
//! without a rebalance and fences the earlier incarnation).

#![cfg(feature = "test-broker")]
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::time::{Duration, Instant};

use krafka::consumer::{Consumer, ConsumerGroupMetadata, IsolationLevel};
use krafka::error::{ErrorCode, KrafkaError};
use krafka::producer::{TopicPartitionOffset, TransactionalProducer};
use krafka::testing::FakeBroker;
use krafka::{CloseOptions, GroupMembershipOperation};

const TOPIC: &str = "orders";
const GROUP: &str = "owners";

async fn broker() -> FakeBroker {
    let broker = FakeBroker::start().await.unwrap();
    broker.create_topic(TOPIC, 1);
    broker
}

async fn client(broker: &FakeBroker) -> krafka::Kafka {
    krafka::Kafka::builder(broker.bootstrap_servers())
        .request_timeout(Duration::from_secs(2))
        .connect_timeout(Duration::from_secs(2))
        .connect()
        .await
        .expect("connect")
}

async fn producer(broker: &FakeBroker, id: &str) -> TransactionalProducer {
    client(broker)
        .await
        .producer()
        .build_transactional(id)
        .await
        .expect("the producer")
}

/// A read-committed member of [`GROUP`] on [`TOPIC`], `instance` its static
/// id, polled until it holds its assignment.
async fn member(broker: &FakeBroker, instance: Option<&str>) -> Consumer {
    let mut builder = client(broker)
        .await
        .consumer(GROUP)
        .isolation_level(IsolationLevel::ReadCommitted)
        .heartbeat_interval(Duration::from_millis(100));
    if let Some(instance) = instance {
        builder = builder.group_instance_id(instance);
    }
    let consumer = builder.build().await.expect("the member");
    consumer.subscribe([TOPIC]).await.expect("subscribed");
    while consumer.assignment().await.is_empty() {
        consumer
            .poll(Duration::from_millis(50))
            .await
            .expect("a poll");
    }
    consumer
}

async fn metadata(consumer: &Consumer) -> ConsumerGroupMetadata {
    consumer.group_metadata().await.expect("joined")
}

fn offset(next: i64) -> [TopicPartitionOffset; 1] {
    [TopicPartitionOffset::new(TOPIC, 0, next)]
}

fn code(error: &KrafkaError) -> Option<ErrorCode> {
    match error {
        KrafkaError::Broker { code, .. } => Some(*code),
        _ => None,
    }
}

/// KIP-447: an offset a transaction staged is unstable until the
/// transaction ends. A stable fetch whose deadline has passed asks once and
/// is refused `UNSTABLE_OFFSET_COMMIT`; one that waits returns the
/// transaction's offset once it commits. A plain fetch under
/// `read_uncommitted` is not held.
#[tokio::test]
async fn a_staged_offset_is_unstable_until_its_transaction_commits() {
    let broker = broker().await;
    let consumer = member(&broker, None).await;
    let producer = producer(&broker, "owner-0").await;
    producer.begin().expect("begun");
    producer
        .send_offsets(&offset(5), &metadata(&consumer).await)
        .await
        .expect("staged");

    let refused = consumer
        .committed_stable(&[(TOPIC, 0)], Instant::now())
        .await
        .expect_err("staged, not ended");
    assert_eq!(code(&refused), Some(ErrorCode::UnstableOffsetCommit));

    let waiting =
        consumer.committed_stable(&[(TOPIC, 0)], Instant::now() + Duration::from_secs(60));
    let (stable, committed) = tokio::join!(waiting, async {
        producer.commit().await.expect("committed");
    });
    let _: () = committed;
    let stable = stable.expect("stable once committed");
    assert_eq!(stable[&(TOPIC.to_string(), 0)].offset, 5);
    assert_eq!(broker.committed_offset(GROUP, TOPIC, 0), Some(5));
}

/// An aborted transaction's staged offset is never committed: the stable
/// fetch that waited on it returns what the group held before.
#[tokio::test]
async fn a_staged_offset_aborted_leaves_the_committed_offset_as_it_was() {
    let broker = broker().await;
    let consumer = member(&broker, None).await;
    let producer = producer(&broker, "owner-0").await;
    producer.begin().expect("begun");
    producer
        .send_offsets(&offset(3), &metadata(&consumer).await)
        .await
        .expect("staged");
    producer.commit().await.expect("committed");
    producer.begin().expect("begun");
    producer
        .send_offsets(&offset(9), &metadata(&consumer).await)
        .await
        .expect("staged");

    let waiting =
        consumer.committed_stable(&[(TOPIC, 0)], Instant::now() + Duration::from_secs(60));
    let (stable, ()) = tokio::join!(waiting, async {
        producer.abort().await.expect("aborted");
    });
    assert_eq!(stable.expect("stable")[&(TOPIC.to_string(), 0)].offset, 3);
}

/// KIP-345: a process that joins with a static member's instance id while
/// the group is stable takes the member's place in the same generation (no
/// rebalance) with its assignment.
#[tokio::test]
async fn a_static_member_restarted_takes_its_place_back_without_a_rebalance() {
    let broker = broker().await;
    let first = member(&broker, Some("worker-0")).await;
    let before = metadata(&first).await;
    // The process dies: nothing is sent, the session has not lapsed.
    first
        .close_with(
            CloseOptions::default()
                .group_membership_operation(GroupMembershipOperation::RemainInGroup),
        )
        .await
        .expect("closed");

    let second = member(&broker, Some("worker-0")).await;
    let after = metadata(&second).await;
    assert_eq!(after.generation_id(), before.generation_id());
    assert_ne!(after.member_id(), before.member_id());
    assert_eq!(second.assignment().await[TOPIC], vec![0]);
}

/// KIP-345: the earlier incarnation of a static member, still running when a
/// process joins with its instance id, is fenced: its heartbeat is answered
/// `FENCED_INSTANCE_ID` (its poll fails with it), and so is a transaction's
/// offset commit under its metadata, which leaves the producer abortable.
#[tokio::test]
async fn a_second_member_with_the_same_instance_id_fences_the_first() {
    let broker = broker().await;
    let first = member(&broker, Some("worker-0")).await;
    let zombie = metadata(&first).await;
    let _second = member(&broker, Some("worker-0")).await;

    let producer = producer(&broker, "owner-0").await;
    producer.begin().expect("begun");
    let refused = producer
        .send_offsets(&offset(4), &zombie)
        .await
        .expect_err("a fenced incarnation's offsets");
    assert_eq!(code(&refused), Some(ErrorCode::FencedInstanceId));
    producer.abort().await.expect("abortable, not fatal");
    assert_eq!(broker.committed_offset(GROUP, TOPIC, 0), None);

    let fenced = loop {
        match first.poll(Duration::from_millis(50)).await {
            Ok(_) => continue,
            Err(error) => break error,
        }
    };
    assert_eq!(code(&fenced), Some(ErrorCode::FencedInstanceId));
}

/// A static member that closes with `LeaveGroup` leaves at once, its
/// instance id named, so its partitions move without waiting out the
/// session.
#[tokio::test]
async fn a_static_member_leaving_on_shutdown_names_its_instance() {
    let broker = broker().await;
    let consumer = member(&broker, Some("worker-0")).await;
    consumer
        .close_with(
            CloseOptions::default()
                .group_membership_operation(GroupMembershipOperation::LeaveGroup),
        )
        .await
        .expect("closed");
    let left = broker.leave_group_members();
    assert_eq!(left.len(), 1);
    assert_eq!(left[0].group_instance_id.as_deref(), Some("worker-0"));
    assert!(broker.with_state(|state| state.groups[GROUP].members.is_empty()));
}

/// KIP-447: a member that left its group is unknown to it, so a
/// transaction's offsets under its metadata are refused
/// `UNKNOWN_MEMBER_ID`, though the group is empty now.
#[tokio::test]
async fn a_member_that_left_commits_no_offsets() {
    let broker = broker().await;
    let consumer = member(&broker, None).await;
    let gone = metadata(&consumer).await;
    consumer.close().await.expect("closed");

    let producer = producer(&broker, "owner-0").await;
    producer.begin().expect("begun");
    let refused = producer
        .send_offsets(&offset(4), &gone)
        .await
        .expect_err("a departed member's offsets");
    assert_eq!(code(&refused), Some(ErrorCode::UnknownMemberId));
    producer.abort().await.expect("abortable, not fatal");
    assert_ne!(broker.committed_offset(GROUP, TOPIC, 0), Some(4));
}
