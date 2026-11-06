//! What wakes a poll that has nothing to fetch, counted on a paused clock
//! against the in-memory fake broker.
//!
//! A poll whose partitions are all paused (or backing off, or whose buffer
//! is full of paused partitions' records) cannot change its answer until
//! something happens: a resume, a seek, an assignment change, a group event,
//! close, the end of a backoff, or its own deadline. It waits for those, by a
//! signal or a timer set to the instant they are due, and wakes once per
//! event; it never wakes on a period to look again.
//!
//! Each poll runs on a task of its own behind a future that counts how many
//! times it is polled: the first poll starts it, every later one is a wake.
//!
//! Run: `cargo test --features test-broker --test idle_poll_wakeups`
#![cfg(feature = "test-broker")]
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll};
use std::time::Duration;

use krafka::consumer::{AutoOffsetReset, Consumer, ConsumerBuilder, ConsumerRecord};
use krafka::error::ErrorCode;
use krafka::producer::Record;
use krafka::testing::{ApiKey, Control, FakeBroker, RecordedRequest};
use tokio::task::JoinHandle;
use tokio::time::Instant;

const MS: Duration = Duration::from_millis(1);
/// Every measured poll's timeout.
const TIMEOUT: Duration = Duration::from_secs(1);

/// A future that counts how often it is polled.
struct Counted<F> {
    inner: Pin<Box<F>>,
    polls: Arc<AtomicUsize>,
}

impl<F: Future> Future for Counted<F> {
    type Output = F::Output;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<F::Output> {
        self.polls.fetch_add(1, Ordering::SeqCst);
        self.inner.as_mut().poll(cx)
    }
}

/// One poll on a task of its own, and how often it was polled.
struct Measured {
    polls: Arc<AtomicUsize>,
    task: JoinHandle<(krafka::Result<Vec<ConsumerRecord>>, Instant)>,
}

impl Measured {
    fn start(consumer: &Arc<Consumer>, timeout: Duration) -> Self {
        let polls = Arc::new(AtomicUsize::new(0));
        let consumer = Arc::clone(consumer);
        let inner = Box::pin(async move {
            let polled = consumer.poll(timeout).await;
            (polled, Instant::now())
        });
        let task = tokio::spawn(Counted {
            inner,
            polls: Arc::clone(&polls),
        });
        Self { polls, task }
    }

    /// Wakes so far: every poll after the first.
    fn wakes(&self) -> usize {
        self.polls.load(Ordering::SeqCst).saturating_sub(1)
    }

    /// The poll's records, when it returned, and how often it woke.
    async fn finish(self) -> (Vec<ConsumerRecord>, Instant, usize) {
        let (records, at) = self.task.await.unwrap();
        let wakes = self.polls.load(Ordering::SeqCst) - 1;
        (records.unwrap(), at, wakes)
    }
}

fn broker(partitions: i32) -> FakeBroker {
    let broker = FakeBroker::start_in_memory(1);
    broker.create_topic("t", partitions);
    broker
}

async fn produce(broker: &FakeBroker, partition: i32, count: usize) {
    let producer = broker
        .kafka()
        .connect()
        .await
        .unwrap()
        .producer()
        .linger(Duration::ZERO)
        .build()
        .await
        .unwrap();
    for _ in 0..count {
        let handle = producer
            .enqueue(Record::new("t", b"v".to_vec()).partition(partition))
            .await
            .unwrap();
        let _ = handle.await.unwrap();
    }
    producer.close().await.unwrap();
}

/// A group-less consumer of `t`, every partition in `partitions` assigned
/// and sought to 0.
async fn assigned(
    broker: &FakeBroker,
    partitions: &[i32],
    configure: impl FnOnce(ConsumerBuilder) -> ConsumerBuilder,
) -> Arc<Consumer> {
    let builder = broker
        .kafka()
        .connect()
        .await
        .unwrap()
        .consumer_without_group();
    let consumer = configure(builder).build().await.unwrap();
    consumer.assign("t", partitions.to_vec()).await.unwrap();
    for &partition in partitions {
        consumer.seek("t", partition, 0).await.unwrap();
    }
    Arc::new(consumer)
}

async fn close(consumer: Arc<Consumer>) {
    Arc::into_inner(consumer)
        .expect("the poll task let go of the consumer")
        .close()
        .await
        .unwrap();
}

fn requests(broker: &FakeBroker, api: ApiKey) -> Vec<RecordedRequest> {
    broker
        .requests()
        .into_iter()
        .filter(|r| r.api_key == api)
        .collect()
}

/// A consumer whose every partition is paused wakes once in a poll: at its
/// deadline.
#[tokio::test(start_paused = true)]
async fn an_all_paused_poll_wakes_once_at_its_deadline() {
    let broker = broker(2);
    let consumer = assigned(&broker, &[0, 1], |b| b).await;
    consumer.pause("t", &[0, 1]).await;
    let fetches = broker.request_count(ApiKey::Fetch);
    let start = Instant::now();

    let (records, at, wakes) = Measured::start(&consumer, TIMEOUT).finish().await;

    assert!(records.is_empty());
    assert_eq!(at - start, TIMEOUT, "returned before or after its deadline");
    assert_eq!(
        wakes, 1,
        "an all-paused poll woke {wakes} times in {TIMEOUT:?}"
    );
    assert_eq!(broker.request_count(ApiKey::Fetch), fetches, "fetched");
    close(consumer).await;
}

/// A resume wakes an all-paused poll once, at the resume, and the poll
/// returns the record the resumed partition had buffered.
#[tokio::test(start_paused = true)]
async fn a_resume_wakes_an_all_paused_poll_once() {
    let broker = broker(1);
    produce(&broker, 0, 2).await;
    let consumer = assigned(&broker, &[0], |b| b.max_poll_records(1)).await;
    assert_eq!(consumer.poll(TIMEOUT).await.unwrap().len(), 1);
    consumer.pause("t", &[0]).await;
    let start = Instant::now();

    let poll = Measured::start(&consumer, TIMEOUT);
    tokio::time::sleep_until(start + Duration::from_millis(300)).await;
    assert_eq!(poll.wakes(), 0, "woke before the resume");
    consumer.resume("t", &[0]).await;
    let (records, at, wakes) = poll.finish().await;

    assert_eq!(records.len(), 1, "the buffered record");
    assert_eq!(records[0].offset, 1);
    assert_eq!(at - start, Duration::from_millis(300), "not at the resume");
    assert_eq!(wakes, 1, "woke {wakes} times for one resume");
    close(consumer).await;
}

/// Closing the consumer ends an all-paused poll in another task at once,
/// with one wake.
#[tokio::test(start_paused = true)]
async fn close_wakes_an_all_paused_poll_once() {
    let broker = broker(1);
    let consumer = assigned(&broker, &[0], |b| b).await;
    consumer.pause("t", &[0]).await;
    let start = Instant::now();

    let poll = Measured::start(&consumer, TIMEOUT);
    tokio::time::sleep_until(start + Duration::from_millis(300)).await;
    assert_eq!(poll.wakes(), 0, "woke before the close");
    consumer.close().await.unwrap();
    let (records, at, wakes) = poll.finish().await;

    assert!(records.is_empty());
    assert_eq!(at - start, Duration::from_millis(300), "not at the close");
    assert_eq!(wakes, 1, "woke {wakes} times for one close");
}

/// A poll whose buffer is full of a paused partition's records cannot fetch
/// for the others: it waits for a change, here none, until its deadline,
/// instead of returning at once to a caller that polls again.
#[tokio::test(start_paused = true)]
async fn a_poll_with_a_full_buffer_waits_for_its_deadline() {
    let broker = broker(2);
    produce(&broker, 0, 2).await;
    let consumer = assigned(&broker, &[0, 1], |b| {
        b.max_poll_records(1).max_buffered_records(1)
    })
    .await;
    assert_eq!(consumer.poll(TIMEOUT).await.unwrap().len(), 1);
    consumer.pause("t", &[0]).await;
    let start = Instant::now();

    let (records, at, wakes) = Measured::start(&consumer, TIMEOUT).finish().await;

    assert!(records.is_empty());
    assert_eq!(at - start, TIMEOUT, "returned before its deadline");
    assert_eq!(wakes, 1, "woke {wakes} times in {TIMEOUT:?}");
    close(consumer).await;
}

/// A partition backing off after a failed fetch wakes the poll when its
/// backoff (100 ms) ends, and the retry goes out at that instant; nothing
/// wakes the poll while it waits.
#[tokio::test(start_paused = true)]
async fn a_backoff_wakes_the_poll_when_it_ends_and_not_before() {
    let backoff = Duration::from_millis(100);
    let broker = broker(1);
    let consumer = assigned(&broker, &[0], |b| b).await;
    broker.on_once(ApiKey::Fetch, |_| Control::Disconnect);
    let fetches = broker.request_count(ApiKey::Fetch);

    let poll = Measured::start(&consumer, TIMEOUT);
    // Let the failed fetch happen and the partition back off.
    tokio::time::sleep(MS).await;
    let failed = requests(&broker, ApiKey::Fetch);
    assert_eq!(failed.len(), fetches + 1, "the first fetch");
    let failed_at = failed[fetches].at;
    let backing_off = poll.wakes();
    tokio::time::sleep(backoff - 2 * MS).await;
    assert_eq!(
        poll.wakes(),
        backing_off,
        "the poll woke while its only partition backed off"
    );
    assert_eq!(
        broker.request_count(ApiKey::Fetch),
        fetches + 1,
        "retried early"
    );
    tokio::time::sleep(2 * MS).await;
    let retried = requests(&broker, ApiKey::Fetch);
    assert_eq!(retried.len(), fetches + 2, "not retried after the backoff");
    assert_eq!(retried[fetches + 1].at - failed_at, backoff);
    let _ = poll.finish().await;
    close(consumer).await;
}

/// A group event during an all-paused poll wakes it, and the poll acts on
/// it then: the coordinator's REBALANCE_IN_PROGRESS on a heartbeat starts
/// the rejoin at that heartbeat, inside the same poll. The heartbeats that
/// changed nothing woke nothing.
#[tokio::test(start_paused = true)]
async fn a_rebalance_wakes_an_all_paused_poll_and_it_rejoins() {
    let heartbeat = Duration::from_secs(1);
    let broker = broker(1);
    let consumer = broker
        .kafka()
        .connect()
        .await
        .unwrap()
        .consumer("g")
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .heartbeat_interval(heartbeat)
        .build()
        .await
        .unwrap();
    consumer.subscribe(["t"]).await.unwrap();
    consumer.poll(Duration::from_millis(100)).await.unwrap();
    assert_eq!(consumer.assignment().await.get("t"), Some(&vec![0]));
    consumer.pause("t", &[0]).await;
    let consumer = Arc::new(consumer);
    let joins = broker.request_count(ApiKey::JoinGroup);
    let beats = broker.request_count(ApiKey::Heartbeat);
    let start = Instant::now();

    let poll = Measured::start(&consumer, Duration::from_secs(10));
    tokio::time::sleep_until(start + heartbeat * 3 + heartbeat / 2).await;
    assert!(
        broker.request_count(ApiKey::Heartbeat) >= beats + 3,
        "heartbeats while the poll waited"
    );
    assert_eq!(
        poll.wakes(),
        0,
        "heartbeats that changed nothing woke the poll"
    );
    assert_eq!(broker.request_count(ApiKey::JoinGroup), joins);

    broker.on_once(ApiKey::Heartbeat, |_| {
        Control::Error(ErrorCode::RebalanceInProgress)
    });
    tokio::time::sleep(heartbeat).await;
    let join = requests(&broker, ApiKey::JoinGroup);
    assert_eq!(
        join.len(),
        joins + 1,
        "the poll did not rejoin at the event"
    );
    let signalled = requests(&broker, ApiKey::Heartbeat)
        .into_iter()
        .rfind(|r| r.at <= join[joins].at)
        .unwrap();
    assert_eq!(
        join[joins].at, signalled.at,
        "the rejoin did not start at the heartbeat that asked for it"
    );
    let _ = poll.finish().await;
    close(consumer).await;
}

/// A partition without a leader backs off after each metadata refresh that
/// found none (100 ms doubling), so a poll refreshes at the end of each
/// backoff and wakes for nothing in between, instead of refreshing again at
/// once.
#[tokio::test(start_paused = true)]
async fn a_partition_without_a_leader_refreshes_once_per_backoff() {
    let broker = broker(1);
    assert!(broker.set_leader("t", 0, -1));
    let consumer = assigned(&broker, &[0], |b| b).await;
    let refreshes = broker.request_count(ApiKey::Metadata);

    let (records, _, _) = Measured::start(&consumer, TIMEOUT).finish().await;

    assert!(records.is_empty());
    let metadata = &requests(&broker, ApiKey::Metadata)[refreshes..];
    let gaps: Vec<Duration> = metadata.windows(2).map(|w| w[1].at - w[0].at).collect();
    // Four refreshes: at the start, then after backoffs of 100, 200 and
    // 400 ms. The first backoff is shorter than the metadata writer's own
    // jittered interval between refreshes (100 ms and up), which the second
    // refresh waits out too; the later ones are the partition's backoff
    // exactly.
    assert_eq!(gaps.len(), 3, "refreshes in a {TIMEOUT:?} poll: {gaps:?}");
    assert!(gaps[0] >= Duration::from_millis(100), "{gaps:?}");
    assert_eq!(
        gaps[1..],
        [200, 400].map(Duration::from_millis),
        "the backoff between refreshes for a leaderless partition"
    );
    close(consumer).await;
}
