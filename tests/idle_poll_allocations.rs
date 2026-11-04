//! What an idle poll costs the consumer, counted: the allocations a poll
//! that brings nothing makes on the consumer's thread (its connection's
//! tasks included), at 60 and at 600 assigned partitions of the same topics.
//! Its bookkeeping (gauges, lag, pauses, routing, the request) allocates
//! the same whatever the partition count: nothing is rebuilt per partition
//! per poll.
//!
//! The fake broker runs on a thread of its own, so its allocations are not
//! counted.
//!
//! Run: `cargo test --features test-broker --test idle_poll_allocations`
#![cfg(feature = "test-broker")]
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic, unsafe_code)]

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::time::Duration;

use krafka::testing::FakeBroker;

struct Counting;

thread_local! {
    static ALLOCS: Cell<usize> = const { Cell::new(0) };
}

// SAFETY: every method forwards to `System` with the caller's arguments; the
// counting touches only a thread-local `Cell` and never allocates.
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, l: Layout) -> *mut u8 {
        // `try_with`: the allocator may run during thread teardown.
        let _ = ALLOCS.try_with(|a| a.set(a.get() + 1));
        // SAFETY: forwarded unchanged from the caller, who upholds `GlobalAlloc`'s contract.
        unsafe { System.alloc(l) }
    }
    unsafe fn dealloc(&self, p: *mut u8, l: Layout) {
        // SAFETY: forwarded unchanged from the caller, who upholds `GlobalAlloc`'s contract.
        unsafe { System.dealloc(p, l) }
    }
    unsafe fn realloc(&self, p: *mut u8, l: Layout, n: usize) -> *mut u8 {
        let _ = ALLOCS.try_with(|a| a.set(a.get() + 1));
        // SAFETY: forwarded unchanged from the caller, who upholds `GlobalAlloc`'s contract.
        unsafe { System.realloc(p, l, n) }
    }
}

#[global_allocator]
static GLOBAL: Counting = Counting;

/// Topics assigned; the partition count varies per topic only.
const TOPICS: usize = 10;
/// Polls measured, after as many to warm up.
const POLLS: usize = 20;
/// How long the broker holds each idle fetch.
const WAIT: Duration = Duration::from_millis(5);

/// The allocations of [`POLLS`] idle polls on this thread and the fetches
/// they sent, every one of `TOPICS` topics of `partitions` partitions
/// assigned, sought and fetched to its end before.
fn idle_polls_allocate(partitions: i32) -> (usize, usize) {
    let (address, stop) = std::sync::mpsc::channel::<String>();
    let (stopped, until) = tokio::sync::oneshot::channel::<()>();
    let broker = std::thread::spawn(move || {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(async move {
                let broker = FakeBroker::start().await.unwrap();
                for topic in 0..TOPICS {
                    broker.create_topic(&format!("topic-{topic}"), partitions);
                }
                address.send(broker.bootstrap_servers()).unwrap();
                let _ = until.await;
            });
    });
    let bootstrap = stop.recv().unwrap();
    let counted = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async move {
            let consumer = krafka::Kafka::builder(bootstrap)
                .connect()
                .await
                .unwrap()
                .consumer_without_group()
                .fetch_max_wait(WAIT)
                .build()
                .await
                .unwrap();
            for topic in 0..TOPICS {
                let name = format!("topic-{topic}");
                consumer
                    .assign(&name, (0..partitions).collect())
                    .await
                    .unwrap();
                for partition in 0..partitions {
                    consumer.seek(&name, partition, 0).await.unwrap();
                }
            }
            for _ in 0..POLLS {
                assert!(consumer.poll(WAIT).await.unwrap().is_empty());
            }
            let fetches = consumer.metrics().consumer.fetches;
            let before = ALLOCS.with(Cell::get);
            for _ in 0..POLLS {
                assert!(consumer.poll(WAIT).await.unwrap().is_empty());
            }
            let counted = ALLOCS.with(Cell::get) - before;
            let fetched = consumer.metrics().consumer.fetches - fetches;
            consumer.close().await.unwrap();
            (counted, usize::try_from(fetched).unwrap())
        });
    stopped.send(()).unwrap();
    broker.join().unwrap();
    counted
}

#[test]
fn an_idle_poll_allocates_the_same_at_60_and_at_600_partitions() {
    let (at_60, fetches_60) = idle_polls_allocate(6);
    let (at_600, fetches_600) = idle_polls_allocate(60);
    // A poll whose held fetch the broker answers a moment early fetches
    // again, so allocations are compared per fetch, exactly.
    assert!(fetches_60 > 0 && fetches_600 > 0);
    assert_eq!(
        at_600 * fetches_60,
        at_60 * fetches_600,
        "idle polls allocated {at_60} times over {fetches_60} fetches at 60 \
         partitions and {at_600} over {fetches_600} at 600: an idle poll's \
         bookkeeping grows with the partitions"
    );
}
