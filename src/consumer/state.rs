//! The consumer's subscription state: what it is subscribed to, which
//! partitions it owns, and where each of them stands.
//!
//! One [`SubscriptionState`] per consumer, behind one `parking_lot::Mutex`
//! that is never held across an `.await`. Network work happens outside the
//! lock; its result is applied in one short critical section, and only if the
//! partition's [`version`](PartitionRecord::version) still matches the one the
//! request was issued against.
//!
//! # Position
//!
//! A partition's position is the offset of the next record to hand to the
//! application. It advances only when records leave the consumer — in
//! [`SubscriptionState::complete_delivery`], after deserialization — never
//! when they are fetched and never when an offset is committed. Records that
//! have been fetched but not handed out sit in the partition's
//! [`CompletedFetch`], and a partition is fetched again only once that buffer
//! is empty, so the next fetch offset is always the buffered fetch's
//! [`next_offset`](CompletedFetch::next_offset) or, with nothing buffered, the
//! position.
//!
//! # Bookkeeping per change
//!
//! What the gauges report (paused, buffered, lag) and which partitions are
//! waiting for a position, a reset or a validation are kept as a running
//! [`Tally`]: every change to a partition's record recounts that one
//! partition, so reading them costs nothing per poll however many partitions
//! are assigned, and an idle poll that changes nothing counts nothing. The
//! same recount notes the partitions whose fetch position moved, for
//! [`SubscriptionState::take_moved`]. A partition's topic name is shared by
//! every key of that topic (one allocation per topic, [`PartitionKey`]), so
//! copying a key allocates nothing.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::Arc;
use std::time::Duration;
use tokio::time::Instant;

use ahash::{AHashMap as HashMap, AHashSet as HashSet};

use super::PartitionLag;
use super::config::{AutoOffsetReset, IsolationLevel};
use super::fetch_session::FetchSessionCache;
use super::record::{ConsumerRecord, TopicPartition};
use crate::error::{KrafkaError, Result};
use crate::{BrokerId, Offset, PartitionId};

/// A `(topic, partition)` key. The topic's name is shared by every key of
/// the topic the state holds, so a copy allocates nothing.
pub(super) type PartitionKey = (Arc<str>, PartitionId);

/// First wait after a failed or fenced attempt on a partition.
const BACKOFF_INITIAL: Duration = Duration::from_millis(100);
/// Longest wait between attempts on a failing partition.
const BACKOFF_MAX: Duration = Duration::from_secs(30);

/// Where a partition's position is reset to when it has none.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum OffsetReset {
    /// The log start offset.
    Earliest,
    /// The end of the log.
    Latest,
    /// The first record at or after now minus this duration; the end of the
    /// log when there is none.
    ByDuration(Duration),
}

impl OffsetReset {
    /// The `ListOffsets` timestamp that resolves this reset, reading the clock
    /// for [`ByDuration`](Self::ByDuration).
    pub(super) fn timestamp(self) -> i64 {
        match self {
            Self::Earliest => -2,
            Self::Latest => -1,
            Self::ByDuration(duration) => {
                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default();
                i64::try_from(now.saturating_sub(duration).as_millis()).unwrap_or(i64::MAX)
            }
        }
    }

    /// The reset `auto_offset_reset` asks for, or `None` when it forbids one.
    pub(super) fn from_auto(reset: AutoOffsetReset) -> Option<Self> {
        match reset {
            AutoOffsetReset::Earliest => Some(Self::Earliest),
            AutoOffsetReset::Latest => Some(Self::Latest),
            AutoOffsetReset::ByDuration(duration) => Some(Self::ByDuration(duration)),
            AutoOffsetReset::None => None,
        }
    }
}

/// One partition's fetched records that have not been handed out yet.
#[derive(Debug)]
pub(super) struct CompletedFetch {
    /// Decoded records, in offset order.
    pub(super) records: VecDeque<ConsumerRecord>,
    /// Where the next fetch starts once `records` is empty. It can lie beyond
    /// the last record: control batches, aborted transactions and batches the
    /// log cleaner emptied are walked through without producing records.
    pub(super) next_offset: Offset,
    /// Leader epoch of the batch `next_offset` sits just past.
    pub(super) next_epoch: Option<i32>,
}

/// Everything the consumer tracks for one assigned partition.
#[derive(Debug, Default)]
pub(super) struct PartitionRecord {
    /// Changes on every reposition and on (re)assignment, drawn from one
    /// consumer-wide counter so a partition that is revoked and assigned again
    /// never reuses a value. Results of requests issued against an older
    /// version are discarded.
    pub(super) version: u64,
    /// Offset of the next record to hand to the application. `None` while the
    /// partition waits for its committed offset or for a reset.
    pub(super) position: Option<Offset>,
    /// Leader epoch of the record just before `position` (KIP-320), sent as
    /// `last_fetched_epoch` and committed with the offset. `None` when the
    /// position came from a seek or a reset.
    pub(super) epoch: Option<i32>,
    /// A pending reset; set by `seek_to_beginning`, `seek_to_end`, an
    /// out-of-range fetch, or a new partition with no committed offset.
    pub(super) reset: Option<OffsetReset>,
    /// Whether `position` has been checked against the leader's log with
    /// `OffsetForLeaderEpoch` since it was last set.
    pub(super) validated: bool,
    /// Set by `pause()`; a paused partition is neither fetched nor delivered.
    pub(super) paused: bool,
    /// Records of this partition are on their way out in a [`Delivery`].
    pub(super) delivering: bool,
    /// Fetched records not handed out yet.
    pub(super) buffered: Option<CompletedFetch>,
    /// Latest high watermark from a fetch response.
    pub(super) high_watermark: Option<Offset>,
    /// When `high_watermark` was last updated.
    pub(super) watermark_updated_at: Option<Instant>,
    /// Latest last stable offset from a fetch response (Fetch v4+).
    pub(super) last_stable_offset: Option<Offset>,
    /// Latest log start offset from a fetch response (Fetch v5+).
    pub(super) log_start_offset: Option<Offset>,
    /// KIP-392 preferred read replica and when it expires.
    pub(super) preferred_replica: Option<(BrokerId, Instant)>,
    /// Earliest time of the next attempt on this partition and the current
    /// interval, after a failed offset lookup, a fenced fetch or a failed
    /// validation. Cleared by the first attempt that succeeds.
    pub(super) backoff: Option<(Instant, Duration)>,
    /// What this partition contributes to the [`Tally`], as of its last
    /// change.
    counted: Counted,
}

impl PartitionRecord {
    /// The highest offset this consumer may read: the last stable offset
    /// under `read_committed` when the broker reported one, else the high
    /// watermark.
    pub(super) fn readable_end_offset(&self, isolation_level: IsolationLevel) -> Option<Offset> {
        match isolation_level {
            IsolationLevel::ReadCommitted => self.last_stable_offset.or(self.high_watermark),
            IsolationLevel::ReadUncommitted => self.high_watermark,
        }
    }

    fn backoff_elapsed(&self, now: Instant) -> bool {
        self.backoff.is_none_or(|(next, _)| now >= next)
    }

    /// The next offset a fetch for this partition would start from.
    pub(super) fn fetch_position(&self) -> Option<Offset> {
        match &self.buffered {
            Some(fetch) => Some(fetch.next_offset),
            None => self.position,
        }
    }

    fn lag(&self, isolation_level: IsolationLevel) -> Option<u64> {
        let end = self.readable_end_offset(isolation_level)?;
        let position = self.position?;
        Some((end - position).max(0) as u64)
    }

    fn buffered_records(&self) -> usize {
        self.buffered
            .as_ref()
            .map_or(0, |fetch| fetch.records.len())
    }
}

/// What one partition's record contributes to the [`Tally`].
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct Counted {
    paused: bool,
    buffered: usize,
    /// Unpaused, not delivering, with records buffered.
    deliverable: bool,
    lag: Option<u64>,
    /// Neither a position nor a pending reset.
    needs_position: bool,
    awaiting_reset: bool,
    /// Positioned, unpaused and not validated.
    unvalidated: bool,
    fetch_position: Option<Offset>,
}

impl Counted {
    fn of(record: &PartitionRecord, isolation_level: IsolationLevel) -> Self {
        let buffered = record.buffered_records();
        Self {
            paused: record.paused,
            buffered,
            deliverable: !record.paused && !record.delivering && buffered > 0,
            lag: record.lag(isolation_level),
            needs_position: record.position.is_none() && record.reset.is_none(),
            awaiting_reset: record.reset.is_some(),
            unvalidated: !record.validated && !record.paused && record.position.is_some(),
            fetch_position: record.fetch_position(),
        }
    }
}

/// The sums of every assigned partition's [`Counted`].
#[derive(Debug, Default)]
struct Tally {
    paused: usize,
    buffered: usize,
    deliverable: usize,
    needs_position: usize,
    awaiting_reset: usize,
    unvalidated: usize,
    /// Total lag of the partitions whose lag is known; wide enough that no
    /// sum of `u64` lags overflows it.
    lag: u128,
    /// How many partitions have each known lag: the largest is the last.
    lags: BTreeMap<u64, usize>,
}

impl Tally {
    fn add(&mut self, counted: &Counted) {
        self.paused += usize::from(counted.paused);
        self.buffered += counted.buffered;
        self.deliverable += usize::from(counted.deliverable);
        self.needs_position += usize::from(counted.needs_position);
        self.awaiting_reset += usize::from(counted.awaiting_reset);
        self.unvalidated += usize::from(counted.unvalidated);
        if let Some(lag) = counted.lag {
            self.lag += u128::from(lag);
            *self.lags.entry(lag).or_default() += 1;
        }
    }

    fn remove(&mut self, counted: &Counted) {
        self.paused -= usize::from(counted.paused);
        self.buffered -= counted.buffered;
        self.deliverable -= usize::from(counted.deliverable);
        self.needs_position -= usize::from(counted.needs_position);
        self.awaiting_reset -= usize::from(counted.awaiting_reset);
        self.unvalidated -= usize::from(counted.unvalidated);
        if let Some(lag) = counted.lag {
            self.lag -= u128::from(lag);
            if let std::collections::btree_map::Entry::Occupied(mut entry) = self.lags.entry(lag) {
                *entry.get_mut() -= 1;
                if *entry.get() == 0 {
                    entry.remove();
                }
            }
        }
    }
}

/// A partition whose fetch position moved, as [`SubscriptionState::take_moved`]
/// reports it.
#[derive(Debug, Clone)]
pub(super) struct MovedPartition {
    pub(super) key: PartitionKey,
    /// Where its next fetch starts now.
    pub(super) fetch_position: Option<Offset>,
    /// Its cached readable end offset.
    pub(super) readable_end: Option<Offset>,
}

/// A partition about to be fetched, captured with the version the response
/// will be checked against.
#[derive(Debug, Clone)]
pub(super) struct FetchTarget {
    pub(super) key: PartitionKey,
    pub(super) version: u64,
    /// The offset the fetch starts at.
    pub(super) offset: Offset,
    /// Sent as `last_fetched_epoch`; `-1` when unknown.
    pub(super) last_fetched_epoch: i32,
    /// A live KIP-392 preferred replica, if any.
    pub(super) preferred_replica: Option<BrokerId>,
    /// The leader epoch the metadata cache knows, sent as
    /// `current_leader_epoch` (KIP-320); `-1` until the fetch is routed.
    pub(super) leader_epoch: i32,
}

/// A partition whose position has to be (re)established.
#[derive(Debug, Clone)]
pub(super) struct PendingPosition {
    pub(super) key: PartitionKey,
    pub(super) version: u64,
}

/// Per-partition bookkeeping of a [`Delivery`].
#[derive(Debug)]
struct DeliveredPartition {
    key: PartitionKey,
    version: u64,
    /// How many of the delivery's records belong to this partition. Records
    /// are grouped by partition in the delivery, in this order.
    count: usize,
}

/// Records taken out of the buffered fetches, on their way to the
/// application. Positions have not moved yet.
#[derive(Debug, Default)]
pub(super) struct Delivery {
    pub(super) records: Vec<ConsumerRecord>,
    partitions: Vec<DeliveredPartition>,
}

/// The consumer's mutable state. See the module documentation.
#[derive(Debug)]
pub(super) struct SubscriptionState {
    /// Topics subscribed to (group or group-less `subscribe`) or assigned
    /// with `assign`.
    pub(super) subscription: HashSet<String>,
    /// Topics a group-less `subscribe()` resolves from cluster metadata.
    pub(super) standalone_topics: HashSet<String>,
    /// When the group-less subscription was last resolved.
    pub(super) standalone_resolved: Option<Instant>,
    /// Assigned partitions; the key set is the assignment. Every record's
    /// change goes through [`Self::recount`].
    partitions: BTreeMap<PartitionKey, PartitionRecord>,
    /// The one shared name of every topic a key was made for.
    names: HashSet<Arc<str>>,
    /// The lag every partition counts is read under this level.
    isolation_level: IsolationLevel,
    tally: Tally,
    /// Partitions whose fetch position moved since [`Self::take_moved`].
    moved: BTreeSet<PartitionKey>,
    next_version: u64,
    /// KIP-227 fetch sessions, one per broker.
    pub(super) fetch_sessions: FetchSessionCache,
    /// Round-robin cursor over the fetchable partitions, advanced per fetch.
    rotation: usize,
    /// When the last auto-commit was attempted.
    pub(super) last_auto_commit: Instant,
}

impl Default for SubscriptionState {
    fn default() -> Self {
        Self::new(IsolationLevel::default())
    }
}

impl SubscriptionState {
    /// An empty state whose lag is read under `isolation_level`.
    pub(super) fn new(isolation_level: IsolationLevel) -> Self {
        Self {
            subscription: HashSet::new(),
            standalone_topics: HashSet::new(),
            standalone_resolved: None,
            partitions: BTreeMap::new(),
            names: HashSet::new(),
            isolation_level,
            tally: Tally::default(),
            moved: BTreeSet::new(),
            next_version: 0,
            fetch_sessions: FetchSessionCache::new(),
            rotation: 0,
            last_auto_commit: Instant::now(),
        }
    }

    fn bump(&mut self) -> u64 {
        self.next_version += 1;
        self.next_version
    }

    fn not_assigned(topic: &str, partition: PartitionId) -> KrafkaError {
        KrafkaError::illegal_state(format!(
            "no current assignment for partition {topic}-{partition}"
        ))
    }

    /// The key of `topic`'s `partition`: its topic's shared name when the
    /// state knows the topic, so no allocation.
    pub(super) fn key(&self, topic: &str, partition: PartitionId) -> PartitionKey {
        (self.topic(topic), partition)
    }

    /// `topic`'s shared name, or a new one for a topic never assigned.
    pub(super) fn topic(&self, topic: &str) -> Arc<str> {
        self.names
            .get(topic)
            .cloned()
            .unwrap_or_else(|| Arc::from(topic))
    }

    fn intern(&mut self, topic: &Arc<str>) -> Arc<str> {
        if let Some(name) = self.names.get(&**topic) {
            return name.clone();
        }
        self.names.insert(topic.clone());
        topic.clone()
    }

    /// Bring the [`Tally`] and the moved partitions up to date with one
    /// changed record.
    fn recount(
        tally: &mut Tally,
        moved: &mut BTreeSet<PartitionKey>,
        isolation_level: IsolationLevel,
        key: &PartitionKey,
        record: &mut PartitionRecord,
    ) {
        let now = Counted::of(record, isolation_level);
        let was = record.counted;
        if now == was {
            return;
        }
        tally.remove(&was);
        tally.add(&now);
        if now.fetch_position != was.fetch_position && !moved.contains(key) {
            moved.insert(key.clone());
        }
        record.counted = now;
    }

    /// Change an assigned partition's record, recounting it.
    fn update<R>(
        &mut self,
        key: &PartitionKey,
        change: impl FnOnce(&mut PartitionRecord) -> R,
    ) -> Option<R> {
        let record = self.partitions.get_mut(key)?;
        let out = change(record);
        Self::recount(
            &mut self.tally,
            &mut self.moved,
            self.isolation_level,
            key,
            record,
        );
        Some(out)
    }

    // ── assignment ──────────────────────────────────────────────────────

    /// The current assignment, partitions sorted.
    pub(super) fn assignment(&self) -> HashMap<String, Vec<PartitionId>> {
        let mut out: HashMap<String, Vec<PartitionId>> = HashMap::new();
        for (topic, partition) in self.partitions.keys() {
            out.entry(topic.to_string()).or_default().push(*partition);
        }
        out
    }

    /// Every assigned partition, in key order.
    pub(super) fn assigned_keys(&self) -> Vec<PartitionKey> {
        self.partitions.keys().cloned().collect()
    }

    pub(super) fn assigned_count(&self) -> usize {
        self.partitions.len()
    }

    pub(super) fn is_assigned(&self, key: &PartitionKey) -> bool {
        self.partitions.contains_key(key)
    }

    pub(super) fn partition(&self, key: &PartitionKey) -> Option<&PartitionRecord> {
        self.partitions.get(key)
    }

    /// Add partitions with no position. Already assigned partitions keep
    /// their state.
    pub(super) fn add_partitions(&mut self, keys: impl IntoIterator<Item = PartitionKey>) {
        for (topic, partition) in keys {
            let key = (self.intern(&topic), partition);
            if self.partitions.contains_key(&key) {
                continue;
            }
            let version = self.bump();
            let mut record = PartitionRecord {
                version,
                ..PartitionRecord::default()
            };
            Self::recount(
                &mut self.tally,
                &mut self.moved,
                self.isolation_level,
                &key,
                &mut record,
            );
            self.partitions.insert(key, record);
        }
    }

    /// Drop partitions and everything known about them, buffered records
    /// included.
    pub(super) fn remove_partitions<'a>(
        &mut self,
        keys: impl IntoIterator<Item = &'a PartitionKey>,
    ) {
        for key in keys {
            if let Some(record) = self.partitions.remove(key) {
                self.tally.remove(&record.counted);
            }
            self.moved.remove(key);
        }
    }

    /// Drop every partition.
    pub(super) fn clear_assignment(&mut self) {
        self.partitions.clear();
        self.tally = Tally::default();
        self.moved.clear();
    }

    // ── positions ───────────────────────────────────────────────────────

    /// Move an assigned partition to `offset`, dropping what is buffered for
    /// it.
    pub(super) fn seek(&mut self, key: &PartitionKey, offset: Offset) -> Result<()> {
        if !self.partitions.contains_key(key) {
            return Err(Self::not_assigned(&key.0, key.1));
        }
        let version = self.bump();
        self.update(key, |record| {
            record.version = version;
            record.position = Some(offset);
            record.epoch = None;
            record.reset = None;
            record.validated = false;
            record.buffered = None;
            record.delivering = false;
            record.backoff = None;
        });
        Ok(())
    }

    /// Ask for an assigned partition's position to be reset, dropping its
    /// position and what is buffered for it.
    pub(super) fn request_reset(&mut self, key: &PartitionKey, reset: OffsetReset) -> Result<()> {
        if !self.partitions.contains_key(key) {
            return Err(Self::not_assigned(&key.0, key.1));
        }
        let version = self.bump();
        self.update(key, |record| {
            record.version = version;
            record.position = None;
            record.epoch = None;
            record.reset = Some(reset);
            record.validated = false;
            record.buffered = None;
            record.delivering = false;
            record.backoff = None;
        });
        Ok(())
    }

    /// Reset a partition whose fetch was answered out of range, if nothing
    /// moved it since the fetch was issued.
    pub(super) fn reset_if_current(
        &mut self,
        key: &PartitionKey,
        version: u64,
        reset: OffsetReset,
    ) {
        if self
            .partitions
            .get(key)
            .is_some_and(|r| r.version == version)
        {
            let _ = self.request_reset(key, reset);
        }
    }

    /// Partitions that have neither a position nor a pending reset and are
    /// not backing off: newly assigned ones waiting for their committed
    /// offset.
    pub(super) fn partitions_needing_position(&self, now: Instant) -> Vec<PendingPosition> {
        if self.tally.needs_position == 0 {
            return Vec::new();
        }
        self.partitions
            .iter()
            .filter(|(_, r)| r.position.is_none() && r.reset.is_none() && r.backoff_elapsed(now))
            .map(|(key, r)| PendingPosition {
                key: key.clone(),
                version: r.version,
            })
            .collect()
    }

    /// Partitions with a pending reset that are not backing off.
    pub(super) fn partitions_awaiting_reset(
        &self,
        now: Instant,
    ) -> Vec<(PendingPosition, OffsetReset)> {
        if self.tally.awaiting_reset == 0 {
            return Vec::new();
        }
        self.partitions
            .iter()
            .filter(|(_, r)| r.backoff_elapsed(now))
            .filter_map(|(key, r)| {
                r.reset.map(|reset| {
                    (
                        PendingPosition {
                            key: key.clone(),
                            version: r.version,
                        },
                        reset,
                    )
                })
            })
            .collect()
    }

    /// Install a position found for a partition that has none, if the
    /// partition is still the one the lookup was made for.
    pub(super) fn set_initial_position(
        &mut self,
        key: &PartitionKey,
        version: u64,
        offset: Offset,
        epoch: Option<i32>,
    ) -> bool {
        self.update(key, |r| {
            if r.version != version || r.position.is_some() {
                return false;
            }
            r.position = Some(offset);
            r.epoch = epoch;
            r.reset = None;
            r.validated = false;
            r.backoff = None;
            true
        })
        .unwrap_or(false)
    }

    /// Record that a partition without a committed offset falls back to
    /// `reset`.
    pub(super) fn set_pending_reset(
        &mut self,
        key: &PartitionKey,
        version: u64,
        reset: OffsetReset,
    ) {
        self.update(key, |r| {
            if r.version == version && r.position.is_none() {
                r.reset = Some(reset);
            }
        });
    }

    /// Back off a partition after a failed attempt (100 ms doubling to 30 s).
    pub(super) fn back_off(&mut self, key: &PartitionKey, version: Option<u64>, now: Instant) {
        self.update(key, |r| {
            if version.is_none_or(|v| v == r.version) {
                let previous = r.backoff.map(|(_, d)| d).unwrap_or(Duration::ZERO);
                let next = (previous * 2).clamp(BACKOFF_INITIAL, BACKOFF_MAX);
                r.backoff = Some((now + next, next));
            }
        });
    }

    /// Rewind a partition to `end_offset` after the broker reported that its
    /// log diverged (KIP-320). Buffered records came from the discarded log.
    pub(super) fn truncate(
        &mut self,
        key: &PartitionKey,
        version: Option<u64>,
        end_offset: Offset,
    ) -> Option<Offset> {
        let current = self.partitions.get(key)?;
        if version.is_some_and(|v| v != current.version) {
            return None;
        }
        let old = current.position;
        let new_version = self.bump();
        self.update(key, |r| {
            r.version = new_version;
            r.position = Some(end_offset);
            r.epoch = None;
            r.reset = None;
            r.validated = true;
            r.buffered = None;
            r.delivering = false;
        })?;
        old
    }

    /// Mark a validated position.
    pub(super) fn mark_validated(&mut self, key: &PartitionKey, version: u64) {
        self.update(key, |r| {
            if r.version == version {
                r.validated = true;
            }
        });
    }

    /// Mark a position as needing validation, e.g. after a fenced fetch.
    pub(super) fn mark_unvalidated(&mut self, key: &PartitionKey) {
        self.update(key, |r| r.validated = false);
    }

    /// Positioned partitions whose position has not been validated, with the
    /// epoch to validate against.
    pub(super) fn unvalidated(
        &self,
        now: Instant,
    ) -> Vec<(PartitionKey, u64, Offset, Option<i32>)> {
        if self.tally.unvalidated == 0 {
            return Vec::new();
        }
        self.partitions
            .iter()
            .filter(|(_, r)| !r.validated && !r.paused && r.backoff_elapsed(now))
            .filter_map(|(key, r)| r.position.map(|p| (key.clone(), r.version, p, r.epoch)))
            .collect()
    }

    // ── pause ───────────────────────────────────────────────────────────

    /// Pause or resume assigned partitions; unassigned ones are ignored.
    /// Returns how many assigned partitions it applied to.
    pub(super) fn set_paused(
        &mut self,
        topic: &str,
        partitions: &[PartitionId],
        paused: bool,
    ) -> usize {
        let topic = self.topic(topic);
        let mut applied = 0;
        for &partition in partitions {
            if self
                .update(&(topic.clone(), partition), |r| r.paused = paused)
                .is_some()
            {
                applied += 1;
            }
        }
        applied
    }

    /// The paused partitions, built for the caller who asks: the gauge reads
    /// [`Self::paused_count`].
    pub(super) fn paused(&self) -> HashSet<(String, PartitionId)> {
        self.partitions
            .iter()
            .filter(|(_, r)| r.paused)
            .map(|((topic, partition), _)| (topic.to_string(), *partition))
            .collect()
    }

    /// How many assigned partitions are paused.
    pub(super) fn paused_count(&self) -> usize {
        self.tally.paused
    }

    // ── fetching ────────────────────────────────────────────────────────

    /// Number of fetched records not handed out yet.
    pub(super) fn buffered_count(&self) -> usize {
        self.tally.buffered
    }

    /// Whether any unpaused partition has records ready to hand out.
    pub(super) fn has_deliverable(&self) -> bool {
        self.tally.deliverable > 0
    }

    /// The partitions a fetch may be issued for, rotated by one per call so
    /// that the head of the list changes from fetch to fetch (the response
    /// size limits are consumed in request order).
    ///
    /// A partition is fetchable when it is positioned, unpaused, not backing
    /// off, and has nothing buffered or in delivery.
    pub(super) fn fetch_targets(&mut self, now: Instant) -> Vec<FetchTarget> {
        let unpaused = self.partitions.len() - self.tally.paused;
        if unpaused == 0 {
            // A consumer whose every partition is paused (a group member
            // whose records others fetch) fetches nothing, and nothing is
            // scanned to find that out.
            return Vec::new();
        }
        let mut targets: Vec<FetchTarget> = Vec::with_capacity(unpaused);
        for (key, r) in self.partitions.iter_mut() {
            if let Some((_, expiry)) = r.preferred_replica
                && now >= expiry
            {
                r.preferred_replica = None;
            }
            let Some(offset) = r.position else { continue };
            if r.paused
                || r.delivering
                || r.buffered.is_some()
                || r.reset.is_some()
                || !r.backoff_elapsed(now)
            {
                continue;
            }
            targets.push(FetchTarget {
                key: key.clone(),
                version: r.version,
                offset,
                last_fetched_epoch: r.epoch.unwrap_or(-1),
                preferred_replica: r.preferred_replica.map(|(id, _)| id),
                leader_epoch: -1,
            });
        }
        if !targets.is_empty() {
            let turn = self.rotation % targets.len();
            self.rotation = self.rotation.wrapping_add(1);
            targets.rotate_left(turn);
        }
        targets
    }

    /// When the next unpaused partition's backoff ends, if one is backing
    /// off: the one instant at which time alone makes a partition
    /// fetchable, positioned or validated again. A consumer whose every
    /// partition is paused has none, and nothing is scanned to find that out.
    pub(super) fn next_backoff_end(&self, now: Instant) -> Option<Instant> {
        if self.partitions.len() == self.tally.paused {
            return None;
        }
        self.partitions
            .values()
            .filter(|r| !r.paused)
            .filter_map(|r| r.backoff.map(|(next, _)| next))
            .filter(|&next| next > now)
            .min()
    }

    /// Install a fetch result, if the partition is still where the fetch was
    /// issued from. A result with no records but a later next offset (control
    /// batches, compaction gaps) moves the position directly. Returns whether
    /// it was applied.
    pub(super) fn install_fetch(&mut self, target: &FetchTarget, fetch: CompletedFetch) -> bool {
        self.update(&target.key, |r| {
            if r.version != target.version
                || r.position != Some(target.offset)
                || r.buffered.is_some()
                || r.delivering
            {
                return false;
            }
            r.backoff = None;
            if fetch.records.is_empty() {
                if fetch.next_offset > target.offset {
                    r.position = Some(fetch.next_offset);
                    if fetch.next_epoch.is_some() {
                        r.epoch = fetch.next_epoch;
                    }
                }
            } else {
                r.buffered = Some(fetch);
            }
            true
        })
        .unwrap_or(false)
    }

    /// Clear a partition's backoff after an unfenced fetch.
    pub(super) fn clear_backoff(&mut self, key: &PartitionKey, version: u64) {
        self.update(key, |r| {
            if r.version == version {
                r.backoff = None;
            }
        });
    }

    /// Record the watermarks a fetch response reported.
    pub(super) fn update_watermarks(
        &mut self,
        key: &PartitionKey,
        high_watermark: Option<Offset>,
        last_stable_offset: Option<Offset>,
        log_start_offset: Option<Offset>,
        now: Instant,
    ) {
        self.update(key, |r| {
            if let Some(hw) = high_watermark {
                r.high_watermark = Some(hw);
                r.watermark_updated_at = Some(now);
            }
            if last_stable_offset.is_some() {
                r.last_stable_offset = last_stable_offset;
            }
            if log_start_offset.is_some() {
                r.log_start_offset = log_start_offset;
            }
        });
    }

    /// Set or clear a partition's preferred read replica (KIP-392).
    pub(super) fn set_preferred_replica(
        &mut self,
        key: &PartitionKey,
        replica: Option<(BrokerId, Instant)>,
    ) {
        self.update(key, |r| r.preferred_replica = replica);
    }

    // ── delivery ────────────────────────────────────────────────────────

    /// Take up to `max` buffered records out for delivery, without moving any
    /// position. The partitions involved are marked as delivering until
    /// [`complete_delivery`](Self::complete_delivery) or
    /// [`abort_delivery`](Self::abort_delivery).
    pub(super) fn take_delivery(&mut self, max: usize) -> Option<Delivery> {
        if max == 0 || self.tally.deliverable == 0 {
            return None;
        }
        let mut delivery = Delivery::default();
        let ready: Vec<PartitionKey> = self
            .partitions
            .iter()
            .filter(|(_, r)| {
                !r.paused
                    && !r.delivering
                    && r.buffered.as_ref().is_some_and(|f| !f.records.is_empty())
            })
            .map(|(key, _)| key.clone())
            .collect();
        if ready.is_empty() {
            return None;
        }
        let start = self.rotation % ready.len();
        self.rotation = self.rotation.wrapping_add(1);
        for key in ready.iter().cycle().skip(start).take(ready.len()) {
            let remaining = max - delivery.records.len();
            if remaining == 0 {
                break;
            }
            let Some(r) = self.partitions.get_mut(key) else {
                continue;
            };
            let Some(fetch) = r.buffered.as_mut() else {
                continue;
            };
            let take = remaining.min(fetch.records.len());
            delivery.records.extend(fetch.records.drain(..take));
            r.delivering = true;
            delivery.partitions.push(DeliveredPartition {
                key: key.clone(),
                version: r.version,
                count: take,
            });
            Self::recount(
                &mut self.tally,
                &mut self.moved,
                self.isolation_level,
                key,
                r,
            );
        }
        Some(delivery)
    }

    /// Hand out the first `handed_out` records of `delivery` and put the rest
    /// back. Positions advance past what is handed out, in this one step.
    ///
    /// Records of a partition that was repositioned or revoked since the
    /// delivery was taken are dropped: they describe a place in the log the
    /// consumer has left.
    pub(super) fn complete_delivery(
        &mut self,
        delivery: Delivery,
        handed_out: usize,
    ) -> Vec<ConsumerRecord> {
        let Delivery {
            records,
            partitions,
        } = delivery;
        let mut records = records.into_iter();
        let mut out = Vec::with_capacity(handed_out.min(records.len()));
        let mut remaining_out = handed_out;
        for part in partitions {
            let chunk: Vec<ConsumerRecord> = records.by_ref().take(part.count).collect();
            let deliver = remaining_out.min(chunk.len());
            remaining_out -= deliver;

            let Some(r) = self.partitions.get_mut(&part.key) else {
                continue;
            };
            if r.version != part.version {
                continue;
            }
            r.delivering = false;
            let mut chunk = chunk.into_iter();
            let delivered: Vec<ConsumerRecord> = chunk.by_ref().take(deliver).collect();
            let put_back: Vec<ConsumerRecord> = chunk.collect();

            if let Some(last) = delivered.last() {
                r.position = Some(last.offset.saturating_add(1));
                if last.leader_epoch.is_some() {
                    r.epoch = last.leader_epoch;
                }
            }
            if let Some(fetch) = r.buffered.as_mut() {
                for record in put_back.into_iter().rev() {
                    fetch.records.push_front(record);
                }
                if fetch.records.is_empty() {
                    let next = fetch.next_offset;
                    let next_epoch = fetch.next_epoch;
                    r.buffered = None;
                    if r.position.is_none_or(|p| next > p) {
                        r.position = Some(next);
                        if next_epoch.is_some() {
                            r.epoch = next_epoch;
                        }
                    }
                }
            }
            Self::recount(
                &mut self.tally,
                &mut self.moved,
                self.isolation_level,
                &part.key,
                r,
            );
            out.extend(delivered);
        }
        out
    }

    /// Put every record of `delivery` back where it was taken from.
    pub(super) fn abort_delivery(&mut self, delivery: Delivery) {
        let _ = self.complete_delivery(delivery, 0);
    }

    // ── reading ─────────────────────────────────────────────────────────

    /// Positions to commit: every assigned, positioned partition.
    pub(super) fn committable(&self) -> Vec<(PartitionKey, Offset, Option<i32>)> {
        self.partitions
            .iter()
            .filter_map(|(key, r)| r.position.map(|p| (key.clone(), p, r.epoch)))
            .collect()
    }

    /// `(total, max)` lag over the partitions whose position and end offset
    /// are known.
    pub(super) fn aggregate_lag(&self) -> (u64, u64) {
        (
            u64::try_from(self.tally.lag).unwrap_or(u64::MAX),
            self.tally.lags.keys().next_back().copied().unwrap_or(0),
        )
    }

    /// The partitions whose fetch position moved since the last call, each
    /// with where its next fetch starts and its cached readable end offset;
    /// the partitions revoked since are left out. Costs what moved, never
    /// what is assigned.
    pub(super) fn take_moved(&mut self) -> Vec<MovedPartition> {
        let moved = std::mem::take(&mut self.moved);
        moved
            .into_iter()
            .filter_map(|key| {
                let record = self.partitions.get(&key)?;
                Some(MovedPartition {
                    fetch_position: record.fetch_position(),
                    readable_end: record.readable_end_offset(self.isolation_level),
                    key,
                })
            })
            .collect()
    }

    /// Per-partition lag and the partitions whose watermark is older than
    /// `threshold`.
    pub(super) fn lag_report(
        &self,
        now: Instant,
        threshold: Duration,
    ) -> HashMap<TopicPartition, PartitionLag> {
        self.partitions
            .iter()
            .map(|((topic, partition), r)| {
                let stale = r
                    .watermark_updated_at
                    .is_none_or(|t| now.saturating_duration_since(t) > threshold);
                (
                    TopicPartition::new(topic.to_string(), *partition),
                    PartitionLag {
                        position: r.position,
                        log_start_offset: r.log_start_offset,
                        high_watermark: r.high_watermark,
                        last_stable_offset: r.last_stable_offset,
                        lag: r.lag(self.isolation_level),
                        stale,
                    },
                )
            })
            .collect()
    }

    /// Recount every partition from scratch and compare with the running
    /// tally: what the gauges read must be what a full scan finds.
    #[cfg(test)]
    fn tally_matches_a_scan(&self) -> bool {
        let mut scan = Tally::default();
        for record in self.partitions.values() {
            scan.add(&Counted::of(record, self.isolation_level));
        }
        scan.paused == self.tally.paused
            && scan.buffered == self.tally.buffered
            && scan.deliverable == self.tally.deliverable
            && scan.needs_position == self.tally.needs_position
            && scan.awaiting_reset == self.tally.awaiting_reset
            && scan.unvalidated == self.tally.unvalidated
            && scan.lag == self.tally.lag
            && scan.lags == self.tally.lags
    }
}

/// A [`Delivery`] that puts its records back when dropped unfinished.
///
/// This is what makes a dropped `poll()` future lose nothing: the records
/// travel out of the buffer inside the guard, and only
/// [`finish`](Self::finish) — synchronous, after deserialization — advances
/// the positions.
pub(super) struct DeliveryGuard<'a> {
    state: &'a parking_lot::Mutex<SubscriptionState>,
    delivery: Option<Delivery>,
}

impl<'a> DeliveryGuard<'a> {
    pub(super) fn new(
        state: &'a parking_lot::Mutex<SubscriptionState>,
        delivery: Delivery,
    ) -> Self {
        Self {
            state,
            delivery: Some(delivery),
        }
    }

    /// The records on their way out.
    pub(super) fn records_mut(&mut self) -> &mut [ConsumerRecord] {
        match self.delivery.as_mut() {
            Some(delivery) => &mut delivery.records,
            None => &mut [],
        }
    }

    /// Hand out the first `handed_out` records and put the rest back.
    pub(super) fn finish(mut self, handed_out: usize) -> Vec<ConsumerRecord> {
        match self.delivery.take() {
            Some(delivery) => self.state.lock().complete_delivery(delivery, handed_out),
            None => Vec::new(),
        }
    }
}

impl Drop for DeliveryGuard<'_> {
    fn drop(&mut self) {
        if let Some(delivery) = self.delivery.take() {
            self.state.lock().abort_delivery(delivery);
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    fn key(topic: &str, partition: PartitionId) -> PartitionKey {
        (Arc::from(topic), partition)
    }

    fn record(topic: &str, partition: PartitionId, offset: Offset) -> ConsumerRecord {
        let mut record = ConsumerRecord::new(topic, partition, offset, None, None);
        record.leader_epoch = Some(3);
        record
    }

    fn fetched(
        topic: &str,
        partition: PartitionId,
        offsets: std::ops::Range<Offset>,
    ) -> CompletedFetch {
        CompletedFetch {
            next_offset: offsets.end,
            records: offsets.map(|o| record(topic, partition, o)).collect(),
            next_epoch: Some(3),
        }
    }

    /// A state with `t-0` at position 0 and records 0..10 buffered.
    fn buffered_state() -> SubscriptionState {
        let mut state = SubscriptionState::default();
        state.add_partitions([key("t", 0)]);
        state.seek(&key("t", 0), 0).unwrap();
        let targets = state.fetch_targets(Instant::now());
        assert_eq!(targets.len(), 1);
        assert!(state.install_fetch(&targets[0], fetched("t", 0, 0..10)));
        state
    }

    #[test]
    fn fetching_does_not_move_the_position() {
        let state = buffered_state();
        let r = state.partition(&key("t", 0)).unwrap();
        assert_eq!(r.position, Some(0));
        assert_eq!(r.fetch_position(), Some(10));
    }

    #[test]
    fn delivery_advances_the_position_only_when_finished() {
        let mut state = buffered_state();
        let delivery = state.take_delivery(4).unwrap();
        assert_eq!(delivery.records.len(), 4);
        assert_eq!(state.partition(&key("t", 0)).unwrap().position, Some(0));

        let out = state.complete_delivery(delivery, 4);
        assert_eq!(
            out.iter().map(|r| r.offset).collect::<Vec<_>>(),
            vec![0, 1, 2, 3]
        );
        let r = state.partition(&key("t", 0)).unwrap();
        assert_eq!(r.position, Some(4));
        assert_eq!(r.epoch, Some(3));
    }

    #[test]
    fn an_aborted_delivery_puts_every_record_back() {
        let mut state = buffered_state();
        let delivery = state.take_delivery(4).unwrap();
        state.abort_delivery(delivery);
        let r = state.partition(&key("t", 0)).unwrap();
        assert_eq!(r.position, Some(0));
        let again = state.take_delivery(100).unwrap();
        assert_eq!(
            again.records.iter().map(|r| r.offset).collect::<Vec<_>>(),
            (0..10).collect::<Vec<_>>()
        );
    }

    #[test]
    fn a_dropped_guard_puts_the_records_back() {
        let state = parking_lot::Mutex::new(buffered_state());
        let delivery = state.lock().take_delivery(5).unwrap();
        drop(DeliveryGuard::new(&state, delivery));
        let mut state = state.into_inner();
        assert_eq!(state.partition(&key("t", 0)).unwrap().position, Some(0));
        assert_eq!(state.take_delivery(100).unwrap().records.len(), 10);
    }

    #[test]
    fn a_partial_hand_out_keeps_the_rest_in_order() {
        let mut state = buffered_state();
        let delivery = state.take_delivery(6).unwrap();
        let out = state.complete_delivery(delivery, 2);
        assert_eq!(out.len(), 2);
        assert_eq!(state.partition(&key("t", 0)).unwrap().position, Some(2));
        let next = state.take_delivery(100).unwrap();
        assert_eq!(
            next.records.iter().map(|r| r.offset).collect::<Vec<_>>(),
            (2..10).collect::<Vec<_>>()
        );
    }

    #[test]
    fn draining_the_buffer_moves_to_the_next_fetch_offset() {
        let mut state = SubscriptionState::default();
        state.add_partitions([key("t", 0)]);
        state.seek(&key("t", 0), 0).unwrap();
        let target = state.fetch_targets(Instant::now()).remove(0);
        // Records 0..3; the batch walk reached 7 (a compacted tail).
        let mut fetch = fetched("t", 0, 0..3);
        fetch.next_offset = 7;
        assert!(state.install_fetch(&target, fetch));
        let delivery = state.take_delivery(10).unwrap();
        let _ = state.complete_delivery(delivery, 3);
        let r = state.partition(&key("t", 0)).unwrap();
        assert_eq!(r.position, Some(7));
        assert!(r.buffered.is_none());
    }

    #[test]
    fn a_partition_with_buffered_records_is_not_fetched() {
        let mut state = buffered_state();
        assert!(state.fetch_targets(Instant::now()).is_empty());
        let delivery = state.take_delivery(100).unwrap();
        assert!(
            state.fetch_targets(Instant::now()).is_empty(),
            "nor while delivering"
        );
        let _ = state.complete_delivery(delivery, 10);
        let targets = state.fetch_targets(Instant::now());
        assert_eq!(targets.len(), 1);
        assert_eq!(targets[0].offset, 10);
    }

    #[test]
    fn a_seek_drops_the_buffer_and_a_stale_delivery() {
        let mut state = buffered_state();
        let delivery = state.take_delivery(3).unwrap();
        state.seek(&key("t", 0), 100).unwrap();
        let out = state.complete_delivery(delivery, 3);
        assert!(
            out.is_empty(),
            "records from before the seek are not handed out"
        );
        let r = state.partition(&key("t", 0)).unwrap();
        assert_eq!(r.position, Some(100));
        assert!(r.buffered.is_none());
    }

    #[test]
    fn a_stale_fetch_is_not_installed() {
        let mut state = SubscriptionState::default();
        state.add_partitions([key("t", 0)]);
        state.seek(&key("t", 0), 0).unwrap();
        let target = state.fetch_targets(Instant::now()).remove(0);
        state.seek(&key("t", 0), 50).unwrap();
        assert!(!state.install_fetch(&target, fetched("t", 0, 0..10)));
        assert_eq!(state.partition(&key("t", 0)).unwrap().position, Some(50));
    }

    #[test]
    fn a_fetch_for_a_revoked_and_reassigned_partition_is_not_installed() {
        let mut state = SubscriptionState::default();
        state.add_partitions([key("t", 0)]);
        state.seek(&key("t", 0), 0).unwrap();
        let target = state.fetch_targets(Instant::now()).remove(0);
        state.remove_partitions([&key("t", 0)]);
        state.add_partitions([key("t", 0)]);
        assert!(!state.install_fetch(&target, fetched("t", 0, 0..10)));
    }

    #[test]
    fn seek_rejects_an_unassigned_partition_and_stores_nothing() {
        let mut state = SubscriptionState::default();
        assert!(state.seek(&key("t", 0), 7).is_err());
        assert!(
            state
                .request_reset(&key("t", 0), OffsetReset::Earliest)
                .is_err()
        );
        state.add_partitions([key("t", 0)]);
        assert_eq!(state.partition(&key("t", 0)).unwrap().position, None);
    }

    #[test]
    fn a_new_partition_waits_for_its_position() {
        let mut state = SubscriptionState::default();
        state.add_partitions([key("t", 0)]);
        let pending = state.partitions_needing_position(Instant::now());
        assert_eq!(pending.len(), 1);
        assert!(state.fetch_targets(Instant::now()).is_empty());
        assert!(state.set_initial_position(&key("t", 0), pending[0].version, 42, Some(5)));
        let r = state.partition(&key("t", 0)).unwrap();
        assert_eq!((r.position, r.epoch), (Some(42), Some(5)));
    }

    #[test]
    fn a_reset_lookup_for_a_moved_partition_is_discarded() {
        let mut state = SubscriptionState::default();
        state.add_partitions([key("t", 0)]);
        state
            .request_reset(&key("t", 0), OffsetReset::Earliest)
            .unwrap();
        let (pending, reset) = state.partitions_awaiting_reset(Instant::now()).remove(0);
        assert_eq!(reset, OffsetReset::Earliest);
        state.seek(&key("t", 0), 9).unwrap();
        assert!(!state.set_initial_position(&pending.key, pending.version, 0, None));
        assert_eq!(state.partition(&key("t", 0)).unwrap().position, Some(9));
    }

    #[test]
    fn a_paused_partition_keeps_its_buffer_and_position() {
        let mut state = buffered_state();
        state.set_paused("t", &[0], true);
        assert!(state.take_delivery(10).is_none());
        assert_eq!(state.buffered_count(), 10);
        state.set_paused("t", &[0], false);
        assert_eq!(state.take_delivery(10).unwrap().records.len(), 10);
    }

    #[test]
    fn backoff_doubles_and_is_capped() {
        let mut state = SubscriptionState::default();
        state.add_partitions([key("t", 0)]);
        let now = Instant::now();
        let mut waits = Vec::new();
        for _ in 0..12 {
            state.back_off(&key("t", 0), None, now);
            waits.push(state.partition(&key("t", 0)).unwrap().backoff.unwrap().1);
        }
        assert_eq!(waits[0], Duration::from_millis(100));
        assert_eq!(waits[1], Duration::from_millis(200));
        assert_eq!(*waits.last().unwrap(), Duration::from_secs(30));
        assert!(state.partitions_needing_position(now).is_empty());
    }

    #[test]
    fn lag_is_measured_from_the_delivered_position() {
        let mut state = buffered_state();
        let now = Instant::now();
        state.update_watermarks(&key("t", 0), Some(10), None, None, now);
        let lag = |state: &SubscriptionState| {
            state.lag_report(now, Duration::from_secs(60))[&TopicPartition::new("t", 0)].clone()
        };
        assert_eq!(lag(&state).lag, Some(10));
        assert_eq!(lag(&state).high_watermark, Some(10));
        assert!(!lag(&state).stale);
        let delivery = state.take_delivery(4).unwrap();
        let _ = state.complete_delivery(delivery, 4);
        assert_eq!(lag(&state).lag, Some(6));
        assert_eq!(lag(&state).position, Some(4));
    }

    #[test]
    fn readable_end_offset_follows_the_isolation_level() {
        let record = PartitionRecord {
            high_watermark: Some(100),
            last_stable_offset: Some(80),
            ..PartitionRecord::default()
        };
        assert_eq!(
            record.readable_end_offset(IsolationLevel::ReadCommitted),
            Some(80)
        );
        assert_eq!(
            record.readable_end_offset(IsolationLevel::ReadUncommitted),
            Some(100)
        );
        let no_lso = PartitionRecord {
            high_watermark: Some(100),
            ..PartitionRecord::default()
        };
        assert_eq!(
            no_lso.readable_end_offset(IsolationLevel::ReadCommitted),
            Some(100)
        );
    }

    #[test]
    fn delivery_rotates_across_partitions() {
        let mut state = SubscriptionState::default();
        state.add_partitions([key("t", 0), key("t", 1)]);
        for p in 0..2 {
            state.seek(&key("t", p), 0).unwrap();
        }
        for target in state.fetch_targets(Instant::now()) {
            let p = target.key.1;
            assert!(state.install_fetch(&target, fetched("t", p, 0..5)));
        }
        let delivery = state.take_delivery(5).unwrap();
        let first: HashSet<PartitionId> = delivery.records.iter().map(|r| r.partition).collect();
        assert_eq!(first.len(), 1, "the cap is filled from one partition first");
        let _ = state.complete_delivery(delivery, 5);
        let next = state.take_delivery(5).unwrap();
        assert!(next.records.iter().all(|r| !first.contains(&r.partition)));
    }

    #[test]
    fn truncation_rewinds_and_drops_the_buffer() {
        let mut state = buffered_state();
        let delivery = state.take_delivery(10).unwrap();
        let _ = state.complete_delivery(delivery, 10);
        assert_eq!(state.truncate(&key("t", 0), None, 6), Some(10));
        let r = state.partition(&key("t", 0)).unwrap();
        assert_eq!(r.position, Some(6));
        assert!(r.validated);
        assert!(r.epoch.is_none());
    }

    /// Every way a record changes keeps the running tally equal to a full
    /// scan: what the gauges read per poll without scanning is what a scan
    /// would find.
    #[test]
    fn the_tally_is_a_scan_after_every_change() {
        let mut state = SubscriptionState::new(IsolationLevel::ReadCommitted);
        let now = Instant::now();
        state.add_partitions((0..4).map(|p| key("t", p)));
        assert!(state.tally_matches_a_scan());
        assert_eq!(state.partitions_needing_position(now).len(), 4);
        for p in 0..3 {
            state.seek(&key("t", p), 0).unwrap();
            assert!(state.tally_matches_a_scan());
        }
        state
            .request_reset(&key("t", 3), OffsetReset::Earliest)
            .unwrap();
        assert_eq!(state.partitions_awaiting_reset(now).len(), 1);
        assert_eq!(state.unvalidated(now).len(), 3);
        for target in state.fetch_targets(now) {
            let p = target.key.1;
            state.update_watermarks(&target.key, Some(20), Some(15 + i64::from(p)), Some(0), now);
            assert!(state.install_fetch(&target, fetched("t", p, 0..5)));
            assert!(state.tally_matches_a_scan());
        }
        assert_eq!(state.buffered_count(), 15);
        assert_eq!(state.aggregate_lag(), (15 + 16 + 17, 17));
        state.set_paused("t", &[1], true);
        assert_eq!(state.paused_count(), 1);
        let delivery = state.take_delivery(7).unwrap();
        assert!(state.tally_matches_a_scan());
        let _ = state.complete_delivery(delivery, 4);
        assert!(state.tally_matches_a_scan());
        state.mark_validated(&key("t", 0), state.partition(&key("t", 0)).unwrap().version);
        state.mark_unvalidated(&key("t", 2));
        assert!(state.truncate(&key("t", 2), None, 1).is_some());
        assert!(state.tally_matches_a_scan());
        state.remove_partitions([&key("t", 1)]);
        assert!(state.tally_matches_a_scan());
        assert_eq!(state.paused_count(), 0);
        state.clear_assignment();
        assert!(state.tally_matches_a_scan());
        assert_eq!(state.aggregate_lag(), (0, 0));
    }

    /// The moved partitions are exactly those whose fetch position changed,
    /// each once, and a revoked one is not reported.
    #[test]
    fn the_moved_partitions_are_the_ones_whose_fetch_position_changed() {
        let mut state = SubscriptionState::default();
        state.add_partitions((0..3).map(|p| key("t", p)));
        assert!(state.take_moved().is_empty(), "no position yet");
        for p in 0..3 {
            state.seek(&key("t", p), 10).unwrap();
        }
        let moved: Vec<PartitionId> = state.take_moved().iter().map(|m| m.key.1).collect();
        assert_eq!(moved, vec![0, 1, 2]);
        assert!(state.take_moved().is_empty(), "taken once");
        let targets = state.fetch_targets(Instant::now());
        state.update_watermarks(&targets[0].key, Some(30), None, None, Instant::now());
        assert!(state.take_moved().is_empty(), "a watermark is not a move");
        let target = targets.iter().find(|t| t.key.1 == 1).unwrap();
        assert!(state.install_fetch(target, fetched("t", 1, 10..12)));
        let moved = state.take_moved();
        assert_eq!(moved.len(), 1);
        assert_eq!((moved[0].key.1, moved[0].fetch_position), (1, Some(12)));
        state.seek(&key("t", 2), 4).unwrap();
        state.remove_partitions([&key("t", 2)]);
        assert!(state.take_moved().is_empty(), "a revoked partition is gone");
    }

    /// Every key of a topic shares one name.
    #[test]
    fn the_keys_of_a_topic_share_its_name() {
        let mut state = SubscriptionState::default();
        state.add_partitions((0..3).map(|p| key("t", p)));
        let names: Vec<*const u8> = state
            .assigned_keys()
            .iter()
            .map(|(topic, _)| topic.as_ptr())
            .collect();
        assert!(names.iter().all(|name| *name == names[0]));
        assert_eq!(state.key("t", 7).0.as_ptr(), names[0]);
    }
}
