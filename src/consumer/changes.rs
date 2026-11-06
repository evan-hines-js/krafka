//! The one signal a poll with nothing to fetch waits on.
//!
//! Everything that can change such a poll's answer raises it: a resume, a
//! seek, an assignment or subscription change, close, and the group events
//! the heartbeat tasks and join rounds hand to the poll path. The poll takes
//! a [`mark`](Changes::mark) before it looks at its state and waits for a
//! change [`since`](Changes::since) that mark, so a change that lands between
//! the look and the wait is never lost, and one that landed before the look
//! does not wake it again.

use std::sync::atomic::{AtomicU64, Ordering};

use tokio::sync::Notify;

/// A count of changes and the waiters for the next one.
#[derive(Debug, Default)]
pub(crate) struct Changes {
    raised: AtomicU64,
    waiters: Notify,
}

impl Changes {
    /// Record a change and wake every poll waiting for one.
    pub(crate) fn raise(&self) {
        self.raised.fetch_add(1, Ordering::SeqCst);
        self.waiters.notify_waiters();
    }

    /// The changes so far, to wait from.
    pub(crate) fn mark(&self) -> u64 {
        self.raised.load(Ordering::SeqCst)
    }

    /// Wait until a change is raised after `mark` was taken; at once if one
    /// already was.
    ///
    /// The waiter is registered before the count is read again: a raise
    /// either moved the count already or wakes the registered waiter.
    pub(crate) async fn since(&self, mark: u64) {
        let notified = self.waiters.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        if self.mark() != mark {
            return;
        }
        notified.await;
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;
    use futures::FutureExt;

    #[test]
    fn a_change_after_the_mark_ends_the_wait_and_one_before_it_does_not() {
        let changes = Changes::default();
        changes.raise();
        let mark = changes.mark();
        assert!(
            changes.since(mark).now_or_never().is_none(),
            "an earlier change woke it"
        );
        changes.raise();
        assert!(
            changes.since(mark).now_or_never().is_some(),
            "a later change did not"
        );
    }

    #[tokio::test]
    async fn a_change_wakes_a_waiter_already_waiting() {
        let changes = std::sync::Arc::new(Changes::default());
        let mark = changes.mark();
        let waiting = tokio::spawn({
            let changes = std::sync::Arc::clone(&changes);
            async move { changes.since(mark).await }
        });
        tokio::task::yield_now().await;
        assert!(!waiting.is_finished());
        changes.raise();
        waiting.await.unwrap();
    }
}
