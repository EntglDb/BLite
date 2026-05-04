using System.Collections.Concurrent;
using System.Threading.Channels;

namespace BLite.Core.Storage;

public sealed partial class StorageEngine
{
    // -------------------------------------------------------------------------
    // Group Commit writer
    //
    // Instead of each CommitTransactionAsync() acquiring _commitLock and waiting
    // for a WAL flush individually, callers post a PendingCommit to this channel
    // and await a TaskCompletionSource.  A single background writer drains the
    // channel, batches ALL pending commits into one WAL segment, issues ONE
    // FlushAsync, then signals every caller at once.
    //
    // Benefits:
    //   • The WAL flush cost (~5–30 µs) is amortised across N concurrent commits.
    //   • _commitLock is held only by the writer, for the duration of the batch
    //     write + flush — not by every individual caller for the full round trip.
    //   • BeginTransaction no longer competes with an in-flight flush.
    //   • The sync CommitTransactionAsync(ulong) path and checkpoint operations still
    //     use _commitLock normally and are fully compatible.
    // -------------------------------------------------------------------------

    private sealed class PendingCommit
    {
        public readonly ulong TransactionId;
        public readonly ConcurrentDictionary<uint, byte[]>? Pages;
        public readonly TaskCompletionSource<bool> Completion =
            new(TaskCreationOptions.RunContinuationsAsynchronously);

        public PendingCommit(ulong txId, ConcurrentDictionary<uint, byte[]>? pages)
        {
            TransactionId = txId;
            Pages = pages;
        }
    }

    private async Task GroupCommitWriterAsync(CancellationToken ct)
    {
        var batch = new List<PendingCommit>(32);

        while (true)
        {
            batch.Clear();

            // Block until at least one commit arrives, or the channel closes / ct fires.
            try
            {
                var first = await _commitChannel.Reader.ReadAsync(ct).ConfigureAwait(false);
                batch.Add(first);
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch (ChannelClosedException)
            {
                break;
            }

            // Drain any additional commits that are already pending — no extra I/O cost.
            while (batch.Count < 64 && _commitChannel.Reader.TryRead(out var next))
                batch.Add(next);

            try
            {
                await ProcessBatchAsync(batch).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                // Safety net: if ProcessBatchAsync throws for any unforeseen reason,
                // signal all pending commits with the exception so callers don't hang.
                foreach (var commit in batch)
                    commit.Completion.TrySetException(ex);
            }
        }

        // Shutdown drain: process everything still sitting in the channel.
        batch.Clear();
        while (_commitChannel.Reader.TryRead(out var pending))
        {
            batch.Add(pending);
            if (batch.Count >= 64)
            {
                try
                {
                    await ProcessBatchAsync(batch).ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    foreach (var commit in batch)
                        commit.Completion.TrySetException(ex);
                }
                batch.Clear();
            }
        }
        if (batch.Count > 0)
        {
            try
            {
                await ProcessBatchAsync(batch).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                foreach (var commit in batch)
                    commit.Completion.TrySetException(ex);
            }
        }
    }

    private async Task ProcessBatchAsync(List<PendingCommit> batch)
    {
        bool needsCheckpoint = false;
        Exception? failure = null;

        // Acquire the commit lock once for the entire batch.
        // If this times out, signal all waiters with the exception so they don't hang.
        if (!await _commitLock.WaitAsync(_config.LockTimeout.WriteTimeoutMs).ConfigureAwait(false))
        {
            failure = new TimeoutException("Timed out acquiring commit lock (GroupCommit).");
            foreach (var commit in batch)
                commit.Completion.TrySetException(failure);
            return;
        }

        // ── Cross-process writer serialization ──────────────────────────────
        // When multi-process access is enabled, an inner OS-level lock guards the
        // single shared WAL stream against any other process. The mandatory order is
        // _commitLock (in-process) → SHM writer lock (cross-process), and the reverse
        // on release. The checkpoint truncate path in StorageEngine.Recovery.cs
        // acquires the same two locks in the same order around `TruncateAsync`, so
        // there is no cross-lock deadlock.
        bool shmLockHeld = false;
        if (_shm != null)
        {
            shmLockHeld = _shm.TryAcquireWriterLock(_config.LockTimeout.WriteTimeoutMs);
            if (!shmLockHeld)
            {
                failure = new TimeoutException(
                    "Timed out acquiring cross-process WAL writer lock (.wal-shm).");
                _commitLock.Release();
                foreach (var commit in batch)
                    commit.Completion.TrySetException(failure);
                return;
            }
        }

        try
        {
            // In multi-process mode we record the WAL byte offset at the start of each
            // Write record so the SHM hash table can map pageId → offset. We use
            // WriteDataRecordAndGetOffsetAsync which atomically captures the position
            // inside the WAL write lock, ensuring PrepareTransactionAsync callers that
            // also write to the WAL (without _commitLock) cannot shift the position
            // between the snapshot and the actual write.
            var wal = (_shm != null) ? (_wal as Transactions.WriteAheadLog) : null;
            var pageOffsets = wal != null ? new List<(uint pageId, long walOffset)>() : null;

            foreach (var commit in batch)
            {
                if (commit.Pages == null || commit.Pages.IsEmpty)
                {
                    // Read-only or empty transaction: just a commit tombstone.
                    await _wal.WriteCommitRecordAsync(commit.TransactionId).ConfigureAwait(false);
                }
                else
                {
                    await _wal.WriteBeginRecordAsync(commit.TransactionId).ConfigureAwait(false);
                    foreach (var (pageId, data) in commit.Pages)
                    {
                        // When multi-process mode is active, use the offset-returning variant
                        // so the record start offset is captured atomically inside the WAL
                        // write lock, preventing concurrent PrepareTransactionAsync writers
                        // from shifting the stream position between snapshot and write.
                        // In single-process mode, fall back to the standard write.
                        if (wal != null)
                            pageOffsets!.Add((pageId, await wal.WriteDataRecordAndGetOffsetAsync(
                                commit.TransactionId, pageId, data).ConfigureAwait(false)));
                        else
                            await _wal.WriteDataRecordAsync(commit.TransactionId, pageId, data).ConfigureAwait(false);
                    }
                    await _wal.WriteCommitRecordAsync(commit.TransactionId).ConfigureAwait(false);
                }
            }

            // ONE flush for all transactions in this batch.
            await _wal.FlushAsync().ConfigureAwait(false);

            // Promote to WAL index (makes pages visible to readers).
            foreach (var commit in batch)
            {
                if (commit.Pages != null)
                {
                    _walCache.TryRemove(commit.TransactionId, out _);
                    foreach (var kvp in commit.Pages)
                        _walIndex[kvp.Key] = kvp.Value;
                }
            }

            // Update cross-process WAL index: populate the SHM hash table and the
            // in-process _walOffsets dict so the Phase-6 checkpoint can apply the
            // GetMinReaderOffset() safe boundary per entry.
            if (_shm != null && pageOffsets != null && pageOffsets.Count > 0)
            {
                _shm.UpdatePageOffsets(pageOffsets);
                if (_walOffsets != null)
                {
                    foreach (var (pageId, walOffset) in pageOffsets)
                        _walOffsets[pageId] = walOffset;
                }
            }

            // Publish the new WAL end offset to other processes so their readers /
            // checkpointers see a consistent view.
            long newWalEnd = _wal.GetCurrentSize();
            _shm?.AdvanceWalEndOffset(newWalEnd);

            // Advance _lastKnownWalEndOffset so that the next BeginTransaction call on
            // this engine does not redundantly replay WAL records that we just committed
            // (they are already in _walIndex from the lines above). This is safe because
            // we hold _commitLock + SHM writer lock: no concurrent writer can insert
            // records between our last write and the position we record here.
            if (_shm != null)
                Volatile.Write(ref _lastKnownWalEndOffset, newWalEnd);

            // Check if checkpoint is needed, but defer until after releasing the lock
            needsCheckpoint = newWalEnd > MaxWalSize;
        }
        catch (Exception ex)
        {
            failure = ex;
        }
        finally
        {
            // Release in reverse order: SHM writer lock first, then in-process commit lock.
            if (shmLockHeld)
            {
                try { _shm!.ReleaseWriterLock(); } catch { /* best-effort */ }
            }
            _commitLock.Release();
        }

        // Signal all waiters outside the lock so their continuations run freely.
        foreach (var commit in batch)
        {
            if (failure != null)
                commit.Completion.TrySetException(failure);
            else
                commit.Completion.TrySetResult(true);
        }

        // Emit group-commit batch metric (only real batches, not the sentinel).
        if (_metrics != null && batch.Count > 0)
        {
            _metrics.Publish(new Metrics.MetricEvent
            {
                Timestamp = System.Diagnostics.Stopwatch.GetTimestamp(),
                Type      = Metrics.MetricEventType.GroupCommitBatch,
                BatchSize = batch.Count,
                Success   = failure == null,
            });
        }

        // Fire checkpoint on a separate task so the group-commit writer stays
        // responsive.  CheckpointAsync guards against concurrent runs internally.
        if (needsCheckpoint && failure == null)
        {
            _ = Task.Run(() => CheckpointAsync());
        }
    }

    /// <summary>
    /// Ensures all pending committed transactions have been written to disk.
    /// Call before app suspend (e.g. MAUI <c>Window.Destroying</c>) to guarantee
    /// durability without fully closing the engine.
    /// </summary>
    public async Task FlushPendingCommitsAsync(CancellationToken ct = default)
    {
        // Post a sentinel with txId = 0 (no WAL records emitted for it) and await it.
        // When the sentinel's TCS is set, all commits ahead of it in the channel
        // have been flushed.
        var sentinel = new PendingCommit(0, null);
        await _commitChannel.Writer.WriteAsync(sentinel, ct).ConfigureAwait(false);
        await sentinel.Completion.Task.ConfigureAwait(false);
    }
}
