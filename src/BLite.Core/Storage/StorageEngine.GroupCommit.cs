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
    //   • The sync CommitTransaction(ulong) path and checkpoint operations still
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

            await ProcessBatchAsync(batch).ConfigureAwait(false);
        }

        // Shutdown drain: process everything still sitting in the channel.
        batch.Clear();
        while (_commitChannel.Reader.TryRead(out var pending))
        {
            batch.Add(pending);
            if (batch.Count >= 64)
            {
                await ProcessBatchAsync(batch).ConfigureAwait(false);
                batch.Clear();
            }
        }
        if (batch.Count > 0)
            await ProcessBatchAsync(batch).ConfigureAwait(false);
    }

    private async Task ProcessBatchAsync(List<PendingCommit> batch)
    {
        // Acquire the commit lock once for the entire batch.
        await _commitLock.WaitAsync().ConfigureAwait(false);
        Exception? failure = null;
        try
        {
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
                        await _wal.WriteDataRecordAsync(commit.TransactionId, pageId, data).ConfigureAwait(false);
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

            // Auto-checkpoint if WAL has grown too large.
            if (_wal.GetCurrentSize() > MaxWalSize)
                CheckpointInternal();
        }
        catch (Exception ex)
        {
            failure = ex;
        }
        finally
        {
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
