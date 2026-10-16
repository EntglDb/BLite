using System.Diagnostics;
using BLite.Core.Transactions;

namespace BLite.Core.Storage;

public sealed partial class StorageEngine
{
    #region Transaction Management

    public Transaction BeginTransaction(IsolationLevel isolationLevel = IsolationLevel.ReadCommitted)
    {
        // Phase 7: Incremental WAL replay for cross-process read freshness.
        // Compare the SHM WAL end offset against the last offset this engine has
        // replayed. If another process has committed since then, replay the new
        // WAL records into _walIndex so reads in this transaction see fresh data.
        // The replay runs under _commitLock (see TryCatchUpWithSharedWal); if a local commit
        // holds it for longer than the read timeout, the transaction starts without the
        // replay: its reads may be stale, and the next transaction or commit catches up.
        if (_shm != null && _wal is Transactions.WriteAheadLog walForReplay
            && (_shm.ReadWalEndOffset() != Volatile.Read(ref _lastKnownWalEndOffset)
                || _shm.ReadWalGeneration() != Volatile.Read(ref _knownWalGeneration))
            && _commitLock.Wait(_config.LockTimeout.ReadTimeoutMs))
        {
            try { TryCatchUpWithSharedWal(walForReplay); }
            finally { _commitLock.Release(); }
        }

        // In multi-process mode, allocate the transaction id from the shared SHM counter
        // so two processes never observe the same id. Falls back to the in-process
        // Interlocked counter when the SHM sidecar is not configured.
        var txnId = _shm != null
            ? _shm.AllocateTransactionId()
            : (ulong)Interlocked.Increment(ref _nextTransactionId);
        var transaction = new Transaction(txnId, this, isolationLevel);
        _activeTransactions[txnId] = transaction;

        // Phase 5: Register a reader slot in the SHM so the checkpoint algorithm
        // can determine the safe truncation boundary (minimum offset still needed
        // by any active reader across all processes).
        if (_shm != null)
        {
            long walEnd = _shm.ReadWalEndOffset();
            if (_shm.TryAcquireReaderSlot(out int slotIndex, walEnd))
                transaction.ShmReaderSlotIndex = slotIndex;
            // If TryAcquireReaderSlot returns false (all slots full), the transaction
            // proceeds without a slot — a degraded-but-safe fallback.
        }

        _metrics?.Publish(new Metrics.MetricEvent
        {
            Timestamp  = Stopwatch.GetTimestamp(),
            Type       = Metrics.MetricEventType.TransactionBegin,
            Success    = true,
        });
        return transaction;
    }

    /// <summary>
    /// Replays into <c>_walIndex</c> the records other processes have committed to the shared
    /// WAL since this engine last caught up with it, <c>[_lastKnownWalEndOffset, SHM WAL end)</c>,
    /// and advances <c>_lastKnownWalEndOffset</c> to that end. Returns <c>false</c>, leaving the
    /// bookmark where it was, if the records could not be read.
    /// <para>
    /// The caller must hold <c>_commitLock</c>, so no local commit or truncation interleaves.
    /// The bookmark then never runs ahead of what this engine holds, which
    /// <see cref="TryTruncateWalAsync"/> relies on: every record before it is in
    /// <c>_walIndex</c> or already checkpointed. The replayed range runs to the end of the WAL,
    /// so the latest committed image of each page in it is the newest there is and replaces
    /// whatever this engine holds for that page, including an image it committed itself.
    /// </para>
    /// <para>
    /// If another process has truncated the WAL since (a new WAL generation), everything this
    /// engine holds from the old WAL is discarded first and the new WAL is replayed from its start.
    /// Without the SHM writer lock, a truncation during the replay makes it return <c>false</c>.
    /// </para>
    /// </summary>
    private bool TryCatchUpWithSharedWal(Transactions.WriteAheadLog wal)
    {
        long generation = _shm!.ReadWalGeneration();
        long shmEnd = _shm.ReadWalEndOffset();
        long lastKnown = Volatile.Read(ref _lastKnownWalEndOffset);

        // Another process truncated the WAL: a new generation, or an end before the bookmark (as
        // a process that does not count generations leaves it). It had checkpointed every record
        // first, so the page files hold each page this engine replayed or read from the old WAL
        // in that version or a newer one. Keeping them would serve stale reads and let a
        // checkpoint write them back over the newer data (#197). Replay the new WAL from its start.
        if (generation != Volatile.Read(ref _knownWalGeneration) || shmEnd < lastKnown)
        {
            DiscardSharedWalState(generation);
            lastKnown = 0;
        }

        if (shmEnd > lastKnown)
        {
            List<(uint pageId, byte[] data, long walOffset)> newPages;
            bool complete;
            long stoppedAt;
            try
            {
                newPages = wal.ReadCommittedPagesSince(lastKnown, shmEnd, out complete, out stoppedAt);
            }
            catch (Exception ex)
            {
                // Stale reads until the next catch-up retries, never a skipped record.
                ReportFailedCatchUp(wal, lastKnown, shmEnd, ex.Message);
                return false;
            }

            // Without the SHM writer lock (BeginTransaction), the WAL can be truncated and
            // rewritten while it is read; the bytes read then belong to neither generation.
            if (_shm.ReadWalGeneration() != generation)
                return false;

            // A record that could not be read ends the scan early: the records after it were not
            // replayed. Moving the bookmark past them would let this engine skip them and truncate
            // the WAL that still holds them (#198). Keep it, and with it the WAL, until a replay
            // gets through. If the record stays unreadable, every catch-up stops there: reported
            // so that the stall is visible.
            if (!complete)
            {
                ReportFailedCatchUp(wal, lastKnown, shmEnd,
                    stoppedAt < shmEnd
                        ? $"the record at offset {stoppedAt} is unreadable (torn, corrupt, or past the end of the file)"
                        : $"the last record ends at offset {stoppedAt}, past the published end");
                return false;
            }

            foreach (var (pid, data, walOffset) in newPages)
            {
                _walIndex[pid] = data;
                // Record the WAL offset so CheckpointAsync can apply the
                // GetMinReaderOffset() safe boundary for this page (Phase 6).
                _walOffsets?[pid] = walOffset;
            }
        }

        Volatile.Write(ref _lastKnownWalEndOffset, shmEnd);
        _failedCatchUps = 0;
        return true;
    }

    /// <summary>
    /// Failed catch-ups in a row from which <see cref="ReportFailedCatchUp"/> calls the replay
    /// stalled. A record inside the published range that stays unreadable stops every catch-up
    /// of every process whose bookmark is before it.
    /// </summary>
    internal const int CatchUpStallThreshold = 16;

    /// <summary>
    /// Counts the catch-ups in a row that could not replay the shared WAL, and reports the first
    /// one and then every power of two as a warning (<see cref="Trace"/>): a WAL that stays
    /// unreadable is visible without flooding the log. Until a catch-up succeeds, this engine's
    /// reads may miss other processes' commits and its checkpoints write nothing, so it never
    /// truncates the WAL. Called under <c>_commitLock</c>.
    /// </summary>
    private void ReportFailedCatchUp(Transactions.WriteAheadLog wal, long fromOffset, long toOffset, string reason)
    {
        int failures = ++_failedCatchUps;
        if ((failures & (failures - 1)) != 0)
            return;
        Trace.TraceWarning(
            $"BLite: could not replay the shared WAL '{wal.FilePath}' from offset {fromOffset} to {toOffset} " +
            $"({failures} attempt(s) in a row): {reason}. Until it can, reads may miss other processes' commits " +
            "and checkpoints write nothing, so the WAL is not truncated." +
            (failures >= CatchUpStallThreshold
                ? " The replay has stalled: the WAL and this engine's memory use grow until the record can be read or another process truncates the WAL."
                : ""));
    }

    /// <summary>
    /// Drops every page this engine holds from an earlier generation of the shared WAL and
    /// records <paramref name="generation"/> as the current one. Each of them is on disk in that
    /// version or a newer one: the process that truncated the WAL had checkpointed it. Its own
    /// commits are no exception, since a commit replays the WAL (and so notices a truncation)
    /// before appending.
    /// <para>
    /// An encrypted WAL also forgets its key: the new WAL starts with a header of its own, which
    /// the next read or write loads (or writes, if the WAL is still empty), so every process
    /// encrypts and decrypts the new WAL with the same key (#203). Called under <c>_commitLock</c>,
    /// so no record of this engine is being written meanwhile.
    /// </para>
    /// </summary>
    private void DiscardSharedWalState(long generation)
    {
        (_wal as Transactions.WriteAheadLog)?.ResetCryptoHeader();
        lock (_replayLock)
        {
            _walIndex.Clear();
            _walOffsets?.Clear();
            _replayEpoch++;
            Volatile.Write(ref _knownWalGeneration, generation);
            Volatile.Write(ref _lastKnownWalEndOffset, 0);
        }
    }

    /// <summary>
    /// Reads the latest committed image of <paramref name="pageId"/> straight from the shared WAL
    /// through the SHM page index (Phase 4), or returns <c>null</c> if the WAL has none or was
    /// truncated while it was read. The image is cached in <c>_walIndex</c> only if this engine's
    /// view of the WAL is still the one it was read from: a page cached after a discard would be a
    /// stale image that a checkpoint could write back.
    /// </summary>
    private byte[]? ReadFromSharedWal(uint pageId)
    {
        if (_shm == null || _wal is not Transactions.WriteAheadLog walFile)
            return null;

        long epoch = Interlocked.Read(ref _replayEpoch);
        long generation = _shm.ReadWalGeneration();
        long walOffset = _shm.LookupPageOffset(pageId);
        if (walOffset <= 0)
            return null;

        var pageData = walFile.ReadPageAt(walOffset, pageId);
        if (pageData == null)
            return null;

        lock (_replayLock)
        {
            // Truncated meanwhile: the page files hold every record of the old WAL.
            if (_shm.ReadWalGeneration() != generation)
                return null;

            // Populate local _walIndex and _walOffsets to amortise future reads
            // and ensure CheckpointAsync respects the reader boundary (Phase 6).
            if (_replayEpoch == epoch && _knownWalGeneration == generation)
            {
                _walIndex[pageId] = pageData;
                _walOffsets?[pageId] = walOffset;
            }
        }
        return pageData;
    }

    /// <summary>
    /// Forgets a transaction once it has committed or rolled back through the <c>ulong</c>
    /// overloads, which callers use directly without going through the <see cref="Transaction"/> ones.
    /// </summary>
    private void CompleteTransaction(ulong transactionId) => _activeTransactions.TryRemove(transactionId, out _);

    public async Task CommitTransactionAsync(Transaction transaction, CancellationToken ct = default)
    {
        if (!_activeTransactions.ContainsKey(transaction.TransactionId))
            throw new InvalidOperationException($"Transaction {transaction.TransactionId} is not active.");

        try
        {
            await CommitTransactionAsync(transaction.TransactionId, ct);
        }
        finally
        {
            // Always clean up, even if the commit throws (timeout, etc.).
            _activeTransactions.TryRemove(transaction.TransactionId, out _);
        }
    }

    public async Task RollbackTransactionAsync(Transaction transaction)
    {
        await RollbackTransactionAsync(transaction.TransactionId);
        _activeTransactions.TryRemove(transaction.TransactionId, out _);
    }
    
    // Rollback doesn't usually require async logic unless logging abort record is async, 
    // but for consistency we might consider it. For now, sync is fine as it's not the happy path bottleneck.

    #endregion

    /// <summary>
    /// Prepares a transaction: writes all changes to WAL but doesn't commit yet.
    /// Part of 2-Phase Commit protocol.
    /// </summary>
    /// <param name="transactionId">Transaction ID</param>
    /// <param name="writeSet">All writes to record in WAL</param>
    /// <returns>True if preparation succeeded</returns>
    /// <exception cref="NotSupportedException">Multi-process mode (see <see cref="ThrowIfMultiProcessAccess"/>).</exception>
    public async Task<bool> PrepareTransactionAsync(ulong transactionId)
    {
        ThrowIfMultiProcessAccess(nameof(PrepareTransactionAsync));
        try
        {
            await _wal.WriteBeginRecordAsync(transactionId);

            foreach (var walEntry in _walCache[transactionId])
            {
                await _wal.WriteDataRecordAsync(transactionId, walEntry.Key, walEntry.Value);
            }

            await _wal.FlushAsync(); // Ensure WAL is persisted
            return true;
        }
        catch (Exception ex)
        {
            System.Diagnostics.Debug.WriteLine($"[BLite] PrepareTransaction({transactionId}) failed: {ex}");
            return false;
        }
    }

    /// <exception cref="NotSupportedException">Multi-process mode (see <see cref="ThrowIfMultiProcessAccess"/>).</exception>
    public async Task<bool> PrepareTransactionAsync(ulong transactionId, CancellationToken ct = default)
    {
        ThrowIfMultiProcessAccess(nameof(PrepareTransactionAsync));
        try
        {
            await _wal.WriteBeginRecordAsync(transactionId, ct);

            if (_walCache.TryGetValue(transactionId, out var changes))
            {
                foreach (var walEntry in changes)
                {
                    await _wal.WriteDataRecordAsync(transactionId, walEntry.Key, walEntry.Value, ct);
                }
            }
            
            await _wal.FlushAsync(ct); // Ensure WAL is persisted
            return true;
        }
        catch
        {
            return false;
        }
    }

    /// <summary>
    /// Commits a transaction:
    /// 1. Writes all changes to WAL (for durability)
    /// 2. Writes commit record
    /// 3. Flushes WAL to disk
    /// 4. Moves pages from cache to WAL index (for future reads)
    /// 5. Clears WAL cache
    /// </summary>
    /// <param name="transactionId">Transaction to commit</param>
    /// <param name="writeSet">All writes performed in this transaction (unused, kept for compatibility)</param>
    /// <exception cref="NotSupportedException">
    /// Multi-process mode (see <see cref="ThrowIfMultiProcessAccess"/>); commit through
    /// <see cref="Transaction.CommitAsync"/> or <c>CommitTransactionAsync(transactionId, cancellationToken)</c>.
    /// </exception>
    public async Task CommitTransactionAsync(ulong transactionId)
    {
        ThrowIfMultiProcessAccess(nameof(CommitTransactionAsync) + "(ulong)");

        // NOTE: No admission gate here. Nothing in the library calls this direct path;
        // Transaction.CommitAsync goes through group commit (the CancellationToken overload).
        bool needsCheckpoint = false;
        bool completed = false;

        if (!await _commitLock.WaitAsync(_config.LockTimeout.WriteTimeoutMs))
            throw new TimeoutException("Timed out acquiring commit lock (CommitTransaction).");
        try
        {
            // Get ALL pages from WAL cache (includes both data and index pages)
            if (!_walCache.TryGetValue(transactionId, out var pages))
            {
                // No writes for this transaction, just write commit record
                await _wal.WriteCommitRecordAsync(transactionId);
                await _wal.FlushAsync();
                FreePagesOfCommittedTransaction(transactionId);
                completed = true;
                goto Completed;
            }

            // 1. Write all changes to WAL (from cache, not writeSet!)
            await _wal.WriteBeginRecordAsync(transactionId);

            foreach (var (pageId, data) in pages)
            {
                await _wal.WriteDataRecordAsync(transactionId, pageId, data);
            }

            // 2. Write commit record and flush
            await _wal.WriteCommitRecordAsync(transactionId);
            await _wal.FlushAsync(); // Durability: ensure WAL is on disk

            // 3. Move pages from cache to WAL index (for reads). Index first, then drop the
            // cache: a page must never be in neither (FreePage relies on finding its WAL image).
            foreach (var kvp in pages)
            {
                _walIndex[kvp.Key] = kvp.Value;
            }
            _walCache.TryRemove(transactionId, out _);

            // The commit is durable: release the pages it stopped using (#233).
            FreePagesOfCommittedTransaction(transactionId);

            // Check if checkpoint is needed, but defer it until after releasing the lock
            needsCheckpoint = _wal.GetCurrentSize() > MaxWalSize;
            completed = true;
        }
        finally
        {
            _commitLock.Release();
        }

        Completed:
        if (completed)
            CompleteTransaction(transactionId);

        // Fire checkpoint on a separate task so the caller isn't blocked.
        if (needsCheckpoint)
        {
            _ = Task.Run(() => CheckpointAsync());
        }
    }

    public async Task CommitTransactionAsync(ulong transactionId, CancellationToken ct = default)
    {
        // Admission gate: wait up to 1/16 of the write timeout before rejecting.
        // Gives a slot time to free up without blocking for the full write budget,
        // preventing deep queues on the WAL/commit locks that cause latency spikes.
        int gateTimeoutMs = _config.LockTimeout.WriteTimeoutMs switch
        {
            > 0 => Math.Max(1, _config.LockTimeout.WriteTimeoutMs / 16),
            -1 => -1,
            _ => 0
        };
        if (_writerGate != null && !await _writerGate.WaitAsync(gateTimeoutMs, ct).ConfigureAwait(false))
            throw new TimeoutException("Too many concurrent writers — admission gate full.");

        // ── AUDIT: Phase 2 — Activity span (NET5+ only) ──────────────────────
#if NET5_0_OR_GREATER
        Activity? activity = _auditOptions?.EnableDiagnosticSource == true
            ? Audit.BLiteDiagnostics.ActivitySource.StartActivity(Audit.BLiteDiagnostics.CommitActivityName)
            : null;
        activity?.SetTag("db.system", "blite");
        activity?.SetTag("db.blite.transaction_id", (long)transactionId);
#endif
        // ────────────────────────────────────────────────────────────────────

        // ── AUDIT: Phase 1 — stopwatch ───────────────────────────────────────
        var auditVsw = _auditOptions is not null ? Metrics.ValueStopwatch.StartNew() : default;
        // ────────────────────────────────────────────────────────────────────

        var sw = _metrics != null ? Metrics.ValueStopwatch.StartNew() : default;
        bool success = false;
        int pagesWritten = 0;
        try
        {
            // Capture page count before the group commit removes the entry from the cache.
            _walCache.TryGetValue(transactionId, out var pages);
            pagesWritten = pages?.Count ?? 0;

            // Group commit path: post to the background writer and await its TCS.
            // The writer batches this commit with any other pending ones, issues one
            // WAL flush for the entire batch, then signals all waiters.
            var pending = new PendingCommit(transactionId, pages);
            await _commitChannel.Writer.WriteAsync(pending, ct).ConfigureAwait(false);
            await pending.Completion.Task.ConfigureAwait(false);
            success = true;
            CompleteTransaction(transactionId);
        }
        finally
        {
            _writerGate?.Release();
            if (sw.IsActive)
                _metrics?.Publish(new Metrics.MetricEvent
                {
                    Timestamp     = sw.StartTimestamp,
                    Type          = Metrics.MetricEventType.TransactionCommit,
                    ElapsedMicros = sw.GetElapsedMicros(),
                    Success       = success,
                });

            // ── AUDIT: emit ──────────────────────────────────────────────────
            if (auditVsw.IsActive)
            {
                var elapsed  = auditVsw.GetElapsed();
                var walSize  = _wal.GetCurrentSize();
                var userId   = (_auditOptions!.ContextProvider ?? Audit.AmbientAuditContext.Instance).GetCurrentUserId();

#if NET5_0_OR_GREATER
                activity?.SetTag("db.blite.pages_written", pagesWritten);
                activity?.SetTag("db.blite.wal_size_bytes", walSize);
                if (!success) activity?.SetStatus(ActivityStatusCode.Error, "Commit failed");
                activity?.Dispose();
                activity = null;
#endif

                if (success)
                {
                    var evt = new Audit.CommitAuditEvent(
                        TransactionId:  transactionId,
                        CollectionName: string.Empty,
                        PagesWritten:   pagesWritten,
                        WalSizeBytes:   walSize,
                        Elapsed:        elapsed,
                        UserId:         userId);

                    _auditOptions.Sink?.OnCommit(evt);
                    AuditMetrics?.RecordCommit();

                    // Slow-commit detection
                    if (_auditOptions.SlowOperationThreshold is { } threshold && elapsed > threshold)
                    {
                        _auditOptions.Sink?.OnSlowOperation(new Audit.SlowOperationEvent(
                            Audit.SlowOperationType.Commit,
                            CollectionName: string.Empty,
                            Elapsed:        elapsed,
                            Detail:         $"TxnId={transactionId}, Pages={pagesWritten}"));
                    }
                }
            }
#if NET5_0_OR_GREATER
            else
            {
                activity?.Dispose();
            }
#endif
            // ────────────────────────────────────────────────────────────────
        }
    }
    
    /// <summary>
    /// Marks a transaction as committed after WAL writes.
    /// Used for 2PC: after PrepareAsync() writes to WAL, this finalizes the commit.
    /// </summary>
    /// <param name="transactionId">Transaction to mark committed</param>
    /// <exception cref="NotSupportedException">Multi-process mode (see <see cref="ThrowIfMultiProcessAccess"/>).</exception>
    public async Task MarkTransactionCommittedAsync(ulong transactionId)
    {
        ThrowIfMultiProcessAccess(nameof(MarkTransactionCommittedAsync));
        bool needsCheckpoint = false;

        _commitLock.Wait();
        try
        {
            await _wal.WriteCommitRecordAsync(transactionId);
            await _wal.FlushAsync();
            
            // Move from cache to WAL index. Index first, then drop the cache: a page must
            // never be in neither (FreePage relies on finding its WAL image).
            if (_walCache.TryGetValue(transactionId, out var pages))
            {
                foreach (var kvp in pages)
                {
                    _walIndex[kvp.Key] = kvp.Value;
                }
                _walCache.TryRemove(transactionId, out _);
            }

            // The commit is durable: release the pages it stopped using (#233).
            FreePagesOfCommittedTransaction(transactionId);

            // Check if checkpoint is needed, but defer it until after releasing the lock
            needsCheckpoint = _wal.GetCurrentSize() > MaxWalSize;
        }
        finally
        {
            _commitLock.Release();
        }

        CompleteTransaction(transactionId);

        if (needsCheckpoint)
        {
            _ = Task.Run(() => CheckpointAsync());
        }
    }

    /// <summary>
    /// Rejects the direct commit path (<c>CommitTransactionAsync(ulong)</c>) and two-phase commit
    /// (<see cref="PrepareTransactionAsync(ulong)"/>, <see cref="MarkTransactionCommittedAsync"/>) in
    /// multi-process mode. They append to the shared WAL at this engine's own write position,
    /// without the cross-process writer lock, without catching up with the shared WAL first and
    /// without publishing the new WAL end (#188). Their records can overwrite another process's,
    /// and stay invisible to other processes, which then checkpoint and truncate the WAL without
    /// them. This engine then discards them along with everything else it holds from the old WAL,
    /// so the commit is lost. Group commit (<see cref="Transaction.CommitAsync"/>) does all of
    /// that under both locks.
    /// </summary>
    private void ThrowIfMultiProcessAccess(string operation)
    {
        if (_shm != null)
            throw new NotSupportedException(
                $"{operation} is not supported with PageFileConfig.AllowMultiProcessAccess. " +
                "Commit through Transaction.CommitAsync or CommitTransactionAsync(transactionId, cancellationToken).");
    }

    /// <summary>
    /// Rolls back a transaction: discards all uncommitted changes.
    /// </summary>
    /// <param name="transactionId">Transaction to rollback</param>
    public async Task RollbackTransactionAsync(ulong transactionId)
    {
        _walCache.TryRemove(transactionId, out _);
        // The committed state still uses the pages this transaction meant to free (#233).
        DiscardFreesOnCommit(transactionId);
        try
        {
            // The abort record is advisory: replay and recovery only apply transactions with a
            // commit record. In multi-process mode it would be appended outside the commit path,
            // without the SHM writer lock and at this engine's own write position, which other
            // processes' appends or a truncation may have made stale. Written there, it overwrites
            // their records or leaves a gap of zeros that ends every other process's replay early.
            if (_shm == null)
                await _wal.WriteAbortRecordAsync(transactionId);
        }
        finally
        {
            CompleteTransaction(transactionId);
        }
        _metrics?.Publish(new Metrics.MetricEvent
        {
            Timestamp = Stopwatch.GetTimestamp(),
            Type      = Metrics.MetricEventType.TransactionRollback,
            Success   = true,
        });
    }

    /// <summary>
    /// Gets the number of active transactions (diagnostics).
    /// </summary>
    public int ActiveTransactionCount => _walCache.Count;
}
