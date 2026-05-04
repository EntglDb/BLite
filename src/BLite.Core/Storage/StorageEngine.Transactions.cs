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
        if (_shm != null && _wal is Transactions.WriteAheadLog walForReplay)
        {
            long shmEnd   = _shm.ReadWalEndOffset();
            long lastKnown = Volatile.Read(ref _lastKnownWalEndOffset);

            // Detect WAL truncation by another process: shmEnd < lastKnown means the WAL
            // was reset to 0 by a checkpoint and new records are already being appended.
            // Reset our bookmark so the next iteration replays from the new base.
            if (shmEnd < lastKnown)
            {
                Interlocked.CompareExchange(ref _lastKnownWalEndOffset, 0, lastKnown);
                lastKnown = Volatile.Read(ref _lastKnownWalEndOffset);
            }

            if (shmEnd > lastKnown)
            {
                // Attempt to claim the replay range; only the first thread wins —
                // others will see the updated _lastKnownWalEndOffset and skip.
                if (Interlocked.CompareExchange(ref _lastKnownWalEndOffset, shmEnd, lastKnown) == lastKnown)
                {
                    try
                    {
                        var newPages = walForReplay.ReadCommittedPagesSince(lastKnown, shmEnd);
                        foreach (var (pid, data, walOffset) in newPages)
                        {
                            // Only populate _walIndex if this engine hasn't already committed
                            // a newer local version of this page. `_walOffsets` tracks pages
                            // committed by THIS engine; if the page is present there, our
                            // local _walIndex already has the fresher version.
                            if (_walOffsets == null || !_walOffsets.ContainsKey(pid))
                            {
                                _walIndex[pid] = data;
                                // Record the WAL offset so CheckpointAsync can apply the
                                // GetMinReaderOffset() safe boundary for this page (Phase 6).
                                _walOffsets?[pid] = walOffset;
                            }
                        }
                    }
                    catch
                    {
                        // Best-effort: a failed replay only means stale reads,
                        // never data corruption. Reset so the next transaction retries.
                        Interlocked.CompareExchange(ref _lastKnownWalEndOffset, lastKnown, shmEnd);
                    }
                }
            }
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
    public async Task<bool> PrepareTransactionAsync(ulong transactionId)
    {
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

    public async Task<bool> PrepareTransactionAsync(ulong transactionId, CancellationToken ct = default)
    {
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
    public async Task CommitTransactionAsync(ulong transactionId)
    {
        // NOTE: No admission gate here — the caller (CommitTransactionAsync(Transaction, ct))
        // already acquired the gate. This method is also called from the group commit writer
        // which should not be gated.
        bool needsCheckpoint = false;

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
                return;
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

            // 3. Move pages from cache to WAL index (for reads)
            _walCache.TryRemove(transactionId, out _);
            foreach (var kvp in pages)
            {
                _walIndex[kvp.Key] = kvp.Value;
            }

            // Check if checkpoint is needed, but defer it until after releasing the lock
            needsCheckpoint = _wal.GetCurrentSize() > MaxWalSize;
        }
        finally
        {
            _commitLock.Release();
        }

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
    public async Task MarkTransactionCommittedAsync(ulong transactionId)
    {
        bool needsCheckpoint = false;

        _commitLock.Wait();
        try
        {
            await _wal.WriteCommitRecordAsync(transactionId);
            await _wal.FlushAsync();
            
            // Move from cache to WAL index
            if (_walCache.TryRemove(transactionId, out var pages))
            {
                foreach (var kvp in pages)
                {
                    _walIndex[kvp.Key] = kvp.Value;
                }
            }

            // Check if checkpoint is needed, but defer it until after releasing the lock
            needsCheckpoint = _wal.GetCurrentSize() > MaxWalSize;
        }
        finally
        {
            _commitLock.Release();
        }

        if (needsCheckpoint)
        {
            _ = Task.Run(() => CheckpointAsync());
        }
    }

    /// <summary>
    /// Rolls back a transaction: discards all uncommitted changes.
    /// </summary>
    /// <param name="transactionId">Transaction to rollback</param>
    public async Task RollbackTransactionAsync(ulong transactionId)
    {
        _walCache.TryRemove(transactionId, out _);
        await _wal.WriteAbortRecordAsync(transactionId);
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
