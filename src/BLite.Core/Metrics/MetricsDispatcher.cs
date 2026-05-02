using System.Collections.Concurrent;
using System.Threading.Channels;

namespace BLite.Core.Metrics;

/// <summary>
/// Collects <see cref="MetricEvent"/> values published by the storage engine and
/// aggregates them into atomic counters that can be queried at any time via
/// <see cref="GetSnapshot"/>.
///
/// Architecture (mirrors <c>ChangeStreamDispatcher</c>):
/// <list type="bullet">
///   <item>The hot path calls <see cref="Publish"/> which does a single non-blocking
///   <c>TryWrite</c> on an unbounded channel — effectively zero overhead when
///   no consumer is active.</item>
///   <item>A single background reader drains the channel and updates <c>long</c>
///   counters via <c>Interlocked</c> — no locks, no allocations.</item>
///   <item>Callers read the accumulated counters by calling <see cref="GetSnapshot"/>,
///   which takes a point-in-time copy.</item>
/// </list>
/// </summary>
internal sealed class MetricsDispatcher : IDisposable
{
    private readonly Channel<MetricEvent> _channel;
    private readonly CancellationTokenSource _cts = new();

    // ── Transaction counters ────────────────────────────────────────────────
    private long _txBegins;
    private long _txCommits;
    private long _txRollbacks;
    private long _txCommitLatencySum;   // microseconds

    // ── Group commit counters ───────────────────────────────────────────────
    private long _gcBatches;
    private long _gcBatchSizeSum;

    // ── Checkpoint counters ─────────────────────────────────────────────────
    private long _checkpoints;
    private long _checkpointLatencySum; // microseconds

    // ── Global collection counters ──────────────────────────────────────────
    private long _inserts;
    private long _updates;
    private long _deletes;
    private long _finds;
    private long _queries;
    private long _insertLatencySum;
    private long _updateLatencySum;
    private long _deleteLatencySum;
    private long _queryLatencySum;

    // ── Per-collection counters ─────────────────────────────────────────────
    // ConcurrentDictionary<collectionName, CollectionCounters>
    private readonly ConcurrentDictionary<string, CollectionCounters> _collections
        = new(StringComparer.OrdinalIgnoreCase);

    // ── Security / audit counters ───────────────────────────────────────────
    private readonly ConcurrentDictionary<string, AuditEventCounter> _auditEvents
        = new(StringComparer.OrdinalIgnoreCase);
    private long _securityFailedQueries;
    private long _vacuumLastRunAtTicks;    // DateTimeOffset.UtcNow.Ticks; 0 = never
    private long _vacuumBytesFreed;        // bytes compacted in last vacuum
    private long _backupLastSuccessAtTicks;// DateTimeOffset.UtcNow.Ticks; 0 = never
    private long _backupLastDurationMs;    // duration of last backup in milliseconds

#if NET6_0_OR_GREATER
    // ── Shared library-level OpenTelemetry / System.Diagnostics.Metrics instruments ──
    // A single Meter per process prevents duplicate metric scopes when multiple engines run
    // in the same process. Instruments are lazily created alongside the Meter.
    private static readonly System.Diagnostics.Metrics.Meter s_meter =
        new System.Diagnostics.Metrics.Meter("BLite.Core");
    private static readonly System.Diagnostics.Metrics.Counter<long> s_meterSecurityFailedQueries =
        s_meter.CreateCounter<long>(
            "blite.security.failed_queries",
            unit: "queries",
            description: "Total queries rejected by BLQL hardening.");
    private static readonly System.Diagnostics.Metrics.Counter<long> s_meterVacuumTotal =
        s_meter.CreateCounter<long>(
            "blite.vacuum.total",
            unit: "operations",
            description: "Total VACUUM passes completed.");
    private static readonly System.Diagnostics.Metrics.Counter<long> s_meterBackupTotal =
        s_meter.CreateCounter<long>(
            "blite.backup.completed.total",
            unit: "operations",
            description: "Total successful hot-backup operations.");
    private static readonly System.Diagnostics.Metrics.Counter<long> s_meterAuditEvents =
        s_meter.CreateCounter<long>(
            "blite.audit.events_total",
            unit: "events",
            description: "Total audit events emitted, by event type.");

    // Per-instance flag: set to true when EnableDiagnosticSource was requested.
    // Controls whether this dispatcher contributes to the shared OTel instruments.
    private readonly bool _diagnosticSourceEnabled;
#endif

    public MetricsDispatcher(bool enableDiagnosticSource = false)
    {
        _channel = Channel.CreateUnbounded<MetricEvent>(new UnboundedChannelOptions
        {
            SingleReader = true,
            SingleWriter = false,
            AllowSynchronousContinuations = false
        });

#if NET6_0_OR_GREATER
        _diagnosticSourceEnabled = enableDiagnosticSource;
#endif

        Task.Run(ProcessEventsAsync);
    }

    /// <summary>
    /// Publishes a metric event. Non-blocking; safe to call on any thread.
    /// </summary>
    public void Publish(MetricEvent evt)
    {
        _channel.Writer.TryWrite(evt);
    }

    /// <summary>
    /// Returns a point-in-time immutable snapshot of all accumulated counters.
    /// </summary>
    public MetricsSnapshot GetSnapshot()
    {
        long txCommits    = Interlocked.Read(ref _txCommits);
        long txLatSum     = Interlocked.Read(ref _txCommitLatencySum);
        long gcBatches    = Interlocked.Read(ref _gcBatches);
        long gcSizeSum    = Interlocked.Read(ref _gcBatchSizeSum);
        long cpCount      = Interlocked.Read(ref _checkpoints);
        long cpLatSum     = Interlocked.Read(ref _checkpointLatencySum);
        long inserts      = Interlocked.Read(ref _inserts);
        long updates      = Interlocked.Read(ref _updates);
        long deletes      = Interlocked.Read(ref _deletes);
        long finds        = Interlocked.Read(ref _finds);
        long queries      = Interlocked.Read(ref _queries);
        long insLatSum    = Interlocked.Read(ref _insertLatencySum);
        long updLatSum    = Interlocked.Read(ref _updateLatencySum);
        long delLatSum    = Interlocked.Read(ref _deleteLatencySum);
        long qryLatSum    = Interlocked.Read(ref _queryLatencySum);

        var colSnapshots = new Dictionary<string, CollectionMetricsSnapshot>(_collections.Count, StringComparer.OrdinalIgnoreCase);
        foreach (var (name, c) in _collections)
        {
            long cIns = Interlocked.Read(ref c.Inserts);
            long cUpd = Interlocked.Read(ref c.Updates);
            long cDel = Interlocked.Read(ref c.Deletes);
            long cFnd = Interlocked.Read(ref c.Finds);
            long cQry = Interlocked.Read(ref c.Queries);
            long cInsLat = Interlocked.Read(ref c.InsertLatencySum);
            long cUpdLat = Interlocked.Read(ref c.UpdateLatencySum);
            long cDelLat = Interlocked.Read(ref c.DeleteLatencySum);
            long cQryLat = Interlocked.Read(ref c.QueryLatencySum);

            colSnapshots[name] = new CollectionMetricsSnapshot
            {
                Name             = name,
                InsertCount      = cIns,
                UpdateCount      = cUpd,
                DeleteCount      = cDel,
                FindCount        = cFnd,
                QueryCount       = cQry,
                AvgInsertLatencyUs = cIns > 0 ? (double)cInsLat / cIns : 0,
                AvgUpdateLatencyUs = cUpd > 0 ? (double)cUpdLat / cUpd : 0,
                AvgDeleteLatencyUs = cDel > 0 ? (double)cDelLat / cDel : 0,
            };
        }

        // ── Security / audit snapshot ───────────────────────────────────────
        var auditSnapshot = new Dictionary<string, long>(_auditEvents.Count, StringComparer.OrdinalIgnoreCase);
        foreach (var (k, v) in _auditEvents)
            auditSnapshot[k] = Interlocked.Read(ref v.Count);

        long vacuumTicks  = Interlocked.Read(ref _vacuumLastRunAtTicks);
        long backupTicks  = Interlocked.Read(ref _backupLastSuccessAtTicks);

        return new MetricsSnapshot
        {
            TransactionBeginsTotal    = Interlocked.Read(ref _txBegins),
            TransactionCommitsTotal   = txCommits,
            TransactionRollbacksTotal = Interlocked.Read(ref _txRollbacks),
            AvgCommitLatencyUs        = txCommits > 0 ? (double)txLatSum / txCommits : 0,
            GroupCommitBatchesTotal   = gcBatches,
            GroupCommitAvgBatchSize   = gcBatches > 0 ? (double)gcSizeSum / gcBatches : 0,
            CheckpointsTotal          = cpCount,
            AvgCheckpointLatencyUs    = cpCount > 0 ? (double)cpLatSum / cpCount : 0,
            InsertsTotal              = inserts,
            UpdatesTotal              = updates,
            DeletesTotal              = deletes,
            FindsTotal                = finds,
            QueriesTotal              = queries,
            AvgInsertLatencyUs        = inserts > 0 ? (double)insLatSum / inserts : 0,
            AvgUpdateLatencyUs        = updates > 0 ? (double)updLatSum / updates : 0,
            AvgDeleteLatencyUs        = deletes > 0 ? (double)delLatSum / deletes : 0,
            AvgQueryLatencyUs         = queries > 0 ? (double)qryLatSum / queries : 0,
            Collections               = colSnapshots,
            AuditEventsTotal          = auditSnapshot,
            SecurityFailedQueriesTotal = Interlocked.Read(ref _securityFailedQueries),
            VacuumLastRunAt           = vacuumTicks  > 0 ? new DateTimeOffset(vacuumTicks,  TimeSpan.Zero) : null,
            VacuumBytesFreed          = Interlocked.Read(ref _vacuumBytesFreed),
            BackupLastSuccessAt       = backupTicks  > 0 ? new DateTimeOffset(backupTicks,  TimeSpan.Zero) : null,
            BackupLastDurationMs      = Interlocked.Read(ref _backupLastDurationMs),
            SnapshotTimestamp         = DateTimeOffset.UtcNow,
        };
    }

    private async Task ProcessEventsAsync()
    {
        try
        {
            var reader = _channel.Reader;
            while (await reader.WaitToReadAsync(_cts.Token).ConfigureAwait(false))
            {
                while (reader.TryRead(out var evt))
                {
                    Aggregate(evt);
                }
            }
        }
        catch (OperationCanceledException) { }
        catch (Exception) { /* metrics must never crash the host */ }
    }

    private void Aggregate(MetricEvent evt)
    {
        long micros = evt.ElapsedMicros;

        switch (evt.Type)
        {
            case MetricEventType.TransactionBegin:
                Interlocked.Increment(ref _txBegins);
                break;

            case MetricEventType.TransactionCommit:
                Interlocked.Increment(ref _txCommits);
                Interlocked.Add(ref _txCommitLatencySum, micros);
                break;

            case MetricEventType.TransactionRollback:
                Interlocked.Increment(ref _txRollbacks);
                break;

            case MetricEventType.GroupCommitBatch:
                Interlocked.Increment(ref _gcBatches);
                Interlocked.Add(ref _gcBatchSizeSum, evt.BatchSize);
                break;

            case MetricEventType.Checkpoint:
                Interlocked.Increment(ref _checkpoints);
                Interlocked.Add(ref _checkpointLatencySum, micros);
                break;

            case MetricEventType.CollectionInsert:
                Interlocked.Increment(ref _inserts);
                Interlocked.Add(ref _insertLatencySum, micros);
                if (evt.CollectionName != null)
                {
                    var c = GetOrAddCollection(evt.CollectionName);
                    Interlocked.Increment(ref c.Inserts);
                    Interlocked.Add(ref c.InsertLatencySum, micros);
                }
                break;

            case MetricEventType.CollectionUpdate:
                Interlocked.Increment(ref _updates);
                Interlocked.Add(ref _updateLatencySum, micros);
                if (evt.CollectionName != null)
                {
                    var c = GetOrAddCollection(evt.CollectionName);
                    Interlocked.Increment(ref c.Updates);
                    Interlocked.Add(ref c.UpdateLatencySum, micros);
                }
                break;

            case MetricEventType.CollectionDelete:
                Interlocked.Increment(ref _deletes);
                Interlocked.Add(ref _deleteLatencySum, micros);
                if (evt.CollectionName != null)
                {
                    var c = GetOrAddCollection(evt.CollectionName);
                    Interlocked.Increment(ref c.Deletes);
                    Interlocked.Add(ref c.DeleteLatencySum, micros);
                }
                break;

            case MetricEventType.CollectionFind:
                Interlocked.Increment(ref _finds);
                if (evt.CollectionName != null)
                {
                    var c = GetOrAddCollection(evt.CollectionName);
                    Interlocked.Increment(ref c.Finds);
                }
                break;

            case MetricEventType.CollectionQuery:
                Interlocked.Increment(ref _queries);
                Interlocked.Add(ref _queryLatencySum, micros);
                if (evt.CollectionName != null)
                {
                    var c = GetOrAddCollection(evt.CollectionName);
                    Interlocked.Increment(ref c.Queries);
                    Interlocked.Add(ref c.QueryLatencySum, micros);
                }
                break;

            case MetricEventType.AuditEvent:
                if (evt.Tag != null)
                {
                    IncrementAuditEvent(evt.Tag);
#if NET6_0_OR_GREATER
                    if (_diagnosticSourceEnabled)
                        s_meterAuditEvents.Add(1, new KeyValuePair<string, object?>("event_type", evt.Tag));
#endif
                }
                break;

            case MetricEventType.SecurityFailedQuery:
                Interlocked.Increment(ref _securityFailedQueries);
                IncrementAuditEvent("security.failed_query");
#if NET6_0_OR_GREATER
                if (_diagnosticSourceEnabled)
                {
                    s_meterSecurityFailedQueries.Add(1);
                    s_meterAuditEvents.Add(1, new KeyValuePair<string, object?>("event_type", "security.failed_query"));
                }
#endif
                break;

            case MetricEventType.Vacuum:
                if (evt.Success)
                {
                    Interlocked.Exchange(ref _vacuumLastRunAtTicks, DateTimeOffset.UtcNow.Ticks);
                    Interlocked.Exchange(ref _vacuumBytesFreed, evt.BytesFreed);
                }
                IncrementAuditEvent("vacuum");
#if NET6_0_OR_GREATER
                if (_diagnosticSourceEnabled)
                {
                    s_meterVacuumTotal.Add(1);
                    s_meterAuditEvents.Add(1, new KeyValuePair<string, object?>("event_type", "vacuum"));
                }
#endif
                break;

            case MetricEventType.BackupCompleted:
                Interlocked.Exchange(ref _backupLastSuccessAtTicks, DateTimeOffset.UtcNow.Ticks);
                Interlocked.Exchange(ref _backupLastDurationMs, evt.ElapsedMicros / 1000);
                IncrementAuditEvent("backup.completed");
#if NET6_0_OR_GREATER
                if (_diagnosticSourceEnabled)
                {
                    s_meterBackupTotal.Add(1);
                    s_meterAuditEvents.Add(1, new KeyValuePair<string, object?>("event_type", "backup.completed"));
                }
#endif
                break;
        }
    }

    private void IncrementAuditEvent(string eventType)
    {
        var counter = _auditEvents.GetOrAdd(eventType, _ => new AuditEventCounter());
        Interlocked.Increment(ref counter.Count);
    }

    private CollectionCounters GetOrAddCollection(string name)
        => _collections.GetOrAdd(name, _ => new CollectionCounters());

    public void Dispose()
    {
        _cts.Cancel();
        _channel.Writer.TryComplete();
        _cts.Dispose();
        // The shared static Meter (s_meter) is intentionally NOT disposed here —
        // it lives for the lifetime of the process and is shared across all engines.
    }

    // ── Mutable per-collection accumulator (fields accessed via Interlocked) ─
    private sealed class CollectionCounters
    {
        public long Inserts;
        public long Updates;
        public long Deletes;
        public long Finds;
        public long Queries;
        public long InsertLatencySum;
        public long UpdateLatencySum;
        public long DeleteLatencySum;
        public long QueryLatencySum;
    }

    // ── Mutable per-audit-event-type counter (field accessed via Interlocked) ─
    private sealed class AuditEventCounter
    {
        public long Count;
    }
}
