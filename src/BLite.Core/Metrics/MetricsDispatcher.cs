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
    private long _insertLatencySum;
    private long _updateLatencySum;
    private long _deleteLatencySum;

    // ── Per-collection counters ─────────────────────────────────────────────
    // ConcurrentDictionary<collectionName, CollectionCounters>
    private readonly ConcurrentDictionary<string, CollectionCounters> _collections
        = new(StringComparer.OrdinalIgnoreCase);

    public MetricsDispatcher()
    {
        _channel = Channel.CreateUnbounded<MetricEvent>(new UnboundedChannelOptions
        {
            SingleReader = true,
            SingleWriter = false,
            AllowSynchronousContinuations = false
        });

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
        long insLatSum    = Interlocked.Read(ref _insertLatencySum);
        long updLatSum    = Interlocked.Read(ref _updateLatencySum);
        long delLatSum    = Interlocked.Read(ref _deleteLatencySum);

        var colSnapshots = new Dictionary<string, CollectionMetricsSnapshot>(_collections.Count, StringComparer.OrdinalIgnoreCase);
        foreach (var (name, c) in _collections)
        {
            long cIns = Interlocked.Read(ref c.Inserts);
            long cUpd = Interlocked.Read(ref c.Updates);
            long cDel = Interlocked.Read(ref c.Deletes);
            long cFnd = Interlocked.Read(ref c.Finds);
            long cInsLat = Interlocked.Read(ref c.InsertLatencySum);
            long cUpdLat = Interlocked.Read(ref c.UpdateLatencySum);
            long cDelLat = Interlocked.Read(ref c.DeleteLatencySum);

            colSnapshots[name] = new CollectionMetricsSnapshot
            {
                Name             = name,
                InsertCount      = cIns,
                UpdateCount      = cUpd,
                DeleteCount      = cDel,
                FindCount        = cFnd,
                AvgInsertLatencyUs = cIns > 0 ? (double)cInsLat / cIns : 0,
                AvgUpdateLatencyUs = cUpd > 0 ? (double)cUpdLat / cUpd : 0,
                AvgDeleteLatencyUs = cDel > 0 ? (double)cDelLat / cDel : 0,
            };
        }

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
            AvgInsertLatencyUs        = inserts > 0 ? (double)insLatSum / inserts : 0,
            AvgUpdateLatencyUs        = updates > 0 ? (double)updLatSum / updates : 0,
            AvgDeleteLatencyUs        = deletes > 0 ? (double)delLatSum / deletes : 0,
            Collections               = colSnapshots,
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
        }
    }

    private CollectionCounters GetOrAddCollection(string name)
        => _collections.GetOrAdd(name, _ => new CollectionCounters());

    public void Dispose()
    {
        _cts.Cancel();
        _channel.Writer.TryComplete();
        _cts.Dispose();
    }

    // ── Mutable per-collection accumulator (fields accessed via Interlocked) ─
    private sealed class CollectionCounters
    {
        public long Inserts;
        public long Updates;
        public long Deletes;
        public long Finds;
        public long InsertLatencySum;
        public long UpdateLatencySum;
        public long DeleteLatencySum;
    }
}
