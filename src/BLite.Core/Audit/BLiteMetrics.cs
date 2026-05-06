using System.Threading;

namespace BLite.Core.Audit;

/// <summary>
/// In-memory, allocation-free counters for BLite operations. Updated from
/// hot paths via <see cref="Interlocked"/> so reads and writes are safe
/// across threads. Instantiated only when
/// <see cref="BLiteAuditOptions.EnableMetrics"/> is <c>true</c>.
/// </summary>
public sealed class BLiteMetrics
{
    private long _insertCount;
    private long _queryCount;
    private long _commitCount;
    private long _indexHitCount;
    private long _bsonScanCount;
    private long _fullScanCount;

    private long _insertElapsedTicks;
    private long _queryElapsedTicks;
    private long _commitElapsedTicks;

    public long InsertCount => Interlocked.Read(ref _insertCount);
    public long QueryCount => Interlocked.Read(ref _queryCount);
    public long CommitCount => Interlocked.Read(ref _commitCount);
    public long IndexHitCount => Interlocked.Read(ref _indexHitCount);
    public long BsonScanCount => Interlocked.Read(ref _bsonScanCount);
    public long FullScanCount => Interlocked.Read(ref _fullScanCount);

    public double AvgInsertMs
    {
        get
        {
            long count = Interlocked.Read(ref _insertCount);
            if (count == 0) return 0d;
            long ticks = Interlocked.Read(ref _insertElapsedTicks);
            return TicksToMs(ticks) / count;
        }
    }

    public double AvgQueryMs
    {
        get
        {
            long count = Interlocked.Read(ref _queryCount);
            if (count == 0) return 0d;
            long ticks = Interlocked.Read(ref _queryElapsedTicks);
            return TicksToMs(ticks) / count;
        }
    }

    public double AvgCommitMs
    {
        get
        {
            long count = Interlocked.Read(ref _commitCount);
            if (count == 0) return 0d;
            long ticks = Interlocked.Read(ref _commitElapsedTicks);
            return TicksToMs(ticks) / count;
        }
    }

    /// <summary>
    /// Fraction of queries served by an index scan, in [0, 1]. Returns 0
    /// when no queries have been recorded yet.
    /// </summary>
    public double CacheHitRate
    {
        get
        {
            long total = Interlocked.Read(ref _queryCount);
            if (total == 0) return 0d;
            long hits = Interlocked.Read(ref _indexHitCount);
            return (double)hits / total;
        }
    }

    public void RecordInsert(System.TimeSpan elapsed)
    {
        Interlocked.Increment(ref _insertCount);
        Interlocked.Add(ref _insertElapsedTicks, elapsed.Ticks);
    }

    public void RecordQuery(QueryStrategy strategy, System.TimeSpan elapsed)
    {
        Interlocked.Increment(ref _queryCount);
        Interlocked.Add(ref _queryElapsedTicks, elapsed.Ticks);

        switch (strategy)
        {
            case QueryStrategy.IndexScan:
                Interlocked.Increment(ref _indexHitCount);
                break;
            case QueryStrategy.BsonScan:
                Interlocked.Increment(ref _bsonScanCount);
                break;
            case QueryStrategy.FullScan:
                Interlocked.Increment(ref _fullScanCount);
                break;
        }
    }

    public void RecordCommit(System.TimeSpan elapsed)
    {
        Interlocked.Increment(ref _commitCount);
        Interlocked.Add(ref _commitElapsedTicks, elapsed.Ticks);
    }

    public void Reset()
    {
        Interlocked.Exchange(ref _insertCount, 0);
        Interlocked.Exchange(ref _queryCount, 0);
        Interlocked.Exchange(ref _commitCount, 0);
        Interlocked.Exchange(ref _indexHitCount, 0);
        Interlocked.Exchange(ref _bsonScanCount, 0);
        Interlocked.Exchange(ref _fullScanCount, 0);
        Interlocked.Exchange(ref _insertElapsedTicks, 0);
        Interlocked.Exchange(ref _queryElapsedTicks, 0);
        Interlocked.Exchange(ref _commitElapsedTicks, 0);
    }

    private static double TicksToMs(long ticks) =>
        (double)ticks / System.TimeSpan.TicksPerMillisecond;
}
