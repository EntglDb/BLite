using System.Threading;

namespace BLite.Core.Audit;

/// <summary>
/// Cumulative thread-safe counters for BLite audit metrics.
/// Accessible via <c>BLiteEngine.Metrics</c> / <c>DocumentDbContext.Metrics</c>
/// when <see cref="BLiteAuditOptions.EnableMetrics"/> is <see langword="true"/>.
/// </summary>
/// <remarks>
/// All updates use <see cref="Interlocked"/> operations (~10-20 ns overhead each).
/// </remarks>
public sealed class BLiteMetrics
{
    private long _totalInserts;
    private long _totalQueriesIndexScan;
    private long _totalQueriesBsonScan;
    private long _totalQueriesFullScan;
    private long _totalCommits;
    private long _totalInsertMs;
    private long _totalQueryMs;

    // ── Read (point-in-time snapshot) ──────────────────────────────────────

    /// <summary>Total number of document inserts recorded since the engine was configured.</summary>
    public long TotalInserts => InterlockedRead(ref _totalInserts);

    /// <summary>Total number of index-scan queries.</summary>
    public long TotalQueriesIndexScan => InterlockedRead(ref _totalQueriesIndexScan);

    /// <summary>Total number of BSON-predicate scan queries.</summary>
    public long TotalQueriesBsonScan => InterlockedRead(ref _totalQueriesBsonScan);

    /// <summary>Total number of full-scan queries.</summary>
    public long TotalQueriesFullScan => InterlockedRead(ref _totalQueriesFullScan);

    /// <summary>Total number of transaction commits.</summary>
    public long TotalCommits => InterlockedRead(ref _totalCommits);

    /// <summary>Total queries across all strategies.</summary>
    public long TotalQueries =>
        TotalQueriesIndexScan + TotalQueriesBsonScan + TotalQueriesFullScan;

    /// <summary>Average insert duration in milliseconds (0 when no inserts).</summary>
    public double AvgInsertMs
    {
        get
        {
            var ins = TotalInserts;
            return ins == 0 ? 0 : (double)InterlockedRead(ref _totalInsertMs) / ins;
        }
    }

    /// <summary>Average query duration in milliseconds (0 when no queries).</summary>
    public double AvgQueryMs
    {
        get
        {
            var qry = TotalQueries;
            return qry == 0 ? 0 : (double)InterlockedRead(ref _totalQueryMs) / qry;
        }
    }

    // ── Update (internal to BLite.Core) ────────────────────────────────────

    internal void RecordInsert(TimeSpan elapsed)
    {
        Interlocked.Increment(ref _totalInserts);
        Interlocked.Add(ref _totalInsertMs, (long)elapsed.TotalMilliseconds);
    }

    internal void RecordQuery(QueryStrategy strategy, TimeSpan elapsed)
    {
        if (strategy == QueryStrategy.IndexScan)
            Interlocked.Increment(ref _totalQueriesIndexScan);
        else if (strategy == QueryStrategy.BsonScan)
            Interlocked.Increment(ref _totalQueriesBsonScan);
        else
            Interlocked.Increment(ref _totalQueriesFullScan);

        Interlocked.Add(ref _totalQueryMs, (long)elapsed.TotalMilliseconds);
    }

    internal void RecordCommit() => Interlocked.Increment(ref _totalCommits);

    /// <summary>Resets all counters to zero (useful for periodic resets or tests).</summary>
    public void Reset()
    {
        Interlocked.Exchange(ref _totalInserts, 0);
        Interlocked.Exchange(ref _totalQueriesIndexScan, 0);
        Interlocked.Exchange(ref _totalQueriesBsonScan, 0);
        Interlocked.Exchange(ref _totalQueriesFullScan, 0);
        Interlocked.Exchange(ref _totalCommits, 0);
        Interlocked.Exchange(ref _totalInsertMs, 0);
        Interlocked.Exchange(ref _totalQueryMs, 0);
    }

    // ── Polyfill for Interlocked.Read (not available on netstandard2.1) ────
    private static long InterlockedRead(ref long field)
        => Interlocked.CompareExchange(ref field, 0, 0);
}
