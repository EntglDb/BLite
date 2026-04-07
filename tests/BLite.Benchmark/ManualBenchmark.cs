using BLite.Shared;
using System.Diagnostics;
using System.IO;
using System.Text;

namespace BLite.Benchmark;

public class ManualBenchmark
{
    private static readonly StringBuilder _log = new();

    private static void Log(string message = "")
    {
        Console.WriteLine(message);
        _log.AppendLine(message);
    }

    public static void Run()
    {
        _log.Clear();
        Log("=== MANUAL BENCHMARK: BLite vs LiteDB vs SQLite+JSON vs CouchbaseLite vs DuckDB ===");
        Log($"Date: {DateTime.Now}");
        Log("Document: CustomerOrder (nested objects + collections)\n");

        var sw = Stopwatch.StartNew();

        // ── Batch Insert (1000) ─────────────────────────────────────
        Log("1. Batch Insert (1000 CustomerOrders)");

        long Measure(Action action) { sw.Restart(); action(); sw.Stop(); return sw.ElapsedMilliseconds; }
        long MeasureAsync(Func<Task> action) { sw.Restart(); action().GetAwaiter().GetResult(); sw.Stop(); return sw.ElapsedMilliseconds; }

        var b1 = new InsertBenchmarks(); b1.Setup(); b1.IterationSetup();
        var bliteBatch = MeasureAsync(b1.BLite_Insert_Batch);
        Log($"   BLite:        {bliteBatch} ms");

        var b2 = new InsertBenchmarks(); b2.Setup(); b2.IterationSetup();
        var liteBatch = Measure(b2.LiteDb_Insert_Batch);
        Log($"   LiteDB:       {liteBatch} ms");

        var b3 = new InsertBenchmarks(); b3.Setup(); b3.IterationSetup();
        var sqliteBatch = Measure(b3.Sqlite_Insert_Batch);
        Log($"   SQLite+JSON:  {sqliteBatch} ms");

        var b4 = new InsertBenchmarks(); b4.Setup(); b4.IterationSetup();
        var cblBatch = Measure(b4.CBL_Insert_Batch);
        Log($"   CouchbaseLite:{cblBatch} ms");

        var b5 = new InsertBenchmarks(); b5.Setup(); b5.IterationSetup();
        var duckBatch = Measure(b5.DuckDB_Insert_Batch);
        Log($"   DuckDB:       {duckBatch} ms\n");

        b1.Cleanup(); b2.Cleanup(); b3.Cleanup(); b4.Cleanup(); b5.Cleanup();

        // ── Single Insert ───────────────────────────────────────────
        Log("2. Single Insert");

        var s1 = new InsertBenchmarks(); s1.Setup(); s1.IterationSetup();
        var bliteSingle = MeasureAsync(s1.BLite_Insert_Single);
        Log($"   BLite:        {bliteSingle} ms");

        var s2 = new InsertBenchmarks(); s2.Setup(); s2.IterationSetup();
        var liteSingle = Measure(s2.LiteDb_Insert_Single);
        Log($"   LiteDB:       {liteSingle} ms");

        var s3 = new InsertBenchmarks(); s3.Setup(); s3.IterationSetup();
        var sqliteSingle = Measure(s3.Sqlite_Insert_Single);
        Log($"   SQLite+JSON:  {sqliteSingle} ms");

        var s4 = new InsertBenchmarks(); s4.Setup(); s4.IterationSetup();
        var cblSingle = Measure(s4.CBL_Insert_Single);
        Log($"   CouchbaseLite:{cblSingle} ms");

        var s5 = new InsertBenchmarks(); s5.Setup(); s5.IterationSetup();
        var duckSingle = Measure(s5.DuckDB_Insert_Single);
        Log($"   DuckDB:       {duckSingle} ms\n");

        s1.Cleanup(); s2.Cleanup(); s3.Cleanup(); s4.Cleanup(); s5.Cleanup();

        // ── FindById (1000 ops) ─────────────────────────────────────
        Log("3. FindById Performance (1000 operations over 1000 documents)");
        var r = new ReadBenchmarks();
        r.Setup().GetAwaiter().GetResult();

        sw.Restart();
        for (int i = 0; i < 1000; i++) r.BLite_FindById().GetAwaiter().GetResult();
        var bliteRead = sw.ElapsedMilliseconds;
        Log($"   BLite:        {bliteRead} ms  ({bliteRead / 1000.0:F3} ms/op)");

        sw.Restart();
        for (int i = 0; i < 1000; i++) r.LiteDb_FindById();
        var liteRead = sw.ElapsedMilliseconds;
        Log($"   LiteDB:       {liteRead} ms  ({liteRead / 1000.0:F3} ms/op)");

        sw.Restart();
        for (int i = 0; i < 1000; i++) r.Sqlite_FindById();
        var sqliteRead = sw.ElapsedMilliseconds;
        Log($"   SQLite+JSON:  {sqliteRead} ms  ({sqliteRead / 1000.0:F3} ms/op)");

        sw.Restart();
        for (int i = 0; i < 1000; i++) r.CBL_FindById();
        var cblRead = sw.ElapsedMilliseconds;
        Log($"   CouchbaseLite:{cblRead} ms  ({cblRead / 1000.0:F3} ms/op)");

        sw.Restart();
        for (int i = 0; i < 1000; i++) r.DuckDB_FindById();
        var duckRead = sw.ElapsedMilliseconds;
        Log($"   DuckDB:       {duckRead} ms  ({duckRead / 1000.0:F3} ms/op)\n");

        // ── Scan by Status (100 ops) ────────────────────────────────
        Log("4. Scan by Status = \"shipped\" (~250 results, 100 operations)");

        sw.Restart();
        for (int i = 0; i < 100; i++) r.BLite_Scan().GetAwaiter().GetResult();
        var bliteScan = sw.ElapsedMilliseconds;
        Log($"   BLite:        {bliteScan} ms  ({bliteScan / 100.0:F1} ms/op)");

        sw.Restart();
        for (int i = 0; i < 100; i++) r.LiteDb_Scan();
        var liteScan = sw.ElapsedMilliseconds;
        Log($"   LiteDB:       {liteScan} ms  ({liteScan / 100.0:F1} ms/op)");

        sw.Restart();
        for (int i = 0; i < 100; i++) r.Sqlite_Scan();
        var sqliteScan = sw.ElapsedMilliseconds;
        Log($"   SQLite+JSON:  {sqliteScan} ms  ({sqliteScan / 100.0:F1} ms/op)");

        sw.Restart();
        for (int i = 0; i < 100; i++) r.CBL_Scan();
        var cblScan = sw.ElapsedMilliseconds;
        Log($"   CouchbaseLite:{cblScan} ms  ({cblScan / 100.0:F1} ms/op)");

        sw.Restart();
        for (int i = 0; i < 100; i++) r.DuckDB_Scan();
        var duckScan = sw.ElapsedMilliseconds;
        Log($"   DuckDB:       {duckScan} ms  ({duckScan / 100.0:F1} ms/op)\n");

        r.Cleanup();

        // ════════════════════════════════════════════════════════════
        // INSERT MICRO-BENCHMARK (repeated iterations, fresh DB each)
        // Baseline BenchmarkDotNet: BLite Batch 13 495 μs, Single 113 μs
        // ════════════════════════════════════════════════════════════
        Log("── Insert Micro-Benchmark (fresh DB per iteration) ─────────────────────────\n");
        Log("  (Baseline BenchmarkDotNet pre-feature: BLite Batch 13 495 μs, Single 113 μs)\n");

        const int insertIters = 30;

        double insertBatchBlite = 0, insertBatchLite = 0, insertBatchSql = 0;
        double insertSingleBlite = 0, insertSingleLite = 0, insertSingleSql = 0;
        long singleTicksBlite = 0, singleTicksLite = 0, singleTicksSql = 0;

        for (int iter = 0; iter < insertIters; iter++)
        {
            var ib = new InsertBenchmarks(); ib.Setup(); ib.IterationSetup();
            sw.Restart(); MeasureAsync(ib.BLite_Insert_Batch); insertBatchBlite  += sw.ElapsedMilliseconds; ib.Cleanup();

            var il = new InsertBenchmarks(); il.Setup(); il.IterationSetup();
            sw.Restart(); il.LiteDb_Insert_Batch();                             insertBatchLite   += sw.ElapsedMilliseconds; il.Cleanup();

            var isq = new InsertBenchmarks(); isq.Setup(); isq.IterationSetup();
            sw.Restart(); isq.Sqlite_Insert_Batch();                            insertBatchSql    += sw.ElapsedMilliseconds; isq.Cleanup();

            var sb2 = new InsertBenchmarks(); sb2.Setup(); sb2.IterationSetup();
            sw.Restart(); MeasureAsync(sb2.BLite_Insert_Single); singleTicksBlite += sw.ElapsedTicks; sb2.Cleanup();

            var sl = new InsertBenchmarks(); sl.Setup(); sl.IterationSetup();
            sw.Restart(); sl.LiteDb_Insert_Single();                            singleTicksLite  += sw.ElapsedTicks; sl.Cleanup();

            var ssq = new InsertBenchmarks(); ssq.Setup(); ssq.IterationSetup();
            sw.Restart(); ssq.Sqlite_Insert_Single();                           singleTicksSql   += sw.ElapsedTicks; ssq.Cleanup();
        }

        double ticksPerUs = Stopwatch.Frequency / 1_000_000.0;
        double batchBliteUs  = insertBatchBlite  * 1000.0 / insertIters;
        double batchLiteUs   = insertBatchLite   * 1000.0 / insertIters;
        double batchSqlUs    = insertBatchSql    * 1000.0 / insertIters;
        double singleBliteUs = singleTicksBlite / ticksPerUs / insertIters;
        double singleLiteUs  = singleTicksLite  / ticksPerUs / insertIters;
        double singleSqlUs   = singleTicksSql   / ticksPerUs / insertIters;

        Log($"10. Batch Insert 1000  [{insertIters} iters]");
        Log($"   BLite:        {batchBliteUs,10:F0} μs/op   [baseline pre-feature: 13 495 μs]");
        Log($"   LiteDB:       {batchLiteUs,10:F0} μs/op   [baseline: 26 793 μs]");
        Log($"   SQLite+JSON:  {batchSqlUs,10:F0} μs/op   [baseline: 22 863 μs]");
        Log($"11. Single Insert  [{insertIters} iters]");
        Log($"   BLite:        {singleBliteUs,10:F1} μs/op   [baseline pre-feature: 113 μs]");
        Log($"   LiteDB:       {singleLiteUs,10:F1} μs/op   [baseline: 716 μs]");
        Log($"   SQLite+JSON:  {singleSqlUs,10:F1} μs/op   [baseline: 2 546 μs]\n");

        // ════════════════════════════════════════════════════════════
        // OLAP SECTION  (10 000 documents, single run each)
        // ════════════════════════════════════════════════════════════
        Log("── OLAP Analytical Operations (10 000 documents) ──────────────────────────\n");
        var olap = new OlapBenchmarks();
        olap.Setup().GetAwaiter().GetResult();

        Log("5. Aggregate – SUM / AVG / COUNT");
        var oBliteAgg  = MeasureAsync(() => olap.BLite_Aggregate());  Log($"   BLite:        {oBliteAgg} ms");
        var oLiteAgg   = Measure(() => { olap.LiteDb_Aggregate(); }); Log($"   LiteDB:       {oLiteAgg} ms");
        var oSqlAgg    = Measure(() => { olap.Sqlite_Aggregate(); }); Log($"   SQLite+JSON:  {oSqlAgg} ms");
        var oCblAgg    = Measure(() => { olap.CBL_Aggregate(); });    Log($"   CouchbaseLite:{oCblAgg} ms");
        var oDuckAgg   = Measure(() => { olap.DuckDB_Aggregate(); }); Log($"   DuckDB:       {oDuckAgg} ms\n");

        Log("6. GroupBy Status – COUNT + SUM(Total)");
        var oBliteGrp  = MeasureAsync(() => olap.BLite_GroupBy());    Log($"   BLite:        {oBliteGrp} ms");
        var oLiteGrp   = Measure(() => { olap.LiteDb_GroupBy(); });   Log($"   LiteDB:       {oLiteGrp} ms");
        var oSqlGrp    = Measure(() => { olap.Sqlite_GroupBy(); });   Log($"   SQLite+JSON:  {oSqlGrp} ms");
        var oCblGrp    = Measure(() => { olap.CBL_GroupBy(); });      Log($"   CouchbaseLite:{oCblGrp} ms");
        var oDuckGrp   = Measure(() => { olap.DuckDB_GroupBy(); });   Log($"   DuckDB:       {oDuckGrp} ms\n");

        Log("7. Range Filter – Total > 4000 (~top 25 %)");
        var oBliteRng  = MeasureAsync(() => olap.BLite_Range());      Log($"   BLite:        {oBliteRng} ms");
        var oLiteRng   = Measure(() => { olap.LiteDb_Range(); });     Log($"   LiteDB:       {oLiteRng} ms");
        var oSqlRng    = Measure(() => { olap.Sqlite_Range(); });     Log($"   SQLite+JSON:  {oSqlRng} ms");
        var oCblRng    = Measure(() => { olap.CBL_Range(); });        Log($"   CouchbaseLite:{oCblRng} ms");
        var oDuckRng   = Measure(() => { olap.DuckDB_Range(); });     Log($"   DuckDB:       {oDuckRng} ms\n");

        Log("8. Top-10 by Total DESC");
        var oBliteTop  = MeasureAsync(() => olap.BLite_TopN());       Log($"   BLite:        {oBliteTop} ms");
        var oLiteTop   = Measure(() => { olap.LiteDb_TopN(); });      Log($"   LiteDB:       {oLiteTop} ms");
        var oSqlTop    = Measure(() => { olap.Sqlite_TopN(); });      Log($"   SQLite+JSON:  {oSqlTop} ms");
        var oCblTop    = Measure(() => { olap.CBL_TopN(); });         Log($"   CouchbaseLite:{oCblTop} ms");
        var oDuckTop   = Measure(() => { olap.DuckDB_TopN(); });      Log($"   DuckDB:       {oDuckTop} ms\n");

        olap.Cleanup();

        // ════════════════════════════════════════════════════════════
        // FIELD OFFSET VECTOR — non-indexed predicate micro-benchmark
        // Tests O(1) field seek vs sequential scan.
        // Baseline (BenchmarkDotNet, pre-offset-table): 2 464 μs/op
        // ════════════════════════════════════════════════════════════
        Log("── Field Offset Vector — non-indexed predicate (10 000 documents) ─────────\n");
        Log("  (Baseline BenchmarkDotNet pre-feature: BLite 2 464 μs/op, SQLite 327 μs/op)\n");

        var fov = new CountBenchmarks();
        fov.Setup().GetAwaiter().GetResult();

        // warm up
        for (int w = 0; w < 5; w++) fov.BLite_CountNonIndexed().GetAwaiter().GetResult();

        const int fovIters = 200;

        long fovBlite, fovLite, fovSql, fovDuck;

        sw.Restart();
        for (int i = 0; i < fovIters; i++) fov.BLite_CountNonIndexed().GetAwaiter().GetResult();
        fovBlite = sw.ElapsedMilliseconds;

        sw.Restart();
        for (int i = 0; i < fovIters; i++) fov.LiteDb_CountNonIndexed();
        fovLite = sw.ElapsedMilliseconds;

        sw.Restart();
        for (int i = 0; i < fovIters; i++) fov.Sqlite_CountNonIndexed();
        fovSql = sw.ElapsedMilliseconds;

        sw.Restart();
        for (int i = 0; i < fovIters; i++) fov.DuckDb_CountNonIndexed();
        fovDuck = sw.ElapsedMilliseconds;

        fov.Cleanup();

        Log($"9. CountAsync(Currency == \"EUR\")  [{fovIters} iterations × 10 000 docs]");
        Log($"   BLite:        {fovBlite * 1000.0 / fovIters,8:F0} μs/op   ({fovBlite} ms total)");
        Log($"   LiteDB:       {fovLite  * 1000.0 / fovIters,8:F0} μs/op   ({fovLite} ms total)");
        Log($"   SQLite+JSON:  {fovSql   * 1000.0 / fovIters,8:F0} μs/op   ({fovSql} ms total)");
        Log($"   DuckDB:       {fovDuck  * 1000.0 / fovIters,8:F0} μs/op   ({fovDuck} ms total)");
        Log($"   [Baseline pre-feature: BLite 2464 μs/op, SQLite 327 μs/op]\n");

        // ── Results ─────────────────────────────────────────────────
        Log("============================================================================");
        Log("RESULTS SUMMARY — OLTP");
        Log("============================================================================");
        Log($"  {"Operation",-30} {"BLite",10} {"LiteDB",10} {"SQLite+JSON",14} {"CouchbaseLite",15} {"DuckDB",10}");
        Log($"  {new string('-', 95)}");
        Log($"  {"Batch Insert 1000",-30} {bliteBatch,8} ms {liteBatch,8} ms {sqliteBatch,12} ms {cblBatch,13} ms {duckBatch,8} ms");
        Log($"  {"Single Insert",-30} {bliteSingle,8} ms {liteSingle,8} ms {sqliteSingle,12} ms {cblSingle,13} ms {duckSingle,8} ms");
        Log($"  {"FindById (avg, 1000x)",-30} {bliteRead / 1000.0,9:F3} {liteRead / 1000.0,9:F3} {sqliteRead / 1000.0,12:F3} {cblRead / 1000.0,13:F3} {duckRead / 1000.0,9:F3} ms");
        Log($"  {"Scan Status (avg, 100x)",-30} {bliteScan / 100.0,9:F1} {liteScan / 100.0,9:F1} {sqliteScan / 100.0,12:F1} {cblScan / 100.0,13:F1} {duckScan / 100.0,9:F1} ms");
        Log("============================================================================");
        Log("");
        Log("============================================================================");
        Log("RESULTS SUMMARY — OLAP (10 000 docs, single run)");
        Log("============================================================================");
        Log($"  {"Operation",-30} {"BLite",10} {"LiteDB",10} {"SQLite+JSON",14} {"CouchbaseLite",15} {"DuckDB",10}");
        Log($"  {new string('-', 95)}");
        Log($"  {"Aggregate SUM/AVG/COUNT",-30} {oBliteAgg,8} ms {oLiteAgg,8} ms {oSqlAgg,12} ms {oCblAgg,13} ms {oDuckAgg,8} ms");
        Log($"  {"GroupBy Status",-30} {oBliteGrp,8} ms {oLiteGrp,8} ms {oSqlGrp,12} ms {oCblGrp,13} ms {oDuckGrp,8} ms");
        Log($"  {"Range Total > 4000",-30} {oBliteRng,8} ms {oLiteRng,8} ms {oSqlRng,12} ms {oCblRng,13} ms {oDuckRng,8} ms");
        Log($"  {"Top-10 by Total",-30} {oBliteTop,8} ms {oLiteTop,8} ms {oSqlTop,12} ms {oCblTop,13} ms {oDuckTop,8} ms");
        Log("============================================================================");
        Log("");
        Log("============================================================================");
        Log("RESULTS SUMMARY — FIELD OFFSET VECTOR (μs/op, 10 000 docs, 200 iters)");
        Log("============================================================================");
        Log($"  {"Operation",-35} {"BLite",10} {"LiteDB",10} {"SQLite",10} {"DuckDB",10}");
        Log($"  {new string('-', 77)}");
        Log($"  {"CountNonIndexed (Currency=EUR)",-35} {fovBlite * 1000.0 / fovIters,9:F0} {fovLite * 1000.0 / fovIters,9:F0} {fovSql * 1000.0 / fovIters,9:F0} {fovDuck * 1000.0 / fovIters,9:F0} μs");
        Log($"  {"  Baseline (pre-feature)",-35} {"2464",10} {"13630",10} {"327",10} {"12043",10} μs");
        Log("============================================================================");        Log("");
        Log("============================================================================");
        Log("RESULTS SUMMARY — INSERT MICRO-BENCHMARK (μs/op, fresh DB)");
        Log("============================================================================");
        Log($"  {"Operation",-28} {"BLite",12} {"LiteDB",12} {"SQLite",12}");
        Log($"  {new string('-', 66)}");
        Log($"  {"Batch Insert 1000",-28} {batchBliteUs,10:F0} μs {batchLiteUs,10:F0} μs {batchSqlUs,10:F0} μs");
        Log($"  {"  Baseline (pre-feature)",-28} {"13495",12} {"26793",12} {"22863",12} μs");
        Log($"  {"Single Insert",-28} {singleBliteUs,10:F1} μs {singleLiteUs,10:F1} μs {singleSqlUs,10:F1} μs");
        Log($"  {"  Baseline (pre-feature)",-28} {"113",12} {"716",12} {"2546",12} μs");
        Log("============================================================================");
        var artifactsDir = Path.Combine(AppContext.BaseDirectory, "BenchmarkDotNet.Artifacts", "results");
        if (!Directory.Exists(artifactsDir)) Directory.CreateDirectory(artifactsDir);
        var filePath = Path.Combine(artifactsDir, "manual_report.txt");
        File.WriteAllText(filePath, _log.ToString());
        Console.WriteLine($"\nReport saved to: {filePath}");
    }
}
