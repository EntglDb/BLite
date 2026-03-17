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

        var b1 = new InsertBenchmarks(); b1.Setup(); b1.IterationSetup();
        var bliteBatch = Measure(b1.BLite_Insert_Batch);
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
        var bliteSingle = Measure(s1.BLite_Insert_Single);
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
        r.Setup();

        sw.Restart();
        for (int i = 0; i < 1000; i++) r.BLite_FindById();
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
        for (int i = 0; i < 100; i++) r.BLite_Scan();
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
        // OLAP SECTION  (10 000 documents, single run each)
        // ════════════════════════════════════════════════════════════
        Log("── OLAP Analytical Operations (10 000 documents) ──────────────────────────\n");
        var olap = new OlapBenchmarks();
        olap.Setup();

        Log("5. Aggregate – SUM / AVG / COUNT");
        var oBliteAgg  = Measure(() => { olap.BLite_Aggregate(); });  Log($"   BLite:        {oBliteAgg} ms");
        var oLiteAgg   = Measure(() => { olap.LiteDb_Aggregate(); }); Log($"   LiteDB:       {oLiteAgg} ms");
        var oSqlAgg    = Measure(() => { olap.Sqlite_Aggregate(); }); Log($"   SQLite+JSON:  {oSqlAgg} ms");
        var oCblAgg    = Measure(() => { olap.CBL_Aggregate(); });    Log($"   CouchbaseLite:{oCblAgg} ms");
        var oDuckAgg   = Measure(() => { olap.DuckDB_Aggregate(); }); Log($"   DuckDB:       {oDuckAgg} ms\n");

        Log("6. GroupBy Status – COUNT + SUM(Total)");
        var oBliteGrp  = Measure(() => { olap.BLite_GroupBy(); });    Log($"   BLite:        {oBliteGrp} ms");
        var oLiteGrp   = Measure(() => { olap.LiteDb_GroupBy(); });   Log($"   LiteDB:       {oLiteGrp} ms");
        var oSqlGrp    = Measure(() => { olap.Sqlite_GroupBy(); });   Log($"   SQLite+JSON:  {oSqlGrp} ms");
        var oCblGrp    = Measure(() => { olap.CBL_GroupBy(); });      Log($"   CouchbaseLite:{oCblGrp} ms");
        var oDuckGrp   = Measure(() => { olap.DuckDB_GroupBy(); });   Log($"   DuckDB:       {oDuckGrp} ms\n");

        Log("7. Range Filter – Total > 4000 (~top 25 %)");
        var oBliteRng  = Measure(() => { olap.BLite_Range(); });      Log($"   BLite:        {oBliteRng} ms");
        var oLiteRng   = Measure(() => { olap.LiteDb_Range(); });     Log($"   LiteDB:       {oLiteRng} ms");
        var oSqlRng    = Measure(() => { olap.Sqlite_Range(); });     Log($"   SQLite+JSON:  {oSqlRng} ms");
        var oCblRng    = Measure(() => { olap.CBL_Range(); });        Log($"   CouchbaseLite:{oCblRng} ms");
        var oDuckRng   = Measure(() => { olap.DuckDB_Range(); });     Log($"   DuckDB:       {oDuckRng} ms\n");

        Log("8. Top-10 by Total DESC");
        var oBliteTop  = Measure(() => { olap.BLite_TopN(); });       Log($"   BLite:        {oBliteTop} ms");
        var oLiteTop   = Measure(() => { olap.LiteDb_TopN(); });      Log($"   LiteDB:       {oLiteTop} ms");
        var oSqlTop    = Measure(() => { olap.Sqlite_TopN(); });      Log($"   SQLite+JSON:  {oSqlTop} ms");
        var oCblTop    = Measure(() => { olap.CBL_TopN(); });         Log($"   CouchbaseLite:{oCblTop} ms");
        var oDuckTop   = Measure(() => { olap.DuckDB_TopN(); });      Log($"   DuckDB:       {oDuckTop} ms\n");

        olap.Cleanup();

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

        var artifactsDir = Path.Combine(AppContext.BaseDirectory, "BenchmarkDotNet.Artifacts", "results");
        if (!Directory.Exists(artifactsDir)) Directory.CreateDirectory(artifactsDir);
        var filePath = Path.Combine(artifactsDir, "manual_report.txt");
        File.WriteAllText(filePath, _log.ToString());
        Console.WriteLine($"\nReport saved to: {filePath}");
    }
}
