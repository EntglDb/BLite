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
        Log("=== MANUAL BENCHMARK: BLite vs LiteDB vs SQLite+JSON ===");
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
        Log($"   SQLite+JSON:  {sqliteBatch} ms\n");

        b1.Cleanup(); b2.Cleanup(); b3.Cleanup();

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
        Log($"   SQLite+JSON:  {sqliteSingle} ms\n");

        s1.Cleanup(); s2.Cleanup(); s3.Cleanup();

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
        Log($"   SQLite+JSON:  {sqliteRead} ms  ({sqliteRead / 1000.0:F3} ms/op)\n");

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
        Log($"   SQLite+JSON:  {sqliteScan} ms  ({sqliteScan / 100.0:F1} ms/op)\n");

        r.Cleanup();

        // ── Results ─────────────────────────────────────────────────
        Log("============================================================================");
        Log("RESULTS SUMMARY");
        Log("============================================================================");
        Log($"  {"Operation",-30} {"BLite",10} {"LiteDB",10} {"SQLite+JSON",14}");
        Log($"  {new string('-', 68)}");
        Log($"  {"Batch Insert 1000",-30} {bliteBatch,8} ms {liteBatch,8} ms {sqliteBatch,12} ms");
        Log($"  {"Single Insert",-30} {bliteSingle,8} ms {liteSingle,8} ms {sqliteSingle,12} ms");
        Log($"  {"FindById (avg, 1000x)",-30} {bliteRead / 1000.0,9:F3} {liteRead / 1000.0,9:F3} {sqliteRead / 1000.0,12:F3} ms");
        Log($"  {"Scan Status (avg, 100x)",-30} {bliteScan / 100.0,9:F1} {liteScan / 100.0,9:F1} {sqliteScan / 100.0,12:F1} ms");
        Log("============================================================================");

        var artifactsDir = Path.Combine(AppContext.BaseDirectory, "BenchmarkDotNet.Artifacts", "results");
        if (!Directory.Exists(artifactsDir)) Directory.CreateDirectory(artifactsDir);
        var filePath = Path.Combine(artifactsDir, "manual_report.txt");
        File.WriteAllText(filePath, _log.ToString());
        Console.WriteLine($"\nReport saved to: {filePath}");
    }
}
