using System.Diagnostics;
using System.IO;
using System.Text;

namespace BLite.Benchmark;

public class ManualBenchmark
{
    private static StringBuilder _log = new();

    private static void Log(string message = "")
    {
        Console.WriteLine(message);
        _log.AppendLine(message);
    }

    public static void Run()
    {
        _log.Clear();
        Log("=== MANUAL BENCHMARK: DocumentDb vs SQLite ===");
        Log($"Date: {DateTime.Now}");
        Log("Testing: Complex Objects (Nested Documents + Collections)\n");
        
        // === BATCH INSERT (1000 items) ===
        Log("1. Batch Insert (1000 items)");
        
        Log("  Setting up DocumentDb batch...");
        var insertBench = new InsertBenchmarks();
        insertBench.Setup();
        insertBench.IterationSetup();
        
        Log("  Executing DocumentDb batch insert (1000 items)...");
        var sw = Stopwatch.StartNew();
        insertBench.DocumentDb_Insert_Batch();
        sw.Stop();
        var docDbBatchTime = sw.ElapsedMilliseconds;
        Log($"   DocumentDb (InsertBulk): {docDbBatchTime} ms");
        
        Log("  Setting up SQLite batch...");
        var insertBench2 = new InsertBenchmarks();
        insertBench2.Setup();
        insertBench2.IterationSetup();
        
        Log("  Executing SQLite batch insert (1000 items)...");
        sw.Restart();
        insertBench2.Sqlite_Insert_Batch();
        sw.Stop();
        var sqliteBatchTime = sw.ElapsedMilliseconds;
        Log($"   SQLite (1 Txn):          {sqliteBatchTime} ms");
        
        Log("  Executing SQLite batch insert (1000 items) + Forced Checkpoint...");
        var insertBench3 = new InsertBenchmarks();
        insertBench3.Setup();
        insertBench3.IterationSetup();
        
        sw.Restart();
        insertBench3.Sqlite_Insert_Batch_ForcedCheckpoint();
        sw.Stop();
        var sqliteCheckpointTime = sw.ElapsedMilliseconds;
        Log($"   SQLite (Forced CP):      {sqliteCheckpointTime} ms\n");
        
        // === READ PERFORMANCE ===
        Log("2. FindById Performance (1000 operations)");
        Log("  Setting up ReadBenchmarks (inserts 1000 documents)...");
        var readBench = new ReadBenchmarks();
        readBench.Setup();
        Log("  Setup complete.\n");
        
        Log("  Running DocumentDb FindById...");
        sw.Restart();
        for(int i=0; i<1000; i++)
        {
            Person? p = readBench.DocumentDb_FindById();
        }
        sw.Stop();
        var docDbReadTime = sw.ElapsedMilliseconds;
        
        Log("  Running SQLite FindById...");
        sw.Restart();
        for(int i=0; i<1000; i++)
        {
            Person p = readBench.Sqlite_FindById();
        }
        sw.Stop();
        var sqliteReadTime = sw.ElapsedMilliseconds;
        
        // Clean up read benchmark resources
        readBench.Cleanup();
        
        // === SINGLE INSERT COMPARISON ===
        Log("\n3. Single Insert (Fair Comparison - No Batching)");
        
        var singleBench1 = new InsertBenchmarks();
        singleBench1.Setup();
        singleBench1.IterationSetup();
        sw.Restart();
        singleBench1.DocumentDb_Insert_Single();
        sw.Stop();
        var docDbSingleTime = sw.ElapsedMilliseconds;
        
        var singleBench2 = new InsertBenchmarks();
        singleBench2.Setup();
        singleBench2.IterationSetup();
        sw.Restart();
        singleBench2.Sqlite_Insert_Single();
        sw.Stop();
        var sqliteSingleTime = sw.ElapsedMilliseconds;
        
        // === RESULTS ===
        Log("\n============================================================================");
        Log("BENCHMARK RESULTS:");
        Log("============================================================================");
        Log($"\n📊 Batch Insert (1000 items) - Complex Objects:");
        Log($"  DocumentDb:          {docDbBatchTime} ms");
        Log($"  SQLite (WAL):        {sqliteBatchTime} ms");
        Log($"  SQLite (Forced CP):  {sqliteCheckpointTime} ms");
        
        if (docDbBatchTime < sqliteCheckpointTime)
        {
             var speedup = (double)sqliteCheckpointTime / docDbBatchTime;
             Log($"  ✅ DocumentDb is {speedup:F2}x FASTER than SQLite with forced checkpoint");
        }
        
        Log($"\n📊 FindById Performance (1000 operations):");
        Log($"  DocumentDb: {docDbReadTime} ms ({(double)docDbReadTime/1000:F3} ms per operation)");
        Log($"  SQLite:     {sqliteReadTime} ms ({(double)sqliteReadTime/1000:F3} ms per operation)");
        
        if (docDbReadTime < sqliteReadTime)
        {
            var speedup = (double)sqliteReadTime / docDbReadTime;
            Log($"  ✅ DocumentDb is {speedup:F2}x FASTER (BSON zero-allocation advantage!)");
            Log($"     Saved: {sqliteReadTime - docDbReadTime} ms over 1000 operations");
        }
        else
        {
            Log($"  ⚠️  SQLite is {(double)docDbReadTime/sqliteReadTime:F2}x FASTER");
        }
        
        Log($"\n📊 Single Insert (Fair Comparison):");
        Log($"  DocumentDb: {docDbSingleTime} ms");
        Log($"  SQLite:     {sqliteSingleTime} ms");
        
        Log("\n============================================================================");
        
        // Save to file
        var artifactsDir = Path.Combine(AppContext.BaseDirectory, "BenchmarkDotNet.Artifacts", "results");
        if (!Directory.Exists(artifactsDir)) Directory.CreateDirectory(artifactsDir);
        
        var filePath = Path.Combine(artifactsDir, "manual_report.txt");
        File.WriteAllText(filePath, _log.ToString());
        Console.WriteLine($"\nReport saved to: {filePath}");
    }
}
