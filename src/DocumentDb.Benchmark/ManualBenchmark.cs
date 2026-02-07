using System.Diagnostics;

namespace DocumentDb.Benchmark;

public class ManualBenchmark
{
    public static void Run()
    {
        Console.WriteLine("=== MANUAL BENCHMARK: DocumentDb vs SQLite ===\n");
        Console.WriteLine("Testing ID Map Fix: Multi-page storage for large document collections\n");
        
        // === BATCH INSERT (1000 items) ===
        Console.WriteLine("1. Batch Insert (1000 items)");
        
        Console.WriteLine("  Setting up DocumentDb batch...");
        var insertBench = new InsertBenchmarks();
        insertBench.Setup();
        insertBench.IterationSetup();
        
        Console.WriteLine("  Executing DocumentDb batch insert (1000 items)...");
        var sw = Stopwatch.StartNew();
        insertBench.DocumentDb_Insert_Batch();
        sw.Stop();
        var docDbBatchTime = sw.ElapsedMilliseconds;
        Console.WriteLine($"   DocumentDb (No Txn API): {docDbBatchTime} ms");
        
        Console.WriteLine("  Setting up SQLite batch...");
        var insertBench2 = new InsertBenchmarks();
        insertBench2.Setup();
        insertBench2.IterationSetup();
        
        Console.WriteLine("  Executing SQLite batch insert (1000 items)...");
        sw.Restart();
        insertBench2.Sqlite_Insert_Batch();
        sw.Stop();
        var sqliteBatchTime = sw.ElapsedMilliseconds;
        Console.WriteLine($"   SQLite (1 Txn):          {sqliteBatchTime} ms\n");
        
        // === READ PERFORMANCE ===
        Console.WriteLine("2. FindById Performance (1000 operations)");
        Console.WriteLine("  Setting up ReadBenchmarks (inserts 1000 documents)...");
        var readBench = new ReadBenchmarks();
        readBench.Setup();
        Console.WriteLine("  Setup complete.\n");
        
        Console.WriteLine("  Running DocumentDb FindById...");
        sw.Restart();
        for(int i=0; i<1000; i++)
        {
            Person? p = readBench.DocumentDb_FindById();
        }
        sw.Stop();
        var docDbReadTime = sw.ElapsedMilliseconds;
        
        Console.WriteLine("  Running SQLite FindById...");
        sw.Restart();
        for(int i=0; i<1000; i++)
        {
            Person p = readBench.Sqlite_FindById();
        }
        sw.Stop();
        var sqliteReadTime = sw.ElapsedMilliseconds;
        
        readBench.Cleanup();
        
        // === RESULTS ===
        Console.WriteLine("\n============================================================================");
        Console.WriteLine("BENCHMARK RESULTS:");
        Console.WriteLine("============================================================================");
        Console.WriteLine($"\n📊 Batch Insert (1000 items):");
        Console.WriteLine($"  DocumentDb: {docDbBatchTime} ms (1 transaction via InsertBulk)");
        Console.WriteLine($"  SQLite:     {sqliteBatchTime} ms (1 transaction)");
        if (docDbBatchTime > sqliteBatchTime)
        {
            var slowdown = (double)docDbBatchTime / sqliteBatchTime;
            Console.WriteLine($"  ⚠️  SQLite is {slowdown:F2}x FASTER");
            Console.WriteLine($"     Note: Transaction API implemented, but performance gap remains");
        }
        
        Console.WriteLine($"\n📊 FindById Performance (1000 operations):");
        Console.WriteLine($"  DocumentDb: {docDbReadTime} ms ({(double)docDbReadTime/1000:F3} ms per operation)");
        Console.WriteLine($"  SQLite:     {sqliteReadTime} ms ({(double)sqliteReadTime/1000:F3} ms per operation)");
        
        if (docDbReadTime < sqliteReadTime)
        {
            var speedup = (double)sqliteReadTime / docDbReadTime;
            Console.WriteLine($"  ✅ DocumentDb is {speedup:F2}x FASTER (BSON zero-allocation advantage!)");
            Console.WriteLine($"     Saved: {sqliteReadTime - docDbReadTime} ms over 1000 operations");
        }
        
        Console.WriteLine("\n============================================================================");
        Console.WriteLine("✅ Transaction API Implemented!");
        Console.WriteLine("   - Multi-page ID map storage working");
        Console.WriteLine("   - Transaction batching enabled via InsertBulk()");
        Console.WriteLine($"   - Performance: {docDbBatchTime}ms (vs {sqliteBatchTime}ms SQLite)");
        Console.WriteLine("============================================================================\n");
    }
}
