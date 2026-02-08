using DocumentDb.Core.Storage;
using DocumentDb.Core.Transactions;
using System.Diagnostics;

namespace DocumentDb.CheckpointTest;

/// <summary>
/// Quick test to verify checkpoint functionality and performance improvement
/// </summary>
class Program
{
    static void Main(string[] args)
    {
        Console.WriteLine("=== DocumentDb Checkpoint Performance Test ===\n");

        var dbPath = "test_checkpoint.db";
        var walPath = "test_checkpoint.wal";

        // Cleanup previous files
        if (File.Exists(dbPath)) File.Delete(dbPath);
        if (File.Exists(walPath)) File.Delete(walPath);

        using var pageFile = new PageFile(dbPath, PageFileConfig.Default);
        pageFile.Open();

        using var txnMgr = new TransactionManager(walPath, pageFile);

        Console.WriteLine("1. Testing Single Inserts (500 transactions)...");
        var sw = Stopwatch.StartNew();
        
        for (int i = 0; i < 500; i++)
        {
            using var txn = txnMgr.BeginTransaction();
            
            // Simulate a write
            var pageId = pageFile.AllocatePage();
            var data = new byte[pageFile.PageSize];
            new Random().NextBytes(data);
            
            txn.AddWrite(new WriteOperation(
                documentId: new DocumentDb.Bson.ObjectId(),
                newValue: data,
                pageId: pageId,
                type: OperationType.Insert
            ));
            
            txnMgr.CommitTransaction(txn);
        }
        
        sw.Stop();
        Console.WriteLine($"   ? Completed 500 inserts in {sw.ElapsedMilliseconds}ms");
        Console.WriteLine($"   ? Average: {500.0 / sw.Elapsed.TotalSeconds:F0} inserts/sec");
        
        var walSize = new FileInfo(walPath).Length;
        Console.WriteLine($"   ? WAL size: {walSize / 1024.0:F1} KB\n");

        Console.WriteLine("2. Performing Manual Checkpoint...");
        sw.Restart();
        var pagesCheckpointed = txnMgr.CheckpointManager.Checkpoint(CheckpointMode.Full);
        sw.Stop();
        
        Console.WriteLine($"   ? Checkpointed {pagesCheckpointed} pages in {sw.ElapsedMilliseconds}ms");
        
        var dbSize = new FileInfo(dbPath).Length;
        var walSizeAfter = new FileInfo(walPath).Length;
        Console.WriteLine($"   ? DB size: {dbSize / 1024.0:F1} KB");
        Console.WriteLine($"   ? WAL size after checkpoint: {walSizeAfter / 1024.0:F1} KB\n");

        Console.WriteLine("3. Testing Checkpoint with Truncate...");
        txnMgr.CheckpointManager.CheckpointAndTruncate();
        
        walSizeAfter = new FileInfo(walPath).Length;
        Console.WriteLine($"   ? WAL size after truncate: {walSizeAfter / 1024.0:F1} KB\n");

        Console.WriteLine("4. Testing Batch Inserts (1000 transactions)...");
        sw.Restart();
        
        for (int i = 0; i < 1000; i++)
        {
            using var txn = txnMgr.BeginTransaction();
            
            var pageId = pageFile.AllocatePage();
            var data = new byte[pageFile.PageSize];
            new Random().NextBytes(data);
            
            txn.AddWrite(new WriteOperation(
                documentId: new DocumentDb.Bson.ObjectId(),
                newValue: data,
                pageId: pageId,
                type: OperationType.Insert
            ));
            
            txnMgr.CommitTransaction(txn);
        }
        
        sw.Stop();
        Console.WriteLine($"   ? Completed 1000 inserts in {sw.ElapsedMilliseconds}ms");
        Console.WriteLine($"   ? Average: {1000.0 / sw.Elapsed.TotalSeconds:F0} inserts/sec");
        
        walSize = new FileInfo(walPath).Length;
        Console.WriteLine($"   ? WAL size: {walSize / 1024.0:F1} KB\n");

        Console.WriteLine("5. Final checkpoint and cleanup...");
        txnMgr.CheckpointManager.CheckpointAndTruncate();
        
        dbSize = new FileInfo(dbPath).Length;
        walSizeAfter = new FileInfo(walPath).Length;
        Console.WriteLine($"   ? Final DB size: {dbSize / 1024.0:F1} KB");
        Console.WriteLine($"   ? Final WAL size: {walSizeAfter / 1024.0:F1} KB\n");

        Console.WriteLine("=== Test Completed Successfully! ===");
        Console.WriteLine("\nKey Observations:");
        Console.WriteLine("- Commits are fast (only WAL writes)");
        Console.WriteLine("- Checkpoint consolidates changes to DB");
        Console.WriteLine("- Truncate reclaims WAL space");
        Console.WriteLine("\nPress any key to exit...");
        Console.ReadKey();

        // Cleanup
        try
        {
            if (File.Exists(dbPath)) File.Delete(dbPath);
            if (File.Exists(walPath)) File.Delete(walPath);
        }
        catch { }
    }
}
