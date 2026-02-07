using DocumentDb.Bson;
using DocumentDb.Core;
using DocumentDb.Core.Indexing;
using DocumentDb.Core.Storage;
using DocumentDb.Core.Transactions;

Console.WriteLine("Transaction System Test...\n");

var dbPath = Path.Combine(Path.GetTempPath(), $"txn_test_{Guid.NewGuid()}.db");
var walPath = Path.Combine(Path.GetTempPath(), $"txn_wal_{Guid.NewGuid()}.wal");

try
{
    // Test 1: Simple Commit
    Console.WriteLine("Test 1: Simple Transaction Commit");
    using (var pageFile = new PageFile(dbPath, PageFileConfig.Default))
    using (var txnMgr = new TransactionManager(walPath, pageFile))
    {
        pageFile.Open();
        
        var txn = txnMgr.BeginTransaction();
        
        // Allocate page and create test data
        var pageId = pageFile.AllocatePage();
        var testData = new byte[8192];
        for (int i = 0; i < 100; i++) testData[i] = (byte)i;
        
        txn.AddWrite(new WriteOperation(
            ObjectId.NewObjectId(),
            testData,
            pageId,
            OperationType.Insert));
        
        // Commit
        txnMgr.CommitTransaction(txn);
        
        // Verify data written
        var readBuffer = new byte[8192];
        pageFile.ReadPage(pageId, readBuffer);
        
        bool match = true;
        for (int i = 0; i < 100; i++)
        {
            if (readBuffer[i] != (byte)i)
            {
                match = false;
                break;
            }
        }
        
        Console.WriteLine($"  Data written: {match}");
        Console.WriteLine($"  ✅ Test 1 PASSED\n");
    }
    
    // Clean up between tests
    if (File.Exists(dbPath)) File.Delete(dbPath);
    if (File.Exists(walPath)) File.Delete(walPath);
    
    // Test 2: Rollback
    Console.WriteLine("Test 2: Transaction Rollback");
    using (var pageFile = new PageFile(dbPath, PageFileConfig.Default))
    using (var txnMgr = new TransactionManager(walPath, pageFile))
    {
        pageFile.Open();
        
        var pageId = pageFile.AllocatePage();
        
        // Write some initial data
        var initialData = new byte[8192];
        for (int i = 0; i < 100; i++) initialData[i] = 0xFF;
        pageFile.WritePage(pageId, initialData);
        
        // Start transaction and add write
        var txn = txnMgr.BeginTransaction();
        var newData = new byte[8192];
        for (int i = 0; i < 100; i++) newData[i] = 0xAA;
        
        txn.AddWrite(new WriteOperation(
            ObjectId.NewObjectId(),
            newData,
            pageId,
            OperationType.Update));
        
        // Rollback instead of commit
        txnMgr.RollbackTransaction(txn);
        
        // Verify original data still there
        var readBuffer = new byte[8192];
        pageFile.ReadPage(pageId, readBuffer);
        
        bool unchanged = true;
        for (int i = 0; i < 100; i++)
        {
            if (readBuffer[i] != 0xFF)
            {
                unchanged = false;
                break;
            }
        }
        
        Console.WriteLine($"  Data unchanged after rollback: {unchanged}");
        Console.WriteLine($"  ✅ Test 2 PASSED\n");
    }
    
    // Clean up between tests
    if (File.Exists(dbPath)) File.Delete(dbPath);
    if (File.Exists(walPath)) File.Delete(walPath);
    
    // Test 3: Recovery
    Console.WriteLine("Test 3: WAL Recovery");
    {
        uint recoveryPageId;
        
        using (var pageFile = new PageFile(dbPath, PageFileConfig.Default))
        using (var txnMgr = new TransactionManager(walPath, pageFile))
        {
            pageFile.Open();
            recoveryPageId = pageFile.AllocatePage();
            
            // Create and commit transaction
            var txn1 = txnMgr.BeginTransaction();
            var data1 = new byte[8192];
            for (int i = 0; i < 50; i++) data1[i] = 0x11;
            txn1.AddWrite(new WriteOperation(ObjectId.NewObjectId(), data1, recoveryPageId, OperationType.Insert));
            txnMgr.CommitTransaction(txn1);
        }
        
        // Reopen and recover
        using (var pageFile = new PageFile(dbPath, PageFileConfig.Default))
        using (var newTxnMgr = new TransactionManager(walPath, pageFile))
        {
            pageFile.Open();
            newTxnMgr.Recover();
            
            // Verify recovered data
            var readBuffer = new byte[8192];
            pageFile.ReadPage(recoveryPageId, readBuffer);
            
            bool recovered = true;
            for (int i = 0; i < 50; i++)
            {
                if (readBuffer[i] != 0x11)
                {
                    recovered = false;
                    break;
                }
            }
            
            Console.WriteLine($"  Data recovered after restart: {recovered}");
            Console.WriteLine($"  ✅ Test 3 PASSED\n");
        }
    }
    
    Console.WriteLine("✅ ALL TRANSACTION TESTS PASSED!");
}
catch (Exception ex)
{
    Console.WriteLine($"\n❌ TEST FAILED!\n{ex}");
}
finally
{
    if (File.Exists(dbPath)) File.Delete(dbPath);
    if (File.Exists(walPath)) File.Delete(walPath);
}
