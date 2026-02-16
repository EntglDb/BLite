using BLite.Bson;
using BLite.Core;
using BLite.Core.Collections;
using BLite.Core.Indexing;
using BLite.Core.Storage;
using BLite.Core.Transactions;
using BLite.Shared;
using BLite.Tests.TestDbContext_TestDbContext_Mappers;
using System.Buffers;
using Xunit;
using Xunit.Abstractions;

namespace BLite.Tests;

public class WalIndexTests : IDisposable
{
    private readonly string _dbPath;
    private readonly string _walPath;
    private readonly TestDbContext _db;
    private readonly ITestOutputHelper _output;

    public WalIndexTests(ITestOutputHelper output)
    {
        _output = output;
        _dbPath = Path.Combine(Path.GetTempPath(), $"test_wal_index_{Guid.NewGuid()}.db");
        // WAL defaults to .wal next to db
        _walPath = Path.ChangeExtension(_dbPath, ".wal");

        _db = new TestDbContext(_dbPath);
    }

    [Fact]
    public void IndexWritesAreLoggedToWal()
    {
        // 2. Start a transaction
        using var txn = _db.BeginTransaction();
        _output.WriteLine($"Started Transaction: {txn.TransactionId}");
        
        // 3. Insert a user
        var user = new User { Name = "Alice", Age = 30 };
        _db.Users.Insert(user);
        
        // 4. Commit
        txn.Commit();
        _output.WriteLine("Committed Transaction");
        
        // 5. Verify WAL
        // Dispose current storage to release file locks, BUT skip checkpoint/truncate
        _db.Dispose();
        
        Assert.True(File.Exists(_walPath), "WAL file should exist");
        
        using var walReader = new WriteAheadLog(_walPath);
        var records = walReader.ReadAll();
        
        _output.WriteLine($"Found {records.Count} WAL records");
        
        // Filter for this transaction
        var txnRecords = records.Where(r => r.TransactionId == txn.TransactionId).ToList();
        
        Assert.Contains(txnRecords, r => r.Type == WalRecordType.Begin);
        Assert.Contains(txnRecords, r => r.Type == WalRecordType.Commit);
        
        var writeRecords = txnRecords.Where(r => r.Type == WalRecordType.Write).ToList();
        _output.WriteLine($"Found {writeRecords.Count} Write records for Txn {txn.TransactionId}");
        
        // Analyze pages
        int indexPageCount = 0;
        int dataPageCount = 0;
        
        foreach (var record in writeRecords)
        {
            var pageType = ParsePageType(record.AfterImage);
            _output.WriteLine($"Page {record.PageId}: Type={pageType}, Size={record.AfterImage?.Length}");
            
            if (pageType == PageType.Index) indexPageCount++;
            else if (pageType == PageType.Data) dataPageCount++;
        }
        
        Assert.True(indexPageCount > 0, $"Expected at least 1 Index page write, found {indexPageCount}");
        Assert.True(dataPageCount > 0, $"Expected at least 1 Data page write, found {dataPageCount}");
    }

    private PageType ParsePageType(byte[]? pageData)
    {
        if (pageData == null || pageData.Length < 32) return (PageType)0;
        // PageType is at offset 4 (1 byte)
        return (PageType)pageData[4]; // Casting byte to PageType
    }

    public void Dispose()
    {
        try 
        { 
            _db?.Dispose(); // Safe to call multiple times
        } 
        catch {}
        
        try { if (File.Exists(_dbPath)) File.Delete(_dbPath); } catch {}
        try { if (File.Exists(_walPath)) File.Delete(_walPath); } catch {}
    }
}
