using BLite.Core;
using BLite.Core.Storage;
using System.Text;

namespace BLite.Tests;

public class DictionaryPageTests
{
    private const int PageSize = 16384;

    [Fact]
    public void Initialize_ShouldSetupEmptyPage()
    {
        var page = new byte[PageSize];
        DictionaryPage.Initialize(page, 1);

        var header = PageHeader.ReadFrom(page);
        Assert.Equal(PageType.Dictionary, header.PageType);
        Assert.Equal(1u, header.PageId);
        
        var count = BitConverter.ToUInt16(page, 32); // CountOffset
        Assert.Equal(0, count);

        var freeSpaceEnd = BitConverter.ToUInt16(page, 34); // FreeSpaceEndOffset
        Assert.Equal(PageSize, freeSpaceEnd);
    }

    [Fact]
    public void Insert_ShouldAddEntryAndSort()
    {
        var page = new byte[PageSize];
        DictionaryPage.Initialize(page, 1);

        // Insert "B"
        bool inserted = DictionaryPage.Insert(page, "B", 20);
        Assert.True(inserted);

        // Insert "A" (should go before B)
        inserted = DictionaryPage.Insert(page, "A", 10);
        Assert.True(inserted);

        // Insert "C" (should go after B)
        inserted = DictionaryPage.Insert(page, "C", 30);
        Assert.True(inserted);

        // Verify Order
        var entries = DictionaryPage.GetAll(page).ToList();
        Assert.Equal(3, entries.Count);
        
        Assert.Equal("A", entries[0].Key);
        Assert.Equal(10, entries[0].Value);

        Assert.Equal("B", entries[1].Key);
        Assert.Equal(20, entries[1].Value);

        Assert.Equal("C", entries[2].Key);
        Assert.Equal(30, entries[2].Value);
    }

    [Fact]
    public void TryFind_ShouldReturnCorrectValue()
    {
        var page = new byte[PageSize];
        DictionaryPage.Initialize(page, 1);

        DictionaryPage.Insert(page, "Key1", 100);
        DictionaryPage.Insert(page, "Key2", 200);
        DictionaryPage.Insert(page, "Key3", 300);

        bool found = DictionaryPage.TryFind(page, Encoding.UTF8.GetBytes("Key2"), out ushort value);
        Assert.True(found);
        Assert.Equal(200, value);

        found = DictionaryPage.TryFind(page, Encoding.UTF8.GetBytes("Key999"), out value);
        Assert.False(found);
    }

    [Fact]
    public void Overflow_ShouldReturnFalse_WhenFull()
    {
        var page = new byte[PageSize];
        DictionaryPage.Initialize(page, 1);

        string bigKey = new string('X', 250); 
        
        int count = 0;
        while(true)
        {
            // Use unique keys
            var key = bigKey + count;
            if (!DictionaryPage.Insert(page, key, (ushort)count))
            {
                // Should fail here
                break;
            }
            count++;
            if (count > 1000) Assert.Fail("Should have filled the page much earlier"); 
        }

        // Now page is full enough that `bigKey` (250 bytes) shouldn't fit.
        // We can't guarantee a small key won't fit (fragmentation/remaining space), 
        // but a key of the SAME size that triggered the break should definitely fail.
        bool inserted = DictionaryPage.Insert(page, bigKey + "X", 9999);
        Assert.False(inserted);
    }

    [Fact]
    public void Chaining_ShouldFindKeysInLinkedPages()
    {
        var dbPath = Path.Combine(Path.GetTempPath(), $"test_dict_chain_{Guid.NewGuid()}.db");
        using var storage = new StorageEngine(dbPath, PageFileConfig.Default);
        
        // 1. Create First Page
        var page1Id = storage.AllocatePage();
        var pageBuffer = new byte[storage.PageSize];
        DictionaryPage.Initialize(pageBuffer, page1Id);
        
        // Fill Page 1
        DictionaryPage.Insert(pageBuffer, "Key1", 100);
        DictionaryPage.Insert(pageBuffer, "KeyA", 200);
        
        // 2. Create Second Page
        var page2Id = storage.AllocatePage();
        var page2Buffer = new byte[storage.PageSize];
        DictionaryPage.Initialize(page2Buffer, page2Id);
        
        // Fill Page 2
        DictionaryPage.Insert(page2Buffer, "Key2", 300);
        DictionaryPage.Insert(page2Buffer, "KeyB", 400); // 400
        
        // 3. Link Page 1 -> Page 2
        var header1 = PageHeader.ReadFrom(pageBuffer);
        header1.NextPageId = page2Id;
        header1.WriteTo(pageBuffer);
        
        // 4. Write pages to storage
        storage.WritePageImmediate(page1Id, pageBuffer);
        storage.WritePageImmediate(page2Id, page2Buffer);
        
        // 5. Test Global Find
        // Find in Page 1
        bool found = DictionaryPage.TryFindGlobal(storage, page1Id, "Key1", out ushort val);
        Assert.True(found);
        Assert.Equal(100, val);
        
        // Find in Page 2
        found = DictionaryPage.TryFindGlobal(storage, page1Id, "KeyB", out val);
        Assert.True(found);
        Assert.Equal(400, val);
        
        // Not Found
        found = DictionaryPage.TryFindGlobal(storage, page1Id, "KeyMissing", out val);
        Assert.False(found);
        
        storage.Dispose();
        if (File.Exists(dbPath)) File.Delete(dbPath);
        if (File.Exists(Path.ChangeExtension(dbPath, ".wal"))) File.Delete(Path.ChangeExtension(dbPath, ".wal"));
    }

    [Fact]
    public void FindAllGlobal_ShouldRetrieveAllKeys()
    {
        var dbPath = Path.Combine(Path.GetTempPath(), $"test_dict_findall_{Guid.NewGuid()}.db");
        using var storage = new StorageEngine(dbPath, PageFileConfig.Default);
        
        // 1. Create Chain of 3 Pages
        var page1Id = storage.AllocatePage();
        var page2Id = storage.AllocatePage();
        var page3Id = storage.AllocatePage();

        var buf = new byte[storage.PageSize];

        // Page 1
        DictionaryPage.Initialize(buf, page1Id);
        DictionaryPage.Insert(buf, "P1_A", 10);
        DictionaryPage.Insert(buf, "P1_B", 11);
        var h1 = PageHeader.ReadFrom(buf);
        h1.NextPageId = page2Id;
        h1.WriteTo(buf);
        storage.WritePageImmediate(page1Id, buf);

        // Page 2
        DictionaryPage.Initialize(buf, page2Id);
        DictionaryPage.Insert(buf, "P2_A", 20);
        var h2 = PageHeader.ReadFrom(buf);
        h2.NextPageId = page3Id;
        h2.WriteTo(buf);
        storage.WritePageImmediate(page2Id, buf);

        // Page 3
        DictionaryPage.Initialize(buf, page3Id);
        DictionaryPage.Insert(buf, "P3_A", 30);
        DictionaryPage.Insert(buf, "P3_B", 31);
        DictionaryPage.Insert(buf, "P3_C", 32);
        storage.WritePageImmediate(page3Id, buf);

        // 2. Execute FindAllGlobal
        var allEntries = DictionaryPage.FindAllGlobal(storage, page1Id).ToList();

        // 3. Verify
        Assert.Equal(6, allEntries.Count);
        Assert.Contains(allEntries, e => e.Key == "P1_A" && e.Value == 10);
        Assert.Contains(allEntries, e => e.Key == "P2_A" && e.Value == 20);
        Assert.Contains(allEntries, e => e.Key == "P3_C" && e.Value == 32);

        storage.Dispose();
        if (File.Exists(dbPath)) File.Delete(dbPath);
        if (File.Exists(Path.ChangeExtension(dbPath, ".wal"))) File.Delete(Path.ChangeExtension(dbPath, ".wal"));
    }
}
