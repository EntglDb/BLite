using System.Runtime.InteropServices;
using System.Text;
using BLite.Core;

namespace BLite.Core.Storage;

/// <summary>
/// Page for storing dictionary entries (Key -> Value map).
/// Uses a sorted list of keys for binary search within the page.
/// Supports chaining via PageHeader.NextPageId for dictionaries larger than one page.
/// </summary>
public struct DictionaryPage
{
    // Layout:
    // [PageHeader (32)]
    // [Count (2)]
    // [FreeSpaceEnd (2)]
    // [Offsets (Count * 2)] ...
    // ... Free Space ...
    // ... Data (Growing Downwards) ...

    private const int HeaderSize = 32;
    private const int CountOffset = 32;
    private const int FreeSpaceEndOffset = 34;
    private const int OffsetsStart = 36;

    /// <summary>
    /// Values 0-100 are reserved for internal system keys (e.g. _id, _v).
    /// </summary>
    public const ushort ReservedValuesEnd = 100;

    /// <summary>
    /// Initialize a new dictionary page
    /// </summary>
    public static void Initialize(Span<byte> page, uint pageId)
    {
        // 1. Write Page Header
        var header = new PageHeader
        {
            PageId = pageId,
            PageType = PageType.Dictionary,
            FreeBytes = (ushort)(page.Length - OffsetsStart),
            NextPageId = 0,
            TransactionId = 0,
            Checksum = 0
        };
        header.WriteTo(page);

        // 2. Initialize Counts
        System.Buffers.Binary.BinaryPrimitives.WriteUInt16LittleEndian(page.Slice(CountOffset), 0);
        System.Buffers.Binary.BinaryPrimitives.WriteUInt16LittleEndian(page.Slice(FreeSpaceEndOffset), (ushort)page.Length);
    }

    /// <summary>
    /// Inserts a key-value pair into the page.
    /// Returns false if there is not enough space.
    /// </summary>
    public static bool Insert(Span<byte> page, string key, ushort value)
    {
        var keyByteCount = Encoding.UTF8.GetByteCount(key);
        if (keyByteCount > 255) throw new ArgumentException("Key length must be <= 255 bytes");

        // Entry Size: KeyLen(1) + Key(N) + Value(2)
        var entrySize = 1 + keyByteCount + 2;
        var requiredSpace = entrySize + 2; // +2 for Offset entry

        var count = System.Buffers.Binary.BinaryPrimitives.ReadUInt16LittleEndian(page.Slice(CountOffset));
        var freeSpaceEnd = System.Buffers.Binary.BinaryPrimitives.ReadUInt16LittleEndian(page.Slice(FreeSpaceEndOffset));
        
        var offsetsEnd = OffsetsStart + (count * 2);
        var freeSpace = freeSpaceEnd - offsetsEnd;

        if (freeSpace < requiredSpace)
        {
            return false; // Page Full
        }

        // 1. Prepare Data
        var insertionOffset = (ushort)(freeSpaceEnd - entrySize);
        page[insertionOffset] = (byte)keyByteCount; // Write Key Length
        Encoding.UTF8.GetBytes(key, page.Slice(insertionOffset + 1, keyByteCount)); // Write Key
        System.Buffers.Binary.BinaryPrimitives.WriteUInt16LittleEndian(page.Slice(insertionOffset + 1 + keyByteCount), value); // Write Value

        // 2. Insert Offset into Sorted List
        // Find insert Index using spans
        ReadOnlySpan<byte> keyBytes = page.Slice(insertionOffset + 1, keyByteCount);
        int insertIndex = FindInsertIndex(page, count, keyBytes);

        // Shift offsets if needed
        if (insertIndex < count)
        {
            var src = page.Slice(OffsetsStart + (insertIndex * 2), (count - insertIndex) * 2);
            var dest = page.Slice(OffsetsStart + ((insertIndex + 1) * 2));
            src.CopyTo(dest);
        }

        // Write new offset
        System.Buffers.Binary.BinaryPrimitives.WriteUInt16LittleEndian(page.Slice(OffsetsStart + (insertIndex * 2)), insertionOffset);

        // 3. Update Metadata
        System.Buffers.Binary.BinaryPrimitives.WriteUInt16LittleEndian(page.Slice(CountOffset), (ushort)(count + 1));
        System.Buffers.Binary.BinaryPrimitives.WriteUInt16LittleEndian(page.Slice(FreeSpaceEndOffset), insertionOffset);
        
        // Update FreeBytes in header (approximate)
        var pageHeader = PageHeader.ReadFrom(page);
        pageHeader.FreeBytes = (ushort)(insertionOffset - (OffsetsStart + ((count + 1) * 2)));
        pageHeader.WriteTo(page);

        return true;
    }

    /// <summary>
    /// Tries to find a value for the given key in THIS page.
    /// </summary>
    public static bool TryFind(ReadOnlySpan<byte> page, ReadOnlySpan<byte> keyBytes, out ushort value)
    {
        value = 0;
        var count = System.Buffers.Binary.BinaryPrimitives.ReadUInt16LittleEndian(page.Slice(CountOffset));
        if (count == 0) return false;

        // Binary Search
        int low = 0;
        int high = count - 1;

        while (low <= high)
        {
            int mid = low + (high - low) / 2;
            var offset = System.Buffers.Binary.BinaryPrimitives.ReadUInt16LittleEndian(page.Slice(OffsetsStart + (mid * 2)));
            
            // Read Key at Offset
            var keyLen = page[offset];
            var entryKeySpan = page.Slice(offset + 1, keyLen);
            
            int comparison = entryKeySpan.SequenceCompareTo(keyBytes);

            if (comparison == 0)
            {
                value = System.Buffers.Binary.BinaryPrimitives.ReadUInt16LittleEndian(page.Slice(offset + 1 + keyLen));
                return true;
            }

            if (comparison < 0)
                low = mid + 1;
            else
                high = mid - 1;
        }

        return false;
    }

    /// <summary>
    /// Tries to find a value for the given key across a chain of DictionaryPages.
    /// </summary>
    public static bool TryFindGlobal(StorageEngine storage, uint startPageId, string key, out ushort value, ulong? transactionId = null)
    {
        var keyByteCount = Encoding.UTF8.GetByteCount(key);
        Span<byte> keyBytes = keyByteCount <= 256 ? stackalloc byte[keyByteCount] : new byte[keyByteCount];
        Encoding.UTF8.GetBytes(key, keyBytes);

        var pageId = startPageId;
        var pageBuffer = System.Buffers.ArrayPool<byte>.Shared.Rent(storage.PageSize);
        try
        {
            while (pageId != 0)
            {
                 // Read page
                 storage.ReadPage(pageId, transactionId, pageBuffer);
                 
                 // TryFind in this page
                 if (TryFind(pageBuffer, keyBytes, out value))
                 {
                     return true;
                 }
                 
                 // Move to next page
                 var header = PageHeader.ReadFrom(pageBuffer);
                 pageId = header.NextPageId;
            }
        }
        finally
        {
            System.Buffers.ArrayPool<byte>.Shared.Return(pageBuffer);
        }
        
        value = 0;
        return false;
    }

    private static int FindInsertIndex(ReadOnlySpan<byte> page, int count, ReadOnlySpan<byte> keyBytes)
    {
        int low = 0;
        int high = count - 1;

        while (low <= high)
        {
            int mid = low + (high - low) / 2;
            var offset = System.Buffers.Binary.BinaryPrimitives.ReadUInt16LittleEndian(page.Slice(OffsetsStart + (mid * 2)));
            
            var keyLen = page[offset];
            var entryKeySpan = page.Slice(offset + 1, keyLen);
            
            int comparison = entryKeySpan.SequenceCompareTo(keyBytes);

            if (comparison == 0) return mid; 
            if (comparison < 0)
                low = mid + 1;
            else
                high = mid - 1;
        }
        return low;
    }

    /// <summary>
    /// Gets all entries in the page (for debugging/dumping)
    /// </summary>
    public static IEnumerable<(string Key, ushort Value)> GetAll(ReadOnlySpan<byte> page)
    {
         var count = System.Buffers.Binary.BinaryPrimitives.ReadUInt16LittleEndian(page.Slice(CountOffset));
         var list = new List<(string Key, ushort Value)>();
         for(int i=0; i<count; i++)
         {
            var offset = System.Buffers.Binary.BinaryPrimitives.ReadUInt16LittleEndian(page.Slice(OffsetsStart + (i * 2)));
            var keyLen = page[offset];
            var keyStr = Encoding.UTF8.GetString(page.Slice(offset + 1, keyLen));
            var val = System.Buffers.Binary.BinaryPrimitives.ReadUInt16LittleEndian(page.Slice(offset + 1 + keyLen));
            list.Add((keyStr, val));
         }
         return list;
    }
    /// <summary>
    /// Retrieves all key-value pairs across a chain of DictionaryPages.
    /// Used for rebuilding the in-memory cache.
    /// </summary>
    public static IEnumerable<(string Key, ushort Value)> FindAllGlobal(StorageEngine storage, uint startPageId, ulong? transactionId = null)
    {
        var pageId = startPageId;
        var pageBuffer = System.Buffers.ArrayPool<byte>.Shared.Rent(storage.PageSize);
        try
        {
            while (pageId != 0)
            {
                 // Read page
                 storage.ReadPage(pageId, transactionId, pageBuffer);
                 
                 // Get all entries in this page
                 foreach (var entry in GetAll(pageBuffer))
                 {
                     yield return entry;
                 }
                 
                 // Move to next page
                 var header = PageHeader.ReadFrom(pageBuffer);
                 pageId = header.NextPageId;
            }
        }
        finally
        {
            System.Buffers.ArrayPool<byte>.Shared.Return(pageBuffer);
        }
    }
}
