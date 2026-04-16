using System.Collections.Concurrent;

namespace BLite.Core.Storage;

public sealed partial class StorageEngine
{
    // -----------------------------------------------------------------------
    // Multi-file pageId encoding
    //
    //  Main file page  — bit 31 = 0, bits [30:0] = physical page number
    //  Index file page — bits [31:30] = 10, bits [29:0] = local page number
    //  Collection page — bits [31:30] = 11, bits [29:24] = slot (0-63), bits [23:0] = local page number
    //
    // In single-file (embedded) mode the encoding is never applied; all 32 bits
    // are the physical page number, preserving full backward compatibility.
    // -----------------------------------------------------------------------
    private const uint IndexPageMarker      = 0x8000_0000u; // bit 31=1, bit 30=0
    private const uint CollectionPageMarker = 0xC000_0000u; // bits 31-30 = 11
    private const uint SubFileTypeMask      = 0xC000_0000u; // selects bits 31-30
    private const uint IndexLocalMask       = 0x3FFF_FFFFu; // bits 29:0
    private const uint CollLocalMask        = 0x00FF_FFFFu; // bits 23:0
    private const uint CollSlotMask         = 0x3F00_0000u; // bits 29:24
    private const int  CollSlotShift        = 24;

    /// <summary>Maximum number of named per-collection files in multi-file mode.</summary>
    public const int MaxCollections = 64;

    // -----------------------------------------------------------------------
    // Page allocation
    // -----------------------------------------------------------------------

    /// <summary>Allocates a new page in the main data file.</summary>
    public uint AllocatePage() => _pageFile.AllocatePage();

    /// <summary>
    /// Allocates a new index page. When <see cref="PageFileConfig.IndexFilePath"/> is configured the
    /// page is created in the index file and its ID is encoded with the index-file marker bits
    /// (<c>0x80000000 | localId</c>). Falls back to the main data file otherwise.
    /// </summary>
    public uint AllocateIndexPage()
    {
        if (_indexFile == null)
            return _pageFile.AllocatePage();

        var localId = _indexFile.AllocatePage();
        return IndexPageMarker | (localId & IndexLocalMask);
    }

    /// <summary>
    /// Allocates a new data page for the specified collection.
    /// When <see cref="PageFileConfig.CollectionDataDirectory"/> is configured the page is created in
    /// the collection's dedicated file and its ID is encoded with the collection-file marker bits.
    /// Falls back to the main data file otherwise.
    /// </summary>
    public uint AllocateCollectionPage(string collectionName)
    {
        if (_collectionFiles == null)
            return _pageFile.AllocatePage();

        ValidateCollectionName(collectionName);
        var slot = GetOrAssignCollectionSlot(collectionName);
        var file = GetOrCreateCollectionFile(collectionName);
        var localId = file.AllocatePage();
        return CollectionPageMarker | ((uint)(slot & 0x3F) << CollSlotShift) | (localId & CollLocalMask);
    }

    /// <summary>Frees a page, routing to the correct sub-file via the encoded page ID.</summary>
    public void FreePage(uint pageId)
    {
        GetPageFile(pageId, out var physId).FreePage(physId);
    }

    // -----------------------------------------------------------------------
    // Internal routing
    // -----------------------------------------------------------------------

    /// <summary>
    /// Returns the <see cref="IPageStorage"/> that owns the given (possibly encoded) page ID
    /// and outputs the physical (file-local) page number to pass to that storage backend.
    /// Routing is determined entirely from the high bits of <paramref name="pageId"/>:
    /// no in-memory dictionaries are required and routing survives engine restarts.
    /// </summary>
    private IPageStorage GetPageFile(uint pageId, out uint physicalPageId)
    {
        var fileTag = pageId & SubFileTypeMask;

        if (fileTag == IndexPageMarker)
        {
            physicalPageId = pageId & IndexLocalMask;
            return _indexFile ?? _pageFile;
        }

        if (fileTag == CollectionPageMarker)
        {
            var slot = (int)((pageId & CollSlotMask) >> CollSlotShift);
            physicalPageId = pageId & CollLocalMask;
            var collName = GetCollectionNameBySlot(slot);
            if (collName != null)
                return GetOrCreateCollectionFile(collName);
            // Slot not found (e.g. file was dropped) — fall through to main file
        }

        physicalPageId = pageId;
        return _pageFile;
    }

    private string? GetCollectionNameBySlot(int slot)
        => (_collectionSlotToName != null && _collectionSlotToName.TryGetValue(slot, out var name))
            ? name : null;

    private IPageStorage GetOrCreateCollectionFile(string collectionName)
    {
        if (_collectionFiles == null)
            return _pageFile;

        return _collectionFiles.GetOrAdd(collectionName, name =>
            new Lazy<IPageStorage>(() =>
            {
                var filePath = CollectionFilePath(name);
                var pf = new PageFile(filePath, AsStandaloneConfig(_config));
                pf.Open();
                return pf;
            })).Value;
    }

    // -----------------------------------------------------------------------
    // Collection file helpers
    // -----------------------------------------------------------------------

    /// <summary>
    /// Returns the absolute path for a collection's dedicated data file.
    /// The filename is always lowercased to avoid case-sensitive file-system ambiguity.
    /// </summary>
    private string CollectionFilePath(string collectionName)
        => Path.Combine(_config.CollectionDataDirectory!, collectionName.ToLowerInvariant() + ".db");

    /// <summary>
    /// Drops a collection's dedicated file, reclaiming disk space immediately.
    /// No-op in single-file (embedded) mode.
    /// </summary>
    public void DropCollectionFile(string collectionName)
    {
        if (_collectionFiles == null) return;

        if (_collectionFiles.TryRemove(collectionName, out var lazy))
        {
            if (lazy.IsValueCreated) lazy.Value.Dispose();
            var filePath = CollectionFilePath(collectionName);
            if (File.Exists(filePath))
                File.Delete(filePath);
        }
    }

    /// <summary>
    /// Returns an enumerable of all (encoded) page IDs for the specified collection.
    /// <list type="bullet">
    ///   <item>Single-file mode: enumerates all pages 0..N from the main file
    ///   (DocumentCollection filters by <see cref="PageType.Data"/>).</item>
    ///   <item>Multi-file mode: enumerates only the pages in the collection's dedicated file,
    ///   using encoded IDs that survive engine restarts.</item>
    /// </list>
    /// </summary>
    public IEnumerable<uint> GetCollectionPageIds(string collectionName)
    {
        if (_collectionFiles == null)
        {
            var mainCount = _pageFile.NextPageId;
            for (uint i = 0; i < mainCount; i++)
                yield return i;
            yield break;
        }

        if (!_collectionNameToSlot!.TryGetValue(collectionName, out var slot))
            yield break; // No pages allocated for this collection yet

        var file = GetOrCreateCollectionFile(collectionName);
        var localCount = file.NextPageId;
        var encBase = CollectionPageMarker | ((uint)(slot & 0x3F) << CollSlotShift);
        for (uint localId = 0; localId < localCount; localId++)
            yield return encBase | (localId & CollLocalMask);
    }

    // -----------------------------------------------------------------------
    // Collection slot management
    // -----------------------------------------------------------------------

    private int GetOrAssignCollectionSlot(string collectionName)
    {
        if (_collectionNameToSlot!.TryGetValue(collectionName, out var existing))
            return existing;

        lock (_collectionSlotLock)
        {
            if (_collectionNameToSlot.TryGetValue(collectionName, out existing))
                return existing;

            if (_nextSlotIndex >= MaxCollections)
                throw new InvalidOperationException(
                    $"Maximum number of per-collection files ({MaxCollections}) reached. "
                  + $"Use single-file mode (no CollectionDataDirectory) for more than {MaxCollections} collections.");

            var newSlot = _nextSlotIndex++;
            _collectionNameToSlot[collectionName] = newSlot;
            _collectionSlotToName![newSlot] = collectionName;
            SaveCollectionSlots();
            return newSlot;
        }
    }

    private void LoadCollectionSlots()
    {
        if (_slotsFilePath == null || !File.Exists(_slotsFilePath))
            return;

        foreach (var line in File.ReadAllLines(_slotsFilePath))
        {
            var sep = line.IndexOf(',');
            if (sep < 0) continue;
#if NET5_0_OR_GREATER
            if (!int.TryParse(line.AsSpan(0, sep), out var slot)) continue;
#else
            if (!int.TryParse(line.Substring(0, sep), out var slot)) continue;
#endif
            var name = line.Substring(sep + 1);
            if (string.IsNullOrEmpty(name)) continue;
            _collectionNameToSlot![name] = slot;
            _collectionSlotToName![slot] = name;
            if (slot >= _nextSlotIndex)
                _nextSlotIndex = slot + 1;
        }
    }

    private void SaveCollectionSlots()
    {
        if (_slotsFilePath == null) return;

        string[] lines;
        lock (_collectionSlotLock)
        {
            lines = _collectionSlotToName!
                .OrderBy(kvp => kvp.Key)
                .Select(kvp => $"{kvp.Key},{kvp.Value}")
                .ToArray();
        }
        File.WriteAllLines(_slotsFilePath, lines);
    }

    /// <summary>Validates that a collection name can safely be used as a filename component.</summary>
    private static void ValidateCollectionName(string collectionName)
    {
        if (string.IsNullOrWhiteSpace(collectionName))
            throw new ArgumentException("Collection name must not be null or whitespace.", nameof(collectionName));

        if (Path.IsPathRooted(collectionName) ||
            collectionName.Contains("..") ||
            collectionName.IndexOfAny(Path.GetInvalidFileNameChars()) >= 0)
            throw new ArgumentException(
                $"Collection name '{collectionName}' contains characters that are invalid in a filename.",
                nameof(collectionName));
    }
}
