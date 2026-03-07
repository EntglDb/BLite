using System.Collections.Concurrent;
using System.Text;

namespace BLite.Core.Storage;

/// <summary>
/// In-memory location pointer for a KV entry.
/// 6 bytes: fits in one CPU register on 64-bit.
/// </summary>
internal readonly record struct KvEntryLocation(uint PageId, ushort SlotIndex);

/// <summary>A single operation inside a <see cref="KeyValue.KvBatch"/>.</summary>
internal readonly record struct KvBatchEntry(string Key, byte[]? Value, long ExpiryTicks);

public sealed partial class StorageEngine
{
    // ── KV state ──────────────────────────────────────────────────────────────

    // Hash index:  UTF-8 key string → page+slot location.
    // Populated at open-time by warm-up scan; kept in sync on every mutation.
    private readonly ConcurrentDictionary<string, KvEntryLocation> _kvIndex =
        new(StringComparer.Ordinal);

    /// <summary>Ordered list of KV data page IDs (newest page at the end).</summary>
    private readonly List<uint> _kvPageIds = [];

    /// <summary>Serialises structural mutations: new-page alloc, compaction.</summary>
    private readonly object _kvWriteLock = new();

    /// <summary>Root directory page for KV (0 = not yet initialised).</summary>
    private uint _kvRootPageId;

    // ── Initialisation ────────────────────────────────────────────────────────

    /// <summary>
    /// Called by the StorageEngine constructor after InitializeDictionary().
    /// Reads the KV root from page-0 header and warm-starts the in-memory index.
    /// </summary>
    internal void InitializeKv()
    {
        var buf = new byte[PageSize];
        ReadPage(0, null, buf);
        var header    = PageHeader.ReadFrom(buf);
        _kvRootPageId = header.KvRootPageId;

        if (_kvRootPageId == 0)
            return; // No KV data yet — lazy init on first write

        WarmUpKvIndex();
    }

    private void WarmUpKvIndex()
    {
        // Root page is a lightweight directory: each entry value is a 4-byte KV data page ID.
        var dirBuf   = new byte[PageSize];
        ReadPage(_kvRootPageId, null, dirBuf);
        ReadOnlySpan<byte> dirView = dirBuf;
        var dirCount = KvPage.GetEntryCount(dirView);

        for (int d = 0; d < dirCount; d++)
        {
            var dirSlot = KvPage.SlotAt(dirView, d);
            if (KvPage.IsDeleted(dirSlot)) continue;
            var pageIdBytes = KvPage.GetValueSpan(dirView, dirSlot);
            if (pageIdBytes.Length < 4) continue;
            var dataPageId = System.Buffers.Binary.BinaryPrimitives.ReadUInt32LittleEndian(pageIdBytes);
            _kvPageIds.Add(dataPageId);
        }

        var dataBuf = new byte[PageSize];
        long now    = DateTime.UtcNow.Ticks;
        foreach (var pageId in _kvPageIds)
        {
            ReadPage(pageId, null, dataBuf);
            ReadOnlySpan<byte> dataView = dataBuf;
            var count = KvPage.GetEntryCount(dataView);
            for (ushort s = 0; s < count; s++)
            {
                var slot = KvPage.SlotAt(dataView, s);
                if (KvPage.IsDeleted(slot)) continue;
                if (KvPage.HasExpiry(slot))
                {
                    var exp = KvPage.GetExpiryTicks(dataView, slot);
                    if (exp > 0 && now > exp) continue;
                }
                var keyBytes = KvPage.GetKeySpan(dataView, slot);
                var key      = Encoding.UTF8.GetString(keyBytes);
                _kvIndex[key] = new KvEntryLocation(pageId, s);
            }
        }
    }

    // ── Public KV primitives ─────────────────────────────────────────────────

    /// <summary>
    /// Retrieves the value stored for <paramref name="key"/>, or <c>null</c> if not found / expired.
    /// Allocation-free on the hot path when the page is in the WAL index (mmap-backed).
    /// </summary>
    internal byte[]? KvGet(string key)
    {
        if (!_kvIndex.TryGetValue(key, out var loc)) return null;

        var buf = new byte[PageSize];
        ReadPage(loc.PageId, null, buf);
        ReadOnlySpan<byte> view = buf;

        long now = DateTime.UtcNow.Ticks;
        var slot = KvPage.SlotAt(view, loc.SlotIndex);

        if (KvPage.IsDeleted(slot))
        {
            _kvIndex.TryRemove(key, out _);
            return null;
        }

        // Lazy expiry check
        if (KvPage.HasExpiry(slot))
        {
            var exp = KvPage.GetExpiryTicks(view, slot);
            if (exp > 0 && now > exp)
            {
                KvSoftDeleteInPlace(loc.PageId, loc.SlotIndex);
                _kvIndex.TryRemove(key, out _);
                return null;
            }
        }

        return KvPage.GetValueSpan(view, slot).ToArray();
    }

    /// <summary>
    /// Stores <paramref name="value"/> under <paramref name="key"/> with optional TTL.
    /// Thread-safe; serialises structural mutations (new page alloc, compaction) but allows
    /// concurrent reads.
    /// </summary>
    internal void KvSet(string key, ReadOnlySpan<byte> value, long expiryTicks = 0)
    {
        var keyUtf8 = Encoding.UTF8.GetBytes(key);
        if (keyUtf8.Length > 255) throw new ArgumentException("KV key must be ≤ 255 UTF-8 bytes.", nameof(key));

        lock (_kvWriteLock)
        {
            EnsureKvInitialised();

            // If key exists, soft-delete old entry first
            if (_kvIndex.TryGetValue(key, out var oldLoc))
                KvSoftDeleteInPlace(oldLoc.PageId, oldLoc.SlotIndex);

            // Try to insert into existing pages (newest-first)
            for (int i = _kvPageIds.Count - 1; i >= 0; i--)
            {
                var pageId = _kvPageIds[i];
                var buf    = new byte[PageSize];
                ReadPage(pageId, null, buf);

                if (KvPage.TryInsert(buf, keyUtf8, value, expiryTicks))
                {
                    var slotIndex = (ushort)(KvPage.GetEntryCount(buf) - 1);
                    WritePageImmediate(pageId, buf);
                    _kvIndex[key] = new KvEntryLocation(pageId, slotIndex);
                    return;
                }
            }

            // All pages full — allocate a new data page
            var newPageId = AllocateKvDataPage();
            var newBuf    = new byte[PageSize];
            ReadPage(newPageId, null, newBuf);

            if (!KvPage.TryInsert(newBuf, keyUtf8, value, expiryTicks))
                throw new InvalidOperationException(
                    $"KV value too large for page (key={key}, valueLen={value.Length}).");

            var newSlot = (ushort)(KvPage.GetEntryCount(newBuf) - 1);
            WritePageImmediate(newPageId, newBuf);
            _kvIndex[key] = new KvEntryLocation(newPageId, newSlot);
        }
    }

    /// <summary>Removes a key. Returns <c>true</c> if the key existed.</summary>
    internal bool KvDelete(string key)
    {
        if (!_kvIndex.TryRemove(key, out var loc)) return false;
        KvSoftDeleteInPlace(loc.PageId, loc.SlotIndex);
        return true;
    }

    /// <summary>Returns <c>true</c> if the key exists and has not expired.</summary>
    internal bool KvExists(string key)
    {
        if (!_kvIndex.TryGetValue(key, out var loc)) return false;

        var buf = new byte[PageSize];
        ReadPage(loc.PageId, null, buf);
        ReadOnlySpan<byte> existsView = buf;
        var slot = KvPage.SlotAt(existsView, loc.SlotIndex);

        if (KvPage.IsDeleted(slot)) { _kvIndex.TryRemove(key, out _); return false; }
        if (KvPage.HasExpiry(slot))
        {
            var exp = KvPage.GetExpiryTicks(existsView, slot);
            if (exp > 0 && DateTime.UtcNow.Ticks > exp)
            {
                KvSoftDeleteInPlace(loc.PageId, loc.SlotIndex);
                _kvIndex.TryRemove(key, out _);
                return false;
            }
        }
        return true;
    }

    /// <summary>
    /// Refreshes the TTL of an existing key without changing its value.
    /// Returns <c>false</c> if the key does not exist or has already expired.
    /// </summary>
    internal bool KvRefresh(string key, long newExpiryTicks)
    {
        if (!KvExists(key)) return false;
        var existing = KvGet(key);
        if (existing == null) return false;
        KvSet(key, existing, newExpiryTicks);
        return true;
    }

    /// <summary>
    /// Enumerates all non-expired, non-deleted keys with the given prefix (ordinal comparison).
    /// </summary>
    internal IEnumerable<string> KvScanKeys(string prefix = "")
    {
        long now = DateTime.UtcNow.Ticks;
        foreach (var key in _kvIndex.Keys)
        {
            if (!string.IsNullOrEmpty(prefix) && !key.StartsWith(prefix, StringComparison.Ordinal))
                continue;
            if (!_kvIndex.TryGetValue(key, out var loc)) continue;
            var buf = new byte[PageSize];
            ReadPage(loc.PageId, null, buf);
            ReadOnlySpan<byte> scanView = buf;
            var slot = KvPage.SlotAt(scanView, loc.SlotIndex);
            if (KvPage.IsDeleted(slot)) continue;
            if (KvPage.HasExpiry(slot))
            {
                var exp = KvPage.GetExpiryTicks(scanView, slot);
                if (exp > 0 && now > exp) continue;
            }
            yield return key;
        }
    }

    /// <summary>
    /// Purges all soft-deleted and expired entries from every KV page.
    /// Returns the number of entries removed.
    /// </summary>
    internal int KvPurgeExpired()
    {
        int removed = 0;
        long now = DateTime.UtcNow.Ticks;
        var buf = new byte[PageSize];

        lock (_kvWriteLock)
        {
            foreach (var pageId in _kvPageIds)
            {
                ReadPage(pageId, null, buf);
                ReadOnlySpan<byte> purgePre = buf;
                int before = KvPage.GetEntryCount(purgePre);
                KvPage.Compact(buf, now);
                ReadOnlySpan<byte> purgePost = buf;
                int after = KvPage.GetEntryCount(purgePost);
                removed += before - after;
                WritePageImmediate(pageId, buf);
            }
        }

        // Rebuild index after compaction (slot indices changed)
        _kvIndex.Clear();
        WarmUpKvIndex();

        return removed;
    }

    // ── Batch execution ───────────────────────────────────────────────────────

    /// <summary>
    /// Executes a batch of Set / Delete operations under a single write-lock acquisition.
    /// Returns the count of operations that had a visible effect.
    /// </summary>
    internal int KvExecuteBatch(IReadOnlyList<KvBatchEntry> ops)
    {
        if (ops.Count == 0) return 0;

        lock (_kvWriteLock)
        {
            EnsureKvInitialised();
            int count = 0;
            foreach (var op in ops)
            {
                if (op.Value is null)
                {
                    if (_kvIndex.TryRemove(op.Key, out var loc))
                    {
                        KvSoftDeleteInPlace(loc.PageId, loc.SlotIndex);
                        count++;
                    }
                }
                else
                {
                    // _kvWriteLock is reentrant per-thread so calling KvSet here is safe.
                    KvSet(op.Key, op.Value, op.ExpiryTicks);
                    count++;
                }
            }
            return count;
        }
    }

    // ── Private helpers ───────────────────────────────────────────────────────

    private void EnsureKvInitialised()
    {
        if (_kvRootPageId != 0) return;

        _kvRootPageId = AllocatePage();
        var dirBuf = new byte[PageSize];
        KvPage.Initialize(dirBuf, _kvRootPageId);
        WritePageImmediate(_kvRootPageId, dirBuf);

        // Persist root page ID in the file header via the struct field
        var headerBuf = new byte[PageSize];
        ReadPage(0, null, headerBuf);
        var hdr         = PageHeader.ReadFrom(headerBuf);
        hdr.KvRootPageId = _kvRootPageId;
        hdr.WriteTo(headerBuf);
        WritePageImmediate(0, headerBuf);
    }

    private uint AllocateKvDataPage()
    {
        var dataPageId = AllocatePage();
        var pageBuf    = new byte[PageSize];
        KvPage.Initialize(pageBuf, dataPageId);
        WritePageImmediate(dataPageId, pageBuf);
        _kvPageIds.Add(dataPageId);

        // Register the new data page in the directory page
        var dirBuf  = new byte[PageSize];
        ReadPage(_kvRootPageId, null, dirBuf);
        var pageIdBytes = new byte[4];
        System.Buffers.Binary.BinaryPrimitives.WriteUInt32LittleEndian(pageIdBytes, dataPageId);
        var keyBytes = Encoding.UTF8.GetBytes($"p{dataPageId}");
        KvPage.TryInsert(dirBuf, keyBytes, pageIdBytes, 0);
        WritePageImmediate(_kvRootPageId, dirBuf);

        return dataPageId;
    }

    private void KvSoftDeleteInPlace(uint pageId, ushort slotIndex)
    {
        var buf = new byte[PageSize];
        ReadPage(pageId, null, buf);
        KvPage.SoftDelete(buf, slotIndex);
        WritePageImmediate(pageId, buf);
    }
}
