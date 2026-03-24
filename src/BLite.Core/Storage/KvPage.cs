using System.Buffers.Binary;
using System.Runtime.CompilerServices;
using System.Text;

namespace BLite.Core.Storage;

/// <summary>
/// Fixed-layout page for Key-Value storage. Purposely simpler than <see cref="SlottedPage"/>:
/// no BSON, no schema — just raw byte values with optional TTL.
///
/// Page layout (16 KB default):
/// ┌──────────────────────────────────┐
/// │  PageHeader        32 bytes      │  standard header (PageType.KeyValue)
/// │  EntryCount         2 bytes      │  number of live entries
/// │  FreeSpaceEnd       2 bytes      │  offset of bottom of free area
/// │  [SlotArray  N × 10 bytes]       │  grows downward from offset 36
/// │  ...free space...                │
/// │  [Entry data grows upward]       │  each entry: key(utf8) + value(bytes) + expiry(8)
/// └──────────────────────────────────┘
///
/// Slot (10 bytes):
///   DataOffset  ushort  2  — offset from page start to entry data
///   KeyLen      byte    1  — key length in bytes (max 255)
///   Flags       byte    1  — bit0=deleted, bit1=hasExpiry
///   ValueLen    uint    4  — value length in bytes
///   Reserved    ushort  2  — future use / alignment
///
/// Entry data (variable, packed):
///   KeyBytes   [KeyLen]    UTF-8 key
///   ValueBytes [ValueLen]  raw payload
///   ExpiresAt  8 bytes     UTC ticks (only when Flags.hasExpiry set)
/// </summary>
internal static class KvPage
{
    // ── Offsets within the page header area ──────────────────────────────────
    private const int HeaderSize      = 32;  // PageHeader
    private const int EntryCountOff   = 32;
    private const int FreeSpaceEndOff = 34;
    private const int SlotsStart      = 36;

    // ── Slot layout ───────────────────────────────────────────────────────────
    private const int SlotSize        = 10;
    private const int SlotOffData     = 0;
    private const int SlotOffKeyLen   = 2;
    private const int SlotOffFlags    = 3;
    private const int SlotOffValueLen = 4;
    // bytes 8-9 = reserved

    // ── Flags ─────────────────────────────────────────────────────────────────
    internal const byte FlagDeleted   = 0x01;
    internal const byte FlagHasExpiry = 0x02;

    // ── Entry trailer ─────────────────────────────────────────────────────────
    private const int ExpirySize = 8;  // long (UTC ticks)

    // ─────────────────────────────────────────────────────────────────────────

    /// <summary>Initialises a freshly allocated page buffer as an empty KV page.</summary>
    public static void Initialize(Span<byte> page, uint pageId)
    {
        page.Clear();
        var hdr = new PageHeader
        {
            PageId    = pageId,
            PageType  = PageType.KeyValue,
            FreeBytes = (ushort)(page.Length - SlotsStart),
        };
        hdr.WriteTo(page);
        BinaryPrimitives.WriteUInt16LittleEndian(page[EntryCountOff..], 0);
        BinaryPrimitives.WriteUInt16LittleEndian(page[FreeSpaceEndOff..], (ushort)page.Length);
    }

    // ─── Read helpers ─────────────────────────────────────────────────────────

    public static ushort GetEntryCount(ReadOnlySpan<byte> page)
        => BinaryPrimitives.ReadUInt16LittleEndian(page[EntryCountOff..]);

    public static ushort GetFreeSpaceEnd(ReadOnlySpan<byte> page)
        => BinaryPrimitives.ReadUInt16LittleEndian(page[FreeSpaceEndOff..]);

    /// <summary>
    /// Returns the slot span for entry <paramref name="slotIndex"/> (10 bytes, mutable).
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static Span<byte> SlotAt(Span<byte> page, int slotIndex)
        => page.Slice(SlotsStart + slotIndex * SlotSize, SlotSize);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static ReadOnlySpan<byte> SlotAt(ReadOnlySpan<byte> page, int slotIndex)
        => page.Slice(SlotsStart + slotIndex * SlotSize, SlotSize);

    public static bool IsDeleted(ReadOnlySpan<byte> slot)
        => (slot[SlotOffFlags] & FlagDeleted) != 0;

    public static bool HasExpiry(ReadOnlySpan<byte> slot)
        => (slot[SlotOffFlags] & FlagHasExpiry) != 0;

    public static ushort GetDataOffset(ReadOnlySpan<byte> slot)
        => BinaryPrimitives.ReadUInt16LittleEndian(slot[SlotOffData..]);

    public static byte GetKeyLen(ReadOnlySpan<byte> slot)
        => slot[SlotOffKeyLen];

    public static uint GetValueLen(ReadOnlySpan<byte> slot)
        => BinaryPrimitives.ReadUInt32LittleEndian(slot[SlotOffValueLen..]);

    /// <summary>Reads the key bytes for a slot into a stack-allocated span.</summary>
    public static ReadOnlySpan<byte> GetKeySpan(ReadOnlySpan<byte> page, ReadOnlySpan<byte> slot)
    {
        var off    = GetDataOffset(slot);
        var keyLen = GetKeyLen(slot);
        return page.Slice(off, keyLen);
    }

    /// <summary>Returns a span over the raw value bytes for a slot (zero-copy).</summary>
    public static ReadOnlySpan<byte> GetValueSpan(ReadOnlySpan<byte> page, ReadOnlySpan<byte> slot)
    {
        var off      = GetDataOffset(slot);
        var keyLen   = GetKeyLen(slot);
        var valueLen = (int)GetValueLen(slot);
        return page.Slice(off + keyLen, valueLen);
    }

    /// <summary>Returns the expiry ticks for a slot, or 0 if no expiry is set.</summary>
    public static long GetExpiryTicks(ReadOnlySpan<byte> page, ReadOnlySpan<byte> slot)
    {
        if (!HasExpiry(slot)) return 0;
        var off      = GetDataOffset(slot);
        var keyLen   = GetKeyLen(slot);
        var valueLen = (int)GetValueLen(slot);
        return BinaryPrimitives.ReadInt64LittleEndian(page.Slice(off + keyLen + valueLen, ExpirySize));
    }

    // ─── Write helpers ────────────────────────────────────────────────────────

    /// <summary>
    /// Tries to insert a new entry. Returns false when the page has insufficient space.
    /// Does not check for duplicate keys — the caller is responsible for deduplication.
    /// </summary>
    public static bool TryInsert(Span<byte> page, ReadOnlySpan<byte> keyUtf8, ReadOnlySpan<byte> value, long expiryTicks)
    {
        if (keyUtf8.Length > 255) throw new ArgumentException("Key length must be ≤ 255 bytes");

        bool hasExpiry  = expiryTicks > 0;
        int  entrySize  = keyUtf8.Length + value.Length + (hasExpiry ? ExpirySize : 0);
        int  needed     = entrySize + SlotSize;

        var count        = GetEntryCount(page);
        var freeSpaceEnd = GetFreeSpaceEnd(page);
        var slotsEnd     = SlotsStart + (count + 1) * SlotSize;
        if (freeSpaceEnd - slotsEnd < entrySize) return false; // not enough room

        // Write entry growing from bottom of free space upward (data grows down)
        var dataOffset = (ushort)(freeSpaceEnd - entrySize);
        keyUtf8.CopyTo(page[dataOffset..]);
        value.CopyTo(page[(dataOffset + keyUtf8.Length)..]);
        if (hasExpiry)
            BinaryPrimitives.WriteInt64LittleEndian(
                page[(dataOffset + keyUtf8.Length + value.Length)..], expiryTicks);

        // Write slot
        var slot = SlotAt(page, count);
        BinaryPrimitives.WriteUInt16LittleEndian(slot[SlotOffData..], dataOffset);
        slot[SlotOffKeyLen]   = (byte)keyUtf8.Length;
        slot[SlotOffFlags]    = hasExpiry ? FlagHasExpiry : (byte)0;
        BinaryPrimitives.WriteUInt32LittleEndian(slot[SlotOffValueLen..], (uint)value.Length);

        // UpdateAsync counters
        BinaryPrimitives.WriteUInt16LittleEndian(page[EntryCountOff..],   (ushort)(count + 1));
        BinaryPrimitives.WriteUInt16LittleEndian(page[FreeSpaceEndOff..], dataOffset);

        // UpdateAsync FreeBytes in header
        var hdr = PageHeader.ReadFrom(page);
        hdr.FreeBytes = (ushort)Math.Max(0, hdr.FreeBytes - needed);
        hdr.WriteTo(page);

        return true;
    }

    /// <summary>Soft-deletes a slot by setting the Deleted flag.</summary>
    public static void SoftDelete(Span<byte> page, int slotIndex)
    {
        var slot = SlotAt(page, slotIndex);
        slot[SlotOffFlags] |= FlagDeleted;
    }

    /// <summary>
    /// Finds the slot index for <paramref name="keyUtf8"/> by linear scan.
    /// Returns -1 if not found or soft-deleted.
    /// Also returns -1 (treating as not-found) when the entry is expired.
    /// </summary>
    public static int FindSlot(ReadOnlySpan<byte> page, ReadOnlySpan<byte> keyUtf8, long nowTicks)
    {
        var count = GetEntryCount(page);
        for (int i = 0; i < count; i++)
        {
            var slot = SlotAt(page, i);
            if (IsDeleted(slot)) continue;
            if (!GetKeySpan(page, slot).SequenceEqual(keyUtf8)) continue;
            if (HasExpiry(slot))
            {
                var expiry = GetExpiryTicks(page, slot);
                if (expiry > 0 && nowTicks > expiry) return -1; // expired
            }
            return i;
        }
        return -1;
    }

    /// <summary>
    /// Returns the fraction of the page occupied by soft-deleted / expired entries.
    /// Values above ~0.5 indicate the page should be compacted.
    /// </summary>
    public static float WastedFraction(ReadOnlySpan<byte> page, long nowTicks)
    {
        var count = GetEntryCount(page);
        if (count == 0) return 0f;
        int wasted = 0;
        for (int i = 0; i < count; i++)
        {
            var slot = SlotAt(page, i);
            if (IsDeleted(slot)) { wasted++; continue; }
            if (HasExpiry(slot) && GetExpiryTicks(page, slot) is var exp && exp > 0 && nowTicks > exp)
                wasted++;
        }
        return (float)wasted / count;
    }

    /// <summary>
    /// Rewrites the page in-place removing all soft-deleted and expired entries.
    /// Uses a temporary buffer of the same size.
    /// </summary>
    public static void Compact(Span<byte> page, long nowTicks)
    {
        // Read all live entries first
        var count = GetEntryCount(page);
        var live  = new List<(byte[] key, byte[] value, long expiry)>(count);

        for (int i = 0; i < count; i++)
        {
            var slot = SlotAt(page, i);
            if (IsDeleted(slot)) continue;
            if (HasExpiry(slot) && GetExpiryTicks(page, slot) is var exp && exp > 0 && nowTicks > exp) continue;

            live.Add((
                GetKeySpan(page, slot).ToArray(),
                GetValueSpan(page, slot).ToArray(),
                HasExpiry(slot) ? GetExpiryTicks(page, slot) : 0
            ));
        }

        // Re-initialise page then re-insert
        var pageId = PageHeader.ReadFrom(page).PageId;
        Initialize(page, pageId);
        foreach (var (k, v, exp) in live)
            TryInsert(page, k, v, exp);
    }

    /// <summary>Computes the minimum page size needed to fit a given entry.</summary>
    public static int RequiredSpace(int keyBytes, int valueBytes, bool hasExpiry)
        => SlotSize + keyBytes + valueBytes + (hasExpiry ? ExpirySize : 0);
}
