using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using BLite.Bson;
using BLite.Core.Indexing;
using BLite.Core.Collections;
using BLite.Core.Retention;

namespace BLite.Core.Storage;

/// <summary>
/// Defines a single source field for text composition during vector embedding.
/// When concatenated with other fields, these strings are vettorized together.
/// </summary>
public class VectorSourceField
{
    public string Path { get; set; } = string.Empty;
    public string? Prefix { get; set; }
    public string? Suffix { get; set; }
}

/// <summary>
/// Configuration for automatic text-to-vector embedding of documents in a collection.
/// Specifies which fields to concatenate and how, so that documents can be
/// vettorized into a specified vector field by an embedding service.
/// </summary>
public class VectorSourceConfig
{
    public List<VectorSourceField> Fields { get; } = new();
    public string Separator { get; set; } = " ";

    /// <summary>
    /// Builds the combined text from a document using the configured fields and separator.
    /// </summary>
    public string BuildText(BsonDocument document)
    {
        if (Fields.Count == 0 || document == null)
            return string.Empty;

        var parts = new List<string>();
        foreach (var field in Fields)
        {
            if (!document.TryGetValue(field.Path, out var value))
                continue;

            var text = ValueToString(value);
            if (string.IsNullOrEmpty(text))
                continue;

            // Build: [prefix] + text + [suffix]
            var part = string.Concat(field.Prefix ?? "", text, field.Suffix ?? "");
            if (!string.IsNullOrEmpty(part))
                parts.Add(part);
        }

        return string.Join(Separator, parts);
    }

    private static string ValueToString(BsonValue value) => value.Type switch
    {
        BsonType.String => value.AsString,
        BsonType.Int32 => value.AsInt32.ToString(),
        BsonType.Int64 => value.AsInt64.ToString(),
        BsonType.Double => value.AsDouble.ToString("G"),
        BsonType.Boolean => value.AsBoolean ? "true" : "false",
        BsonType.Array => string.Join(" ", value.AsArray.Select(ValueToString)),
        BsonType.ObjectId => value.AsObjectId.ToString(),
        BsonType.DateTime => value.AsDateTime.ToString("O"),
        BsonType.DateTimeOffset => value.AsDateTimeOffset.ToString("O"),
        _ => null
    } ?? string.Empty;
}

public class CollectionMetadata
{
    public string Name { get; set; } = string.Empty;
    public uint PrimaryRootPageId { get; set; }
    public uint SchemaRootPageId { get; set; }
    public List<IndexMetadata> Indexes { get; } = new();
    public VectorSourceConfig? VectorSource { get; set; }

    // ── TimeSeries Properties ──
    public bool IsTimeSeries { get; set; }
    public uint TimeSeriesHeadPageId { get; set; }
    public long RetentionPolicyMs { get; set; }
    public string? TtlFieldName { get; set; }
    public long LastPruningTimestamp { get; set; }
    public int InsertedSinceLastPruning { get; set; }

    // ── Sequence (int/long auto-increment) ──
    public long SequenceValue { get; set; }

    // ── Generalized Retention Policy ──
    public RetentionPolicy? GeneralRetentionPolicy { get; set; }

    // ── Required features ──
    /// <summary>
    /// Features this collection depends on that older versions cannot maintain; a version that
    /// does not know one of them refuses to open or modify the collection (see
    /// <see cref="CollectionFeatures"/>). Persisted as stored, including features this version does
    /// not know, so a rewrite of the record never drops them.
    /// </summary>
    internal List<RequiredFeature> RequiredFeatures { get; } = new();
}

public class IndexMetadata
{
    /// <summary>
    /// Key format written before <see cref="KeyFormat"/> existed: <c>decimal</c> values were keyed as
    /// <c>(double)value</c>. Every other key type is encoded identically in both formats.
    /// </summary>
    public const byte LegacyKeyFormat = 0;

    /// <summary>Current key format: <c>decimal</c> values use the exact, order-preserving encoding.</summary>
    public const byte CurrentKeyFormat = 1;

    public string Name { get; set; } = string.Empty;

    /// <summary>
    /// Version of the key encoding the index pages were written with. Indexes persisted by older
    /// releases read back as <see cref="LegacyKeyFormat"/> and are rebuilt on open when the
    /// difference matters for their key type (see <c>DocumentCollection.MigrateLegacyDecimalIndexes</c>).
    /// </summary>
    public byte KeyFormat { get; set; } = LegacyKeyFormat;
    public bool IsUnique { get; set; }
    public IndexType Type { get; set; }
    public string[] PropertyPaths { get; set; } = Array.Empty<string>();
    public int Dimensions { get; set; }
    public VectorMetric Metric { get; set; }
    public uint RootPageId { get; set; }
}

public sealed partial class StorageEngine
{
    public CollectionMetadata? GetCollectionMetadata(string name)
    {
        // Lock-free first: saves only rewrite or move records, so a record this walk finds is the
        // collection's. A miss proves nothing, though: a concurrent compaction (any save, under
        // _metadataLock) can move the record onto a page the walk has already read and rewrite or
        // free the page it was on. Callers take null for "new collection" and save fresh metadata
        // over the existing record, so a miss, or a walk that runs into a page it cannot parse, is
        // repeated under the lock.
        try
        {
            var metadata = FindCollectionMetadata(name);
            if (metadata != null)
                return metadata;
        }
        catch
        {
            // Retried under the lock below, where an error is real and propagates.
        }

        if (!_metadataLock.Wait(_config.LockTimeout.WriteTimeoutMs))
            throw new TimeoutException("Timed out acquiring metadata lock (GetCollectionMetadata).");
        try
        {
            return FindCollectionMetadata(name);
        }
        finally { _metadataLock.Release(); }
    }

    private CollectionMetadata? FindCollectionMetadata(string name)
    {
        var buffer = new byte[PageSize];
        uint pageId = 1;

        while (pageId != 0)
        {
            ReadPage(pageId, null, buffer);
            var header = SlottedPageHeader.ReadFrom(buffer);
            if (header.PageType != PageType.Collection)
                break;

            for (ushort i = 0; i < header.SlotCount; i++)
            {
                var slotOffset = SlottedPageHeader.Size + (i * SlotEntry.Size);
                var slot = SlotEntry.ReadFrom(buffer.AsSpan(slotOffset));

                if ((slot.Flags & SlotFlags.Deleted) != 0) continue;

                try
                {
                    using var ms = new MemoryStream(buffer, slot.Offset, slot.Length, false);
                    using var reader = new BinaryReader(ms);

                    var collName = reader.ReadString();
                    if (!string.Equals(collName, name, StringComparison.OrdinalIgnoreCase))
                        continue;

                    return ReadCollectionMetadata(collName, reader);
                }
                catch
                {
                    // Ignore malformed slots
                }
            }

            pageId = header.NextOverflowPage;
        }

        return null;
    }

    /// <summary>
    /// Deserialises one collection record after its name. The only parser of this record: every
    /// section after the index list is optional and read only while bytes remain, so records written
    /// by older releases load with defaults for the sections they lack.
    /// </summary>
    /// <remarks>
    /// Format rule: new data goes only in new trailing sections, after the last one read here; new
    /// per-index data too (as the key formats do), never inline in the index list. The list is read by
    /// position, with the vector fields as the only per-type extra, so a version that finds bytes it
    /// does not expect there misreads the rest of the record and fails to parse it.
    /// <see cref="GetCollectionMetadata"/> then returns <c>null</c>, a dynamic open treats the collection
    /// as new and saves a fresh record over it, and the required-features refusal
    /// (<see cref="CollectionFeatures"/>) never runs.
    /// </remarks>
    private static CollectionMetadata ReadCollectionMetadata(string name, BinaryReader reader)
    {
        var metadata = new CollectionMetadata { Name = name };
        metadata.PrimaryRootPageId = reader.ReadUInt32();
        metadata.SchemaRootPageId  = reader.ReadUInt32();

        var indexCount = reader.ReadInt32();
        for (int j = 0; j < indexCount; j++)
        {
            var idx = new IndexMetadata
            {
                Name       = reader.ReadString(),
                IsUnique   = reader.ReadBoolean(),
                Type       = (IndexType)reader.ReadByte(),
                RootPageId = reader.ReadUInt32()
            };

            var pathCount = reader.ReadInt32();
            idx.PropertyPaths = new string[pathCount];
            for (int k = 0; k < pathCount; k++)
                idx.PropertyPaths[k] = reader.ReadString();

            if (idx.Type == IndexType.Vector)
            {
                idx.Dimensions = reader.ReadInt32();
                idx.Metric     = (VectorMetric)reader.ReadByte();
            }

            metadata.Indexes.Add(idx);
        }

        // ── TimeSeries (backward-compatible) ────────
        if (reader.BaseStream.Position < reader.BaseStream.Length)
        {
            metadata.IsTimeSeries = reader.ReadBoolean();
            if (metadata.IsTimeSeries)
            {
                metadata.TimeSeriesHeadPageId = reader.ReadUInt32();
                metadata.RetentionPolicyMs = reader.ReadInt64();
                metadata.TtlFieldName = reader.ReadBoolean() ? reader.ReadString() : null;
                metadata.LastPruningTimestamp = reader.ReadInt64();
                metadata.InsertedSinceLastPruning = reader.ReadInt32();
            }
        }

        // ── VectorSource (backward-compatible) ────────
        if (reader.BaseStream.Position < reader.BaseStream.Length)
        {
            bool hasVectorSource = reader.ReadBoolean();
            if (hasVectorSource)
            {
                var config = new VectorSourceConfig
                {
                    Separator = reader.ReadString()
                };

                int fieldCount = reader.ReadInt32();
                for (int j = 0; j < fieldCount; j++)
                {
                    var field = new VectorSourceField
                    {
                        Path = reader.ReadString(),
                        Prefix = reader.ReadBoolean() ? reader.ReadString() : null,
                        Suffix = reader.ReadBoolean() ? reader.ReadString() : null
                    };
                    config.Fields.Add(field);
                }

                metadata.VectorSource = config;
            }
        }

        // ── Sequence (backward-compatible) ────────
        if (reader.BaseStream.Position < reader.BaseStream.Length)
            metadata.SequenceValue = reader.ReadInt64();

        // ── GeneralRetentionPolicy (backward-compatible) ────────
        if (reader.BaseStream.Position < reader.BaseStream.Length)
        {
            bool hasRetentionPolicy = reader.ReadBoolean();
            if (hasRetentionPolicy)
            {
                metadata.GeneralRetentionPolicy = new RetentionPolicy
                {
                    MaxAgeMs           = reader.ReadInt64(),
                    MaxDocumentCount   = reader.ReadInt64(),
                    MaxSizeBytes       = reader.ReadInt64(),
                    ScheduledIntervalMs = reader.ReadInt64(),
                    TimestampField     = reader.ReadBoolean() ? reader.ReadString() : null,
                    Triggers           = (RetentionTrigger)reader.ReadInt32(),
                };
            }
        }

        // ── Index key formats (backward-compatible; absent = LegacyKeyFormat) ────────
        if (reader.BaseStream.Position < reader.BaseStream.Length)
        {
            int formatCount = reader.ReadInt32();
            for (int j = 0; j < formatCount; j++)
            {
                byte format = reader.ReadByte();
                if (j < metadata.Indexes.Count)
                    metadata.Indexes[j].KeyFormat = format;
            }
        }

        // ── Required features (backward-compatible; absent = none) ────────
        if (reader.BaseStream.Position < reader.BaseStream.Length)
        {
            int featureCount = reader.ReadInt32();
            for (int j = 0; j < featureCount; j++)
            {
                var id          = reader.ReadString();
                var requirement = (FeatureRequirement)reader.ReadByte();
                var description = reader.ReadString();
                metadata.RequiredFeatures.Add(new RequiredFeature(id, requirement, description));
            }
        }
        return metadata;
    }

    public void SaveCollectionMetadata(CollectionMetadata metadata)
    {
        if (!_metadataLock.Wait(_config.LockTimeout.WriteTimeoutMs))
            throw new TimeoutException("Timed out acquiring metadata lock (SaveCollectionMetadata).");
        try
        {
            SaveCollectionMetadataLocked(metadata);
        }
        finally { _metadataLock.Release(); }
    }

    /// <summary>
    /// Reads the stored record of <paramref name="name"/> (null if there is none), hands it to
    /// <paramref name="update"/> and saves what that returns, all under the metadata lock, so
    /// two savers of the same record on this engine cannot each merge against a copy the other
    /// is about to overwrite (#192). <paramref name="update"/> must not touch the metadata lock
    /// (it is not reentrant). Across processes the write still wins last; the callers that
    /// create indexes hold the open lock for that.
    /// </summary>
    public void UpdateCollectionMetadata(string name, Func<CollectionMetadata?, CollectionMetadata> update)
    {
        if (update == null) throw new ArgumentNullException(nameof(update));
        if (!_metadataLock.Wait(_config.LockTimeout.WriteTimeoutMs))
            throw new TimeoutException("Timed out acquiring metadata lock (UpdateCollectionMetadata).");
        try
        {
            SaveCollectionMetadataLocked(update(FindCollectionMetadata(name)));
        }
        finally { _metadataLock.Release(); }
    }

    /// <summary>Body of <see cref="SaveCollectionMetadata"/>; call with <c>_metadataLock</c> held.</summary>
    private void SaveCollectionMetadataLocked(CollectionMetadata metadata)
    {
        using var stream = new MemoryStream();
        using var writer = new BinaryWriter(stream);

        writer.Write(metadata.Name);
        writer.Write(metadata.PrimaryRootPageId);
        writer.Write(metadata.SchemaRootPageId);
        writer.Write(metadata.Indexes.Count);
        foreach (var idx in metadata.Indexes)
        {
            writer.Write(idx.Name);
            writer.Write(idx.IsUnique);
            writer.Write((byte)idx.Type);
            writer.Write(idx.RootPageId);
            writer.Write(idx.PropertyPaths.Length);
            foreach (var path in idx.PropertyPaths)
                writer.Write(path);

            if (idx.Type == IndexType.Vector)
            {
                writer.Write(idx.Dimensions);
                writer.Write((byte)idx.Metric);
            }
        }

        // ── TimeSeries Properties ────────
        writer.Write(metadata.IsTimeSeries);
        if (metadata.IsTimeSeries)
        {
            writer.Write(metadata.TimeSeriesHeadPageId);
            writer.Write(metadata.RetentionPolicyMs);
            writer.Write(metadata.TtlFieldName != null);
            if (metadata.TtlFieldName != null)
                writer.Write(metadata.TtlFieldName);
            writer.Write(metadata.LastPruningTimestamp);
            writer.Write(metadata.InsertedSinceLastPruning);
        }

        // ── VectorSource serialization ──────────────────────────────────────────
        writer.Write(metadata.VectorSource != null);
        if (metadata.VectorSource != null)
        {
            writer.Write(metadata.VectorSource.Separator);
            writer.Write(metadata.VectorSource.Fields.Count);
            foreach (var field in metadata.VectorSource.Fields)
            {
                writer.Write(field.Path);
                writer.Write(field.Prefix != null);
                if (field.Prefix != null)
                    writer.Write(field.Prefix);
                writer.Write(field.Suffix != null);
                if (field.Suffix != null)
                    writer.Write(field.Suffix);
            }
        }

        // ── Sequence serialization ─────────────────────────────────────────────
        writer.Write(metadata.SequenceValue);

        // ── GeneralRetentionPolicy serialization ───────────────────────────────
        writer.Write(metadata.GeneralRetentionPolicy != null);
        if (metadata.GeneralRetentionPolicy != null)
        {
            var rp = metadata.GeneralRetentionPolicy;
            writer.Write(rp.MaxAgeMs);
            writer.Write(rp.MaxDocumentCount);
            writer.Write(rp.MaxSizeBytes);
            writer.Write(rp.ScheduledIntervalMs);
            writer.Write(rp.TimestampField != null);
            if (rp.TimestampField != null)
                writer.Write(rp.TimestampField);
            writer.Write((int)rp.Triggers);
        }

        // ── Index key formats (one byte per index, same order as above) ─────────
        writer.Write(metadata.Indexes.Count);
        foreach (var idx in metadata.Indexes)
            writer.Write(idx.KeyFormat);

        // ── Required features (id, requirement, description) ──────────────────
        // Written after every older section, so releases that stop reading at the key formats
        // load the record unchanged. New sections go after this one.
        writer.Write(metadata.RequiredFeatures.Count);
        foreach (var feature in metadata.RequiredFeatures)
        {
            writer.Write(feature.Id);
            writer.Write((byte)feature.Requirement);
            writer.Write(feature.Description);
        }

        var newData = stream.ToArray();
        // Checked before anything is written or allocated: the stored record stays as it is.
        if (newData.Length + SlotEntry.Size > PageSize - SlottedPageHeader.Size)
            throw new InvalidOperationException(
                $"Collection metadata for '{metadata.Name}' ({newData.Length} bytes) does not fit in a page.");

        var chain = LoadMetadataChain();
        if (chain.Count == 0)
            throw new InvalidOperationException("Page 1 is not a collection metadata page.");
        var (foundPage, foundSlot) = FindMetadataRecord(chain, metadata.Name);

        // Never rewrite the record of a collection that requires features this version lacks, or
        // one it cannot parse (see CollectionFeatures). Checked before every write path, the
        // in-place one included. Nothing has been written yet.
        if (foundPage >= 0)
            EnsureCanReplaceRecord(metadata.Name, chain[foundPage].Buffer, foundSlot);

        // ── 1. No larger than the stored record: overwrite it where it is ─────
        // A single page write. Covers the frequent saves (sequence values, root page ids) whose
        // record keeps its size, so they no longer leave a deleted record behind (#195).
        if (foundPage >= 0)
        {
            var (pageId, buffer) = chain[foundPage];
            var slot = ReadSlot(buffer, foundSlot);
            if (newData.Length <= slot.Length)
            {
                newData.CopyTo(buffer.AsSpan(slot.Offset));
                slot.Length = (ushort)newData.Length;
                WriteSlot(buffer, foundSlot, slot);
                WritePageImmediate(pageId, buffer);

                // Shrink a chain grown by earlier releases (or by records that grew) once a page can go.
                CompactMetadataChain(chain, onlyIfShorter: true);
                return;
            }
        }

        // ── 2. Larger or new: reclaim dead space first, so the record goes into free space in the
        //       chain before a page is appended ────────────────────────────────────────────────
        chain = CompactMetadataChain(chain, onlyIfShorter: false);
        (foundPage, foundSlot) = FindMetadataRecord(chain, metadata.Name);

        // ── 3. Write the new record, then delete the old one: a crash in between leaves the old
        //       record in place rather than neither ─────────────────────────────────────────────
        int target = chain.FindIndex(p =>
            SlottedPageHeader.ReadFrom(p.Buffer).AvailableFreeSpace >= newData.Length + SlotEntry.Size);

        if (target < 0)
        {
            // No room anywhere: append a page holding the record, then link it from the last page.
            var newPageId = AllocatePage();
            var newPage = new byte[PageSize];
            NewMetadataPage(newPageId, 0).WriteTo(newPage);
            AppendRecord(newPage, newData);
            WritePageImmediate(newPageId, newPage);

            var (lastId, lastBuf) = chain[chain.Count - 1];
            var lastHdr = SlottedPageHeader.ReadFrom(lastBuf);
            lastHdr.NextOverflowPage = newPageId;
            lastHdr.WriteTo(lastBuf);
            WritePageImmediate(lastId, lastBuf);
        }
        else
        {
            AppendRecord(chain[target].Buffer, newData);
            if (target == foundPage)
            {
                // Same page: one write covers both the new record and the deletion.
                MarkSlotDeleted(chain[foundPage].Buffer, foundSlot);
                foundPage = -1;
            }
            WritePageImmediate(chain[target].PageId, chain[target].Buffer);
        }

        if (foundPage >= 0)
        {
            MarkSlotDeleted(chain[foundPage].Buffer, foundSlot);
            WritePageImmediate(chain[foundPage].PageId, chain[foundPage].Buffer);
        }
    }

    // ── Collection metadata chain (page 1 and its overflow pages) ────────────

    /// <summary>Reads the Collection pages of the metadata chain, starting at page 1.</summary>
    private List<(uint PageId, byte[] Buffer)> LoadMetadataChain()
    {
        var chain = new List<(uint PageId, byte[] Buffer)>();
        uint pageId = 1;
        while (pageId != 0)
        {
            var buffer = new byte[PageSize];
            ReadPage(pageId, null, buffer);
            var header = SlottedPageHeader.ReadFrom(buffer);
            if (header.PageType != PageType.Collection)
                break;
            chain.Add((pageId, buffer));
            pageId = header.NextOverflowPage;
        }
        return chain;
    }

    /// <summary>Finds the first live record of the named collection, or (-1, -1).</summary>
    private static (int Page, int Slot) FindMetadataRecord(List<(uint PageId, byte[] Buffer)> chain, string name)
    {
        for (int p = 0; p < chain.Count; p++)
        {
            var buffer = chain[p].Buffer;
            var header = SlottedPageHeader.ReadFrom(buffer);
            for (int i = 0; i < header.SlotCount; i++)
            {
                var slot = ReadSlot(buffer, i);
                if ((slot.Flags & SlotFlags.Deleted) != 0) continue;
                if (string.Equals(TryReadRecordName(buffer, slot), name, StringComparison.OrdinalIgnoreCase))
                    return (p, i);
            }
        }
        return (-1, -1);
    }

    /// <summary>
    /// Packs the live records of the chain into as few pages as possible, starting at page 1 and in
    /// their current order, and frees the pages no longer needed. Deleted records, bytes left behind
    /// by records overwritten with shorter ones, and duplicates of a record (kept: the first, as
    /// lookups do) are dropped.
    /// </summary>
    /// <param name="onlyIfShorter">
    /// Only rewrite when that frees at least one page; otherwise whenever there is anything to drop.
    /// </param>
    /// <returns>The chain as it is afterwards.</returns>
    /// <remarks>
    /// Pages are rewritten front to back and every record lands on its own page or an earlier one,
    /// so all records are on some page after each write. A crash part-way can leave a record on
    /// two pages; the copies are identical, readers take the first, and the next compaction drops
    /// the other. A crash after the last page is rewritten but before the pages past the new end
    /// are freed leaves those pages unreachable and off the free list: they leak (file space only,
    /// no records are lost), which is accepted to keep the rewrite free of extra bookkeeping.
    /// </remarks>
    private List<(uint PageId, byte[] Buffer)> CompactMetadataChain(List<(uint PageId, byte[] Buffer)> chain, bool onlyIfShorter)
    {
        // A single page cannot get shorter. This is the common case: every in-place save asks.
        if (onlyIfShorter && chain.Count <= 1)
            return chain;

        // Plan from the slot tables alone: which records stay, and the page each one lands on.
        var keep  = new List<(int SourcePage, SlotEntry Slot, int TargetPage)>();
        var names = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        bool anythingToDrop = false;
        int pageCount = 1;
        int room = PageSize - SlottedPageHeader.Size;

        for (int p = 0; p < chain.Count; p++)
        {
            var buffer = chain[p].Buffer;
            var header = SlottedPageHeader.ReadFrom(buffer);
            int liveBytes = 0;
            for (int i = 0; i < header.SlotCount; i++)
            {
                var slot = ReadSlot(buffer, i);
                if ((slot.Flags & SlotFlags.Deleted) != 0) { anythingToDrop = true; continue; }

                var name = TryReadRecordName(buffer, slot);
                if (name != null && !names.Add(name)) { anythingToDrop = true; continue; }
                liveBytes += slot.Length;

                // Greedy packing in order needs no more pages than any in-order layout, the current one included.
                int need = slot.Length + SlotEntry.Size;
                if (need > room)
                {
                    pageCount++;
                    room = PageSize - SlottedPageHeader.Size;
                }
                room -= need;
                keep.Add((p, slot, pageCount - 1));
            }
            if (PageSize - header.FreeSpaceEnd > liveBytes) anythingToDrop = true;
        }

        if (pageCount >= chain.Count && (onlyIfShorter || !anythingToDrop))
            return chain;

        // The records are copied out of the buffers loaded before the first write; each page is
        // rebuilt in a fresh buffer, so those stay intact while the chain is rewritten.
        var result = new List<(uint PageId, byte[] Buffer)>(pageCount);
        int k = 0;
        for (int p = 0; p < pageCount; p++)
        {
            var pageId = chain[p].PageId;
            var next   = p + 1 < pageCount ? chain[p + 1].PageId : 0u;
            var buffer = new byte[PageSize];
            NewMetadataPage(pageId, next).WriteTo(buffer);
            for (; k < keep.Count && keep[k].TargetPage == p; k++)
            {
                var (source, slot, _) = keep[k];
                AppendRecord(buffer, chain[source].Buffer.AsSpan(slot.Offset, slot.Length));
            }
            WritePageImmediate(pageId, buffer);
            result.Add((pageId, buffer));
        }

        // Page 1 is always kept; the pages after the new end of the chain are no longer reachable.
        for (int p = pageCount; p < chain.Count; p++)
            FreePage(chain[p].PageId);

        return result;
    }

    private SlottedPageHeader NewMetadataPage(uint pageId, uint nextPageId) => new()
    {
        PageId           = pageId,
        PageType         = PageType.Collection,
        SlotCount        = 0,
        FreeSpaceStart   = SlottedPageHeader.Size,
        FreeSpaceEnd     = (ushort)PageSize,
        NextOverflowPage = nextPageId,
        TransactionId    = 0
    };

    /// <summary>Adds <paramref name="record"/> as a new slot; the caller checked that it fits.</summary>
    private static void AppendRecord(byte[] page, ReadOnlySpan<byte> record)
    {
        var header = SlottedPageHeader.ReadFrom(page);
        int offset = header.FreeSpaceEnd - record.Length;
        record.CopyTo(page.AsSpan(offset));
        WriteSlot(page, header.SlotCount, new SlotEntry
        {
            Offset = (ushort)offset,
            Length = (ushort)record.Length,
            Flags  = SlotFlags.None
        });
        header.SlotCount++;
        header.FreeSpaceEnd   = (ushort)offset;
        header.FreeSpaceStart = (ushort)(SlottedPageHeader.Size + (header.SlotCount * SlotEntry.Size));
        header.WriteTo(page);
    }

    private static void MarkSlotDeleted(byte[] page, int slotIndex)
    {
        var slot = ReadSlot(page, slotIndex);
        slot.Flags |= SlotFlags.Deleted;
        WriteSlot(page, slotIndex, slot);
    }

    private static SlotEntry ReadSlot(byte[] page, int slotIndex)
        => SlotEntry.ReadFrom(page.AsSpan(SlottedPageHeader.Size + (slotIndex * SlotEntry.Size)));

    private static void WriteSlot(byte[] page, int slotIndex, SlotEntry slot)
        => slot.WriteTo(page.AsSpan(SlottedPageHeader.Size + (slotIndex * SlotEntry.Size)));

    /// <summary>The collection name a record starts with, or <c>null</c> if the record is malformed.</summary>
    private static string? TryReadRecordName(byte[] page, SlotEntry slot)
    {
        try
        {
            using var ms     = new MemoryStream(page, slot.Offset, slot.Length, false);
            using var reader = new BinaryReader(ms);
            return reader.ReadString();
        }
        catch
        {
            return null;
        }
    }

    /// <summary>
    /// Throws unless this version may overwrite or drop the stored record of collection
    /// <paramref name="name"/> in slot <paramref name="slotIndex"/>: the record must parse, and every
    /// feature it requires must be known (see <see cref="CollectionFeatures"/>). A record that does not
    /// parse is refused too, since its features cannot be checked.
    /// </summary>
    /// <exception cref="NotSupportedException">The record cannot be parsed, or requires features this version lacks.</exception>
    private static void EnsureCanReplaceRecord(string name, byte[] page, int slotIndex)
    {
        var slot = ReadSlot(page, slotIndex);
        CollectionMetadata stored;
        try
        {
            using var ms     = new MemoryStream(page, slot.Offset, slot.Length, false);
            using var reader = new BinaryReader(ms);
            stored = ReadCollectionMetadata(reader.ReadString(), reader);
        }
        catch (Exception ex)
        {
            throw CollectionFeatures.UnreadableRecord(name, ex);
        }
        CollectionFeatures.EnsureCanWrite(stored);
    }

    /// <summary>
    /// Like <see cref="EnsureCanReplaceRecord"/>, for the first live record of collection
    /// <paramref name="name"/> (the one <see cref="SaveCollectionMetadata"/> replaces), if there is one.
    /// </summary>
    private void EnsureCanReplaceStoredRecord(string name)
    {
        var chain = LoadMetadataChain();
        var (page, slot) = FindMetadataRecord(chain, name);
        if (page >= 0)
            EnsureCanReplaceRecord(name, chain[page].Buffer, slot);
    }

    /// <summary>
    /// Marks the collection's metadata slot as deleted so that it no longer
    /// appears in <see cref="GetAllCollectionsMetadata"/> or
    /// <see cref="GetCollectionMetadata"/>.
    /// </summary>
    public void DeleteCollectionMetadata(string name)
    {
        if (!_metadataLock.Wait(_config.LockTimeout.WriteTimeoutMs))
            throw new TimeoutException("Timed out acquiring metadata lock (DeleteCollectionMetadata).");
        try
        {
        // Never drop the record of a collection that requires features this version lacks, or one
        // it cannot parse (see CollectionFeatures).
        EnsureCanReplaceStoredRecord(name);

        var buffer = new byte[PageSize];
        uint pageId = 1;

        while (pageId != 0)
        {
            ReadPage(pageId, null, buffer);
            var header = SlottedPageHeader.ReadFrom(buffer);
            if (header.PageType != PageType.Collection)
                break;

            for (ushort i = 0; i < header.SlotCount; i++)
            {
                var slotOffset = SlottedPageHeader.Size + (i * SlotEntry.Size);
                var slot = SlotEntry.ReadFrom(buffer.AsSpan(slotOffset));

                if ((slot.Flags & SlotFlags.Deleted) != 0) continue;

                try
                {
                    using var ms = new MemoryStream(buffer, slot.Offset, slot.Length, false);
                    using var reader = new BinaryReader(ms);

                    var collName = reader.ReadString();
                    if (!string.Equals(collName, name, StringComparison.OrdinalIgnoreCase))
                        continue;

                    // Keep going: a crash while the chain was compacted can leave a second,
                    // identical copy further on (see CompactMetadataChain).
                    slot.Flags |= SlotFlags.Deleted;
                    slot.WriteTo(buffer.AsSpan(slotOffset));
                    WritePageImmediate(pageId, buffer);
                }
                catch { }
            }

            pageId = header.NextOverflowPage;
        }
        } // end try
        finally { _metadataLock.Release(); }
    }

    public IReadOnlyList<CollectionMetadata> GetAllCollectionsMetadata()
    {
        // Under the lock: a concurrent compaction can move records onto a page this walk has
        // already read, and unlike a single lookup the result would not show that they are missing.
        if (!_metadataLock.Wait(_config.LockTimeout.WriteTimeoutMs))
            throw new TimeoutException("Timed out acquiring metadata lock (GetAllCollectionsMetadata).");
        try
        {
        var result = new List<CollectionMetadata>();
        // One entry per collection: the first copy, as GetCollectionMetadata returns it (a crash
        // while the chain was compacted can leave a second one; see CompactMetadataChain).
        var seen = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var buffer = new byte[PageSize];
        uint pageId = 1;

        while (pageId != 0)
        {
            ReadPage(pageId, null, buffer);
            var header = SlottedPageHeader.ReadFrom(buffer);
            if (header.PageType != PageType.Collection)
                break;

            for (ushort i = 0; i < header.SlotCount; i++)
            {
                var slotOffset = SlottedPageHeader.Size + (i * SlotEntry.Size);
                var slot = SlotEntry.ReadFrom(buffer.AsSpan(slotOffset));

                if ((slot.Flags & SlotFlags.Deleted) != 0) continue;

                try
                {
                    using var ms = new MemoryStream(buffer, slot.Offset, slot.Length, false);
                    using var reader = new BinaryReader(ms);

                    var collName = reader.ReadString();
                    if (seen.Contains(collName)) continue;
                    result.Add(ReadCollectionMetadata(collName, reader));
                    seen.Add(collName);
                }
                catch
                {
                    // Ignora slot malformati
                }
            }

            pageId = header.NextOverflowPage;
        }

        return result;
        } // end try
        finally { _metadataLock.Release(); }
    }

    /// <summary>
    /// Registers all BSON keys used by a set of mappers into the global dictionary.
    /// </summary>
    public void RegisterMappers(IEnumerable<IDocumentMapper> mappers)
    {
        var allKeys = mappers.SelectMany(m => m.UsedKeys).Distinct();
        RegisterKeys(allKeys);
    }
}