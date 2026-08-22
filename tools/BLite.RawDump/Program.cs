using System;
using System.Buffers.Binary;
using System.Linq;
using System.Threading.Tasks;
using BLite.Bson;
using BLite.Core;

var dbPath = args.Length > 0 ? args[0] : throw new ArgumentException("usage: BLite.RawDump <db-path> [collection-name]");
var collectionName = args.Length > 1 ? args[1] : "PriceLists";

using var engine = new BLiteEngine(dbPath);
var collection = engine.GetOrCreateCollection(collectionName);

var docIndex = 0;
await foreach (var doc in collection.ScanAsync(_ => true))
{
    docIndex++;
    var raw = doc.RawData.ToArray();
    Console.WriteLine($"=== Doc #{docIndex}  size={doc.Size}  rawLen={raw.Length} ===");
    WalkDocument(raw, doc.GetReader());
}

Console.WriteLine($"Total documents scanned: {docIndex}");

static void WalkDocument(byte[] raw, BsonSpanReader reader)
{
    int docSize;
    try
    {
        docSize = reader.ReadDocumentSize();
    }
    catch (Exception ex)
    {
        Console.WriteLine($"  FAILED reading document size: {ex.Message}");
        DumpHex(raw, 0, 64);
        return;
    }

    // Matches the generated code exactly: computed from the position AFTER
    // ReadDocumentSize() (which skips the C-BSON v2 offset table header for root
    // documents), not the position before the call.
    var docEndPos = reader.Position + docSize - 4;

    while (reader.Position < docEndPos)
    {
        var fieldStartPos = reader.Position;
        BsonType bsonType;
        try
        {
            bsonType = reader.ReadBsonType();
        }
        catch (Exception ex)
        {
            Console.WriteLine($"  FAILED reading bson type at {fieldStartPos}: {ex.Message}");
            DumpHex(raw, Math.Max(0, fieldStartPos - 32), 96);
            return;
        }

        if (bsonType == BsonType.EndOfDocument) break;

        string elementName;
        try
        {
            elementName = reader.ReadElementHeader();
        }
        catch (Exception ex)
        {
            Console.WriteLine($"  FAILED reading element header at {fieldStartPos}: {ex.Message}");
            DumpHex(raw, Math.Max(0, fieldStartPos - 32), 96);
            return;
        }

        Console.WriteLine($"  [{fieldStartPos,5}] type={(int)bsonType,3} ({bsonType,-10}) name={elementName}");

        try
        {
            switch (bsonType)
            {
                case BsonType.Double:
                    Console.WriteLine($"          value={reader.ReadDouble()}");
                    break;
                case BsonType.String:
                    Console.WriteLine($"          value=\"{reader.ReadString()}\"");
                    break;
                case BsonType.Document:
                case BsonType.Array:
                    WalkNested(raw, ref reader, bsonType);
                    break;
                case BsonType.Binary:
                    var bin = reader.ReadBinary(out var subtype);
                    Console.WriteLine($"          binary subtype={subtype} len={bin.Length}");
                    break;
                case BsonType.ObjectId:
                    Console.WriteLine($"          value={reader.ReadObjectId()}");
                    break;
                case BsonType.Boolean:
                    Console.WriteLine($"          value={reader.ReadBoolean()}");
                    break;
                case BsonType.DateTime:
                    Console.WriteLine($"          value={reader.ReadDateTimeOffset()}");
                    break;
                case BsonType.Null:
                    Console.WriteLine("          value=null");
                    break;
                case BsonType.Int32:
                    Console.WriteLine($"          value={reader.ReadInt32()}");
                    break;
                case BsonType.Int64:
                case BsonType.Timestamp:
                    Console.WriteLine($"          value={reader.ReadInt64()}");
                    break;
                case BsonType.Decimal128:
                    Console.WriteLine($"          value={reader.ReadDecimal128()}");
                    break;
                default:
                    Console.WriteLine($"          UNKNOWN TYPE {(int)bsonType} - dumping and stopping");
                    DumpHex(raw, Math.Max(0, fieldStartPos - 32), 128);
                    return;
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"  FAILED reading value for '{elementName}' (type {bsonType}) at {fieldStartPos}: {ex.Message}");
            DumpHex(raw, Math.Max(0, fieldStartPos - 32), 96);
            return;
        }
    }
}

static void WalkNested(byte[] raw, ref BsonSpanReader reader, BsonType containerType)
{
    var arrOrDocStart = reader.Position;
    int size;
    try
    {
        size = reader.ReadDocumentSize();
    }
    catch (Exception ex)
    {
        Console.WriteLine($"          FAILED reading nested {containerType} size at {arrOrDocStart}: {ex.Message}");
        DumpHex(raw, Math.Max(0, arrOrDocStart - 32), 96);
        throw;
    }
    // Matches the generated code exactly: endPos is computed from the position AFTER
    // ReadDocumentSize() (which may have skipped a C-BSON v2 offset table header), not
    // the position before the call.
    var endPos = reader.Position + size - 4;

    if (containerType == BsonType.Array)
    {
        var itemIdx = 0;
        while (reader.Position < endPos)
        {
            var itemStart = reader.Position;
            var itemType = reader.ReadBsonType();
            if (itemType == BsonType.EndOfDocument) break;
            reader.SkipArrayKey();
            Console.WriteLine($"            [{itemStart,5}] array[{itemIdx}] type={(int)itemType} ({itemType})");
            if (itemType == BsonType.Document)
            {
                WalkNested(raw, ref reader, BsonType.Document);
            }
            else
            {
                // scalar array item - best-effort skip using the same switch as SkipValue
                reader.SkipValue(itemType);
            }
            itemIdx++;
        }
        reader.Seek(endPos);
    }
    else
    {
        while (reader.Position < endPos)
        {
            var fieldStartPos = reader.Position;
            var bsonType = reader.ReadBsonType();
            if (bsonType == BsonType.EndOfDocument) break;
            var elementName = reader.ReadElementHeader();
            Console.WriteLine($"            [{fieldStartPos,5}] type={(int)bsonType,3} ({bsonType,-10}) name={elementName}");
            reader.SkipValue(bsonType);
        }
        reader.Seek(endPos);
    }
}

static void DumpHex(byte[] raw, int start, int length)
{
    var end = Math.Min(raw.Length, start + length);
    for (var i = start; i < end; i += 16)
    {
        var lineEnd = Math.Min(end, i + 16);
        var hex = string.Join(' ', raw[i..lineEnd].Select(b => b.ToString("X2")));
        var ascii = string.Concat(raw[i..lineEnd].Select(b => b is >= 32 and < 127 ? (char)b : '.'));
        Console.WriteLine($"    {i,6}: {hex,-48} {ascii}");
    }
}
