// BLite — byte[] / BSON Binary round-trip tests via DocumentDbContext typed path

using BLite.Bson;
using BLite.Core;
using BLite.Shared;
using System;
using System.Collections.Concurrent;
using System.IO;
using System.Text.Json;
using System.Threading.Tasks;
using Xunit;

namespace BLite.Tests;

public class BinaryPropertyTests : IDisposable
{
    private readonly string _dbPath;
    private readonly TestDbContext _db;

    public BinaryPropertyTests()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"test_binary_{Guid.NewGuid()}.db");
        _db = new TestDbContext(_dbPath);
    }

    public void Dispose()
    {
        _db.Dispose();
        DeleteFileIfExists(_dbPath);
        DeleteFileIfExists(Path.ChangeExtension(_dbPath, ".wal"));
    }

    private static void DeleteFileIfExists(string path)
    {
        if (!string.IsNullOrEmpty(path) && File.Exists(path))
        {
            File.Delete(path);
        }
    }

    [Fact]
    public async Task BinaryProperty_NonNull_RoundTrips()
    {
        var entity = new BinaryEntity
        {
            Label = "payload",
            Data = new byte[] { 0x01, 0x02, 0x03, 0xAA, 0xFF }
        };

        var id = await _db.BinaryEntities.InsertAsync(entity);
        await _db.SaveChangesAsync();

        var result = await _db.BinaryEntities.FindByIdAsync(id);

        Assert.NotNull(result);
        Assert.Equal(entity.Label, result!.Label);
        Assert.Equal(entity.Data, result.Data);
    }

    [Fact]
    public async Task BinaryProperty_EmptyArray_RoundTrips()
    {
        var entity = new BinaryEntity
        {
            Label = "empty",
            Data = Array.Empty<byte>()
        };

        var id = await _db.BinaryEntities.InsertAsync(entity);
        await _db.SaveChangesAsync();

        var result = await _db.BinaryEntities.FindByIdAsync(id);

        Assert.NotNull(result);
        Assert.Empty(result!.Data);
    }

    [Fact]
    public async Task BinaryProperty_NullableNull_RoundTrips()
    {
        var entity = new BinaryEntity
        {
            Label = "no-optional",
            Data = new byte[] { 1, 2 },
            OptionalData = null
        };

        var id = await _db.BinaryEntities.InsertAsync(entity);
        await _db.SaveChangesAsync();

        var result = await _db.BinaryEntities.FindByIdAsync(id);

        Assert.NotNull(result);
        Assert.Null(result!.OptionalData);
    }

    [Fact]
    public async Task BinaryProperty_NullableNonNull_RoundTrips()
    {
        var entity = new BinaryEntity
        {
            Label = "with-optional",
            Data = new byte[] { 10, 20 },
            OptionalData = new byte[] { 0xDE, 0xAD, 0xBE, 0xEF }
        };

        var id = await _db.BinaryEntities.InsertAsync(entity);
        await _db.SaveChangesAsync();

        var result = await _db.BinaryEntities.FindByIdAsync(id);

        Assert.NotNull(result);
        Assert.Equal(entity.OptionalData, result!.OptionalData);
    }

    [Fact]
    public async Task BinaryProperty_Update_RoundTrips()
    {
        var entity = new BinaryEntity
        {
            Label = "original",
            Data = new byte[] { 1, 2, 3 }
        };

        var id = await _db.BinaryEntities.InsertAsync(entity);
        await _db.SaveChangesAsync();

        entity.Data = new byte[] { 9, 8, 7, 6 };
        entity.Label = "updated";
        Assert.True(await _db.BinaryEntities.UpdateAsync(entity));

        var result = await _db.BinaryEntities.FindByIdAsync(id);

        Assert.NotNull(result);
        Assert.Equal(new byte[] { 9, 8, 7, 6 }, result!.Data);
        Assert.Equal("updated", result.Label);
    }

    [Fact]
    public async Task BinaryProperty_PersistsAcrossReopen()
    {
        var path = Path.Combine(Path.GetTempPath(), $"test_binary_reopen_{Guid.NewGuid()}.db");
        var expected = new byte[] { 0x11, 0x22, 0x33, 0x44 };
        ObjectId savedId;

        using (var db = new TestDbContext(path))
        {
            var entity = new BinaryEntity { Label = "persist", Data = expected };
            savedId = await db.BinaryEntities.InsertAsync(entity);
            await db.SaveChangesAsync();
            db.ForceCheckpoint();
        }

        using (var db = new TestDbContext(path))
        {
            var result = await db.BinaryEntities.FindByIdAsync(savedId);
            Assert.NotNull(result);
            Assert.Equal(expected, result!.Data);
        }

        DeleteFileIfExists(path);
        DeleteFileIfExists(Path.ChangeExtension(path, ".wal"));
    }

    [Fact]
    public async Task BinaryProperty_LargePayload_RoundTrips()
    {
        var data = new byte[4096];
        for (var i = 0; i < data.Length; i++)
            data[i] = (byte)(i % 256);

        var entity = new BinaryEntity { Label = "large", Data = data };
        var id = await _db.BinaryEntities.InsertAsync(entity);
        await _db.SaveChangesAsync();

        var result = await _db.BinaryEntities.FindByIdAsync(id);

        Assert.NotNull(result);
        Assert.Equal(data, result!.Data);
    }

    // ── BLiteEngine (dynamic/BSON) cross-path ────────────────────────────────

    /// <summary>
    /// Write a binary field via BLiteEngine (BsonValue.FromBinary),
    /// then read it back via the typed DocumentDbContext mapper.
    /// Verifies that the generated deserializer correctly converts BsonType.Binary → byte[].
    /// </summary>
    [Fact]
    public async Task BinaryProperty_BLiteEngine_WritesBsonBinary_TypedReaderGetsBytes()
    {
        var data = new byte[] { 0xCA, 0xFE, 0xBA, 0xBE };
        var label = "engine-to-typed";
        var path = Path.Combine(Path.GetTempPath(), $"test_binary_engine_{Guid.NewGuid()}.db");
        ObjectId savedId;

        // Write via BLiteEngine — stores the field as BsonType.Binary
        using (var engine = new BLiteEngine(path))
        {
            engine.RegisterKeys(["label", "data", "optionaldata"]);
            var bsonId = await engine.InsertAsync("binary_entities",
                engine.CreateDocument(["label", "data", "optionaldata"], b =>
                {
                    b.AddString("label", label);
                    b.Add("data", BsonValue.FromBinary(data));
                    b.Add("optionaldata", BsonValue.Null);
                }));
            savedId = bsonId.AsObjectId();
        }

        // Read back via typed DocumentDbContext mapper
        using (var db = new TestDbContext(path))
        {
            var result = await db.BinaryEntities.FindByIdAsync(savedId);

            Assert.NotNull(result);
            Assert.Equal(label, result!.Label);
            Assert.Equal(data, result.Data);
            Assert.Null(result.OptionalData);
        }

        DeleteFileIfExists(path);
        DeleteFileIfExists(Path.ChangeExtension(path, ".wal"));
    }

    // ── JSON serialization ────────────────────────────────────────────────────

    /// <summary>
    /// BsonDocument → ToJson encodes Binary as base64 string.
    /// FromJson parses that string back as BsonType.String (not Binary) because
    /// JSON has no native binary type — this is a known limitation.
    /// The raw bytes are still recoverable via Convert.FromBase64String.
    /// </summary>
    [Fact]
    public void BinaryProperty_Json_Binary_SerializesAsBase64_TypeNotPreserved()
    {
        var data = new byte[] { 0x01, 0x02, 0x03, 0xAA };

        // ── Build an in-memory BsonDocument containing a Binary field ─────────
        var keyMap = new ConcurrentDictionary<string, ushort>();
        keyMap["_id"]  = 0;
        keyMap["data"] = 1;
        var reverseMap = new ConcurrentDictionary<ushort, string>();
        reverseMap[0] = "_id";
        reverseMap[1] = "data";

        var doc = BsonDocument.Create(keyMap, reverseMap, b =>
        {
            b.AddId(new BsonId(ObjectId.NewObjectId()));
            b.Add("data", BsonValue.FromBinary(data));
        });

        // Verify the field is BsonType.Binary before serialization
        Assert.True(doc.TryGetValue("data", out var beforeJson));
        Assert.True(beforeJson.IsBinary, "Field must be BsonType.Binary before serialization");

        // ── BSON → JSON ──────────────────────────────────────────────────────
        var json = BsonJsonConverter.ToJson(doc, indented: false);

        // The JSON must contain the base64 encoding of the data bytes
        var expectedBase64 = Convert.ToBase64String(data);
        Assert.Contains(expectedBase64, json);
        Assert.True(IsValidJson(json), "ToJson must produce valid JSON");

        // ── JSON → BSON ──────────────────────────────────────────────────────
        var roundTripped = BsonJsonConverter.FromJson(json, keyMap, reverseMap);

        Assert.True(roundTripped.TryGetValue("data", out var reparsed));

        // Known limitation: JSON has no binary type. The base64 string comes back
        // as BsonType.String, not BsonType.Binary.
        Assert.True(reparsed.IsString,
            "After JSON round-trip the field type is String (base64), not Binary");
        Assert.False(reparsed.IsBinary,
            "BsonType.Binary is NOT preserved through JSON serialization");

        // The data bytes are still recoverable by decoding the base64 string
        var recovered = Convert.FromBase64String(reparsed.AsString!);
        Assert.Equal(data, recovered);
    }

    private static bool IsValidJson(string json)
    {
        try
        {
            using var _ = JsonDocument.Parse(json);
            return true;
        }
        catch { return false; }
    }
}
