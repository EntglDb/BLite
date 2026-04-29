using System.Collections.Concurrent;
using BLite.Bson;
using BLite.Core;
using BLite.Core.Storage;
using BLite.Core.Text;

namespace BLite.Tests;

/// <summary>
/// Mutation-coverage tests for StorageEngine.Memory (multi-file page encoding),
/// BLiteMigration edge cases, VectorSourceConfig, and TextNormalizer.
/// Targets NoCoverage mutants in these areas.
/// </summary>
public class StorageMigrationCoverageTests : IDisposable
{
    private readonly string _tempDir;

    public StorageMigrationCoverageTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"blite_sm_{Guid.NewGuid()}");
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        try { if (Directory.Exists(_tempDir)) Directory.Delete(_tempDir, recursive: true); }
        catch { }
    }

    private string DbPath(string name = "test") => Path.Combine(_tempDir, $"{name}.db");

    // ══════════════════════════════════════════════════════════════════════
    //  Multi-file (Server layout) tests
    // ══════════════════════════════════════════════════════════════════════

    [Fact]
    public void ServerConfig_CreatesCorrectPaths()
    {
        var dbPath = DbPath("server");
        var config = PageFileConfig.Server(dbPath);

        Assert.NotNull(config.WalPath);
        Assert.Contains("wal", config.WalPath);
        Assert.NotNull(config.IndexFilePath);
        Assert.EndsWith(".idx", config.IndexFilePath);
        Assert.NotNull(config.CollectionDataDirectory);
        Assert.Contains("collections", config.CollectionDataDirectory);
    }

    [Fact]
    public void ServerConfig_WithBaseConfigSmall_UsesSmallPageSize()
    {
        var dbPath = DbPath("small");
        var config = PageFileConfig.Server(dbPath, PageFileConfig.Small);
        Assert.Equal(8192, config.PageSize);
    }

    [Fact]
    public async Task MultiFile_CreateCollectionAndInsert_Works()
    {
        var dbPath = DbPath("multifile");
        var config = PageFileConfig.Server(dbPath);

        // Create subdirectories manually
        Directory.CreateDirectory(Path.GetDirectoryName(config.WalPath!)!);
        Directory.CreateDirectory(config.CollectionDataDirectory!);

        using var engine = new BLiteEngine(dbPath, config);
        var col = engine.GetOrCreateCollection("users");
        
        var doc = col.CreateDocument(["_id", "name", "age"], b =>
            b.AddString("name", "Alice").AddInt32("age", 30));
        await col.InsertAsync(doc);

        var found = (await col.FindAllAsync().ToListAsync());
        Assert.Single(found);
        found[0].TryGetString("name", out var name);
        Assert.Equal("Alice", name);
    }

    [Fact]
    public async Task MultiFile_MultipleCollections_IndependentFiles()
    {
        var dbPath = DbPath("multicol");
        var config = PageFileConfig.Server(dbPath);

        Directory.CreateDirectory(Path.GetDirectoryName(config.WalPath!)!);
        Directory.CreateDirectory(config.CollectionDataDirectory!);

        using (var engine = new BLiteEngine(dbPath, config))
        {
            var col1 = engine.GetOrCreateCollection("users");
            var col2 = engine.GetOrCreateCollection("orders");

            var doc1 = col1.CreateDocument(["_id", "name"], b => b.AddString("name", "Alice"));
            await col1.InsertAsync(doc1);

            var doc2 = col2.CreateDocument(["_id", "item"], b => b.AddString("item", "Widget"));
            await col2.InsertAsync(doc2);
            await engine.CommitAsync();
            Assert.Single((await col1.FindAllAsync().ToListAsync()));
            Assert.Single((await col2.FindAllAsync().ToListAsync()));
        }

        // Verify collection files exist
        Assert.True(File.Exists(Path.Combine(config.CollectionDataDirectory!, "users.db")));
        Assert.True(File.Exists(Path.Combine(config.CollectionDataDirectory!, "orders.db")));
    }

    [Fact]
    public async Task MultiFile_Reopen_DataPersisted()
    {
        var dbPath = DbPath("reopen");
        var config = PageFileConfig.Server(dbPath);

        Directory.CreateDirectory(Path.GetDirectoryName(config.WalPath!)!);
        Directory.CreateDirectory(config.CollectionDataDirectory!);

        using (var engine = new BLiteEngine(dbPath, config))
        {
            var col = engine.GetOrCreateCollection("items");
            var doc = col.CreateDocument(["_id", "val"], b => b.AddInt32("val", 42));
            await col.InsertAsync(doc);
            await engine.CommitAsync();
        }

        using (var engine = new BLiteEngine(dbPath, config))
        {
            var col = engine.GetOrCreateCollection("items");
            var docs = (await col.FindAllAsync().ToListAsync());
            Assert.Single(docs);
            docs[0].TryGetInt32("val", out var val);
            Assert.Equal(42, val);
        }
    }

    [Fact]
    public async Task MultiFile_DropCollection_RemovesFile()
    {
        var dbPath = DbPath("dropcol");
        var config = PageFileConfig.Server(dbPath);

        Directory.CreateDirectory(Path.GetDirectoryName(config.WalPath!)!);
        Directory.CreateDirectory(config.CollectionDataDirectory!);

        using var engine = new BLiteEngine(dbPath, config);
        var col = engine.GetOrCreateCollection("temp");
        var doc = col.CreateDocument(["_id", "x"], b => b.AddInt32("x", 1));
        await col.InsertAsync(doc);

        engine.DropCollection("temp");

        var colFile = Path.Combine(config.CollectionDataDirectory!, "temp.db");
        Assert.False(File.Exists(colFile));

        var col2 = engine.GetOrCreateCollection("temp");
        Assert.Empty((await col2.FindAllAsync().ToListAsync()));
    }

    [Fact]
    public async Task MultiFile_WithIndex_Works()
    {
        var dbPath = DbPath("withidx");
        var config = PageFileConfig.Server(dbPath);

        Directory.CreateDirectory(Path.GetDirectoryName(config.WalPath!)!);
        Directory.CreateDirectory(config.CollectionDataDirectory!);

        using var engine = new BLiteEngine(dbPath, config);
        var col = engine.GetOrCreateCollection("items");
        await col.CreateIndexAsync("name", "idx_name");

        for (int i = 0; i < 10; i++)
        {
            var doc = col.CreateDocument(["_id", "name", "value"], b =>
                b.AddString("name", $"item_{i:D2}").AddInt32("value", i));
            await col.InsertAsync(doc);
        }

        // Index file should be used
        var results = await col.QueryIndexAsync("idx_name", "item_05", "item_07").ToListAsync();
        Assert.Equal(3, results.Count);
    }

    // ══════════════════════════════════════════════════════════════════════
    //  BLiteMigration — edge cases
    // ══════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task Migration_ToMultiFile_SourceNotFound_Throws()
    {
        var config = PageFileConfig.Server(DbPath("nonexistent"));
        await Assert.ThrowsAsync<FileNotFoundException>(async () =>
            await BLiteMigration.ToMultiFileAsync(DbPath("nonexistent"), config));
    }

    [Fact]
    public async Task Migration_ToMultiFile_NoMultiFilePaths_Throws()
    {
        // Create a file first
        var dbPath = DbPath("nomulti");
        using (var engine = new BLiteEngine(dbPath))
        {
            var col = engine.GetOrCreateCollection("test");
            await col.InsertAsync(col.CreateDocument(["_id", "x"], b => b.AddInt32("x", 1)));
            await engine.CommitAsync();
        }

        var config = PageFileConfig.Default;
        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await BLiteMigration.ToMultiFileAsync(dbPath, config));
    }

    [Fact]
    public async Task Migration_ToSingleFile_SourceNotFound_Throws()
    {
        var sourceConfig = PageFileConfig.Server(DbPath("nonexistent"));
        await Assert.ThrowsAsync<FileNotFoundException>(async () =>
            await BLiteMigration.ToSingleFileAsync(DbPath("nonexistent"), sourceConfig, DbPath("target")));
    }

    [Fact]
    public async Task Migration_ToMultiFile_WithMultipleCollections_PreservesData()
    {
        var dbPath = DbPath("mig_multi");
        // Create single-file DB with data
        using (var engine = new BLiteEngine(dbPath))
        {
            var users = engine.GetOrCreateCollection("users");
            var orders = engine.GetOrCreateCollection("orders");

            for (int i = 0; i < 5; i++)
            {
                var doc = users.CreateDocument(["_id", "name"], b =>
                    b.AddString("name", $"User{i}"));
                await users.InsertAsync(doc);
            }

            for (int i = 0; i < 3; i++)
            {
                var doc = orders.CreateDocument(["_id", "item"], b =>
                    b.AddString("item", $"Order{i}"));
                await orders.InsertAsync(doc);
            }
            await engine.CommitAsync();
        }

        var targetConfig = PageFileConfig.Server(dbPath);
        Directory.CreateDirectory(Path.GetDirectoryName(targetConfig.WalPath!)!);
        Directory.CreateDirectory(targetConfig.CollectionDataDirectory!);

        await BLiteMigration.ToMultiFileAsync(dbPath, targetConfig);

        using (var engine = new BLiteEngine(dbPath, targetConfig))
        {
            var users = engine.GetOrCreateCollection("users");
            var orders = engine.GetOrCreateCollection("orders");
            Assert.Equal(5, (await users.FindAllAsync().ToListAsync()).Count);
            Assert.Equal(3, (await orders.FindAllAsync().ToListAsync()).Count);
        }
    }

    [Fact]
    public async Task Migration_ToMultiFile_WithKvStore_PreservesData()
    {
        var dbPath = DbPath("mig_kv");
        using (var engine = new BLiteEngine(dbPath))
        {
            engine.KvStore.Set("key1", "val1"u8.ToArray());
            engine.KvStore.Set("key2", "val2"u8.ToArray());
            await engine.CommitAsync();
        }

        var targetConfig = PageFileConfig.Server(dbPath);
        Directory.CreateDirectory(Path.GetDirectoryName(targetConfig.WalPath!)!);
        Directory.CreateDirectory(targetConfig.CollectionDataDirectory!);

        await BLiteMigration.ToMultiFileAsync(dbPath, targetConfig);

        using (var engine = new BLiteEngine(dbPath, targetConfig))
        {
            Assert.Equal("val1"u8.ToArray(), engine.KvStore.Get("key1"));
            Assert.Equal("val2"u8.ToArray(), engine.KvStore.Get("key2"));
        }
    }

    [Fact]
    public async Task Migration_ToSingleFile_Simple()
    {
        var dbPath = DbPath("mig_tosingle");
        var targetPath = DbPath("mig_tosingle_out");

        // Create multi-file DB
        var multiConfig = PageFileConfig.Server(dbPath);
        Directory.CreateDirectory(Path.GetDirectoryName(multiConfig.WalPath!)!);
        Directory.CreateDirectory(multiConfig.CollectionDataDirectory!);

        using (var engine = new BLiteEngine(dbPath, multiConfig))
        {
            var col = engine.GetOrCreateCollection("items");
            for (int i = 0; i < 10; i++)
            {
                var doc = col.CreateDocument(["_id", "v"], b => b.AddInt32("v", i));
                await col.InsertAsync(doc);
            }
            await engine.CommitAsync();
        }

        await BLiteMigration.ToSingleFileAsync(dbPath, multiConfig, targetPath);

        using (var engine = new BLiteEngine(targetPath))
        {
            var col = engine.GetOrCreateCollection("items");
            Assert.Equal(10, (await col.FindAllAsync().ToListAsync()).Count);
        }
    }

    [Fact]
    public async Task Migration_ToSingleFile_InPlace()
    {
        var dbPath = DbPath("mig_inplace");
        var multiConfig = PageFileConfig.Server(dbPath);
        Directory.CreateDirectory(Path.GetDirectoryName(multiConfig.WalPath!)!);
        Directory.CreateDirectory(multiConfig.CollectionDataDirectory!);

        using (var engine = new BLiteEngine(dbPath, multiConfig))
        {
            var col = engine.GetOrCreateCollection("data");
            var doc = col.CreateDocument(["_id", "x"], b => b.AddInt32("x", 99));
            await col.InsertAsync(doc);
            await engine.CommitAsync();
        }

        await BLiteMigration.ToSingleFileAsync(dbPath, multiConfig, dbPath);
        // After in-place migration, multi-file components should be cleaned up
        using (var engine = new BLiteEngine(dbPath))
        {
            var col = engine.GetOrCreateCollection("data");
            var docs = (await col.FindAllAsync().ToListAsync());
            Assert.Single(docs);
            docs[0].TryGetInt32("x", out var x);
            Assert.Equal(99, x);
        }
    }

    [Fact]
    public async Task Migration_ToMultiFile_WithSecondaryIndex_RecreatesIndex()
    {
        var dbPath = DbPath("mig_idx");
        using (var engine = new BLiteEngine(dbPath))
        {
            var col = engine.GetOrCreateCollection("items");
            await col.CreateIndexAsync("name", "idx_name");
            for (int i = 0; i < 5; i++)
            {
                var doc = col.CreateDocument(["_id", "name"], b =>
                    b.AddString("name", $"Item{i}"));
                await col.InsertAsync(doc);
            }
            await engine.CommitAsync();
        }

        var targetConfig = PageFileConfig.Server(dbPath);
        Directory.CreateDirectory(Path.GetDirectoryName(targetConfig.WalPath!)!);
        Directory.CreateDirectory(targetConfig.CollectionDataDirectory!);

        await BLiteMigration.ToMultiFileAsync(dbPath, targetConfig);

        using (var engine = new BLiteEngine(dbPath, targetConfig))
        {
            var col = engine.GetOrCreateCollection("items");
            var indexes = col.ListIndexes();
            // Should have recreated the secondary index
            Assert.Contains("idx_name", indexes);
        }
    }

    [Fact]
    public async Task Migration_RoundTrip_SingleToMultiToSingle_PreservesData()
    {
        var dbPath = DbPath("mig_roundtrip");
        var finalPath = DbPath("mig_roundtrip_final");

        // Create single-file DB
        using (var engine = new BLiteEngine(dbPath))
        {
            var col = engine.GetOrCreateCollection("items");
            for (int i = 0; i < 20; i++)
            {
                var doc = col.CreateDocument(["_id", "name", "val"], b =>
                    b.AddString("name", $"Item{i}").AddInt32("val", i));
                await col.InsertAsync(doc);
            }
            engine.KvStore.Set("meta", "test"u8.ToArray());
            await engine.CommitAsync();
        }

        // Single → Multi
        var multiConfig = PageFileConfig.Server(dbPath);
        Directory.CreateDirectory(Path.GetDirectoryName(multiConfig.WalPath!)!);
        Directory.CreateDirectory(multiConfig.CollectionDataDirectory!);
        await BLiteMigration.ToMultiFileAsync(dbPath, multiConfig);

        // Multi → Single
        await BLiteMigration.ToSingleFileAsync(dbPath, multiConfig, finalPath);

        using (var engine = new BLiteEngine(finalPath))
        {
            var col = engine.GetOrCreateCollection("items");
            Assert.Equal(20, (await col.FindAllAsync().ToListAsync()).Count);
            Assert.Equal("test"u8.ToArray(), engine.KvStore.Get("meta"));
        }
    }

    // ══════════════════════════════════════════════════════════════════════
    //  VectorSourceConfig — BuildText coverage
    // ══════════════════════════════════════════════════════════════════════

    [Fact]
    public void VectorSourceConfig_BuildText_EmptyFields_ReturnsEmpty()
    {
        var config = new VectorSourceConfig();
        var doc = BuildSimpleDoc("title", "test");
        Assert.Equal(string.Empty, config.BuildText(doc));
    }

    [Fact]
    public void VectorSourceConfig_BuildText_NullDoc_ReturnsEmpty()
    {
        var config = new VectorSourceConfig();
        config.Fields.Add(new VectorSourceField { Path = "title" });
        Assert.Equal(string.Empty, config.BuildText(null!));
    }

    [Fact]
    public void VectorSourceConfig_BuildText_SingleField_ReturnsValue()
    {
        var config = new VectorSourceConfig();
        config.Fields.Add(new VectorSourceField { Path = "title" });
        var doc = BuildSimpleDoc("title", "Hello World");
        var text = config.BuildText(doc);
        Assert.Equal("Hello World", text);
    }

    [Fact]
    public void VectorSourceConfig_BuildText_MultipleFields_JoinedBySeparator()
    {
        var config = new VectorSourceConfig { Separator = " | " };
        config.Fields.Add(new VectorSourceField { Path = "title" });
        config.Fields.Add(new VectorSourceField { Path = "body" });

        var doc = BuildMultiFieldDoc(("title", "Hello"), ("body", "World"));
        var text = config.BuildText(doc);
        Assert.Equal("Hello | World", text);
    }

    [Fact]
    public void VectorSourceConfig_BuildText_WithPrefixSuffix()
    {
        var config = new VectorSourceConfig();
        config.Fields.Add(new VectorSourceField
        {
            Path = "title",
            Prefix = "[TITLE] ",
            Suffix = " [/TITLE]"
        });

        var doc = BuildSimpleDoc("title", "Test");
        var text = config.BuildText(doc);
        Assert.Equal("[TITLE] Test [/TITLE]", text);
    }

    [Fact]
    public void VectorSourceConfig_BuildText_MissingField_Skipped()
    {
        var config = new VectorSourceConfig();
        config.Fields.Add(new VectorSourceField { Path = "missing" });
        config.Fields.Add(new VectorSourceField { Path = "title" });

        var doc = BuildSimpleDoc("title", "Found");
        var text = config.BuildText(doc);
        Assert.Equal("Found", text);
    }

    [Fact]
    public void VectorSourceConfig_BuildText_IntField()
    {
        var config = new VectorSourceConfig();
        config.Fields.Add(new VectorSourceField { Path = "count" });

        var doc = BuildSimpleDoc("count", 42);
        var text = config.BuildText(doc);
        Assert.Equal("42", text);
    }

    [Fact]
    public void VectorSourceConfig_BuildText_Int64Field()
    {
        var config = new VectorSourceConfig();
        config.Fields.Add(new VectorSourceField { Path = "big" });

        var doc = BuildSimpleDoc("big", 1234567890123L);
        var text = config.BuildText(doc);
        Assert.Equal("1234567890123", text);
    }

    [Fact]
    public void VectorSourceConfig_BuildText_DoubleField()
    {
        var config = new VectorSourceConfig();
        config.Fields.Add(new VectorSourceField { Path = "val" });

        var doc = BuildSimpleDoc("val", 3.14);
        var text = config.BuildText(doc);
        // Locale-independent: "G" format may use ',' in some locales
        Assert.Contains(3.14.ToString("G"), text);
    }

    [Fact]
    public void VectorSourceConfig_BuildText_BoolField()
    {
        var config = new VectorSourceConfig();
        config.Fields.Add(new VectorSourceField { Path = "active" });

        var doc = BuildSimpleDoc("active", true);
        Assert.Equal("true", config.BuildText(doc));

        var docFalse = BuildSimpleDoc("active", false);
        Assert.Equal("false", config.BuildText(docFalse));
    }

    [Fact]
    public void VectorSourceConfig_BuildText_DateTimeField()
    {
        var config = new VectorSourceConfig();
        config.Fields.Add(new VectorSourceField { Path = "date" });

        var dt = new DateTime(2024, 6, 15, 12, 0, 0, DateTimeKind.Utc);
        var doc = BuildSimpleDoc("date", dt);
        var text = config.BuildText(doc);
        Assert.Contains("2024", text);
    }

    [Fact]
    public void VectorSourceConfig_BuildText_ArrayField()
    {
        var config = new VectorSourceConfig();
        config.Fields.Add(new VectorSourceField { Path = "tags" });

        var doc = BsonDocument.Create(_keyMap, _reverseKeyMap, b =>
            b.Add("tags", BsonValue.FromArray(new List<BsonValue>
            {
                BsonValue.FromString("c#"),
                BsonValue.FromString("go")
            })));
        var text = config.BuildText(doc);
        Assert.Contains("c#", text);
        Assert.Contains("go", text);
    }

    // ══════════════════════════════════════════════════════════════════════
    //  TextNormalizer — comprehensive coverage
    // ══════════════════════════════════════════════════════════════════════

    [Fact]
    public void TextNormalizer_Null_ReturnsEmpty()
    {
        Assert.Equal(string.Empty, TextNormalizer.Normalize(null));
    }

    [Fact]
    public void TextNormalizer_Empty_ReturnsEmpty()
    {
        Assert.Equal(string.Empty, TextNormalizer.Normalize(""));
    }

    [Fact]
    public void TextNormalizer_Whitespace_ReturnsEmpty()
    {
        Assert.Equal(string.Empty, TextNormalizer.Normalize("   "));
    }

    [Fact]
    public void TextNormalizer_Simple_LowerCases()
    {
        Assert.Equal("hello world", TextNormalizer.Normalize("Hello World"));
    }

    [Fact]
    public void TextNormalizer_Accents_Removed()
    {
        Assert.Equal("cafe resume naif", TextNormalizer.Normalize("Café Résumé Naïf"));
    }

    [Fact]
    public void TextNormalizer_Punctuation_Becomes_Spaces()
    {
        Assert.Equal("hello world", TextNormalizer.Normalize("hello, world!"));
    }

    [Fact]
    public void TextNormalizer_MultipleSpaces_Collapsed()
    {
        Assert.Equal("a b c", TextNormalizer.Normalize("a    b    c"));
    }

    [Fact]
    public void TextNormalizer_Digits_Preserved()
    {
        Assert.Equal("item 42 test", TextNormalizer.Normalize("item 42 test"));
    }

    [Fact]
    public void TextNormalizer_SpecialChars_Removed()
    {
        Assert.Equal("hello world", TextNormalizer.Normalize("hello@world#"));
    }

    [Fact]
    public void TextNormalizer_German_Umlauts()
    {
        // ß is not a combining diacritical — NFD decomposition keeps it. ö decomposes to o+combining.
        Assert.Equal("uber großen", TextNormalizer.Normalize("Über Größen"));
    }

    [Fact]
    public void TextNormalizer_Spanish_Tilde()
    {
        Assert.Equal("espana nino", TextNormalizer.Normalize("España Niño"));
    }

    [Fact]
    public void TextNormalizer_BuildEmbeddingText_NullDoc_ReturnsEmpty()
    {
        var config = new VectorSourceConfig();
        config.Fields.Add(new VectorSourceField { Path = "title" });
        Assert.Equal(string.Empty, TextNormalizer.BuildEmbeddingText(null!, config));
    }

    [Fact]
    public void TextNormalizer_BuildEmbeddingText_NullConfig_ReturnsEmpty()
    {
        var doc = BuildSimpleDoc("title", "test");
        Assert.Equal(string.Empty, TextNormalizer.BuildEmbeddingText(doc, null!));
    }

    [Fact]
    public void TextNormalizer_BuildEmbeddingText_EmptyFields_ReturnsEmpty()
    {
        var config = new VectorSourceConfig();
        var doc = BuildSimpleDoc("title", "test");
        Assert.Equal(string.Empty, TextNormalizer.BuildEmbeddingText(doc, config));
    }

    [Fact]
    public void TextNormalizer_BuildEmbeddingText_SingleField()
    {
        var config = new VectorSourceConfig();
        config.Fields.Add(new VectorSourceField { Path = "title" });
        var doc = BuildSimpleDoc("title", "Hello World!");
        var result = TextNormalizer.BuildEmbeddingText(doc, config);
        Assert.Equal("title:hello world", result);
    }

    [Fact]
    public void TextNormalizer_BuildEmbeddingText_MultipleFields()
    {
        var config = new VectorSourceConfig();
        config.Fields.Add(new VectorSourceField { Path = "title" });
        config.Fields.Add(new VectorSourceField { Path = "body" });
        var doc = BuildMultiFieldDoc(("title", "Hello"), ("body", "World"));
        var result = TextNormalizer.BuildEmbeddingText(doc, config);
        Assert.Equal("title:hello,body:world", result);
    }

    [Fact]
    public void TextNormalizer_BuildEmbeddingText_MissingField_Skipped()
    {
        var config = new VectorSourceConfig();
        config.Fields.Add(new VectorSourceField { Path = "missing" });
        config.Fields.Add(new VectorSourceField { Path = "title" });
        var doc = BuildSimpleDoc("title", "Test");
        var result = TextNormalizer.BuildEmbeddingText(doc, config);
        Assert.Equal("title:test", result);
    }

    [Fact]
    public void TextNormalizer_BuildEmbeddingText_IntField()
    {
        var config = new VectorSourceConfig();
        config.Fields.Add(new VectorSourceField { Path = "count" });
        var doc = BuildSimpleDoc("count", 42);
        var result = TextNormalizer.BuildEmbeddingText(doc, config);
        Assert.Equal("count:42", result);
    }

    [Fact]
    public void TextNormalizer_BuildEmbeddingText_BoolField()
    {
        var config = new VectorSourceConfig();
        config.Fields.Add(new VectorSourceField { Path = "active" });
        var doc = BuildSimpleDoc("active", true);
        Assert.Equal("active:true", TextNormalizer.BuildEmbeddingText(doc, config));
    }

    [Fact]
    public void TextNormalizer_BuildEmbeddingText_WhitespaceValue_Skipped()
    {
        var config = new VectorSourceConfig();
        config.Fields.Add(new VectorSourceField { Path = "title" });
        var doc = BuildSimpleDoc("title", "   ");
        var result = TextNormalizer.BuildEmbeddingText(doc, config);
        Assert.Equal(string.Empty, result);
    }

    // ══════════════════════════════════════════════════════════════════════
    //  ValidateCollectionName — covered via multi-file
    // ══════════════════════════════════════════════════════════════════════

    [Fact]
    public void MultiFile_InvalidCollectionName_Whitespace_Throws()
    {
        var dbPath = DbPath("validate2");
        using var eng = new BLiteEngine(dbPath);
        Assert.Throws<ArgumentNullException>(() => eng.GetOrCreateCollection(""));
    }

    // ══════════════════════════════════════════════════════════════════════
    //  PageFileConfig — DetectFromFile, presets
    // ══════════════════════════════════════════════════════════════════════

    [Fact]
    public void DetectFromFile_NonExistent_ReturnsNull()
    {
        var result = PageFileConfig.DetectFromFile(Path.Combine(_tempDir, "nonexistent.db"));
        Assert.Null(result);
    }

    [Fact]
    public void DetectFromFile_EmptyFile_ReturnsNull()
    {
        var emptyFile = Path.Combine(_tempDir, "empty.db");
        File.WriteAllBytes(emptyFile, Array.Empty<byte>());
        Assert.Null(PageFileConfig.DetectFromFile(emptyFile));
    }

    [Fact]
    public void DetectFromFile_TooSmall_ReturnsNull()
    {
        var smallFile = Path.Combine(_tempDir, "small.db");
        File.WriteAllBytes(smallFile, new byte[10]);
        Assert.Null(PageFileConfig.DetectFromFile(smallFile));
    }

    [Fact]
    public async Task DetectFromFile_ValidDb_ReturnsConfig()
    {
        var dbPath = DbPath("detect");
        using (var engine = new BLiteEngine(dbPath))
        {
            var col = engine.GetOrCreateCollection("test");
            await col.InsertAsync(col.CreateDocument(["_id", "x"], b => b.AddInt32("x", 1)));
        }

        var config = PageFileConfig.DetectFromFile(dbPath);
        Assert.NotNull(config);
        Assert.Equal(16384, config!.Value.PageSize); // Default page size
    }

    [Fact]
    public void DefaultConfig_HasExpectedValues()
    {
        var config = PageFileConfig.Default;
        Assert.Equal(16384, config.PageSize);
        Assert.True(config.GrowthBlockSize > 0);
    }

    [Fact]
    public void SmallConfig_HasExpectedValues()
    {
        var config = PageFileConfig.Small;
        Assert.Equal(8192, config.PageSize);
    }

    [Fact]
    public void LargeConfig_HasExpectedValues()
    {
        var config = PageFileConfig.Large;
        Assert.Equal(32768, config.PageSize);
    }

    // ══════════════════════════════════════════════════════════════════════
    //  BLiteEngine — basic lifecycle and collection management
    // ══════════════════════════════════════════════════════════════════════

    [Fact]
    public void Engine_NullPath_Throws()
    {
        Assert.Throws<ArgumentNullException>(() => new BLiteEngine(null!));
    }

    [Fact]
    public void Engine_EmptyPath_Throws()
    {
        Assert.Throws<ArgumentNullException>(() => new BLiteEngine(""));
    }

    [Fact]
    public void Engine_WhitespacePath_Throws()
    {
        Assert.Throws<ArgumentNullException>(() => new BLiteEngine("   "));
    }

    [Fact]
    public void Engine_GetCollection_NotFound_ReturnsNull()
    {
        var dbPath = DbPath("getcol");
        using var engine = new BLiteEngine(dbPath);
        Assert.Null(engine.GetCollection("nonexistent"));
    }

    [Fact]
    public void Engine_GetOrCreateCollection_EmptyName_Throws()
    {
        var dbPath = DbPath("emptyname");
        using var engine = new BLiteEngine(dbPath);
        Assert.Throws<ArgumentNullException>(() => engine.GetOrCreateCollection(""));
    }

    [Fact]
    public void Engine_DropCollection_NotFound_ReturnsFalse()
    {
        var dbPath = DbPath("dropnone");
        using var engine = new BLiteEngine(dbPath);
        Assert.False(engine.DropCollection("nonexistent"));
    }

    [Fact]
    public void Engine_DropCollection_Existing_ReturnsTrue()
    {
        var dbPath = DbPath("dropexist");
        using var engine = new BLiteEngine(dbPath);
        engine.GetOrCreateCollection("temp");
        Assert.True(engine.DropCollection("temp"));
    }

    [Fact]
    public void Engine_ListCollections_IncludesAll()
    {
        var dbPath = DbPath("listcol");
        using var engine = new BLiteEngine(dbPath);
        engine.GetOrCreateCollection("alpha");
        engine.GetOrCreateCollection("beta");
        var names = engine.ListCollections();
        Assert.Contains("alpha", names, StringComparer.OrdinalIgnoreCase);
        Assert.Contains("beta", names, StringComparer.OrdinalIgnoreCase);
    }

    [Fact]
    public void Engine_Dispose_ThenGetOrCreate_Throws()
    {
        var dbPath = DbPath("disposed");
        var engine = new BLiteEngine(dbPath);
        engine.Dispose();
        Assert.Throws<ObjectDisposedException>(() => engine.GetOrCreateCollection("test"));
    }

    [Fact]
    public void Engine_KvStore_BasicRoundTrip()
    {
        var dbPath = DbPath("kvbasic");
        using var engine = new BLiteEngine(dbPath);
        engine.KvStore.Set("greeting", "hello"u8.ToArray());
        Assert.Equal("hello"u8.ToArray(), engine.KvStore.Get("greeting"));
    }

    [Fact]
    public async Task Engine_WithConfig_CreatesDb()
    {
        var dbPath = DbPath("withconfig");
        using var engine = new BLiteEngine(dbPath, PageFileConfig.Default);
        var col = engine.GetOrCreateCollection("test");
        await col.InsertAsync(col.CreateDocument(["_id", "x"], b => b.AddInt32("x", 1)));
        Assert.Single((await col.FindAllAsync().ToListAsync()));
    }

    // ══════════════════════════════════════════════════════════════════════
    // Helper methods for building BsonDocuments
    // ══════════════════════════════════════════════════════════════════════

    private static readonly ConcurrentDictionary<string, ushort> _keyMap = BuildKeyMap();
    private static readonly ConcurrentDictionary<ushort, string> _reverseKeyMap = BuildReverseKeyMap();

    private static ConcurrentDictionary<string, ushort> BuildKeyMap()
    {
        var map = new ConcurrentDictionary<string, ushort>(StringComparer.OrdinalIgnoreCase);
        ushort id = 1;
        foreach (var key in new[] { "title", "body", "tags", "count", "active", "date",
            "missing", "big", "val", "0", "1" })
        {
            map[key] = id++;
        }
        return map;
    }

    private static ConcurrentDictionary<ushort, string> BuildReverseKeyMap()
    {
        var map = new ConcurrentDictionary<ushort, string>();
        foreach (var kvp in _keyMap)
            map[kvp.Value] = kvp.Key;
        return map;
    }

    private static BsonDocument BuildSimpleDoc(string field, string value) =>
        BsonDocument.Create(_keyMap, _reverseKeyMap, b => b.AddString(field, value));

    private static BsonDocument BuildSimpleDoc(string field, int value) =>
        BsonDocument.Create(_keyMap, _reverseKeyMap, b => b.AddInt32(field, value));

    private static BsonDocument BuildSimpleDoc(string field, long value) =>
        BsonDocument.Create(_keyMap, _reverseKeyMap, b => b.AddInt64(field, value));

    private static BsonDocument BuildSimpleDoc(string field, double value) =>
        BsonDocument.Create(_keyMap, _reverseKeyMap, b => b.AddDouble(field, value));

    private static BsonDocument BuildSimpleDoc(string field, bool value) =>
        BsonDocument.Create(_keyMap, _reverseKeyMap, b => b.AddBoolean(field, value));

    private static BsonDocument BuildSimpleDoc(string field, DateTime value) =>
        BsonDocument.Create(_keyMap, _reverseKeyMap, b => b.AddDateTime(field, value));

    private static BsonDocument BuildMultiFieldDoc(params (string key, string value)[] fields) =>
        BsonDocument.Create(_keyMap, _reverseKeyMap, b =>
        {
            foreach (var (k, v) in fields)
                b.AddString(k, v);
        });
}
