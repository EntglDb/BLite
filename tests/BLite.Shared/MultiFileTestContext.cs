using BLite.Core;
using BLite.Core.Collections;
using BLite.Core.Encryption;
using BLite.Core.KeyValue;
using BLite.Core.Metadata;
using BLite.Core.Storage;
using System.Threading;
using System.Threading.Tasks;

namespace BLite.Tests;

/// <summary>
/// Entity used in multi-file overflow and end-to-end tests.
/// </summary>
public class MultiFileEntry
{
    public int Id { get; set; }
    public string Payload { get; set; } = "";
    public string Tag { get; set; } = "";
}

/// <summary>
/// Minimal DocumentDbContext for multi-file end-to-end tests.
/// Supports page-overflow documents via DocumentCollection.
/// </summary>
public partial class MultiFileTestDbContext : DocumentDbContext
{
    public DocumentCollection<int, MultiFileEntry> Entries { get; set; } = null!;

    public MultiFileTestDbContext(string databasePath, PageFileConfig config)
        : base(databasePath, config)
    {
        InitializeCollections();
    }

    /// <summary>
    /// Opens a context using unified engine options (sync, supports passphrase and KeyProvider).
    /// </summary>
    public MultiFileTestDbContext(BLiteEngineOptions options)
        : base(options)
    {
    }

    /// <summary>
    /// Private constructor used by the async factory <see cref="CreateAsync"/>.
    /// </summary>
    private MultiFileTestDbContext(StorageEngine storage, EncryptionCoordinator? coordinator, BLiteKvOptions? kvOptions)
        : base(storage, coordinator, kvOptions)
    {
    }

    /// <summary>
    /// Asynchronously creates a context using unified engine options, properly awaiting
    /// <see cref="IKeyProvider.GetKeyAsync"/> when a key provider is configured.
    /// </summary>
    public static async Task<MultiFileTestDbContext> CreateAsync(
        BLiteEngineOptions options, CancellationToken ct = default)
    {
        var (storage, coordinator) = await BuildStorageFromOptionsAsync(options, ct).ConfigureAwait(false);
        return new MultiFileTestDbContext(storage, coordinator, options.KvOptions);
    }

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<MultiFileEntry>()
            .ToCollection("entries")
            .HasKey(e => e.Id);
    }
}
