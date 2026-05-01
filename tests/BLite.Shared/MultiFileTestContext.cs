using BLite.Core;
using BLite.Core.Collections;
using BLite.Core.Encryption;
using BLite.Core.Metadata;
using BLite.Core.Storage;

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
    /// Exercises the encryption-aware constructor
    /// <see cref="DocumentDbContext(string, CryptoOptions, BLite.Core.KeyValue.BLiteKvOptions?)"/>.
    /// </summary>
    public MultiFileTestDbContext(string databasePath, CryptoOptions crypto)
        : base(databasePath, crypto)
    {
        InitializeCollections();
    }

    /// <summary>
    /// Exercises the coordinator-based encryption constructor
    /// <see cref="DocumentDbContext(string, EncryptionCoordinator, PageFileConfig?, BLite.Core.KeyValue.BLiteKvOptions?)"/>.
    /// </summary>
    internal MultiFileTestDbContext(string databasePath, EncryptionCoordinator coordinator, PageFileConfig? baseConfig = null)
        : base(databasePath, coordinator, baseConfig)
    {
        InitializeCollections();
    }

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<MultiFileEntry>()
            .ToCollection("entries")
            .HasKey(e => e.Id);
    }
}
