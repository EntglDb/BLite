using System;
using BLite.Core.Encryption;
using BLite.Core.KeyValue;
using BLite.Core.Storage;

namespace BLite.Core;

/// <summary>
/// Top-level configuration for <see cref="BLiteEngine"/>.
/// </summary>
/// <remarks>
/// Use this options object to configure the database file path, encryption, and
/// Key-Value store settings in one place:
/// <code>
/// var db = new BLiteEngine(new BLiteEngineOptions
/// {
///     Filename = "mydata.db",
///     Encryption = new EncryptionOptions
///     {
///         KeyProvider = new MyAzureKeyVaultProvider()   // production
///         // — or for development only —
///         // Passphrase = "my-secret-passphrase"
///     }
/// });
/// </code>
/// </remarks>
public sealed class BLiteEngineOptions
{
    /// <summary>
    /// Path to the database file.  Must not be null or whitespace.
    /// </summary>
    public string? Filename { get; init; }

    /// <summary>
    /// Optional encryption configuration.  When <c>null</c> (the default), data is stored
    /// in plaintext with zero overhead.
    /// </summary>
    public EncryptionOptions? Encryption { get; init; }

    /// <summary>
    /// Optional Key-Value store configuration.
    /// </summary>
    public BLiteKvOptions? KvOptions { get; init; }

    /// <summary>
    /// Optional page-file configuration (page size, growth block, lock timeouts).
    /// When <c>null</c>, <see cref="PageFileConfig.Default"/> is used for passphrase-based
    /// encryption and plain databases; server layout is used for <see cref="EncryptionOptions.KeyProvider"/>-based
    /// encryption.
    /// </summary>
    public PageFileConfig? PageConfig { get; init; }
}
