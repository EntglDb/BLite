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
    /// When <c>null</c>:
    /// <list type="bullet">
    ///   <item>For <see cref="EncryptionOptions.KeyProvider"/>-based encryption, the server layout
    ///   (<see cref="PageFileConfig.Server"/>) is used as the base — separate WAL directory,
    ///   dedicated index file, and per-collection files.</item>
    ///   <item>For passphrase-based encryption, <see cref="PageFileConfig.Default"/> (16 KB pages,
    ///   single-file layout) is used.</item>
    ///   <item>For plain (unencrypted) databases, the page size is auto-detected from an existing
    ///   file or defaults to <see cref="PageFileConfig.Default"/>.</item>
    /// </list>
    /// When explicitly set, the supplied configuration is used as-is (page size, lock timeouts,
    /// etc.) but for <see cref="EncryptionOptions.KeyProvider"/>-based encryption the multi-file
    /// paths are still derived via <see cref="PageFileConfig.Server"/>, meaning the supplied value
    /// acts as the <c>baseConfig</c> parameter to that method.
    /// </summary>
    public PageFileConfig? PageConfig { get; init; }
}
