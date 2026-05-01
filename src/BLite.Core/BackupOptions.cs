using BLite.Core.Encryption;

namespace BLite.Core;

/// <summary>
/// Options that control how <see cref="BLiteEngine"/> creates a backup.
/// </summary>
public sealed class BackupOptions
{
    /// <summary>
    /// Explicit destination path of the backup's main <c>.db</c> file.
    /// When omitted, <see cref="DestinationPathPattern"/> must be provided.
    /// </summary>
    public string? DestinationPath { get; init; }

    /// <summary>
    /// Optional destination path pattern for the backup's main <c>.db</c> file.
    /// Supported tokens: <c>{databaseName}</c>, <c>{databasePath}</c>, <c>{timestampUtc}</c>.
    /// </summary>
    public string? DestinationPathPattern { get; init; }

    /// <summary>
    /// When true, includes the separate index file in the backup when the source database uses one.
    /// </summary>
    public bool IncludeIndexes { get; init; } = true;

    /// <summary>
    /// Optional encryption provider used to re-encrypt every page when writing the backup files.
    /// <para>
    /// When <c>null</c> (the default), the backup is written with the same encryption as the
    /// source database — or in plaintext when the source database is not encrypted.
    /// </para>
    /// <para>
    /// Supply an <see cref="AesGcmCryptoProvider"/> (passphrase) or a
    /// <see cref="EncryptionCoordinator"/>-derived provider to produce a backup that is
    /// encrypted with a different key, for example a cloud-backup-specific passphrase.
    /// Each physical file in the backup receives a freshly generated salt, so the backup
    /// key material is completely independent of the operational database.
    /// </para>
    /// <para>
    /// <b>Note:</b> the caller retains ownership of the provider.
    /// <see cref="BLiteEngine.BackupAsync(BackupOptions, System.Threading.CancellationToken)"/>
    /// does not dispose it.
    /// </para>
    /// </summary>
    public ICryptoProvider? BackupCryptoProvider { get; init; }
}

/// <summary>
/// Result returned by <see cref="BLiteEngine.BackupAsync(BackupOptions, System.Threading.CancellationToken)"/>.
/// </summary>
public readonly struct BackupResult
{
    public BackupResult(string destinationPath, string manifestPath, TimeSpan duration, int fileCount, long totalBytes)
    {
        DestinationPath = destinationPath;
        ManifestPath = manifestPath;
        Duration = duration;
        FileCount = fileCount;
        TotalBytes = totalBytes;
    }

    public string DestinationPath { get; }
    public string ManifestPath { get; }
    public TimeSpan Duration { get; }
    public int FileCount { get; }
    public long TotalBytes { get; }
}

/// <summary>
/// Event payload emitted when a backup starts.
/// </summary>
public readonly struct BackupStartedEvent
{
    public BackupStartedEvent(string destinationPath, BackupOptions options, DateTimeOffset timestamp)
    {
        DestinationPath = destinationPath;
        Options = options;
        Timestamp = timestamp;
    }

    public string DestinationPath { get; }
    public BackupOptions Options { get; }
    public DateTimeOffset Timestamp { get; }
}

/// <summary>
/// Event payload emitted when a backup completes successfully.
/// </summary>
public readonly struct BackupCompletedEvent
{
    public BackupCompletedEvent(BackupResult result, BackupOptions options, DateTimeOffset timestamp)
    {
        Result = result;
        Options = options;
        Timestamp = timestamp;
    }

    public BackupResult Result { get; }
    public BackupOptions Options { get; }
    public DateTimeOffset Timestamp { get; }
}
