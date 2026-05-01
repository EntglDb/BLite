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
