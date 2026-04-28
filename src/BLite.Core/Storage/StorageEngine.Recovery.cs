using System.Diagnostics;
using System.Security.Cryptography;
using System.Text.Json;
using BLite.Core.Transactions;

namespace BLite.Core.Storage;

public sealed partial class StorageEngine
{
    /// <summary>
    /// Gets the current size of the WAL file.
    /// </summary>
    public long GetWalSize()
    {
        return _wal.GetCurrentSize();
    }

    /// <summary>
    /// Flushes pending memory-mapped (MMF) writes from the PageFile to the OS kernel buffer.
    /// Required for consistency when <see cref="WritePageImmediate"/> is followed by
    /// <see cref="ReadPageAsync"/>, which uses <see cref="System.IO.RandomAccess"/> and
    /// reads from the kernel buffer pool rather than the MMF view.
    /// </summary>
    public void FlushPageFile()
    {
        _pageFile.Flush();
        _indexFile?.Flush();
        if (_collectionFiles != null)
        {
            foreach (var lazy in _collectionFiles.Values)
                if (lazy.IsValueCreated) lazy.Value.Flush();
        }
    }

    /// <summary>
    /// Async opportunistic checkpoint — NEVER blocks commits.
    /// See <see cref="Checkpoint"/> for the strategy.
    /// </summary>
    public async Task CheckpointAsync(CancellationToken ct = default)
    {
        if (_walIndex.IsEmpty) return;
        if (Interlocked.CompareExchange(ref _checkpointRunning, 1, 0) != 0) return;
        var sw = _metrics != null ? Metrics.ValueStopwatch.StartNew() : default;
        try
        {
            var snapshot = _walIndex.ToArray();
            if (snapshot.Length == 0) return;

            foreach (var kvp in snapshot)
                GetPageFile(kvp.Key, out var physId).WritePage(physId, kvp.Value);

            await _pageFile.FlushAsync(ct);
            if (_indexFile != null)
                await _indexFile.FlushAsync(ct);
            if (_collectionFiles != null)
            {
                foreach (var lazy in _collectionFiles.Values)
                    if (lazy.IsValueCreated) await lazy.Value.FlushAsync(ct);
            }

            // Drain _walIndex entries that we successfully flushed to disk.
            // Safe without _commitLock because:
            //  - ConcurrentDictionary ops are thread-safe
            //  - ReferenceEquals ensures we only remove entries we actually flushed;
            //    a concurrent commit that updated the same page uses a different byte[]
            //    reference, so the check fails and the new version is preserved.
            foreach (var kvp in snapshot)
            {
                if (_walIndex.TryGetValue(kvp.Key, out var current) && ReferenceEquals(current, kvp.Value))
                    _walIndex.TryRemove(kvp.Key, out _);
            }

            // Truncate WAL only when _walIndex is fully drained.
            // _commitLock is still needed here to prevent truncating while the
            // group-commit writer is appending new WAL records.
            if (_walIndex.IsEmpty && _commitLock.Wait(0))
            {
                try
                {
                    // Double-check: new commits may have promoted entries
                    // between our IsEmpty check and acquiring the lock.
                    if (_walIndex.IsEmpty)
                        await _wal.TruncateAsync(ct);
                }
                finally
                {
                    _commitLock.Release();
                }
            }
        }
        finally
        {
            Interlocked.Exchange(ref _checkpointRunning, 0);
            if (sw.IsActive)
                _metrics?.Publish(new Metrics.MetricEvent
                {
                    Timestamp     = sw.StartTimestamp,
                    Type          = Metrics.MetricEventType.Checkpoint,
                    ElapsedMicros = sw.GetElapsedMicros(),
                    Success       = true,
                });
        }
    }

    /// <summary>
    /// Creates a consistent backup of this database to <paramref name="destinationDbPath"/>.
    /// <para>
    /// Strategy (fully safe under concurrent writes):
    /// <list type="number">
    ///   <item>Acquire the commit lock — no new transaction can commit while we work.</item>
    ///   <item>CheckpointAsync: merge all committed WAL entries into their backing files and flush.</item>
    ///   <item>Copy the WAL, main file, collection files, and optional index file while holding the lock.</item>
    ///   <item>Release the lock — normal writes resume.</item>
    ///   <item>Hash the copied files and write <c>backup.manifest.json</c>.</item>
    /// </list>
    /// </para>
    /// </summary>
    public async Task BackupAsync(string destinationDbPath, CancellationToken ct = default)
    {
        await BackupDetailedAsync(destinationDbPath, includeIndexes: true, ct);
    }

    internal async Task<StorageBackupStats> BackupDetailedAsync(string destinationDbPath, bool includeIndexes, CancellationToken ct = default)
    {
#if NET8_0_OR_GREATER
        ArgumentException.ThrowIfNullOrWhiteSpace(destinationDbPath);
#else
        if (string.IsNullOrWhiteSpace(destinationDbPath))
            throw new ArgumentException("The value cannot be null, empty, or whitespace.", nameof(destinationDbPath));
#endif

        var backupTimestamp = DateTimeOffset.UtcNow;

        if (!await _commitLock.WaitAsync(_config.LockTimeout.WriteTimeoutMs, ct))
            throw new TimeoutException("Timed out acquiring commit lock (Backup).");

        List<BackupCopyOperation>? operations = null;
        try
        {
            await CheckpointAsync(ct);
            if (_walIndex.IsEmpty)
                await _wal.TruncateAsync(ct).ConfigureAwait(false);

            operations = BuildBackupPlan(destinationDbPath, includeIndexes);

            foreach (var operation in operations)
            {
                ct.ThrowIfCancellationRequested();
                await operation.CopyAsync(ct).ConfigureAwait(false);
            }
        }
        catch
        {
            if (operations != null)
                CleanupPartialBackup(operations, manifestPath: null);
            throw;
        }
        finally
        {
            _commitLock.Release();
        }

        try
        {
            var manifestRoot = Path.GetFullPath(Path.GetDirectoryName(destinationDbPath) ?? ".");
            var manifestPath = Path.Combine(manifestRoot, "backup.manifest.json");
            var files = new List<BackupManifestFile>(operations.Count);
            long totalBytes = 0;

            foreach (var operation in operations)
            {
                var fileInfo = new FileInfo(operation.DestinationPath);
                var size = fileInfo.Length;
                totalBytes += size;

                files.Add(new BackupManifestFile
                {
                    Name = Path.GetRelativePath(manifestRoot, operation.DestinationPath),
                    Size = size,
                    Sha256 = ComputeSha256(operation.DestinationPath)
                });
            }

            await WriteManifestAsync(manifestPath, files, backupTimestamp, ct).ConfigureAwait(false);
            return new StorageBackupStats(manifestPath, files.Count, totalBytes);
        }
        catch
        {
            CleanupPartialBackup(operations, Path.Combine(Path.GetDirectoryName(destinationDbPath) ?? ".", "backup.manifest.json"));
            throw;
        }
    }
    
    /// <summary>
    /// Recovers from crash by replaying WAL.
    /// Applies all committed transactions to PageFile, then truncates WAL.
    /// </summary>
    public async Task RecoverAsync(CancellationToken ct = default)
    {
        if (!await _commitLock.WaitAsync(_config.LockTimeout.WriteTimeoutMs))
            throw new TimeoutException("Timed out acquiring commit lock (Recovery).");
        try
        {
            // 1. Read WAL and identify committed transactions
            var records = _wal.ReadAll();
            var committedTxns = new HashSet<ulong>();
            var txnWrites = new Dictionary<ulong, List<(uint pageId, byte[] data)>>();
            
            foreach (var record in records)
            {
                if (record.Type == WalRecordType.Commit)
                    committedTxns.Add(record.TransactionId);
                else if (record.Type == WalRecordType.Write)
                {
                    if (!txnWrites.ContainsKey(record.TransactionId))
                        txnWrites[record.TransactionId] = new List<(uint, byte[])>();
                    
                    if (record.AfterImage != null)
                    {
                        txnWrites[record.TransactionId].Add((record.PageId, record.AfterImage));
                    }
                }
            }
            
            // 2. Apply committed transactions to the correct PageFile
            foreach (var txnId in committedTxns)
            {
                if (!txnWrites.ContainsKey(txnId))
                    continue;
                    
                foreach (var (pageId, data) in txnWrites[txnId])
                {
                    var targetFile = GetPageFile(pageId, out var physId);
                    targetFile.WritePage(physId, data);
                }
            }
            
            // 3. Flush all PageFiles to ensure durability
            await _pageFile.FlushAsync(ct);
            if (_indexFile != null)
                await _indexFile.FlushAsync(ct);
            if (_collectionFiles != null)
            {
                foreach (var lazy in _collectionFiles.Values)
                    if (lazy.IsValueCreated) lazy.Value.Flush();
            }
            
            // 4. Clear in-memory WAL index (redundant since we just recovered)
            _walIndex.Clear();
            
            // 5. Truncate WAL (all changes now in PageFile)
            await _wal.TruncateAsync(ct);
        }
        finally
        {
            _commitLock.Release();
        }
    }

    private List<BackupCopyOperation> BuildBackupPlan(string destinationDbPath, bool includeIndexes)
    {
        if (_pageFile is not PageFile mainPageFile)
            throw new NotSupportedException("Detailed backup requires the default file-based storage backend.");

        if (_wal is not WriteAheadLog wal)
            throw new NotSupportedException("Detailed backup requires the default file-based WAL implementation.");

        if (!includeIndexes && _indexFile != null)
            throw new NotSupportedException("ExcludeIndexes is not supported for databases that use a separate index file.");

        var targetLayout = GetBackupLayout(destinationDbPath);
        var operations = new List<BackupCopyOperation>
        {
            new(targetLayout.WalPath, ct => wal.BackupAsync(targetLayout.WalPath, ct)),
            new(destinationDbPath, ct => mainPageFile.BackupAsync(destinationDbPath, ct))
        };

        if (_config.CollectionDataDirectory != null)
        {
            foreach (var sourcePath in Directory.EnumerateFiles(_config.CollectionDataDirectory).OrderBy(Path.GetFileName, StringComparer.OrdinalIgnoreCase))
            {
                var fileName = Path.GetFileName(sourcePath);
                if (Path.IsPathRooted(fileName))
                    throw new InvalidOperationException($"Collection file name '{fileName}' must not be an absolute path.");

                var destinationPath = Path.Combine(targetLayout.CollectionDirectory!, fileName);

                if (string.Equals(fileName, ".slots", StringComparison.OrdinalIgnoreCase))
                {
                    operations.Add(new BackupCopyOperation(destinationPath, ct => CopyClosedFileAsync(sourcePath, destinationPath, ct)));
                    continue;
                }

                var collectionName = Path.GetFileNameWithoutExtension(sourcePath);
                if (_collectionFiles != null &&
                    _collectionFiles.TryGetValue(collectionName, out var lazy) &&
                    lazy.IsValueCreated &&
                    lazy.Value is PageFile collectionPageFile)
                {
                    operations.Add(new BackupCopyOperation(destinationPath, ct => collectionPageFile.BackupAsync(destinationPath, ct)));
                }
                else
                {
                    operations.Add(new BackupCopyOperation(destinationPath, ct => CopyClosedFileAsync(sourcePath, destinationPath, ct)));
                }
            }
        }

        if (includeIndexes && _indexFile is PageFile indexPageFile && targetLayout.IndexPath != null)
            operations.Add(new BackupCopyOperation(targetLayout.IndexPath, ct => indexPageFile.BackupAsync(targetLayout.IndexPath, ct)));

        return operations;
    }

    private BackupLayout GetBackupLayout(string destinationDbPath)
    {
        var baseConfig = _config with
        {
            WalPath = null,
            IndexFilePath = null,
            CollectionDataDirectory = null
        };

        if (_config.WalPath == null && _config.IndexFilePath == null && _config.CollectionDataDirectory == null)
        {
            return new BackupLayout(
                Path.ChangeExtension(destinationDbPath, ".wal"),
                null,
                null);
        }

        var serverLayout = PageFileConfig.Server(destinationDbPath, baseConfig);
        return new BackupLayout(
            serverLayout.WalPath!,
            _config.IndexFilePath != null ? serverLayout.IndexFilePath : null,
            _config.CollectionDataDirectory != null ? serverLayout.CollectionDataDirectory : null);
    }

    private static async Task CopyClosedFileAsync(string sourcePath, string destinationPath, CancellationToken ct)
    {
        var destinationDirectory = Path.GetDirectoryName(destinationPath);
        if (!string.IsNullOrEmpty(destinationDirectory))
            Directory.CreateDirectory(destinationDirectory);

        await using var source = new FileStream(sourcePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite,
            bufferSize: 1024 * 1024, FileOptions.Asynchronous);
        await using var destination = new FileStream(destinationPath, FileMode.Create, FileAccess.Write, FileShare.None,
            bufferSize: 1024 * 1024, FileOptions.Asynchronous);

        await source.CopyToAsync(destination, 1024 * 1024, ct).ConfigureAwait(false);
        await destination.FlushAsync(ct).ConfigureAwait(false);
    }

    private static string ComputeSha256(string path)
    {
        using var stream = File.OpenRead(path);
        using var sha256 = SHA256.Create();
        return ToHexString(sha256.ComputeHash(stream));
    }

    private static async Task WriteManifestAsync(string manifestPath, List<BackupManifestFile> files, DateTimeOffset backupTimestamp, CancellationToken ct)
    {
        var directory = Path.GetDirectoryName(manifestPath);
        if (!string.IsNullOrEmpty(directory))
            Directory.CreateDirectory(directory);

        await using var stream = new FileStream(manifestPath, FileMode.Create, FileAccess.Write, FileShare.None,
            bufferSize: 16 * 1024, FileOptions.Asynchronous);
        await using var writer = new Utf8JsonWriter(stream, new JsonWriterOptions { Indented = true });
        writer.WriteStartObject();
        writer.WriteNumber("version", 1);
        writer.WriteString("timestamp", backupTimestamp);
        writer.WriteStartArray("files");
        foreach (var file in files)
        {
            writer.WriteStartObject();
            writer.WriteString("name", file.Name);
            writer.WriteNumber("size", file.Size);
            writer.WriteString("sha256", file.Sha256);
            writer.WriteEndObject();
        }

        writer.WriteEndArray();
        writer.WriteEndObject();
        await writer.FlushAsync(ct).ConfigureAwait(false);
        await stream.FlushAsync(ct).ConfigureAwait(false);
    }

    private static string ToHexString(byte[] bytes)
    {
#if NET5_0_OR_GREATER
        return Convert.ToHexString(bytes);
#else
        return BitConverter.ToString(bytes).Replace("-", string.Empty, StringComparison.Ordinal);
#endif
    }

    private static void CleanupPartialBackup(List<BackupCopyOperation> operations, string? manifestPath)
    {
        var directories = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        foreach (var operation in operations)
        {
            try
            {
                if (File.Exists(operation.DestinationPath))
                    File.Delete(operation.DestinationPath);
            }
            catch (IOException) { }
            catch (UnauthorizedAccessException) { }
            catch (SystemException ex) when (
                ex is DirectoryNotFoundException or
                FileNotFoundException or
                PathTooLongException or
                NotSupportedException) { }

            var directory = Path.GetDirectoryName(operation.DestinationPath);
            if (!string.IsNullOrEmpty(directory))
                directories.Add(directory);
        }

        if (!string.IsNullOrEmpty(manifestPath))
        {
            try
            {
                if (File.Exists(manifestPath))
                    File.Delete(manifestPath);
            }
            catch (IOException) { }
            catch (UnauthorizedAccessException) { }
            catch (SystemException ex) when (
                ex is DirectoryNotFoundException or
                FileNotFoundException or
                PathTooLongException or
                NotSupportedException) { }

            var directory = Path.GetDirectoryName(manifestPath);
            if (!string.IsNullOrEmpty(directory))
                directories.Add(directory);
        }

        foreach (var directory in directories.OrderByDescending(static d => d.Length))
        {
            try
            {
                if (Directory.Exists(directory) && !Directory.EnumerateFileSystemEntries(directory).Any())
                    Directory.Delete(directory);
            }
            catch (IOException) { }
            catch (UnauthorizedAccessException) { }
            catch (SystemException ex) when (
                ex is DirectoryNotFoundException or
                PathTooLongException or
                NotSupportedException) { }
        }
    }

    private readonly record struct BackupCopyOperation(string DestinationPath, Func<CancellationToken, Task> CopyAsync);
    internal readonly record struct StorageBackupStats(string ManifestPath, int FileCount, long TotalBytes);
    private readonly record struct BackupLayout(string WalPath, string? IndexPath, string? CollectionDirectory);

    private sealed class BackupManifestFile
    {
        public string Name { get; init; } = "";
        public long Size { get; init; }
        public string Sha256 { get; init; } = "";
    }
}
