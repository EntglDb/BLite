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
            // Phase 6: Compute the safe upper boundary for this checkpoint run.
            // In multi-process mode we must not flush WAL entries whose records are
            // still within the read window of an active cross-process reader.  The SHM
            // reader-slot array gives us the minimum offset any live reader registered;
            // we only checkpoint entries whose WAL offset is at or below that boundary.
            long safeOffset = long.MaxValue;
            if (_shm != null)
                safeOffset = Math.Min(_wal.GetCurrentSize(), _shm.GetMinReaderOffset());

            var snapshot = _walIndex.ToArray();
            if (snapshot.Length == 0) return;

            // Build the set of entries that are safe to checkpoint.
            var toCheckpoint = new List<KeyValuePair<uint, byte[]>>(snapshot.Length);
            foreach (var kvp in snapshot)
            {
                if (_shm != null && _walOffsets != null)
                {
                    // Only include entries whose WAL record is within the safe window.
                    if (_walOffsets.TryGetValue(kvp.Key, out long entryOffset) && entryOffset > safeOffset)
                        continue; // reader still needs this WAL record — skip for now
                }
                toCheckpoint.Add(kvp);
            }

            if (toCheckpoint.Count == 0) return;

            foreach (var kvp in toCheckpoint)
                GetPageFile(kvp.Key, out var physId).WritePage(physId, kvp.Value);

            // Phase 6: Parallel flush in multi-file mode.
            var flushTasks = new List<Task>();
            flushTasks.Add(_pageFile.FlushAsync(ct));
            if (_indexFile != null)
                flushTasks.Add(_indexFile.FlushAsync(ct));
            if (_collectionFiles != null)
            {
                foreach (var lazy in _collectionFiles.Values)
                    if (lazy.IsValueCreated) flushTasks.Add(lazy.Value.FlushAsync(ct));
            }
            await Task.WhenAll(flushTasks).ConfigureAwait(false);

            // Drain _walIndex (and _walOffsets) entries that we successfully flushed.
            // Track the highest checkpointed WAL offset in the same loop (avoids a
            // second O(n) pass over toCheckpoint).
            // ReferenceEquals check ensures we only remove the exact version we flushed.
            long maxCheckpointedOffset = 0;
            foreach (var kvp in toCheckpoint)
            {
                if (_walIndex.TryGetValue(kvp.Key, out var current) && ReferenceEquals(current, kvp.Value))
                {
                    _walIndex.TryRemove(kvp.Key, out _);
                    // Track max offset while removing from _walOffsets.
                    if (_walOffsets != null && _walOffsets.TryRemove(kvp.Key, out long removedOffset)
                        && removedOffset > maxCheckpointedOffset)
                        maxCheckpointedOffset = removedOffset;
                }
            }

            // Update the SHM checkpointed offset to communicate to other processes how
            // far the page files have been flushed.
            if (_shm != null && maxCheckpointedOffset > 0)
                _shm.WriteCheckpointedOffset(maxCheckpointedOffset);

            // Truncate WAL only when _walIndex is fully drained.
            // _commitLock is still needed here to prevent truncating while the
            // group-commit writer is appending new WAL records.
            //
            // In multi-process mode the SHM cross-process writer lock must also be held
            // for the duration of the truncation: another process could be appending
            // WAL records concurrently (its commit path acquires only its own
            // _commitLock, not ours), and truncating the shared WAL file underneath it
            // would corrupt the on-disk format. The lock order matches the commit path
            // (in-process _commitLock → cross-process SHM writer lock) to avoid any
            // possible cross-lock deadlock.
            if (_walIndex.IsEmpty && _commitLock.Wait(0))
            {
                bool shmHeld = false;
                try
                {
                    if (_shm != null)
                    {
                        shmHeld = _shm.TryAcquireWriterLock(_config.LockTimeout.WriteTimeoutMs);
                        if (!shmHeld)
                        {
                            // Skip this truncation cycle rather than throw — a future
                            // checkpoint will retry. Concurrent commits from another
                            // process are a normal, recoverable condition.
                            return;
                        }
                    }
                    // Double-check: new commits may have promoted entries
                    // between our IsEmpty check and acquiring the lock.
                    if (_walIndex.IsEmpty)
                    {
                        await _wal.TruncateAsync(ct);
                        // Reset the SHM WAL index and end offset so other processes
                        // know the WAL has been cleared (Phase 4 / Phase 6).
                        if (_shm != null)
                        {
                            _shm.RebuildIndex(System.Array.Empty<(uint, long)>());
                            _shm.WriteWalEndOffset(0);
                            _shm.WriteCheckpointedOffset(0);
                            Volatile.Write(ref _lastKnownWalEndOffset, 0);
                        }
                    }
                }
                finally
                {
                    if (shmHeld) { try { _shm!.ReleaseWriterLock(); } catch { /* best-effort */ } }
                    _commitLock.Release();
                }
            }
            else if (_shm != null && !_walIndex.IsEmpty)
            {
                // Partial checkpoint: rebuild the SHM hash table with only the remaining
                // (not-yet-checkpointed) survivors so it doesn't contain stale offsets for
                // pages that were just flushed to the page file.
                // This must be done under _commitLock + SHM writer lock to avoid races.
                if (_commitLock.Wait(0))
                {
                    bool shmHeld = false;
                    try
                    {
                        shmHeld = _shm.TryAcquireWriterLock(_config.LockTimeout.WriteTimeoutMs);
                        if (shmHeld)
                        {
                            var survivors = new List<(uint, long)>(_walIndex.Count);
                            foreach (var kvp in _walIndex)
                            {
                                if (_walOffsets != null && _walOffsets.TryGetValue(kvp.Key, out long off))
                                    survivors.Add((kvp.Key, off));
                            }
                            _shm.RebuildIndex(survivors);
                        }
                    }
                    finally
                    {
                        if (shmHeld) { try { _shm!.ReleaseWriterLock(); } catch { /* best-effort */ } }
                        _commitLock.Release();
                    }
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
