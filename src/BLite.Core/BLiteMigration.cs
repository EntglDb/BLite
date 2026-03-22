using BLite.Bson;
using BLite.Core.Indexing;
using BLite.Core.KeyValue;
using BLite.Core.Storage;

namespace BLite.Core;

/// <summary>
/// Provides one-time migration utilities to convert a BLite database between
/// single-file and multi-file storage layouts without any data loss.
///
/// <para><b>Single-file → Multi-file</b> (<see cref="ToMultiFile"/>): all documents,
/// KV entries, and index definitions are copied from the original monolithic
/// <c>.db</c> file into the new per-file layout described by
/// <paramref name="targetConfig"/>.  After a successful migration the original
/// file is replaced by the new main <c>.db</c>; the WAL, index, and collection
/// files are written directly to the locations specified in
/// <paramref name="targetConfig"/>.</para>
///
/// <para><b>Multi-file → Single-file</b> (<see cref="ToSingleFile"/>): the
/// reverse operation.  The main <c>.db</c>, index file, and all per-collection
/// files are collapsed back into a single database at <paramref name="targetPath"/>.
/// The source multi-file layout is left untouched unless
/// <paramref name="targetPath"/> equals <paramref name="sourcePath"/>
/// (in-place migration), in which case the source is replaced.</para>
///
/// <para><b>Known limitations</b>:
/// <list type="bullet">
///   <item>KV entries with a TTL are migrated with the remaining TTL at the time of
///         migration (the remaining time-to-live is preserved, not the original
///         absolute expiry timestamp).</item>
///   <item>TimeSeries pruning counters are reset in the target database.</item>
///   <item>The migration is not atomic at the OS level: if the process is
///         killed after the target is written but before the old files are
///         deleted, both the original and the migrated database will be present
///         on disk.  Run the migration again to clean up.</item>
/// </list>
/// </para>
/// </summary>
public static class BLiteMigration
{
    private const int BatchSize = 1000;

    // ─────────────────────────────────────────────────────────────────────
    // Public API
    // ─────────────────────────────────────────────────────────────────────

    /// <summary>
    /// Migrates a single-file BLite database at <paramref name="sourcePath"/>
    /// to the multi-file layout described by <paramref name="targetConfig"/>.
    /// </summary>
    /// <param name="sourcePath">
    /// Path to the existing single-file <c>.db</c> file.
    /// </param>
    /// <param name="targetConfig">
    /// Target configuration.  The multi-file fields
    /// (<see cref="PageFileConfig.WalPath"/>,
    /// <see cref="PageFileConfig.IndexFilePath"/>,
    /// <see cref="PageFileConfig.CollectionDataDirectory"/>) determine where the
    /// new sub-files are created.  The page size, growth block, and access mode
    /// are inherited from <paramref name="targetConfig"/> as well.
    /// </param>
    /// <exception cref="FileNotFoundException">
    /// Thrown if <paramref name="sourcePath"/> does not exist.
    /// </exception>
    /// <exception cref="InvalidOperationException">
    /// Thrown if <paramref name="targetConfig"/> has no multi-file paths set
    /// (all of <see cref="PageFileConfig.WalPath"/>,
    /// <see cref="PageFileConfig.IndexFilePath"/>, and
    /// <see cref="PageFileConfig.CollectionDataDirectory"/> are <c>null</c>).
    /// </exception>
    public static void ToMultiFile(string sourcePath, PageFileConfig targetConfig)
    {
        if (!File.Exists(sourcePath))
            throw new FileNotFoundException($"Source database not found: '{sourcePath}'", sourcePath);

        if (targetConfig.WalPath == null && targetConfig.IndexFilePath == null
            && targetConfig.CollectionDataDirectory == null)
            throw new InvalidOperationException(
                "targetConfig has no multi-file paths set. "
                + "Set at least one of WalPath, IndexFilePath, or CollectionDataDirectory, "
                + "or use PageFileConfig.Server() to build a server-layout config.");

        // Detect source page size; fall back to Default if the file is unreadable.
        // Strip any multi-file companion paths: DetectFromFile now probes for .idx / wal /
        // collections/ files, so if they already exist (e.g. left from a previous migration)
        // both the source and target engines would race on the same companion files.
        // Only the page size is needed to correctly read the source.
        var sourceConfig = (PageFileConfig.DetectFromFile(sourcePath) ?? PageFileConfig.Default)
            with { WalPath = null, IndexFilePath = null, CollectionDataDirectory = null };

        // Target main .db is written to a temp path first so that we never
        // leave the source in a partially-overwritten state.
        var tempDbPath = sourcePath + ".migrating";
        try
        {
            using (var source = new BLiteEngine(sourcePath, sourceConfig))
            using (var target = new BLiteEngine(tempDbPath, targetConfig))
            {
                CopyAll(source, target);
            }

            // Atomically replace: delete original, move temp into place.
            File.Delete(sourcePath);
            File.Move(tempDbPath, sourcePath);

            // Clean up the old default-location WAL (if it was adjacent to the db).
            var defaultWalPath = Path.ChangeExtension(sourcePath, ".wal");
            SafeDelete(defaultWalPath);
        }
        catch
        {
            // Best-effort cleanup of the half-written temp file.
            SafeDelete(tempDbPath);
            var tempWal = Path.ChangeExtension(tempDbPath, ".wal");
            SafeDelete(tempWal);
            throw;
        }
    }

    /// <summary>
    /// Migrates a multi-file BLite database at <paramref name="sourcePath"/>
    /// to a single-file layout at <paramref name="targetPath"/>.
    /// </summary>
    /// <param name="sourcePath">
    /// Path to the main <c>.db</c> file of the multi-file database.
    /// </param>
    /// <param name="sourceConfig">
    /// Configuration describing the multi-file layout of the source database
    /// (must have the same <see cref="PageFileConfig.WalPath"/>,
    /// <see cref="PageFileConfig.IndexFilePath"/>, and
    /// <see cref="PageFileConfig.CollectionDataDirectory"/> that were used
    /// when the source was created).
    /// </param>
    /// <param name="targetPath">
    /// Path for the new single-file database.  May equal
    /// <paramref name="sourcePath"/> for an in-place migration; in that case
    /// the multi-file components are deleted after a successful migration.
    /// </param>
    /// <param name="targetPageConfig">
    /// Optional base configuration for the target.  Multi-file fields are
    /// ignored.  When <c>null</c>, the page size is detected from the source
    /// file; if detection fails, <see cref="PageFileConfig.Default"/> is used.
    /// </param>
    /// <exception cref="FileNotFoundException">
    /// Thrown if <paramref name="sourcePath"/> does not exist.
    /// </exception>
    public static void ToSingleFile(
        string sourcePath,
        PageFileConfig sourceConfig,
        string targetPath,
        PageFileConfig? targetPageConfig = null)
    {
        if (!File.Exists(sourcePath))
            throw new FileNotFoundException($"Source database not found: '{sourcePath}'", sourcePath);

        // Build a pure single-file config (strip multi-file paths).
        var detectedBase = PageFileConfig.DetectFromFile(sourcePath) ?? PageFileConfig.Default;
        var @base = targetPageConfig ?? detectedBase;
        var singleFileConfig = @base with
        {
            WalPath = null,
            IndexFilePath = null,
            CollectionDataDirectory = null
        };

        bool isInPlace = string.Equals(
            Path.GetFullPath(sourcePath), Path.GetFullPath(targetPath),
            StringComparison.OrdinalIgnoreCase);

        var actualTargetPath = isInPlace ? targetPath + ".migrating" : targetPath;

        try
        {
            using (var source = new BLiteEngine(sourcePath, sourceConfig))
            using (var target = new BLiteEngine(actualTargetPath, singleFileConfig))
            {
                CopyAll(source, target);
            }

            if (isInPlace)
            {
                // Replace the source with the newly created single-file DB.
                File.Delete(sourcePath);
                File.Move(actualTargetPath, targetPath);

                // Delete multi-file components.
                DeleteMultiFileComponents(sourceConfig, sourcePath);
            }
        }
        catch
        {
            if (isInPlace)
            {
                SafeDelete(actualTargetPath);
                SafeDelete(Path.ChangeExtension(actualTargetPath, ".wal"));
            }
            throw;
        }
    }

    // ─────────────────────────────────────────────────────────────────────
    // Internal helpers
    // ─────────────────────────────────────────────────────────────────────

    /// <summary>Copies all collections and KV entries from source to target.</summary>
    private static void CopyAll(BLiteEngine source, BLiteEngine target)
    {
        // Sync the C-BSON key dictionary first so that raw BSON bytes written by the
        // source engine are decodable by the target without re-serialisation.
        target.ImportDictionary(source.GetKeyReverseMap());

        CopyCollections(source, target);
        CopyKvStore(source, target);
        // Flush all committed WAL records to the main page file before the engine
        // is closed and its files are renamed / deleted.
        target.Checkpoint();
    }

    private static void CopyCollections(BLiteEngine source, BLiteEngine target)
    {
        foreach (var collName in source.ListCollections())
        {
            var meta = source.GetCollectionMetadata(collName);
            var sourceColl = source.GetOrCreateCollection(collName);

            // Detect Id type from the first document; fall back to ObjectId.
            var idType = BsonIdType.ObjectId;
            var firstDoc = sourceColl.FindAll().FirstOrDefault();
            if (firstDoc != null && firstDoc.TryGetId(out var firstId))
                idType = firstId.Type;

            var targetColl = target.GetOrCreateCollection(collName, idType);

            // Configure TimeSeries retention if the source collection has it.
            if (sourceColl.IsTimeSeries && meta != null
                && meta.RetentionPolicyMs > 0 && meta.TtlFieldName != null)
            {
                targetColl.SetTimeSeries(
                    meta.TtlFieldName,
                    TimeSpan.FromMilliseconds(meta.RetentionPolicyMs));
            }

            // Copy documents in batches to avoid holding all of them in memory.
            var batch = new List<BsonDocument>(BatchSize);
            foreach (var doc in sourceColl.FindAll())
            {
                batch.Add(doc);
                if (batch.Count >= BatchSize)
                {
                    targetColl.InsertBulk(batch);
                    batch.Clear();
                }
            }
            if (batch.Count > 0)
                targetColl.InsertBulk(batch);

            target.Commit();

            // Recreate secondary indexes (after documents are inserted so the
            // index is built from the full data set in one pass).
            if (meta != null)
            {
                foreach (var idx in meta.Indexes)
                {
                    if (idx.PropertyPaths.Length == 0) continue;
                    RecreateIndex(targetColl, idx);
                }
                target.Commit();
            }
        }
    }

    private static void RecreateIndex(DynamicCollection col, IndexMetadata idx)
    {
        var field = idx.PropertyPaths[0];
        switch (idx.Type)
        {
            case IndexType.BTree:
                col.CreateIndex(field, idx.Name, idx.IsUnique);
                break;
            case IndexType.Vector:
                col.CreateVectorIndex(field, idx.Dimensions, idx.Metric, idx.Name);
                break;
            case IndexType.Spatial:
                col.CreateSpatialIndex(field, idx.Name);
                break;
        }
    }

    private static void CopyKvStore(BLiteEngine source, BLiteEngine target)
    {
        // Cast to the internal BLiteKvStore to access GetWithExpiry so that TTL
        // information is preserved across the migration.
        var sourceKv = (BLiteKvStore)source.KvStore;
        long now     = DateTime.UtcNow.Ticks;

        foreach (var key in source.KvStore.ScanKeys())
        {
            var (value, expiryTicks) = sourceKv.GetWithExpiry(key);
            if (value == null) continue;

            TimeSpan? ttl = null;
            if (expiryTicks > 0)
            {
                long remainingTicks = expiryTicks - now;
                // Skip entries that have already expired (can race between ScanKeys and GetWithExpiry).
                if (remainingTicks <= 0) continue;
                ttl = TimeSpan.FromTicks(remainingTicks);
            }

            target.KvStore.Set(key, value, ttl);
        }
    }

    /// <summary>
    /// Deletes the WAL, index file, and collection directory created by a
    /// multi-file configuration.  Ignores missing files/directories.
    /// </summary>
    private static void DeleteMultiFileComponents(PageFileConfig config, string mainDbPath)
    {
        // Delete WAL file (either the configured path or the default adjacent one).
        var walPath = config.WalPath ?? Path.ChangeExtension(mainDbPath, ".wal");
        SafeDelete(walPath);

        // Delete index file.
        if (config.IndexFilePath != null)
            SafeDelete(config.IndexFilePath);

        // Delete collection directory and its contents.
        if (config.CollectionDataDirectory != null && Directory.Exists(config.CollectionDataDirectory))
        {
            try { Directory.Delete(config.CollectionDataDirectory, recursive: true); }
            catch { /* best-effort */ }
        }
    }

    private static void SafeDelete(string path)
    {
        try { if (File.Exists(path)) File.Delete(path); }
        catch { /* best-effort */ }
    }
}
