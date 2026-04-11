using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Configs;
using LiteDB;
using System.IO;

namespace BLite.Benchmark;

/// <summary>
/// Real-world read comparison between BLite and LiteDB on a photo-library workload.
///
/// Two query patterns are exercised:
///   1-to-1  : FindOne by FilePath (unique secondary index, equality lookup).
///   1-to-N  : FindAll by SourceId (non-unique secondary index, returns N results per folder).
///
/// Sample sizes mirror what surfaces performance differences clearly:
///  - FilePath queries  : 100 random file paths  (unique hit per query)
///  - SourceId queries  : 2  random folder IDs   (each returns ~250 photos)
///
/// These benchmarks were originally authored by community contributor @LeoYang6, whose
/// real-world scenario drove the performance optimisation work in BLite 4.x.
/// </summary>
[SimpleJob(launchCount: 2, warmupCount: 5, iterationCount: 10, id: "RealWorld")]
[MemoryDiagnoser]
[GroupBenchmarksBy(BenchmarkLogicalGroupRule.ByCategory)]
[HtmlExporter]
[JsonExporterAttribute.Full]
public class RealWorldReadBenchmarks
{
    private const int TotalPhotos  = 5_000;
    private const int TotalFolders = 20;

    private RealWorldPhotoDbContext _blite  = null!;
    private LiteDatabase            _liteDb = null!;
    private ILiteCollection<RealWorldPhotoPo> _liteCol = null!;

    private string _blitePath  = null!;
    private string _liteDbPath = null!;

    private List<string> _sampleFilePaths  = null!;
    private List<string> _sampleSourceIds  = null!;

    // ── Setup / Teardown ──────────────────────────────────────────────────

    [GlobalSetup]
    public async Task Setup()
    {
        var tempDir = Path.Combine(Path.GetTempPath(), $"blite_rw_bench_{Guid.NewGuid():N}");
        Directory.CreateDirectory(tempDir);

        _blitePath  = Path.Combine(tempDir, "blite.db");
        _liteDbPath = Path.Combine(tempDir, "litedb.db");

        var (photos, sourceIds, filePaths) = GenerateData(TotalPhotos, TotalFolders);
        _sampleSourceIds  = sourceIds;
        _sampleFilePaths  = filePaths;

        // Seed BLite
        _blite = new RealWorldPhotoDbContext(_blitePath);
        await _blite.Photos.InsertBulkAsync(photos);
        await _blite.SaveChangesAsync();

        // Seed LiteDB
        _liteDb  = new LiteDatabase($"Filename={_liteDbPath};Connection=Direct");
        _liteCol = _liteDb.GetCollection<RealWorldPhotoPo>("Photos");
        _liteCol.EnsureIndex(x => x.Id,       true);
        _liteCol.EnsureIndex(x => x.SourceId);
        _liteCol.EnsureIndex(x => x.FilePath, true);
        _liteDb.BeginTrans();
        _liteCol.InsertBulk(photos);
        _liteDb.Commit();
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        _blite?.Dispose();
        _liteDb?.Dispose();
        var dir = Path.GetDirectoryName(_blitePath);
        if (dir != null && Directory.Exists(dir))
            Directory.Delete(dir, recursive: true);
    }

    // ── 1-to-1: FilePath exact match ──────────────────────────────────────

    [Benchmark(Baseline = true, Description = "LiteDB: Query by FilePath (1-to-1)")]
    [BenchmarkCategory("1-to-1")]
    public void LiteDB_QueryByFilePath()
    {
        foreach (var fp in _sampleFilePaths)
            _ = _liteCol.FindOne(x => x.FilePath == fp);
    }

    [Benchmark(Description = "BLite: Query by FilePath (1-to-1)")]
    [BenchmarkCategory("1-to-1")]
    public async Task BLite_QueryByFilePath()
    {
        foreach (var fp in _sampleFilePaths)
            _ = await _blite.Photos.FindOneAsync(x => x.FilePath == fp);
    }

    // ── 1-to-N: SourceId range scan ───────────────────────────────────────

    [Benchmark(Baseline = true, Description = "LiteDB: Query by SourceId (1-to-N)")]
    [BenchmarkCategory("1-to-N")]
    public void LiteDB_QueryBySourceId()
    {
        foreach (var sid in _sampleSourceIds)
            _ = _liteCol.Find(x => x.SourceId == sid).ToList();
    }

    [Benchmark(Description = "BLite: Query by SourceId (1-to-N)")]
    [BenchmarkCategory("1-to-N")]
    public async Task BLite_QueryBySourceId()
    {
        foreach (var sid in _sampleSourceIds)
            _ = await _blite.Photos.FindAsync(x => x.SourceId == sid).ToListAsync();
    }

    // ── Data generation ───────────────────────────────────────────────────

    private static (List<RealWorldPhotoPo> Photos, List<string> SampleSourceIds, List<string> SampleFilePaths)
        GenerateData(int totalPhotos, int totalFolders)
    {
        var rng = new Random(42);

        var folderIds = Enumerable.Range(0, totalFolders)
            .Select(_ => Guid.NewGuid().ToString("N")[..8])
            .ToList();

        var photos = Enumerable.Range(0, totalPhotos).Select(i =>
        {
            var sourceId = folderIds[i % totalFolders];
            return new RealWorldPhotoPo
            {
                Id        = Guid.NewGuid(),
                SourceId  = sourceId,
                FilePath  = $@"D:\Photos\{sourceId}\IMG_{Guid.NewGuid().ToString("N")[..8]}.jpg",
                DateTaken = DateTime.UtcNow.AddDays(-rng.Next(0, 3650)),
                FileSize  = rng.NextInt64(1024 * 1024, 1024 * 1024 * 15),
                Width     = new[] { 1920, 2560, 3840, 4000 }[rng.Next(4)],
                Height    = new[] { 1080, 1440, 2160, 3000 }[rng.Next(4)],
            };
        }).ToList();

        var sampleSourceIds  = folderIds.OrderBy(_ => rng.Next()).Take(2).ToList();
        var sampleFilePaths  = photos.OrderBy(_ => rng.Next()).Take(100).Select(p => p.FilePath).ToList();

        return (photos, sampleSourceIds, sampleFilePaths);
    }
}
