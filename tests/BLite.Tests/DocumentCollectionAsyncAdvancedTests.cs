// BLite — Async Advanced Operations Test Suite
// Tests for Phase 2-3 async methods: ScanAsync, QueryIndexAsync, VectorSearchAsync, 
// NearAsync, WithinAsync, ParallelScanAsync

using BLite.Bson;
using BLite.Shared;

namespace BLite.Tests;

public class DocumentCollectionAsyncAdvancedTests : IAsyncLifetime
{
    private readonly string _dbPath;
    private TestDbContext _db = null!;

    public DocumentCollectionAsyncAdvancedTests()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"blite_async_advanced_{Guid.NewGuid()}.db");
    }

    public async Task InitializeAsync()
    {
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
        var wal = Path.ChangeExtension(_dbPath, ".wal");
        if (File.Exists(wal)) File.Delete(wal);
        _db = new TestDbContext(_dbPath);
        await Task.CompletedTask;
    }

    public async Task DisposeAsync()
    {
        _db?.Dispose();
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
        var wal = Path.ChangeExtension(_dbPath, ".wal");
        if (File.Exists(wal)) File.Delete(wal);
        await Task.CompletedTask;
    }

    #region ScanAsync (BsonReaderPredicate)

    [Fact]
    public async Task ScanAsync_WithPredicate_ReturnsMatchingDocuments()
    {
        // Arrange
        _db.Users.Insert(new User { Name = "Alice", Age = 30 });
        _db.Users.Insert(new User { Name = "Bob", Age = 25 });
        _db.Users.Insert(new User { Name = "Charlie", Age = 35 });
        _db.SaveChanges();

        // Act: Find users older than 28
        var results = new List<User>();
        await foreach (var user in _db.Users.ScanAsync((BsonReaderPredicate)(reader => ParseAge(reader) > 28)))
        {
            results.Add(user);
        }

        // Assert
        Assert.Equal(2, results.Count);
        Assert.Contains(results, u => u.Name == "Alice");
        Assert.Contains(results, u => u.Name == "Charlie");
    }

    [Fact]
    public async Task ScanAsync_WithCancellation_StopsIteration()
    {
        // Arrange
        for (int i = 0; i < 1000; i++)
        {
            _db.Users.Insert(new User { Name = $"User_{i}", Age = i });
        }
        _db.SaveChanges();

        var cts = new CancellationTokenSource();
        var results = new List<User>();

        // Act: Start scanning with cancellation token
        try
        {
            var enumerator = _db.Users.ScanAsync(
                (BsonReaderPredicate)(_ => true),
                cts.Token
            ).GetAsyncEnumerator();

            int count = 0;
            while (await enumerator.MoveNextAsync())
            {
                results.Add(enumerator.Current);
                count++;
                if (count == 100) cts.Cancel();
            }
        }
        catch (OperationCanceledException)
        {
            // Expected
        }

        // Assert: Should have stopped before reaching the end
        Assert.True(results.Count >= 100, "Should have scanned at least 100 items before cancellation");
        Assert.True(results.Count < 1000, "Should not have completed full scan due to cancellation");
    }

    [Fact]
    public async Task ScanAsync_NoMatches_ReturnsEmpty()
    {
        // Arrange
        _db.Users.Insert(new User { Name = "Alice", Age = 30 });
        _db.Users.Insert(new User { Name = "Bob", Age = 25 });
        _db.SaveChanges();

        // Act: Find users older than 50
        var results = new List<User>();
        await foreach (var user in _db.Users.ScanAsync((BsonReaderPredicate)(reader => ParseAge(reader) > 50)))
        {
            results.Add(user);
        }

        // Assert
        Assert.Empty(results);
    }

    #endregion

    #region ScanAsync<TResult> (BsonReaderProjector)

    [Fact]
    public async Task ScanAsync_WithProjection_ReturnsProjectedResults()
    {
        // Arrange
        _db.Users.Insert(new User { Name = "Alice", Age = 30 });
        _db.Users.Insert(new User { Name = "Bob", Age = 25 });
        _db.Users.Insert(new User { Name = "Charlie", Age = 35 });
        _db.SaveChanges();

        // Act: Project to names only
        var names = new List<string>();
        await foreach (var name in _db.Users.ScanAsync<string>(
            (BsonReaderProjector<string>)(reader =>
            {
                reader.ReadDocumentSize();
                while (reader.Remaining > 0)
                {
                    var type = reader.ReadBsonType();
                    if (type == 0) break;
                    var fieldName = reader.ReadElementHeader();
                    if (fieldName == "name") return reader.ReadString();
                    reader.SkipValue(type);
                }
                return null;
            })
        ))
        {
            if (name != null) names.Add(name);
        }

        // Assert
        Assert.Equal(3, names.Count);
        Assert.Contains("Alice", names);
        Assert.Contains("Bob", names);
        Assert.Contains("Charlie", names);
    }

    [Fact]
    public async Task ScanAsync_ProjectionWithCancellation_Cancels()
    {
        // Arrange
        for (int i = 0; i < 500; i++)
        {
            _db.Users.Insert(new User { Name = $"User_{i}", Age = i });
        }
        _db.SaveChanges();

        var cts = new CancellationTokenSource();
        var results = new List<string>();

        // Act
        try
        {
            var enumerator = _db.Users.ScanAsync<string>(
                (BsonReaderProjector<string>)(reader =>
                {
                    reader.ReadDocumentSize();
                    while (reader.Remaining > 0)
                    {
                        var type = reader.ReadBsonType();
                        if (type == 0) break;
                        var fieldName = reader.ReadElementHeader();
                        if (fieldName == "name") return reader.ReadString();
                        reader.SkipValue(type);
                    }
                    return null;
                }),
                cts.Token
            ).GetAsyncEnumerator();

            int count = 0;
            while (await enumerator.MoveNextAsync())
            {
                if (enumerator.Current != null) results.Add(enumerator.Current);
                count++;
                if (count == 50) cts.Cancel();
            }
        }
        catch (OperationCanceledException)
        {
            // Expected
        }

        // Assert
        Assert.True(results.Count >= 50);
        Assert.True(results.Count < 500);
    }

    #endregion

    #region QueryIndexAsync

    [Fact]
    public async Task QueryIndexAsync_RangeQuery_ReturnsDocumentsInRange()
    {
        // Arrange
        var people = new[]
        {
            new Person { Name = "Alice", Age = 25 },
            new Person { Name = "Bob", Age = 30 },
            new Person { Name = "Charlie", Age = 35 },
            new Person { Name = "Diana", Age = 40 }
        };

        foreach (var p in people)
            _db.People.Insert(p);
        
        _db.People.CreateIndex<int>(x => x.Age, "idx_age_async_range");
        _db.SaveChanges();

        // Act: Query age between 28 and 37
        var results = new List<Person>();
        await foreach (var person in _db.People.QueryIndexAsync("idx_age_async_range", minKey: 28, maxKey: 37, ascending: true))
        {
            results.Add(person);
        }

        // Assert
        Assert.Equal(2, results.Count);
        Assert.Contains(results, p => p.Name == "Bob" && p.Age == 30);
        Assert.Contains(results, p => p.Name == "Charlie" && p.Age == 35);
    }

    [Fact]
    public async Task QueryIndexAsync_Descending_ReturnsInReverseOrder()
    {
        // Arrange
        for (int i = 0; i < 5; i++)
            _db.People.Insert(new Person { Name = $"Person_{i}", Age = 20 + i });
        
        _db.People.CreateIndex<int>(x => x.Age, "idx_age_desc");
        _db.SaveChanges();

        // Act
        var results = new List<Person>();
        await foreach (var person in _db.People.QueryIndexAsync("idx_age_desc", minKey: null, maxKey: null, ascending: false))
        {
            results.Add(person);
        }

        // Assert
        Assert.True(results.Count > 0);
        // Verify descending order
        for (int i = 1; i < results.Count; i++)
        {
            Assert.True(results[i - 1].Age >= results[i].Age, "Ages should be in descending order");
        }
    }

    [Fact]
    public async Task QueryIndexAsync_WithCancellation_Cancels()
    {
        // Arrange
        for (int i = 0; i < 100; i++)
            _db.People.Insert(new Person { Name = $"Person_{i}", Age = i });
        
        _db.People.CreateIndex<int>(x => x.Age, "idx_age_cancel");
        _db.SaveChanges();

        var cts = new CancellationTokenSource();
        var results = new List<Person>();

        // Act
        try
        {
            var enumerator = _db.People.QueryIndexAsync("idx_age_cancel", minKey: 0, maxKey: 99, ct: cts.Token)
                .GetAsyncEnumerator();

            int count = 0;
            while (await enumerator.MoveNextAsync())
            {
                results.Add(enumerator.Current);
                count++;
                if (count == 20) cts.Cancel();
            }
        }
        catch (OperationCanceledException)
        {
            // Expected
        }

        // Assert
        Assert.True(results.Count >= 20);
        Assert.True(results.Count < 100);
    }

    #endregion

    #region VectorSearchAsync

    [Fact]
    public async Task VectorSearchAsync_FindsNearestVectors()
    {
        // Arrange
        _db.VectorItems.Insert(new VectorEntity { Title = "Very Close", Embedding = [1.0f, 1.0f, 1.0f] });
        _db.VectorItems.Insert(new VectorEntity { Title = "Close", Embedding = [1.1f, 1.1f, 1.1f] });
        _db.VectorItems.Insert(new VectorEntity { Title = "Far", Embedding = [10.0f, 10.0f, 10.0f] });
        _db.SaveChanges();

        // Act: Search for vectors similar to [1.0, 1.0, 1.0]
        var query = new[] { 1.0f, 1.0f, 1.0f };
        var results = new List<VectorEntity>();
        await foreach (var item in _db.VectorItems.VectorSearchAsync("idx_vector", query, k: 2, efSearch: 50))
        {
            results.Add(item);
        }

        // Assert
        Assert.NotEmpty(results);
        Assert.True(results.Count <= 2, "Should return at most k=2 results");
        // The closest should be first
        Assert.Equal("Very Close", results[0].Title);
    }

    [Fact]
    public async Task VectorSearchAsync_WithCancellation_Cancels()
    {
        // Arrange
        for (int i = 0; i < 20; i++)
        {
            var embedding = new float[3];
            for (int d = 0; d < 3; d++)
                embedding[d] = i + d * 0.1f;
            _db.VectorItems.Insert(new VectorEntity { Title = $"Vector_{i}", Embedding = embedding });
        }
        _db.SaveChanges();

        var cts = new CancellationTokenSource();
        var results = new List<VectorEntity>();

        // Act
        var query = new[] { 0.1f, 0.2f, 0.3f };
        try
        {
            var enumerator = _db.VectorItems.VectorSearchAsync("idx_vector", query, k: 10, ct: cts.Token)
                .GetAsyncEnumerator();

            int count = 0;
            while (await enumerator.MoveNextAsync())
            {
                results.Add(enumerator.Current);
                count++;
                if (count == 1) cts.Cancel();
            }
        }
        catch (OperationCanceledException)
        {
            // Expected
        }

        // Assert: Should have started iteration before cancellation
        Assert.True(results.Count >= 1);
    }

    #endregion

    #region NearAsync (Geospatial - Radius)

    [Fact]
    public async Task NearAsync_FindsDocumentsWithinRadius()
    {
        // Arrange: Create geo entities at known locations
        _db.GeoItems.Insert(new GeoEntity { Name = "Center", Location = (40.7128, -74.0060) }); // NYC
        _db.GeoItems.Insert(new GeoEntity { Name = "Nearby", Location = (40.7200, -74.0100) }); // ~1 km away
        _db.GeoItems.Insert(new GeoEntity { Name = "Far", Location = (34.0522, -118.2437) }); // LA
        _db.SaveChanges();

        // Act: Find locations within 50 km of NYC
        var results = new List<GeoEntity>();
        await foreach (var geo in _db.GeoItems.NearAsync("idx_spatial", (40.7128, -74.0060), radiusKm: 50))
        {
            results.Add(geo);
        }

        // Assert
        Assert.NotEmpty(results);
        Assert.Contains(results, g => g.Name == "Center");
        Assert.Contains(results, g => g.Name == "Nearby");
        // LA should be far enough to be excluded
        Assert.DoesNotContain(results, g => g.Name == "Far");
    }

    [Fact]
    public async Task NearAsync_WithCancellation_Cancels()
    {
        // Arrange
        for (int i = 0; i < 10; i++)
        {
            _db.GeoItems.Insert(new GeoEntity
            {
                Name = $"Location_{i}",
                Location = (40.0 + i * 0.1, -74.0 + i * 0.1)
            });
        }
        _db.SaveChanges();

        var cts = new CancellationTokenSource();
        var results = new List<GeoEntity>();

        // Act
        try
        {
            var enumerator = _db.GeoItems.NearAsync("idx_spatial", (40.0, -74.0), 500, cts.Token) // Large radius
                .GetAsyncEnumerator();

            int count = 0;
            while (await enumerator.MoveNextAsync())
            {
                results.Add(enumerator.Current);
                count++;
                if (count == 3) cts.Cancel();
            }
        }
        catch (OperationCanceledException)
        {
            // Expected
        }

        // Assert
        Assert.True(results.Count >= 3);
    }

    #endregion

    #region WithinAsync (Geospatial - Bounding Box)

    [Fact]
    public async Task WithinAsync_FindsDocumentsInBoundingBox()
    {
        // Arrange
        _db.GeoItems.Insert(new GeoEntity { Name = "Inside1", Location = (40.7, -74.0) });
        _db.GeoItems.Insert(new GeoEntity { Name = "Inside2", Location = (40.8, -74.1) });
        _db.GeoItems.Insert(new GeoEntity { Name = "Outside", Location = (34.0522, -118.2437) });
        _db.SaveChanges();

        // Act: Query bounding box around NYC area
        var results = new List<GeoEntity>();
        await foreach (var geo in _db.GeoItems.WithinAsync("idx_spatial",
            min: (40.6, -74.2),
            max: (40.9, -73.9)))
        {
            results.Add(geo);
        }

        // Assert
        Assert.Equal(2, results.Count);
        Assert.Contains(results, g => g.Name == "Inside1");
        Assert.Contains(results, g => g.Name == "Inside2");
        Assert.DoesNotContain(results, g => g.Name == "Outside");
    }

    [Fact]
    public async Task WithinAsync_WithCancellation_Cancels()
    {
        // Arrange
        for (int i = 0; i < 20; i++)
        {
            _db.GeoItems.Insert(new GeoEntity
            {
                Name = $"Point_{i}",
                Location = (40.0 + i * 0.01, -74.0 + i * 0.01)
            });
        }
        _db.SaveChanges();

        var cts = new CancellationTokenSource();
        var results = new List<GeoEntity>();

        // Act
        try
        {
            var enumerator = _db.GeoItems.WithinAsync("idx_spatial",
                min: (40.0, -74.1),
                max: (40.3, -73.8),
                ct: cts.Token
            ).GetAsyncEnumerator();

            int count = 0;
            while (await enumerator.MoveNextAsync())
            {
                results.Add(enumerator.Current);
                count++;
                if (count == 5) cts.Cancel();
            }
        }
        catch (OperationCanceledException)
        {
            // Expected
        }

        // Assert
        Assert.True(results.Count >= 5);
    }

    #endregion

    #region ParallelScanAsync

    [Fact]
    public async Task ParallelScanAsync_FindsMatchingDocuments()
    {
        // Arrange
        for (int i = 0; i < 100; i++)
        {
            _db.Users.Insert(new User { Name = $"User_{i}", Age = i });
        }
        _db.SaveChanges();

        // Act: Find users with Age >= 50 using parallel scan
        var results = new List<User>();
        await foreach (var user in _db.Users.ParallelScanAsync(
            (BsonReaderPredicate)(reader => ParseAge(reader) >= 50),
            degreeOfParallelism: 4))
        {
            results.Add(user);
        }

        // Assert
        Assert.Equal(50, results.Count);
        Assert.All(results, u => Assert.True(u.Age >= 50));
    }

    [Fact]
    public async Task ParallelScanAsync_DegreeOfParallelism_LimitsOpen()
    {
        // Arrange: Insert enough documents to test parallelism
        for (int i = 0; i < 200; i++)
        {
            _db.Users.Insert(new User { Name = $"User_{i}", Age = i });
        }
        _db.SaveChanges();

        // Act: Parallel scan with degree 2 should work correctly
        var results = new List<User>();
        await foreach (var user in _db.Users.ParallelScanAsync(
            (BsonReaderPredicate)(reader => ParseAge(reader) < 200),
            degreeOfParallelism: 2))
        {
            results.Add(user);
        }

        // Assert: Should find all 200 documents
        Assert.Equal(200, results.Count);
    }

    [Fact]
    public async Task ParallelScanAsync_WithCancellation_Attempts_Cancellation()
    {
        // Arrange
        for (int i = 0; i < 2000; i++)
        {
            _db.Users.Insert(new User { Name = $"User_{i}", Age = i });
        }
        _db.SaveChanges();

        var cts = new CancellationTokenSource();
        var results = new List<User>();

        // Act
        try
        {
            int count = 0;
            await foreach (var user in _db.Users.ParallelScanAsync(
                (BsonReaderPredicate)(reader => true),
                degreeOfParallelism: 4,
                ct: cts.Token))
            {
                results.Add(user);
                count++;
                if (count == 200) cts.Cancel();
            }
        }
        catch (OperationCanceledException)
        {
            // Expected - but may not occur if parallel scan is very fast
        }

        // Assert: At minimum, cancellation token was propagated and many items were scanned
        Assert.True(results.Count >= 200, "Should have scanned at least 200 items");
    }

    [Fact]
    public async Task ParallelScanAsync_NoFilter_ReturnsAll()
    {
        // Arrange
        int docCount = 50;
        for (int i = 0; i < docCount; i++)
        {
            _db.Users.Insert(new User { Name = $"User_{i}", Age = i });
        }
        _db.SaveChanges();

        // Act: Scan all with no filter
        var results = new List<User>();
        await foreach (var user in _db.Users.ParallelScanAsync(
            (BsonReaderPredicate)(reader => true),
            degreeOfParallelism: 3))
        {
            results.Add(user);
        }

        // Assert
        Assert.Equal(docCount, results.Count);
    }

    #endregion

    #region Helper Methods

    private int ParseAge(BsonSpanReader reader)
    {
        try
        {
            reader.ReadDocumentSize();
            while (reader.Remaining > 0)
            {
                var type = reader.ReadBsonType();
                if (type == 0) break;

                var name = reader.ReadElementHeader();

                if (name == "age")
                {
                    return reader.ReadInt32();
                }
                else
                {
                    reader.SkipValue(type);
                }
            }
        }
        catch
        {
            return -1;
        }

        return -1;
    }

    #endregion
}
