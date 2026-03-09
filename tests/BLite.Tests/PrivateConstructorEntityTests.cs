using BLite.Bson;
using BLite.Shared;

namespace BLite.Tests;

/// <summary>
/// Verifies that the source generator correctly handles an entity that:
///   - has a private constructor (no public parameterless ctor)
///   - exposes ONLY private setters on every property
///   - contains scalar base types, a nested object with private setters,
///     a collection of nested objects with private setters, and a primitive collection.
///
/// The generator must use RuntimeHelpers.GetUninitializedObject + Expression-Tree
/// setters for the root entity AND for every nested type.
/// </summary>
public class PrivateConstructorEntityTests : IDisposable
{
    private readonly string _dbPath;
    private readonly TestDbContext _db;

    public PrivateConstructorEntityTests()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"test_private_ctor_{Guid.NewGuid()}.db");
        _db = new TestDbContext(_dbPath);
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    private static FullyPrivateEntity BuildSample(
        string name = "Alice",
        int age = 30,
        bool isActive = true,
        decimal score = 9.99m,
        DateTime? createdAt = null,
        Guid? externalId = null)
    {
        var address = PrivateAddress.Create("Via Roma 1", "Milano", "Italy", 20121);
        var entity  = FullyPrivateEntity.Create(
            name, age, isActive, score,
            createdAt  ?? new DateTime(2025, 6, 15, 10, 30, 0, DateTimeKind.Utc),
            externalId ?? Guid.Parse("a1b2c3d4-e5f6-7890-abcd-ef1234567890"),
            address);

        entity.AddTag(PrivateTag.Create("priority", "high"));
        entity.AddTag(PrivateTag.Create("env", "prod"));
        entity.AddNote("First note");
        entity.AddNote("Second note");

        return entity;
    }

    // -----------------------------------------------------------------------
    // Round-trip: scalar base types
    // -----------------------------------------------------------------------

    [Fact]
    public void ScalarProperties_RoundTrip()
    {
        var original = BuildSample(
            name       : "Bob",
            age        : 42,
            isActive   : false,
            score      : 3.14m,
            createdAt  : new DateTime(2024, 1, 1, 0, 0, 0, DateTimeKind.Utc),
            externalId : Guid.Parse("00000000-0000-0000-0000-000000000001"));

        var id = _db.FullyPrivateEntities.Insert(original);
        _db.SaveChanges();

        var retrieved = _db.FullyPrivateEntities.FindById(id);

        Assert.NotNull(retrieved);
        Assert.Equal(id,        retrieved.Id);
        Assert.Equal("Bob",     retrieved.Name);
        Assert.Equal(42,        retrieved.Age);
        Assert.False(           retrieved.IsActive);
        Assert.Equal(3.14m,     retrieved.Score);
        Assert.Equal(new DateTime(2024, 1, 1, 0, 0, 0, DateTimeKind.Utc), retrieved.CreatedAt);
        Assert.Equal(Guid.Parse("00000000-0000-0000-0000-000000000001"), retrieved.ExternalId);
    }

    // -----------------------------------------------------------------------
    // Round-trip: nested object with private setters
    // -----------------------------------------------------------------------

    [Fact]
    public void NestedObject_WithPrivateSetters_RoundTrip()
    {
        var entity = BuildSample();
        var id = _db.FullyPrivateEntities.Insert(entity);
        _db.SaveChanges();

        var retrieved = _db.FullyPrivateEntities.FindById(id);

        Assert.NotNull(retrieved);
        Assert.NotNull(retrieved.HomeAddress);
        Assert.Equal("Via Roma 1", retrieved.HomeAddress.Street);
        Assert.Equal("Milano",     retrieved.HomeAddress.City);
        Assert.Equal("Italy",      retrieved.HomeAddress.Country);
        Assert.Equal(20121,        retrieved.HomeAddress.ZipCode);
    }

    // -----------------------------------------------------------------------
    // Round-trip: collection of nested objects with private setters
    // -----------------------------------------------------------------------

    [Fact]
    public void NestedCollection_WithPrivateSetters_RoundTrip()
    {
        var entity = BuildSample();
        var id = _db.FullyPrivateEntities.Insert(entity);
        _db.SaveChanges();

        var retrieved = _db.FullyPrivateEntities.FindById(id);

        Assert.NotNull(retrieved);
        Assert.NotNull(retrieved.Tags);
        Assert.Equal(2, retrieved.Tags.Count);
        Assert.Contains(retrieved.Tags, t => t.Key == "priority" && t.Value == "high");
        Assert.Contains(retrieved.Tags, t => t.Key == "env"      && t.Value == "prod");
    }

    // -----------------------------------------------------------------------
    // Round-trip: primitive collection
    // -----------------------------------------------------------------------

    [Fact]
    public void PrimitiveCollection_RoundTrip()
    {
        var entity = BuildSample();
        var id = _db.FullyPrivateEntities.Insert(entity);
        _db.SaveChanges();

        var retrieved = _db.FullyPrivateEntities.FindById(id);

        Assert.NotNull(retrieved);
        Assert.NotNull(retrieved.Notes);
        Assert.Equal(2, retrieved.Notes.Count);
        Assert.Contains("First note",  retrieved.Notes);
        Assert.Contains("Second note", retrieved.Notes);
    }

    // -----------------------------------------------------------------------
    // All properties in a single round-trip
    // -----------------------------------------------------------------------

    [Fact]
    public void AllProperties_FullRoundTrip()
    {
        var createdAt  = new DateTime(2025, 3, 9, 12, 0, 0, DateTimeKind.Utc);
        var externalId = Guid.NewGuid();

        var address = PrivateAddress.Create("Corso Sempione 10", "Torino", "Italy", 10121);
        var entity  = FullyPrivateEntity.Create("Charlie", 28, true, 100.5m, createdAt, externalId, address);
        entity.AddTag(PrivateTag.Create("tier", "gold"));
        entity.AddTag(PrivateTag.Create("region", "eu-west"));
        entity.AddTag(PrivateTag.Create("feature", "beta"));
        entity.AddNote("note A");
        entity.AddNote("note B");
        entity.AddNote("note C");

        var id = _db.FullyPrivateEntities.Insert(entity);
        _db.SaveChanges();

        var r = _db.FullyPrivateEntities.FindById(id);

        Assert.NotNull(r);

        // Scalars
        Assert.Equal(id,          r.Id);
        Assert.Equal("Charlie",   r.Name);
        Assert.Equal(28,          r.Age);
        Assert.True(              r.IsActive);
        Assert.Equal(100.5m,      r.Score);
        Assert.Equal(createdAt,   r.CreatedAt);
        Assert.Equal(externalId,  r.ExternalId);

        // Nested object
        Assert.NotNull(r.HomeAddress);
        Assert.Equal("Corso Sempione 10", r.HomeAddress.Street);
        Assert.Equal("Torino",            r.HomeAddress.City);
        Assert.Equal("Italy",             r.HomeAddress.Country);
        Assert.Equal(10121,               r.HomeAddress.ZipCode);

        // Nested collection
        Assert.Equal(3, r.Tags.Count);
        Assert.Contains(r.Tags, t => t.Key == "tier"    && t.Value == "gold");
        Assert.Contains(r.Tags, t => t.Key == "region"  && t.Value == "eu-west");
        Assert.Contains(r.Tags, t => t.Key == "feature" && t.Value == "beta");

        // Primitive collection
        Assert.Equal(3, r.Notes.Count);
        Assert.Contains("note A", r.Notes);
        Assert.Contains("note B", r.Notes);
        Assert.Contains("note C", r.Notes);
    }

    // -----------------------------------------------------------------------
    // Query by scalar property — also verifies full deserialization of the
    // matched documents (nested object + both collections with private setters)
    // -----------------------------------------------------------------------

    [Fact]
    public void Query_ByScalarProperty_Works()
    {
        // All three entities are fully populated (tags, notes, nested address)
        // so that the Find() result exercises complete deserialization, not just
        // the scalar fields used by the predicate.
        var addrYoung  = PrivateAddress.Create("Via Verde 1",  "Roma",   "IT", 11111);
        var addrMiddle = PrivateAddress.Create("Via Blu 2",    "Milano", "IT", 22222);
        var addrOld    = PrivateAddress.Create("Via Rossa 3",  "Napoli", "IT", 33333);

        var e1 = FullyPrivateEntity.Create("Dave",  20, true, 1.5m, DateTime.UtcNow, Guid.NewGuid(), addrYoung);
        // Dave should NOT appear in the query result

        var e2 = FullyPrivateEntity.Create("Eve",   35, true, 9.99m, DateTime.UtcNow, Guid.NewGuid(), addrMiddle);
        e2.AddTag(PrivateTag.Create("role", "user"));
        e2.AddTag(PrivateTag.Create("tier", "gold"));
        e2.AddNote("eve note 1");
        e2.AddNote("eve note 2");

        var e3 = FullyPrivateEntity.Create("Frank", 50, false, 3.14m, DateTime.UtcNow, Guid.NewGuid(), addrOld);
        e3.AddTag(PrivateTag.Create("role", "admin"));
        e3.AddNote("frank note");

        _db.FullyPrivateEntities.Insert(e1);
        _db.FullyPrivateEntities.Insert(e2);
        _db.FullyPrivateEntities.Insert(e3);
        _db.SaveChanges();

        var adults = _db.FullyPrivateEntities.Find(x => x.Age >= 35).ToList();

        Assert.Equal(2, adults.Count);

        var eve = adults.Single(e => e.Name == "Eve");

        // Scalars
        Assert.Equal(35,     eve.Age);
        Assert.True(         eve.IsActive);
        Assert.Equal(9.99m,  eve.Score);

        // Nested object with private setters
        Assert.NotNull(eve.HomeAddress);
        Assert.Equal("Via Blu 2",  eve.HomeAddress.Street);
        Assert.Equal("Milano",     eve.HomeAddress.City);
        Assert.Equal("IT",         eve.HomeAddress.Country);
        Assert.Equal(22222,        eve.HomeAddress.ZipCode);

        // Collection of nested objects with private setters
        Assert.Equal(2, eve.Tags.Count);
        Assert.Contains(eve.Tags, t => t.Key == "role" && t.Value == "user");
        Assert.Contains(eve.Tags, t => t.Key == "tier" && t.Value == "gold");

        // Primitive collection
        Assert.Equal(2, eve.Notes.Count);
        Assert.Contains("eve note 1", eve.Notes);
        Assert.Contains("eve note 2", eve.Notes);

        var frank = adults.Single(e => e.Name == "Frank");

        Assert.Equal(50,     frank.Age);
        Assert.False(        frank.IsActive);
        Assert.Equal(3.14m,  frank.Score);

        Assert.NotNull(frank.HomeAddress);
        Assert.Equal("Via Rossa 3", frank.HomeAddress.Street);
        Assert.Equal("Napoli",      frank.HomeAddress.City);
        Assert.Equal(33333,         frank.HomeAddress.ZipCode);

        Assert.Single(frank.Tags);
        Assert.Equal("admin", frank.Tags[0].Value);

        Assert.Single(frank.Notes);
        Assert.Equal("frank note", frank.Notes[0]);
    }

    // -----------------------------------------------------------------------
    // Empty collections
    // -----------------------------------------------------------------------

    [Fact]
    public void EmptyCollections_RoundTrip()
    {
        var addr   = PrivateAddress.Create("Empty St", "EmptyCity", "XX", 0);
        var entity = FullyPrivateEntity.Create("NoTags", 1, true, 0m, DateTime.UtcNow, Guid.NewGuid(), addr);
        // no tags, no notes added

        var id = _db.FullyPrivateEntities.Insert(entity);
        _db.SaveChanges();

        var retrieved = _db.FullyPrivateEntities.FindById(id);

        Assert.NotNull(retrieved);
        Assert.NotNull(retrieved.Tags);
        Assert.Empty(retrieved.Tags);
        Assert.NotNull(retrieved.Notes);
        Assert.Empty(retrieved.Notes);
    }

    // -----------------------------------------------------------------------
    // Multiple documents + FindAll
    // -----------------------------------------------------------------------

    [Fact]
    public void MultipleDocuments_FindAll_Works()
    {
        var addr = PrivateAddress.Create("X", "Y", "Z", 1);

        for (var i = 1; i <= 5; i++)
        {
            var e = FullyPrivateEntity.Create($"Entity{i}", i * 10, i % 2 == 0, i * 1.5m, DateTime.UtcNow, Guid.NewGuid(), addr);
            e.AddTag(PrivateTag.Create("index", i.ToString()));
            _db.FullyPrivateEntities.Insert(e);
        }
        _db.SaveChanges();

        var all = _db.FullyPrivateEntities.FindAll().ToList();

        Assert.Equal(5, all.Count);
        for (var i = 1; i <= 5; i++)
        {
            var found = all.Single(x => x.Name == $"Entity{i}");
            Assert.Equal(i * 10,   found.Age);
            Assert.Equal(i * 1.5m, found.Score);
            Assert.Single(found.Tags);
            Assert.Equal(i.ToString(), found.Tags[0].Value);
        }
    }

    public void Dispose()
    {
        _db?.Dispose();
        //if (File.Exists(_dbPath))
        //    File.Delete(_dbPath);
    }
}
