using BLite.Bson;
using BLite.Core;
using BLite.Core.Collections;
using System.Linq.Expressions;
using BLite.Core.Metadata;
using BLite.Core.Query;
using BLite.Shared;

namespace BLite.Tests;

/// <summary>
/// Properties renamed with <c>[Column]</c>, <c>[JsonPropertyName]</c> or <c>[BsonProperty]</c> are
/// registered, written, read, filtered, indexed and projected under the renamed field (#184).
/// </summary>
public class RenamedPropertyTests : IDisposable
{
    private readonly string _dbPath;
    private RenamedPropertyDbContext _db;

    public RenamedPropertyTests()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"renamed_{Guid.NewGuid():N}.db");
        _db = new RenamedPropertyDbContext(_dbPath);
    }

    public void Dispose()
    {
        _db.Dispose();
        foreach (var f in new[] { _dbPath, Path.ChangeExtension(_dbPath, ".wal") })
            try { if (File.Exists(f)) File.Delete(f); } catch { /* best-effort */ }
    }

    private static RenamedCustomer Customer(string name, int age, string city = "Rome") => new()
    {
        Name = name,
        Email = $"{name.ToLowerInvariant()}@example.com",
        Age = age,
        Nickname = $"nick-{name}",
        Both = $"both-{name}",
        Alias = $"alias-{name}",
        IsActive = age % 2 == 0,
        Address = new RenamedAddress { City = city, Zip = $"zip-{name}" },
        Tags = [new RenamedTag { Name = $"tag-{name}", Weight = age }]
    };

    private async Task<List<RenamedCustomer>> SeedAsync()
    {
        var customers = new List<RenamedCustomer>
        {
            Customer("Ada", 36, "London"),
            Customer("Grace", 45, "Arlington"),
            Customer("Linus", 28, "Helsinki"),
            Customer("Barbara", 50, "London"),
        };
        await _db.Customers.InsertBulkAsync(customers);
        return customers;
    }

    private void Reopen()
    {
        _db.Dispose();
        _db = new RenamedPropertyDbContext(_dbPath);
    }

    private static void AssertSame(RenamedCustomer expected, RenamedCustomer? actual)
    {
        Assert.NotNull(actual);
        Assert.Equal(expected.Id, actual.Id);
        Assert.Equal(expected.Name, actual.Name);
        Assert.Equal(expected.Email, actual.Email);
        Assert.Equal(expected.Age, actual.Age);
        Assert.Equal(expected.Nickname, actual.Nickname);
        Assert.Equal(expected.Both, actual.Both);
        Assert.Equal(expected.Alias, actual.Alias);
        Assert.Equal(expected.IsActive, actual.IsActive);
        Assert.NotNull(actual.Address);
        Assert.Equal(expected.Address!.City, actual.Address.City);
        Assert.Equal(expected.Address.Zip, actual.Address.Zip);
        Assert.Equal(expected.Tags.Select(t => (t.Name, t.Weight)), actual.Tags.Select(t => (t.Name, t.Weight)));
    }

    // ── Writes ──────────────────────────────────────────────────────────────

    [Fact]
    public async Task Insert_And_FindById_RoundTripEveryRenamedProperty()
    {
        var ada = Customer("Ada", 36);
        var id = await _db.Customers.InsertAsync(ada);

        AssertSame(ada, await _db.Customers.FindByIdAsync(id));
    }

    [Fact]
    public async Task Insert_WritesTheRenamedFieldNames()
    {
        var id = await _db.Customers.InsertAsync(Customer("Ada", 36));
        _db.Dispose();

        using (var engine = new BLiteEngine(_dbPath))
        {
            var doc = await engine.GetOrCreateCollection("renamed_customers").FindByIdAsync(new BsonId(id));
            Assert.NotNull(doc);
            Assert.Equal(
                new[] { "_id", "active", "by_json", "displayname", "full_name", "home", "labels", "mail", "name", "years" },
                doc.EnumerateFields().Select(f => f.Name).Where(n => n != "_v") // schema version
                    .OrderBy(n => n, StringComparer.Ordinal));

            var home = doc.GetValue("home").AsDocument;
            Assert.Equal(new[] { "town", "zip_code" }, home.EnumerateFields().Select(f => f.Name).OrderBy(n => n));
            var label = doc.GetValue("labels").AsArray[0].AsDocument;
            Assert.Equal(new[] { "label", "weight" }, label.EnumerateFields().Select(f => f.Name).OrderBy(n => n));
        }

        _db = new RenamedPropertyDbContext(_dbPath);
    }

    [Fact]
    public async Task Update_And_Delete_UseTheRenamedFields()
    {
        var customers = await SeedAsync();
        var grace = customers[1];

        grace.Name = "Grace Hopper";
        grace.Email = "hopper@example.com";
        grace.Address!.City = "New York";
        Assert.True(await _db.Customers.UpdateAsync(grace));

        AssertSame(grace, await _db.Customers.FindByIdAsync(grace.Id));
        Assert.Single(_db.Customers.AsQueryable().Where(c => c.Email == "hopper@example.com").ToList());
        Assert.Empty(_db.Customers.AsQueryable().Where(c => c.Email == "grace@example.com").ToList());
        Assert.Single(_db.Customers.AsQueryable().Where(c => c.Address!.City == "New York").ToList());

        Assert.True(await _db.Customers.DeleteAsync(grace.Id));
        Assert.Null(await _db.Customers.FindByIdAsync(grace.Id));
        Assert.Empty(_db.Customers.AsQueryable().Where(c => c.Email == "hopper@example.com").ToList());
        Assert.Equal(3, _db.Customers.AsQueryable().Count());
    }

    // ── Queries ─────────────────────────────────────────────────────────────

    [Fact]
    public async Task Where_OnRenamedProperties_ScansTheRenamedFields()
    {
        await SeedAsync();
        var q = _db.Customers.AsQueryable();
        var names = new[] { "Ada", "Linus" };

        Assert.Equal(["Ada"], q.Where(c => c.Name == "Ada").Select(c => c.Name).ToList());
        Assert.Equal(["Barbara", "Grace"], q.Where(c => c.Age > 40).ToList().Select(c => c.Name).OrderBy(n => n));
        Assert.Equal(["Ada", "Barbara", "Linus"], q.Where(c => c.IsActive).ToList().Select(c => c.Name).OrderBy(n => n));
        Assert.Equal(["Grace"], q.Where(c => !c.IsActive).ToList().Select(c => c.Name));
        Assert.Equal(["Barbara"], q.Where(c => c.Name.StartsWith("B")).ToList().Select(c => c.Name));
        Assert.Equal(["Ada", "Linus"], q.Where(c => names.Contains(c.Name)).ToList().Select(c => c.Name).OrderBy(n => n));
        Assert.Equal(["Grace"], q.Where(c => c.Nickname == "nick-Grace").ToList().Select(c => c.Name));
        Assert.Equal(["Linus"], q.Where(c => c.Both == "both-Linus").ToList().Select(c => c.Name));
        // Alias is stored in "name", the conventional field of Name.
        Assert.Equal(["Ada"], q.Where(c => c.Alias == "alias-Ada").ToList().Select(c => c.Name));
        Assert.Empty(q.Where(c => c.Alias == "Ada").ToList());
    }

    [Fact]
    public async Task FindAsync_And_FindOneAsync_OnRenamedProperty()
    {
        await SeedAsync();

        var found = new List<RenamedCustomer>();
        await foreach (var c in _db.Customers.FindAsync(c => c.Age < 40))
            found.Add(c);
        Assert.Equal(["Ada", "Linus"], found.Select(c => c.Name).OrderBy(n => n));

        var one = await _db.Customers.FindOneAsync(c => c.Name == "Grace");
        Assert.NotNull(one);
        Assert.Equal(45, one.Age);
    }

    [Fact]
    public async Task Where_OnRenamedId_FindsTheDocument()
    {
        var customers = await SeedAsync();
        var id = customers[2].Id;

        var found = _db.Customers.AsQueryable().Where(c => c.Id == id).ToList();
        Assert.Single(found);
        Assert.Equal("Linus", found[0].Name);
    }

    [Fact]
    public async Task Where_OnIndexedRenamedProperty_UsesTheIndex()
    {
        await SeedAsync();

        Assert.Contains(_db.Customers.GetIndexes(), i => i.PropertyPaths.SequenceEqual(["Email"]));
        Assert.Equal(["Grace"], _db.Customers.AsQueryable().Where(c => c.Email == "grace@example.com").ToList().Select(c => c.Name));
        Assert.Equal(["Ada", "Barbara"],
            _db.Customers.AsQueryable().Where(c => c.Email.CompareTo("b") < 0 || c.Email.StartsWith("barb")).ToList()
                .Select(c => c.Name).OrderBy(n => n));
    }

    [Fact]
    public async Task Where_OnNestedRenamedProperty()
    {
        await SeedAsync();
        var q = _db.Customers.AsQueryable();

        Assert.Contains(_db.Customers.GetIndexes(), i => i.PropertyPaths.SequenceEqual(["Address.City"]));
        Assert.Equal(["Ada", "Barbara"], q.Where(c => c.Address!.City == "London").ToList().Select(c => c.Name).OrderBy(n => n));
        Assert.Equal(["Linus"], q.Where(c => c.Address!.Zip == "zip-Linus").ToList().Select(c => c.Name));
        Assert.Equal(["Grace"], q.Where(c => c.Tags.Any(t => t.Name == "tag-Grace")).ToList().Select(c => c.Name));
    }

    [Fact]
    public async Task OrderBy_OnRenamedProperty()
    {
        await SeedAsync();
        var q = _db.Customers.AsQueryable();

        Assert.Equal(["Linus", "Ada", "Grace", "Barbara"], q.OrderBy(c => c.Age).Select(c => c.Name).ToList());
        Assert.Equal(["Linus", "Grace", "Barbara", "Ada"], q.OrderByDescending(c => c.Name).Select(c => c.Name).ToList());
        Assert.Equal(["Ada", "Barbara", "Grace", "Linus"], q.OrderBy(c => c.Email).Select(c => c.Name).ToList());
    }

    [Fact]
    public async Task Select_ProjectsTheRenamedFields()
    {
        await SeedAsync();

        var projected = _db.Customers.AsQueryable()
            .Select(c => new { c.Name, c.Email, c.Age, c.Nickname, c.Alias, c.IsActive })
            .ToList()
            .OrderBy(p => p.Name)
            .ToList();

        Assert.Equal(4, projected.Count);
        Assert.Equal("Ada", projected[0].Name);
        Assert.Equal("ada@example.com", projected[0].Email);
        Assert.Equal(36, projected[0].Age);
        Assert.Equal("nick-Ada", projected[0].Nickname);
        Assert.Equal("alias-Ada", projected[0].Alias);
        Assert.True(projected[0].IsActive);

        var filtered = _db.Customers.AsQueryable()
            .Where(c => c.Age > 40)
            .Select(c => new { c.Name, c.Age })
            .ToList();
        Assert.Equal(["Barbara", "Grace"], filtered.Select(p => p.Name).OrderBy(n => n));
    }

    [Fact]
    public async Task Count_WithPredicateOnRenamedProperty()
    {
        await SeedAsync();
        var q = _db.Customers.AsQueryable();

        Assert.Equal(2, q.Count(c => c.Age > 40));
        Assert.Equal(3, q.Where(c => c.IsActive).Count());
        Assert.Equal(1, q.Count(c => c.Email == "ada@example.com"));
        Assert.True(q.Any(c => c.Nickname == "nick-Linus"));
        Assert.False(q.Any(c => c.Name == "nobody"));
    }

    [Fact]
    public async Task Reopen_KeepsTheRenamedFieldsAndIndexes()
    {
        var customers = await SeedAsync();
        Reopen();

        AssertSame(customers[0], await _db.Customers.FindByIdAsync(customers[0].Id));
        Assert.Equal(["Grace"], _db.Customers.AsQueryable().Where(c => c.Email == "grace@example.com").ToList().Select(c => c.Name));
        Assert.Equal(["Barbara", "Grace"], _db.Customers.AsQueryable().Where(c => c.Age > 40).ToList().Select(c => c.Name).OrderBy(n => n));
        Assert.Equal(["Ada", "Barbara"], _db.Customers.AsQueryable().Where(c => c.Address!.City == "London").ToList().Select(c => c.Name).OrderBy(n => n));

        var dennis = Customer("Dennis", 70, "Murray Hill");
        await _db.Customers.InsertAsync(dennis);
        AssertSame(dennis, await _db.Customers.FindByIdAsync(dennis.Id));
    }

    // ── Key property not named Id ───────────────────────────────────────────

    [Fact]
    public async Task RenamedKeyNotNamedId_IsStoredAndQueriedAsId()
    {
        await _db.Keyed.InsertBulkAsync([
            new RenamedKeyed { Code = 7, Title = "seven" },
            new RenamedKeyed { Code = 9, Title = "nine" },
        ]);

        Assert.Equal("nine", (await _db.Keyed.FindByIdAsync(9))?.Title);
        Assert.Equal(["seven"], _db.Keyed.AsQueryable().Where(k => k.Code == 7).Select(k => k.Title).ToList());
        Assert.Equal([9], _db.Keyed.AsQueryable().Where(k => k.Title == "nine").Select(k => k.Code).ToList());
        Assert.Equal([7, 9], _db.Keyed.AsQueryable().Where(k => k.Code > 0).Select(k => k.Code).ToList().OrderBy(c => c));
    }

    // ── Registration ────────────────────────────────────────────────────────

    [Fact]
    public void Schema_UsesTheRenamedFieldNames()
    {
        var keys = BsonSchemaGenerator.FromType<RenamedCustomer>().GetAllKeys().ToHashSet();

        foreach (var key in new[] { "_id", "full_name", "mail", "years", "displayname", "by_json", "name", "active", "home", "labels", "town", "zip_code", "label", "weight" })
            Assert.Contains(key, keys);
        foreach (var key in new[] { "email", "age", "nickname", "both", "by_column", "alias", "isactive", "address", "tags", "city", "zip" })
            Assert.DoesNotContain(key, keys);
    }

    [Fact]
    public void RuntimeFieldNames_MatchTheGeneratedMappers()
    {
        // The generator names the fields Serialize writes; the runtime must name the same ones.
        AssertMatchesMapper<ObjectId, RenamedCustomer>();
        AssertMatchesMapper<int, RenamedKeyed>();
        AssertMatchesMapper<ObjectId, Reading>(); // [Column] names that swap two properties' fields

        static void AssertMatchesMapper<TId, TEntity>() where TEntity : class
        {
            var mapperType = typeof(RenamedPropertyDbContext).Assembly.GetTypes()
                .First(t => !t.IsAbstract && typeof(DocumentMapperBase<TId, TEntity>).IsAssignableFrom(t));
            var mapper = (DocumentMapperBase<TId, TEntity>)Activator.CreateInstance(mapperType)!;

            var compared = 0;
            foreach (var property in typeof(TEntity).GetProperties())
            {
                var generated = mapper.GetVerbatimFieldName(property.Name);
                if (generated == null) continue; // nested or collection: covered by the round trips
                Assert.Equal(generated, BsonFieldNames.OfRoot(typeof(TEntity), property));
                compared++;
            }
            Assert.True(compared > 1);
        }
    }

    [Fact]
    public void Projection_ReadsTheRenamedFields()
    {
        Expression<Func<RenamedCustomer, object>> select = c => new { c.Id, c.Name, c.Email, c.Alias };
        var analysis = ProjectionAnalyzer.Analyze(select);

        Assert.True(analysis.IsSimple);
        Assert.Equal(["_id", "full_name", "mail", "name"], analysis.Fields.Select(f => f.BsonName));
    }

    [Fact]
    public void TimestampFields_UseTheRenamedField()
    {
        var entity = new ModelBuilder().Entity<RenamedKeyed>()
            .HasTimeSeries(k => k.CreatedAt, TimeSpan.FromDays(1))
            .HasRetentionPolicy(p => p.OnField(k => k.CreatedAt));

        Assert.Equal("created_on", entity.TimeSeriesTtlField);
        Assert.Equal("created_on", entity.RetentionPolicy!.TimestampField);

        Expression<Func<RenamedCustomer, string>> nested = c => c.Address!.City;
        Assert.Equal("home.town", BsonFieldNames.PathOf(nested));
    }
}
