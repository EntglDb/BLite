using BLite.Shared;

namespace BLite.Tests;

public class DbContextInheritanceTests : IDisposable
{
    private readonly string _dbPath;
    private readonly TestExtendedDbContext _db;

    public DbContextInheritanceTests()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"blite_inheritance_{Guid.NewGuid()}.db");
        _db = new TestExtendedDbContext(_dbPath);
    }

    public void Dispose()
    {
        _db.Dispose();
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
    }

    [Fact]
    public void ExtendedContext_Should_Initialize_Parent_Collections()
    {
        // Verify parent collections are initialized (from TestDbContext)
        Assert.NotNull(_db.Users);
        Assert.NotNull(_db.People);
        Assert.NotNull(_db.Products);
        Assert.NotNull(_db.AnnotatedUsers);
        Assert.NotNull(_db.ComplexDocuments);
        Assert.NotNull(_db.TestDocuments);
    }

    [Fact]
    public void ExtendedContext_Should_Initialize_Own_Collections()
    {
        // Verify extended context's own collection is initialized
        Assert.NotNull(_db.ExtendedEntities);
    }

    [Fact]
    public async Task ExtendedContext_Can_Use_Parent_Collections()
    {
        // Insert into parent collection
        var user = new User { Name = "TestUser", Age = 30 };
        await _db.Users.InsertAsync(user);
        await _db.SaveChangesAsync();

        // Verify we can read it back
        var retrieved = await _db.Users.FindByIdAsync(user.Id);
        Assert.NotNull(retrieved);
        Assert.Equal("TestUser", retrieved.Name);
        Assert.Equal(30, retrieved.Age);
    }

    [Fact]
    public async Task ExtendedContext_Can_Use_Own_Collections()
    {
        // Insert into extended collection
        var entity = new ExtendedEntity 
        { 
            Id = 1, 
            Description = "Test Extended Entity",
            CreatedAt = DateTime.UtcNow
        };
        await _db.ExtendedEntities.InsertAsync(entity);
        await _db.SaveChangesAsync();

        // Verify we can read it back
        var retrieved = await _db.ExtendedEntities.FindByIdAsync(1);
        Assert.NotNull(retrieved);
        Assert.Equal("Test Extended Entity", retrieved.Description);
    }

    [Fact]
    public async Task ExtendedContext_Can_Use_Both_Parent_And_Own_Collections()
    {
        // Insert into parent collection
        var person = new Person { Id = 100, Name = "John", Age = 25 };
        await _db.People.InsertAsync(person);
        // Insert into extended collection
        var extended = new ExtendedEntity 
        { 
            Id = 200, 
            Description = "Related to John",
            CreatedAt = DateTime.UtcNow
        };
        await _db.ExtendedEntities.InsertAsync(extended);
        
        await _db.SaveChangesAsync();

        // Verify both
        var retrievedPerson = await _db.People.FindByIdAsync(100);
        var retrievedExtended = await _db.ExtendedEntities.FindByIdAsync(200);

        Assert.NotNull(retrievedPerson);
        Assert.Equal("John", retrievedPerson.Name);
        
        Assert.NotNull(retrievedExtended);
        Assert.Equal("Related to John", retrievedExtended.Description);
    }
}
