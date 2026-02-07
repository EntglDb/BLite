using DocumentDb.Bson;
using DocumentDb.Core;
using DocumentDb.Core.Collections;

namespace DocumentDb.Tests;

/// <summary>
/// Test context with manual collection initialization
/// (Source Generator will automate this in the future)
/// </summary>
public class TestDbContext : DocumentDbContext
{
    public DocumentCollection<User> Users { get; private set; } = null!;
    
    public TestDbContext(string databasePath) : base(databasePath)
    {
    }
    
    protected override void InitializeCollections()
    {
        // Manually initialize collections
        // Future: Source Generator will auto-generate this
        Users = CreateCollection(new UserMapper());
    }
}
