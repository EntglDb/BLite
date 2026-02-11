using BLite.Bson;
using BLite.Core;
using BLite.Core.Collections;

namespace BLite.Tests;

/// <summary>
/// Test context with manual collection initialization
/// (Source Generator will automate this in the future)
/// </summary>
public class TestDbContext : DocumentDbContext
{
    public DocumentCollection<ObjectId,User> Users { get; private set; } = null!;
    
    public TestDbContext(string databasePath) : base(databasePath)
    {
        Users = CreateCollection(new UserMapper());
    }
}
