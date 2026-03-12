using BLite.Bson;
using BLite.Core;
using BLite.Core.Collections;
using BLite.Shared;

namespace BLite.Tests;

/// <summary>
/// Minimal DbContext that does NOT override OnModelCreating.
/// The source generator should auto-discover entities from the
/// DocumentCollection properties and generate InitializeCollections.
/// </summary>
public partial class MinimalDbContext : DocumentDbContext
{
    public DocumentCollection<ObjectId, User> Users { get; set; } = null!;

    public MinimalDbContext(string databasePath) : base(databasePath)
    {
        InitializeCollections();
    }
}
