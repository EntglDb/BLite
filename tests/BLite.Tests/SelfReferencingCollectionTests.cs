using BLite.Bson;
using BLite.Shared;

namespace BLite.Tests;

/// <summary>
/// Tests for entities with self-referencing collections (e.g., Item containing List&lt;Item&gt;)
/// to ensure schema generation and CRUD operations don't cause stack overflow.
/// </summary>
public class SelfReferencingCollectionTests : IDisposable
{
    private readonly string _dbPath;

    public SelfReferencingCollectionTests()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"blite_self_ref_{Guid.NewGuid():N}.db");
    }

    public void Dispose()
    {
        if (File.Exists(_dbPath))
            File.Delete(_dbPath);
    }

    [Fact]
    public void Schema_Generation_For_SelfReferencing_Entity_Does_Not_Overflow()
    {
        // This test ensures that schema generation for an entity with self-referencing collections
        // (like TreeNode with Children of type List<TreeNode>) does not cause stack overflow
        using var db = new TestDbContext(_dbPath);

        // If schema generation causes overflow, this line will throw StackOverflowException
        var indexes = db.TreeNodes.GetIndexes();
        Assert.NotNull(indexes);
    }

    [Fact]
    public void Insert_SelfReferencing_Entity_With_Empty_Children_Succeeds()
    {
        using var db = new TestDbContext(_dbPath);

        var root = new TreeNode
        {
            Id = ObjectId.NewObjectId(),
            Name = "Root",
            Value = 100
        };

        db.TreeNodes.Insert(root);

        var retrieved = db.TreeNodes.FindById(root.Id);
        Assert.NotNull(retrieved);
        Assert.Equal("Root", retrieved.Name);
        Assert.Empty(retrieved.Children);
    }

    [Fact]
    public void Insert_SelfReferencing_Entity_With_Nested_Children_Succeeds()
    {
        using var db = new TestDbContext(_dbPath);

        var child1 = new TreeNode
        {
            Id = ObjectId.NewObjectId(),
            Name = "Child1",
            Value = 10
        };

        var child2 = new TreeNode
        {
            Id = ObjectId.NewObjectId(),
            Name = "Child2",
            Value = 20
        };

        var root = new TreeNode
        {
            Id = ObjectId.NewObjectId(),
            Name = "Root",
            Value = 100,
            Children = new List<TreeNode> { child1, child2 }
        };

        db.TreeNodes.Insert(root);

        var retrieved = db.TreeNodes.FindById(root.Id);
        Assert.NotNull(retrieved);
        Assert.Equal("Root", retrieved.Name);
        Assert.Equal(2, retrieved.Children.Count);
        Assert.Contains(retrieved.Children, c => c.Name == "Child1");
        Assert.Contains(retrieved.Children, c => c.Name == "Child2");
    }

    [Fact]
    public void Insert_SelfReferencing_Entity_With_Deeply_Nested_Children_Succeeds()
    {
        using var db = new TestDbContext(_dbPath);

        var grandchild = new TreeNode
        {
            Id = ObjectId.NewObjectId(),
            Name = "Grandchild",
            Value = 1
        };

        var child = new TreeNode
        {
            Id = ObjectId.NewObjectId(),
            Name = "Child",
            Value = 10,
            Children = new List<TreeNode> { grandchild }
        };

        var root = new TreeNode
        {
            Id = ObjectId.NewObjectId(),
            Name = "Root",
            Value = 100,
            Children = new List<TreeNode> { child }
        };

        db.TreeNodes.Insert(root);

        var retrieved = db.TreeNodes.FindById(root.Id);
        Assert.NotNull(retrieved);
        Assert.Equal("Root", retrieved.Name);
        Assert.Single(retrieved.Children);
        Assert.Equal("Child", retrieved.Children[0].Name);
        Assert.Single(retrieved.Children[0].Children);
        Assert.Equal("Grandchild", retrieved.Children[0].Children[0].Name);
    }

    [Fact]
    public void Update_SelfReferencing_Entity_Preserves_Children()
    {
        using var db = new TestDbContext(_dbPath);

        var child = new TreeNode
        {
            Id = ObjectId.NewObjectId(),
            Name = "Child",
            Value = 10
        };

        var root = new TreeNode
        {
            Id = ObjectId.NewObjectId(),
            Name = "Root",
            Value = 100,
            Children = new List<TreeNode> { child }
        };

        db.TreeNodes.Insert(root);

        // Update root value
        root.Value = 200;
        db.TreeNodes.Update(root);

        var retrieved = db.TreeNodes.FindById(root.Id);
        Assert.NotNull(retrieved);
        Assert.Equal(200, retrieved.Value);
        Assert.Single(retrieved.Children);
        Assert.Equal("Child", retrieved.Children[0].Name);
    }

    [Fact]
    public void Query_SelfReferencing_Entity_Works()
    {
        using var db = new TestDbContext(_dbPath);

        var node1 = new TreeNode
        {
            Id = ObjectId.NewObjectId(),
            Name = "Node1",
            Value = 100
        };

        var node2 = new TreeNode
        {
            Id = ObjectId.NewObjectId(),
            Name = "Node2",
            Value = 200,
            Children = new List<TreeNode> { new TreeNode { Id = ObjectId.NewObjectId(), Name = "Child", Value = 50 } }
        };

        db.TreeNodes.Insert(node1);
        db.TreeNodes.Insert(node2);

        var results = db.TreeNodes.AsQueryable()
            .Where(n => n.Value >= 100)
            .ToList();

        Assert.Equal(2, results.Count);
    }
}
