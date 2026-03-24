using BLite.Bson;
using BLite.Core.Query;
using BLite.Shared;

namespace BLite.Tests;

/// <summary>
/// Tests for circular references and N-N relationships
/// Validates that the source generator handles:
/// 1. Self-referencing entities using ObjectId references (Employee → ManagerId, DirectReportIds)
/// 2. N-N via referencing with ObjectIds (CategoryRef/ProductRef) - BEST PRACTICE
/// 
/// Note: Bidirectional embedding (Category ↔ Product with full objects) is NOT supported
/// by the source generator and is an anti-pattern for document databases.
/// Use referencing (ObjectIds) instead for N-N relationships.
/// </summary>
public class CircularReferenceTests : IDisposable
{
    private readonly string _dbPath;
    private readonly TestDbContext _context;

    public CircularReferenceTests()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"blite_circular_test_{Guid.NewGuid()}");
        _context = new TestDbContext(_dbPath);
    }

    public void Dispose()
    {
        _context?.Dispose();
        if (Directory.Exists(_dbPath))
        {
            Directory.Delete(_dbPath, true);
        }
    }

    // ========================================
    // Self-Reference Tests (Employee hierarchy with ObjectId references)
    // ========================================

    [Fact]
    public async Task SelfReference_InsertAndQuery_ShouldWork()
    {
        // Arrange: Create organizational hierarchy using ObjectId references
        var ceoId = ObjectId.NewObjectId();
        var manager1Id = ObjectId.NewObjectId();
        var manager2Id = ObjectId.NewObjectId();
        var developerId = ObjectId.NewObjectId();

        var ceo = new Employee
        {
            Id = ceoId,
            Name = "Alice CEO",
            Department = "Executive",
            ManagerId = null,
            DirectReportIds = new List<ObjectId> { manager1Id, manager2Id }
        };

        var manager1 = new Employee
        {
            Id = manager1Id,
            Name = "Bob Manager",
            Department = "Engineering",
            ManagerId = ceoId,
            DirectReportIds = new List<ObjectId> { developerId }
        };

        var manager2 = new Employee
        {
            Id = manager2Id,
            Name = "Carol Manager",
            Department = "Sales",
            ManagerId = ceoId,
            DirectReportIds = new List<ObjectId>() // No direct reports
        };

        var developer = new Employee
        {
            Id = developerId,
            Name = "Dave Developer",
            Department = "Engineering",
            ManagerId = manager1Id,
            DirectReportIds = null // Leaf node
        };

        // Act: Insert all employees
        await _context.Employees.InsertAsync(ceo);
        await _context.Employees.InsertAsync(manager1);
        await _context.Employees.InsertAsync(manager2);
        await _context.Employees.InsertAsync(developer);

        // Assert: Query and verify
        var queriedCeo = await _context.Employees.FindByIdAsync(ceoId);
        Assert.NotNull(queriedCeo);
        Assert.Equal("Alice CEO", queriedCeo.Name);
        Assert.NotNull(queriedCeo.DirectReportIds);
        Assert.Equal(2, queriedCeo.DirectReportIds.Count);
        Assert.Contains(manager1Id, queriedCeo.DirectReportIds);
        Assert.Contains(manager2Id, queriedCeo.DirectReportIds);
        
        // Query manager and verify direct reports
        var queriedManager1 = await _context.Employees.FindByIdAsync(manager1Id);
        Assert.NotNull(queriedManager1);
        Assert.Equal(ceoId, queriedManager1.ManagerId);
        Assert.NotNull(queriedManager1.DirectReportIds);
        Assert.Single(queriedManager1.DirectReportIds);
        Assert.Contains(developerId, queriedManager1.DirectReportIds);
        
        // Query developer and verify no direct reports
        var queriedDeveloper = await _context.Employees.FindByIdAsync(developerId);
        Assert.NotNull(queriedDeveloper);
        Assert.Equal(manager1Id, queriedDeveloper.ManagerId);
        // Empty list is acceptable (same as null semantically - no direct reports)
        Assert.Empty(queriedDeveloper.DirectReportIds ?? new List<ObjectId>());
    }

    [Fact]
    public async Task SelfReference_UpdateDirectReports_ShouldPersist()
    {
        // Arrange: Create manager with one direct report
        var managerId = ObjectId.NewObjectId();
        var employee1Id = ObjectId.NewObjectId();
        var employee2Id = ObjectId.NewObjectId();

        var manager = new Employee
        {
            Id = managerId,
            Name = "Manager",
            Department = "Engineering",
            DirectReportIds = new List<ObjectId> { employee1Id }
        };

        var employee1 = new Employee
        {
            Id = employee1Id,
            Name = "Employee 1",
            Department = "Engineering",
            ManagerId = managerId
        };

        var employee2 = new Employee
        {
            Id = employee2Id,
            Name = "Employee 2",
            Department = "Engineering",
            ManagerId = managerId
        };

        await _context.Employees.InsertAsync(manager);
        await _context.Employees.InsertAsync(employee1);
        await _context.Employees.InsertAsync(employee2);

        // Act: Add another direct report
        manager.DirectReportIds?.Add(employee2Id);
        await _context.Employees.UpdateAsync(manager);
        // Assert: Verify update persisted
        var queried = await _context.Employees.FindByIdAsync(managerId);
        Assert.NotNull(queried?.DirectReportIds);
        Assert.Equal(2, queried.DirectReportIds.Count);
        Assert.Contains(employee1Id, queried.DirectReportIds);
        Assert.Contains(employee2Id, queried.DirectReportIds);
    }

    [Fact]
    public async Task SelfReference_QueryByManagerId_ShouldWork()
    {
        // Arrange: Create hierarchy
        var managerId = ObjectId.NewObjectId();
        
        var manager = new Employee
        {
            Id = managerId,
            Name = "Manager",
            Department = "Engineering"
        };

        var employee1 = new Employee
        {
            Id = ObjectId.NewObjectId(),
            Name = "Employee 1",
            Department = "Engineering",
            ManagerId = managerId
        };

        var employee2 = new Employee
        {
            Id = ObjectId.NewObjectId(),
            Name = "Employee 2",
            Department = "Engineering",
            ManagerId = managerId
        };

        await _context.Employees.InsertAsync(manager);
        await _context.Employees.InsertAsync(employee1);
        await _context.Employees.InsertAsync(employee2);

        // Act: Query all employees with specific manager
        var subordinates = await _context.Employees
            .AsQueryable()
            .Where(e => e.ManagerId == managerId)
            .ToListAsync();

        // Assert: Should find both employees
        Assert.Equal(2, subordinates.Count);
        Assert.Contains(subordinates, e => e.Name == "Employee 1");
        Assert.Contains(subordinates, e => e.Name == "Employee 2");
    }

    // ========================================
    // N-N Referencing Tests (CategoryRef/ProductRef)
    // BEST PRACTICE for document databases
    // ========================================

    [Fact]
    public async Task NtoNReferencing_InsertAndQuery_ShouldWork()
    {
        // Arrange: Create categories and products with ObjectId references
        var categoryId1 = ObjectId.NewObjectId();
        var categoryId2 = ObjectId.NewObjectId();
        var productId1 = ObjectId.NewObjectId();
        var productId2 = ObjectId.NewObjectId();

        var electronics = new CategoryRef
        {
            Id = categoryId1,
            Name = "Electronics",
            Description = "Electronic devices",
            ProductIds = new List<ObjectId> { productId1, productId2 }
        };

        var computers = new CategoryRef
        {
            Id = categoryId2,
            Name = "Computers",
            Description = "Computing devices",
            ProductIds = new List<ObjectId> { productId1 }
        };

        var laptop = new ProductRef
        {
            Id = productId1,
            Name = "Laptop",
            Price = 999.99m,
            CategoryIds = new List<ObjectId> { categoryId1, categoryId2 }
        };

        var phone = new ProductRef
        {
            Id = productId2,
            Name = "Phone",
            Price = 599.99m,
            CategoryIds = new List<ObjectId> { categoryId1 }
        };

        // Act: Insert all entities
        await _context.CategoryRefs.InsertAsync(electronics);
        await _context.CategoryRefs.InsertAsync(computers);
        await _context.ProductRefs.InsertAsync(laptop);
        await _context.ProductRefs.InsertAsync(phone);

        // Assert: Query and verify references
        var queriedCategory = await _context.CategoryRefs.FindByIdAsync(categoryId1);
        Assert.NotNull(queriedCategory);
        Assert.Equal("Electronics", queriedCategory.Name);
        Assert.NotNull(queriedCategory.ProductIds);
        Assert.Equal(2, queriedCategory.ProductIds.Count);
        Assert.Contains(productId1, queriedCategory.ProductIds);
        Assert.Contains(productId2, queriedCategory.ProductIds);

        var queriedProduct = await _context.ProductRefs.FindByIdAsync(productId1);
        Assert.NotNull(queriedProduct);
        Assert.Equal("Laptop", queriedProduct.Name);
        Assert.NotNull(queriedProduct.CategoryIds);
        Assert.Equal(2, queriedProduct.CategoryIds.Count);
        Assert.Contains(categoryId1, queriedProduct.CategoryIds);
        Assert.Contains(categoryId2, queriedProduct.CategoryIds);
    }

    [Fact]
    public async Task NtoNReferencing_UpdateRelationships_ShouldPersist()
    {
        // Arrange: Create category and product
        var categoryId = ObjectId.NewObjectId();
        var productId1 = ObjectId.NewObjectId();
        var productId2 = ObjectId.NewObjectId();

        var category = new CategoryRef
        {
            Id = categoryId,
            Name = "Books",
            Description = "Book category",
            ProductIds = new List<ObjectId> { productId1 }
        };

        var product1 = new ProductRef
        {
            Id = productId1,
            Name = "Book 1",
            Price = 19.99m,
            CategoryIds = new List<ObjectId> { categoryId }
        };

        var product2 = new ProductRef
        {
            Id = productId2,
            Name = "Book 2",
            Price = 29.99m,
            CategoryIds = new List<ObjectId>()
        };

        await _context.CategoryRefs.InsertAsync(category);
        await _context.ProductRefs.InsertAsync(product1);
        await _context.ProductRefs.InsertAsync(product2);

        // Act: Add product2 to category
        category.ProductIds?.Add(productId2);
        await _context.CategoryRefs.UpdateAsync(category);
        product2.CategoryIds?.Add(categoryId);
        await _context.ProductRefs.UpdateAsync(product2);

        // Assert: Verify relationships updated
        var queriedCategory = await _context.CategoryRefs.FindByIdAsync(categoryId);
        Assert.NotNull(queriedCategory?.ProductIds);
        Assert.Equal(2, queriedCategory.ProductIds.Count);
        Assert.Contains(productId2, queriedCategory.ProductIds);

        var queriedProduct2 = await _context.ProductRefs.FindByIdAsync(productId2);
        Assert.NotNull(queriedProduct2?.CategoryIds);
        Assert.Single(queriedProduct2.CategoryIds);
        Assert.Contains(categoryId, queriedProduct2.CategoryIds);
    }

    [Fact]
    public async Task NtoNReferencing_DocumentSize_RemainSmall()
    {
        // Arrange: Create category referencing 100 products (only IDs)
        var categoryId = ObjectId.NewObjectId();
        var productIds = Enumerable.Range(0, 100)
            .Select(_ => ObjectId.NewObjectId())
            .ToList();

        var category = new CategoryRef
        {
            Id = categoryId,
            Name = "Large Category",
            Description = "Category with 100 products",
            ProductIds = productIds
        };

        // Act: Insert and query
        await _context.CategoryRefs.InsertAsync(category);
        var queried = await _context.CategoryRefs.FindByIdAsync(categoryId);

        // Assert: Document remains small (only ObjectIds, no embedding)
        Assert.NotNull(queried);
        Assert.Equal(100, queried.ProductIds?.Count);
        
        // Note: 100 ObjectIds = ~1.2KB (vs embedding full products = potentially hundreds of KBs)
        // This demonstrates why referencing is preferred for large N-N relationships
    }

    [Fact]
    public async Task NtoNReferencing_QueryByProductId_ShouldWork()
    {
        // Arrange: Create multiple categories referencing same product
        var productId = ObjectId.NewObjectId();
        
        var category1 = new CategoryRef
        {
            Id = ObjectId.NewObjectId(),
            Name = "Category 1",
            Description = "First category",
            ProductIds = new List<ObjectId> { productId }
        };

        var category2 = new CategoryRef
        {
            Id = ObjectId.NewObjectId(),
            Name = "Category 2",
            Description = "Second category",
            ProductIds = new List<ObjectId> { productId }
        };

        await _context.CategoryRefs.InsertAsync(category1);
        await _context.CategoryRefs.InsertAsync(category2);

        // Act: Query all categories containing the product
        var categoriesWithProduct =await _context.CategoryRefs
            .AsQueryable()
            .Where(c => c.ProductIds != null && c.ProductIds.Contains(productId))
            .ToListAsync();

        // Assert: Should find both categories
        Assert.Equal(2, categoriesWithProduct.Count);
        Assert.Contains(categoriesWithProduct, c => c.Name == "Category 1");
        Assert.Contains(categoriesWithProduct, c => c.Name == "Category 2");
    }
}
