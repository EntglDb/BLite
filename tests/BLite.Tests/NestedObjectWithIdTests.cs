using BLite.Bson;
using BLite.Shared;

namespace BLite.Tests;

/// <summary>
/// Regression tests for nested objects that declare a [Key]-decorated Id property
/// WITHOUT being registered as a DocumentCollection in the ModelBuilder.
///
/// Bug: the SourceGenerator was treating such nested types as root entities,
/// deriving their mapper from an XxxMapperBase and serialising the Id as "_id".
/// Fix: nested type mappers now set IsNestedTypeMapper=true, suppressing the
/// root-entity code path and serialising the Id using its BsonFieldName ("id").
/// </summary>
public class NestedObjectWithIdTests : IDisposable
{
    private readonly string _dbPath;
    private readonly TestDbContext _db;

    public NestedObjectWithIdTests()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"test_nested_id_{Guid.NewGuid()}.db");
        _db = new TestDbContext(_dbPath);
    }

    public void Dispose()
    {
        _db.Dispose();
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
        var wal = Path.ChangeExtension(_dbPath, ".wal");
        if (File.Exists(wal)) File.Delete(wal);
    }

    // ------------------------------------------------------------------
    // Single nested object
    // ------------------------------------------------------------------

    [Fact]
    public void Insert_And_FindById_Preserves_Nested_Id_Field()
    {
        // Arrange
        var entity = new PersonWithContact
        {
            Name = "Mario",
            MainContact = new ContactInfo { Id = 42, Email = "mario@example.com", Phone = "555-1234" }
        };

        // Act
        var id = _db.PeopleWithContacts.Insert(entity);
        _db.SaveChanges();
        var found = _db.PeopleWithContacts.FindById(id);

        // Assert
        Assert.NotNull(found);
        Assert.Equal("Mario", found.Name);
        Assert.NotNull(found.MainContact);
        Assert.Equal(42, found.MainContact.Id);
        Assert.Equal("mario@example.com", found.MainContact.Email);
        Assert.Equal("555-1234", found.MainContact.Phone);
    }

    [Fact]
    public void Nested_Id_Zero_RoundTrips_Without_Corruption()
    {
        // A nested Id of 0 must survive the round-trip unchanged.
        var entity = new PersonWithContact
        {
            Name = "Luigi",
            MainContact = new ContactInfo { Id = 0, Email = "luigi@example.com", Phone = "" }
        };

        var id = _db.PeopleWithContacts.Insert(entity);
        _db.SaveChanges();
        var found = _db.PeopleWithContacts.FindById(id);

        Assert.NotNull(found?.MainContact);
        Assert.Equal(0, found.MainContact.Id);
        Assert.Equal("luigi@example.com", found.MainContact.Email);
    }

    [Fact]
    public void Null_Nested_Contact_RoundTrips()
    {
        var entity = new PersonWithContact
        {
            Name = "Peach",
            MainContact = null
        };

        var id = _db.PeopleWithContacts.Insert(entity);
        _db.SaveChanges();
        var found = _db.PeopleWithContacts.FindById(id);

        Assert.NotNull(found);
        Assert.Equal("Peach", found.Name);
        Assert.Null(found.MainContact);
    }

    // ------------------------------------------------------------------
    // Collection of nested objects with [Key] Id
    // ------------------------------------------------------------------

    [Fact]
    public void Collection_Of_NestedObjects_With_Id_RoundTrips()
    {
        var entity = new PersonWithContact
        {
            Name = "Bowser",
            Contacts = new List<ContactInfo>
            {
                new ContactInfo { Id = 1, Email = "a@example.com", Phone = "111" },
                new ContactInfo { Id = 2, Email = "b@example.com", Phone = "222" },
                new ContactInfo { Id = 3, Email = "c@example.com", Phone = "333" }
            }
        };

        var id = _db.PeopleWithContacts.Insert(entity);
        _db.SaveChanges();
        var found = _db.PeopleWithContacts.FindById(id);

        Assert.NotNull(found);
        Assert.Equal(3, found.Contacts.Count);
        Assert.Contains(found.Contacts, c => c.Id == 1 && c.Email == "a@example.com");
        Assert.Contains(found.Contacts, c => c.Id == 2 && c.Email == "b@example.com");
        Assert.Contains(found.Contacts, c => c.Id == 3 && c.Email == "c@example.com");
    }

    // ------------------------------------------------------------------
    // Update
    // ------------------------------------------------------------------

    [Fact]
    public void Update_Nested_Id_Persists()
    {
        var entity = new PersonWithContact
        {
            Name = "Toad",
            MainContact = new ContactInfo { Id = 10, Email = "old@example.com", Phone = "000" }
        };

        var id = _db.PeopleWithContacts.Insert(entity);
        _db.SaveChanges();

        var loaded = _db.PeopleWithContacts.FindById(id)!;
        loaded.MainContact!.Id = 99;
        loaded.MainContact.Email = "new@example.com";
        _db.PeopleWithContacts.Update(loaded);
        _db.SaveChanges();

        var updated = _db.PeopleWithContacts.FindById(id);
        Assert.NotNull(updated?.MainContact);
        Assert.Equal(99, updated.MainContact.Id);
        Assert.Equal("new@example.com", updated.MainContact.Email);
    }

    // ------------------------------------------------------------------
    // Multiple documents (no interference between rows)
    // ------------------------------------------------------------------

    [Fact]
    public void Multiple_Documents_Nested_Ids_Do_Not_Interfere()
    {
        var id1 = _db.PeopleWithContacts.Insert(new PersonWithContact
        {
            Name = "Alice",
            MainContact = new ContactInfo { Id = 100, Email = "alice@example.com", Phone = "1" }
        });
        var id2 = _db.PeopleWithContacts.Insert(new PersonWithContact
        {
            Name = "Bob",
            MainContact = new ContactInfo { Id = 200, Email = "bob@example.com", Phone = "2" }
        });
        _db.SaveChanges();

        var a = _db.PeopleWithContacts.FindById(id1);
        var b = _db.PeopleWithContacts.FindById(id2);

        Assert.Equal(100, a?.MainContact?.Id);
        Assert.Equal(200, b?.MainContact?.Id);
    }
}
