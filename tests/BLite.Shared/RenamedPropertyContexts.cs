using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Text.Json.Serialization;
using BLite.Bson;
using BLite.Core;
using BLite.Core.Collections;
using BLite.Core.Metadata;

namespace BLite.Shared.ThirdParty
{
    /// <summary>
    /// Stands in for a <c>[BsonProperty("name")]</c> attribute from another library: BLite ships
    /// none, and the generator honours any attribute with that name whose first constructor
    /// argument is the field name.
    /// </summary>
    [AttributeUsage(AttributeTargets.Property | AttributeTargets.Field)]
    public sealed class BsonPropertyAttribute : Attribute
    {
        public BsonPropertyAttribute(string name) => Name = name;
        public string Name { get; }
    }
}

namespace BLite.Shared
{
    /// <summary>Nested type whose properties are renamed (#184).</summary>
    public class RenamedAddress
    {
        [JsonPropertyName("town")]
        public string City { get; set; } = "";

        [Column("zip_code")]
        public string Zip { get; set; } = "";
    }

    /// <summary>Item of a nested collection whose property is renamed (#184).</summary>
    public class RenamedTag
    {
        [ThirdParty.BsonProperty("label")]
        public string Name { get; set; } = "";

        public int Weight { get; set; }
    }

    /// <summary>
    /// Every property is stored under a name other than its conventional (lower-cased) one,
    /// through <c>[Column]</c>, <c>[JsonPropertyName]</c> or <c>[BsonProperty]</c> (#184).
    /// </summary>
    public class RenamedCustomer
    {
        /// <summary>Still stored as <c>_id</c>: the key of a root entity ignores renames.</summary>
        [Column("customer_id")]
        public ObjectId Id { get; set; }

        [Column("full_name")]
        public string Name { get; set; } = "";

        [JsonPropertyName("mail")]
        public string Email { get; set; } = "";

        [ThirdParty.BsonProperty("years")]
        public int Age { get; set; }

        /// <summary>A mixed-case name: field names are stored lower-cased.</summary>
        [ThirdParty.BsonProperty("DisplayName")]
        public string Nickname { get; set; } = "";

        /// <summary><c>[JsonPropertyName]</c> takes precedence over <c>[Column]</c>.</summary>
        [Column("by_column")]
        [JsonPropertyName("by_json")]
        public string Both { get; set; } = "";

        /// <summary>Stored as <c>name</c>, the conventional name of <see cref="Name"/>.</summary>
        [Column("name")]
        public string Alias { get; set; } = "";

        [Column("active")]
        public bool IsActive { get; set; }

        [Column("home")]
        public RenamedAddress? Address { get; set; }

        [JsonPropertyName("labels")]
        public List<RenamedTag> Tags { get; set; } = new();
    }

    /// <summary>Key property not named <c>Id</c>, renamed, and still stored as <c>_id</c> (#184).</summary>
    public class RenamedKeyed
    {
        [Key]
        [Column("code_value")]
        public int Code { get; set; }

        [Column("title_text")]
        public string Title { get; set; } = "";

        [JsonPropertyName("created_on")]
        public DateTime CreatedAt { get; set; }
    }

    /// <summary>
    /// <see cref="RenamedCustomer"/> with indexes on a renamed property (<c>Email</c>) and on a
    /// renamed property of a renamed nested object (<c>Address.City</c>).
    /// </summary>
    public partial class RenamedPropertyDbContext : DocumentDbContext
    {
        public DocumentCollection<ObjectId, RenamedCustomer> Customers { get; set; } = null!;
        public DocumentCollection<int, RenamedKeyed> Keyed { get; set; } = null!;

        public RenamedPropertyDbContext(string path) : base(path) => InitializeCollections();

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<RenamedCustomer>().ToCollection("renamed_customers")
                .HasIndex(c => c.Email)
                .HasIndex(c => c.Address!.City);
            modelBuilder.Entity<RenamedKeyed>().ToCollection("renamed_keyed");
        }
    }
}
