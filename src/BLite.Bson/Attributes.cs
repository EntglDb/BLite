using System;

namespace BLite.Bson
{
    [AttributeUsage(AttributeTargets.Property)]
    public class BsonIdAttribute : Attribute
    {
    }

    [AttributeUsage(AttributeTargets.Property)]
    public class BsonIgnoreAttribute : Attribute
    {
    }

    /// <summary>
    /// Marks a class for standalone document mapper code generation.
    /// The source generator will produce an <c>IDocumentMapper&lt;TId, T&gt;</c> implementation
    /// without requiring a <c>DocumentDbContext</c> subclass.
    /// </summary>
    [AttributeUsage(AttributeTargets.Class, AllowMultiple = false, Inherited = false)]
    public sealed class DocumentMapperAttribute : Attribute
    {
        /// <summary>
        /// Optional collection name override. When omitted the entity class name (lowercased) is used.
        /// </summary>
        public string? CollectionName { get; }

        public DocumentMapperAttribute() { }

        public DocumentMapperAttribute(string collectionName)
        {
            CollectionName = collectionName;
        }
    }
}
