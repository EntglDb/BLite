using System.Reflection;
using System.Linq;
using System.Collections;
using System.Collections.Generic;
using System.Collections.Concurrent;
using BLite.Bson;

namespace BLite.Core.Collections;

public static class BsonSchemaGenerator
{
    public static BsonSchema FromType<T>()
    {
        return FromType(typeof(T));
    }

    private static readonly ConcurrentDictionary<Type, BsonSchema> _cache = new();

    public static BsonSchema FromType(Type type)
    {
        return _cache.GetOrAdd(type, GenerateSchema);
    }

    private static BsonSchema GenerateSchema(Type type)
    {
        var schema = new BsonSchema { Title = type.Name };
        var properties = type.GetProperties(BindingFlags.Public | BindingFlags.Instance);
        var fields = type.GetFields(BindingFlags.Public | BindingFlags.Instance);

        foreach (var prop in properties)
        {
            if (prop.GetIndexParameters().Length > 0) continue; // Skip indexers
            if (!prop.CanRead) continue;

            AddField(schema, prop.Name, prop.PropertyType);
        }

        foreach (var field in fields)
        {
            AddField(schema, field.Name, field.FieldType);
        }

        return schema;
    }

    private static void AddField(BsonSchema schema, string name, Type type)
    {
        name = name.ToLowerInvariant();

        // Convention: id -> _id for root document
        if (name.Equals("id", StringComparison.OrdinalIgnoreCase))
        {
            name = "_id";
        }

        var (bsonType, nestedSchema, itemType) = GetBsonType(type);

        schema.Fields.Add(new BsonField
        {
            Name = name,
            Type = bsonType,
            IsNullable = IsNullable(type),
            NestedSchema = nestedSchema,
            ArrayItemType = itemType
        });
    }

    private static (BsonType type, BsonSchema? nested, BsonType? itemType) GetBsonType(Type type)
    {
        // Handle Nullable<T>
        type = Nullable.GetUnderlyingType(type) ?? type;

        if (type == typeof(ObjectId)) return (BsonType.ObjectId, null, null);
        if (type == typeof(string)) return (BsonType.String, null, null);
        if (type == typeof(int)) return (BsonType.Int32, null, null);
        if (type == typeof(long)) return (BsonType.Int64, null, null);
        if (type == typeof(bool)) return (BsonType.Boolean, null, null);
        if (type == typeof(double)) return (BsonType.Double, null, null);
        if (type == typeof(decimal)) return (BsonType.Decimal128, null, null);
        if (type == typeof(DateTime) || type == typeof(DateTimeOffset)) return (BsonType.DateTime, null, null);
        if (type == typeof(Guid)) return (BsonType.Binary, null, null); // Guid is usually Binary subtype
        if (type == typeof(byte[])) return (BsonType.Binary, null, null);

        // Arrays/Lists
        if (type != typeof(string) && typeof(IEnumerable).IsAssignableFrom(type))
        {
            var itemType = GetCollectionItemType(type);
            var (itemBsonType, itemNested, _) = GetBsonType(itemType);
            
            // For arrays, if item is Document, we use NestedSchema to describe the item
            return (BsonType.Array, itemNested, itemBsonType);
        }

        // Nested Objects / Structs
        // If it's not a string, not a primitive, and not an array/list, treat as Document
        if (type != typeof(string) && !type.IsPrimitive && !type.IsEnum)
        {
            // Avoid infinite recursion?
            // Simple approach: generating nested schema
            return (BsonType.Document, FromType(type), null);
        }

        return (BsonType.Undefined, null, null);
    }

    private static bool IsNullable(Type type)
    {
        return !type.IsValueType || Nullable.GetUnderlyingType(type) != null;
    }

    private static Type GetCollectionItemType(Type type)
    {
        if (type.IsArray) return type.GetElementType()!;
        
        // If type itself is IEnumerable<T>
        if (type.IsGenericType && type.GetGenericTypeDefinition() == typeof(IEnumerable<>))
        {
            return type.GetGenericArguments()[0];
        }

        var enumerableType = type.GetInterfaces()
            .FirstOrDefault(i => i.IsGenericType && i.GetGenericTypeDefinition() == typeof(IEnumerable<>));
            
        return enumerableType?.GetGenericArguments()[0] ?? typeof(object);
    }
}
