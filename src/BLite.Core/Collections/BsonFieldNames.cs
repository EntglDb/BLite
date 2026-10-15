using System.Collections.Concurrent;
using System.Diagnostics.CodeAnalysis;
using System.Linq.Expressions;
using System.Reflection;

namespace BLite.Core.Collections;

/// <summary>
/// The BSON field a property or field of an entity is stored under, for the parts of the runtime
/// that start from a CLR member: key registration (<see cref="BsonSchemaGenerator"/>), BSON-level
/// predicates, projections and field-based retention settings.
/// </summary>
/// <remarks>
/// Generated mappers decide the names at compile time (<c>EntityAnalyzer</c> in
/// BLite.SourceGenerators), and this class applies the same rules, so both sides agree (#184):
/// <list type="bullet">
/// <item>The first non-empty name given by <c>[BsonProperty]</c>, <c>[JsonPropertyName]</c> or
/// <c>[Column]</c>, in that order, read from the first constructor argument and matched by
/// attribute name so that no particular library has to be referenced; otherwise the member name.</item>
/// <item>Lower-cased: the key dictionary stores every name lower-cased, so documents are read back
/// with lower-case names whatever was written.</item>
/// <item>In a root document the key is stored as <c>_id</c>, whatever its name or attributes. The
/// key is the property marked <c>[Key]</c> or <c>[BsonId]</c>, otherwise the property named
/// <c>Id</c>. In a nested document the key is an ordinary field.</item>
/// </list>
/// </remarks>
internal static class BsonFieldNames
{
    /// <summary>The field a root document stores its key in.</summary>
    public const string IdField = "_id";

    private static readonly ConcurrentDictionary<MemberInfo, string> s_names = new();
    private static readonly ConcurrentDictionary<Type, string?> s_keyNames = new();

    /// <summary>
    /// The field <paramref name="member"/> is stored under in a nested document, or in a root
    /// document when it is not the key.
    /// </summary>
    public static string Of(MemberInfo member) => s_names.GetOrAdd(member, Resolve);

    /// <summary>
    /// The field <paramref name="member"/> of the root entity <paramref name="entityType"/> is
    /// stored under: <c>_id</c> for the key, <see cref="Of"/> otherwise.
    /// </summary>
    public static string OfRoot(Type entityType, MemberInfo member)
        => IsKey(entityType, member) ? IdField : Of(member);

    /// <summary>
    /// The dotted field path of the member chain <paramref name="selector"/> selects
    /// (<c>x =&gt; x.Address.City</c> gives <c>home.town</c> when both are renamed), or
    /// <c>null</c> when its body is not a chain of members of the parameter.
    /// </summary>
    public static string? PathOf(LambdaExpression selector)
    {
        var body = selector.Body is UnaryExpression { NodeType: ExpressionType.Convert or ExpressionType.ConvertChecked } convert
            ? convert.Operand
            : selector.Body;

        var members = new List<MemberInfo>();
        while (body is MemberExpression member)
        {
            members.Add(member.Member);
            body = member.Expression!;
        }
        if (members.Count == 0 || body != selector.Parameters[0]) return null;

        members.Reverse();
        var segments = new string[members.Count];
        segments[0] = OfRoot(selector.Parameters[0].Type, members[0]);
        for (int i = 1; i < members.Count; i++)
            segments[i] = Of(members[i]);
        return string.Join(".", segments);
    }

    /// <summary>Whether <paramref name="member"/> is the key of <paramref name="entityType"/>.</summary>
    public static bool IsKey(Type entityType, MemberInfo member)
        => member is PropertyInfo && member.Name == s_keyNames.GetOrAdd(entityType, FindKeyName);

    private static string Resolve(MemberInfo member)
    {
        string? bsonProperty = null, jsonPropertyName = null, column = null;
        foreach (var attribute in member.GetCustomAttributesData())
        {
            var name = attribute.AttributeType.Name;
            if (IsNamed(name, "BsonProperty")) bsonProperty ??= FirstStringArgument(attribute);
            else if (IsNamed(name, "JsonPropertyName")) jsonPropertyName ??= FirstStringArgument(attribute);
            else if (IsNamed(name, "Column")) column ??= FirstStringArgument(attribute);
        }

        var fieldName = bsonProperty ?? jsonPropertyName ?? column ?? member.Name;
        return fieldName.ToLowerInvariant();
    }

    [UnconditionalSuppressMessage("Trimming", "IL2070",
        Justification = "Entity properties are preserved: the generated mapper reads and writes each of them.")]
    private static string? FindKeyName(Type entityType)
    {
        string? conventional = null;
        foreach (var property in entityType.GetProperties(BindingFlags.Public | BindingFlags.Instance))
        {
            foreach (var attribute in property.GetCustomAttributesData())
            {
                var name = attribute.AttributeType.Name;
                if (IsNamed(name, "Key") || IsNamed(name, "BsonId"))
                    return property.Name;
            }
            if (property.Name == "Id")
                conventional = property.Name;
        }
        return conventional;
    }

    private static bool IsNamed(string attributeTypeName, string name)
        => attributeTypeName == name || attributeTypeName == name + "Attribute";

    private static string? FirstStringArgument(CustomAttributeData attribute)
        => attribute.ConstructorArguments.Count > 0
           && attribute.ConstructorArguments[0].Value is string { Length: > 0 } value
            ? value
            : null;
}
