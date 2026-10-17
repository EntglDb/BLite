using System.Reflection;
using BLite.Bson;
using BLite.Core;
using BLite.Core.Collections;
using BLite.Shared;
using BLite.SourceGenerators;
using BLite.Tests.TestDbContext_TestDbContext_Mappers;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;

namespace BLite.Tests;

/// <summary>
/// A generated mapper lists the BSON keys it writes itself (#273). The base
/// <see cref="DocumentMapperBase{TId, T}.UsedKeys"/> finds them by reflection over the entity type,
/// which trimming removes under Native AOT: the list comes back empty, no key is registered and the
/// first write throws "BSON Key '...' not found in dictionary cache". Native AOT cannot run here, so
/// these tests check the contract that makes it safe: every generated mapper overrides
/// <c>UsedKeys</c>, and the keys it lists are all its <c>Serialize</c> needs.
/// </summary>
public class GeneratedMapperUsedKeysTests
{
    private static IEnumerable<Type> GeneratedEntityMappers() =>
        typeof(BLite_Shared_ComplexUserMapper).Assembly.GetTypes()
            .Where(t => !t.IsAbstract
                        && t.Namespace != null
                        && (t.Namespace.EndsWith("_Mappers", StringComparison.Ordinal)
                            || t.Namespace.EndsWith(".Mappers", StringComparison.Ordinal))
                        && IsDocumentMapper(t));

    private static bool IsDocumentMapper(Type type)
    {
        for (var t = type.BaseType; t != null; t = t.BaseType)
        {
            if (t.IsGenericType && t.GetGenericTypeDefinition() == typeof(DocumentMapperBase<,>))
                return true;
        }
        return false;
    }

    [Fact]
    public void Every_Generated_Entity_Mapper_Overrides_UsedKeys()
    {
        var mappers = GeneratedEntityMappers().ToList();
        Assert.True(mappers.Count > 20, $"expected the test contexts' generated mappers, found {mappers.Count}");

        var notOverriding = mappers
            .Where(t => t.GetProperty(nameof(DocumentMapperBase<int, object>.UsedKeys),
                            BindingFlags.Public | BindingFlags.Instance | BindingFlags.DeclaredOnly) == null)
            .Select(t => t.FullName)
            .ToList();

        Assert.Empty(notOverriding);
    }

    [Fact]
    public void Generated_UsedKeys_Lists_Own_And_Nested_Keys_In_Write_Order()
    {
        var keys = new BLite_Shared_ComplexUserMapper().UsedKeys.ToList();

        // _id and id for the key property (the rule the reflection-based schema had), then the
        // fields in declaration order, the nested Address and City fields where they are first
        // written, each name once. Secret is [BsonIgnore]d, so it is not written and not listed.
        Assert.Equal(
            new[] { "_id", "id", "name", "mainaddress", "street", "city", "zipcode", "otheraddresses", "tags" },
            keys);
    }

    [Fact]
    public void Generated_UsedKeys_Are_All_Serialize_Needs()
    {
        var mapper = new BLite_Shared_ComplexUserMapper();
        var entity = new ComplexUser
        {
            Id = ObjectId.NewObjectId(),
            Name = "n",
            MainAddress = new Address { Street = "s", City = new City { Name = "c", ZipCode = "z" } },
            OtherAddresses = { new Address { Street = "s2", City = new City { Name = "c2", ZipCode = "z2" } } },
            Tags = { "t" },
            Secret = "x",
        };

        // A key map holding only what UsedKeys lists: what the collection registers before its
        // first write when nothing else has filled the dictionary.
        var keyMap = new Dictionary<string, ushort>(StringComparer.OrdinalIgnoreCase);
        ushort next = 1;
        foreach (var key in mapper.UsedKeys) keyMap[key] = next++;

        var buffer = new byte[4096];
        var length = mapper.Serialize(entity, new BsonSpanWriter(buffer, keyMap));

        Assert.True(length > 0);
    }

    [Fact]
    public void Generator_Emits_UsedKeys_For_Recursive_And_Renamed_Nested_Types()
    {
        const string source = """
            using System.Collections.Generic;
            using BLite.Bson;
            using BLite.Core;
            using BLite.Core.Collections;

            namespace UsedKeysGen
            {
                public partial class UsedKeysDbContext : DocumentDbContext
                {
                    public DocumentCollection<ObjectId, Folder> Folders { get; set; } = null!;
                }

                public class Folder
                {
                    public ObjectId Id { get; set; }
                    [BsonProperty("title")]
                    public string Name { get; set; } = "";
                    public Owner Owner { get; set; } = new();
                    public List<Folder> Children { get; set; } = new();
                }

                // Matched by name, as the generator matches BLite's and System.Text.Json's renames.
                public sealed class BsonPropertyAttribute : System.Attribute
                {
                    public BsonPropertyAttribute(string name) => Name = name;
                    public string Name { get; }
                }

                public class Owner
                {
                    public string Login { get; set; } = "";
                    public List<Owner> Delegates { get; set; } = new();
                }
            }
            """;

        var compilation = CSharpCompilation.Create(
            "UsedKeysGenerationTests",
            new[] { CSharpSyntaxTree.ParseText(source, path: "/tmp/UsedKeysSource.cs") },
            new[]
            {
                MetadataReference.CreateFromFile(typeof(object).Assembly.Location),
                MetadataReference.CreateFromFile(Path.Combine(
                    Path.GetDirectoryName(typeof(object).Assembly.Location)!, "System.Runtime.dll")),
                MetadataReference.CreateFromFile(typeof(Enumerable).Assembly.Location),
                MetadataReference.CreateFromFile(typeof(List<>).Assembly.Location),
                MetadataReference.CreateFromFile(typeof(Task).Assembly.Location),
                MetadataReference.CreateFromFile(typeof(DocumentDbContext).Assembly.Location),
                MetadataReference.CreateFromFile(typeof(ObjectId).Assembly.Location),
            },
            new CSharpCompilationOptions(OutputKind.DynamicallyLinkedLibrary));

        GeneratorDriver driver = CSharpGeneratorDriver.Create(new MapperGenerator());
        driver = driver.RunGeneratorsAndUpdateCompilation(compilation, out var output, out _);
        var run = driver.GetRunResult();

        var text = run.Results.Single().GeneratedSources
            .Single(g => g.HintName.EndsWith(".Mappers.g.cs", StringComparison.Ordinal))
            .SourceText.ToString();

        // Only the entity mapper derives from DocumentMapperBase and overrides UsedKeys; the
        // recursion through Folder.Children and Owner.Delegates ends at the first visit.
        Assert.Single(System.Text.RegularExpressions.Regex.Matches(text, @"override [\w.:<>]+ UsedKeys\b"));
        var listed = System.Text.RegularExpressions.Regex.Match(
            text, @"__usedKeys = new string\[\]\s*\{(?<keys>[^}]*)\}").Groups["keys"].Value;
        var keys = System.Text.RegularExpressions.Regex.Matches(listed, "\"([^\"]*)\"")
            .Select(m => m.Groups[1].Value)
            .ToList();
        Assert.Equal(new[] { "_id", "id", "title", "owner", "login", "delegates", "children" }, keys);

        var generatedPaths = run.GeneratedTrees.Select(t => t.FilePath).ToHashSet();
        var errors = output.GetDiagnostics()
            .Where(d => d.Severity == DiagnosticSeverity.Error
                        && d.Location.SourceTree is not null
                        && generatedPaths.Contains(d.Location.SourceTree.FilePath));
        Assert.Empty(errors);
    }
}
