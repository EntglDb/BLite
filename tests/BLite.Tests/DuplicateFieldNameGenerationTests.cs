using BLite.Bson;
using BLite.Core;
using BLite.SourceGenerators;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;

namespace BLite.Tests;

/// <summary>
/// Two members stored under the same field, compared lower-cased since field names are, are
/// reported by the generator as BLITE001 naming both, and the generated code still compiles: the
/// later member is left out of the mapper instead of producing a duplicate <c>case</c> label (#184).
/// </summary>
public class DuplicateFieldNameGenerationTests
{
    private const string Source = """
        using System.Text.Json.Serialization;
        using BLite.Bson;
        using BLite.Core;
        using BLite.Core.Collections;

        namespace DuplicateFieldNames;

        public partial class DuplicateFieldDbContext : DocumentDbContext
        {
            public DocumentCollection<ObjectId, Clashing> Items { get; set; } = null!;
        }

        public class Clashing
        {
            public ObjectId Id { get; set; }
            public string Name { get; set; } = "";
            [JsonPropertyName("Name")] public string DisplayName { get; set; } = "";
            public Inner Detail { get; set; } = new();
        }

        public class Inner
        {
            public int Code { get; set; }
            [JsonPropertyName("code")] public int OtherCode { get; set; }
        }
        """;

    private static (GeneratorDriverRunResult Run, IReadOnlyList<Diagnostic> CompileErrors) RunGenerator()
    {
        var runtimeDir = Path.GetDirectoryName(typeof(object).Assembly.Location)!;
        var compilation = CSharpCompilation.Create(
            "DuplicateFieldNameGenerationTests",
            [CSharpSyntaxTree.ParseText(Source, path: "/tmp/DuplicateFieldNames.cs")],
            [
                MetadataReference.CreateFromFile(typeof(object).Assembly.Location),
                MetadataReference.CreateFromFile(Path.Combine(runtimeDir, "System.Runtime.dll")),
                MetadataReference.CreateFromFile(typeof(System.Linq.Enumerable).Assembly.Location),
                MetadataReference.CreateFromFile(typeof(System.Collections.Generic.List<>).Assembly.Location),
                MetadataReference.CreateFromFile(typeof(System.Threading.Tasks.Task).Assembly.Location),
                MetadataReference.CreateFromFile(typeof(System.Text.Json.Serialization.JsonPropertyNameAttribute).Assembly.Location),
                MetadataReference.CreateFromFile(typeof(DocumentDbContext).Assembly.Location),
                MetadataReference.CreateFromFile(typeof(ObjectId).Assembly.Location),
            ],
            new CSharpCompilationOptions(OutputKind.DynamicallyLinkedLibrary));

        GeneratorDriver driver = CSharpGeneratorDriver.Create(new MapperGenerator());
        driver = driver.RunGeneratorsAndUpdateCompilation(compilation, out var output, out _);
        var errors = output.GetDiagnostics().Where(d => d.Severity == DiagnosticSeverity.Error).ToList();
        return (driver.GetRunResult(), errors);
    }

    [Fact]
    public void ClashingFieldNames_AreReportedByName_AndTheGeneratedCodeCompiles()
    {
        var (run, compileErrors) = RunGenerator();

        var blite = run.Diagnostics.Where(d => d.Id == "BLITE001").Select(d => d.GetMessage()).ToList();
        Assert.Contains(blite, m => m.Contains("Clashing.DisplayName") && m.Contains("Clashing.Name") && m.Contains("'name'"));
        Assert.Contains(blite, m => m.Contains("Inner.OtherCode") && m.Contains("Inner.Code") && m.Contains("'code'"));
        Assert.All(run.Diagnostics.Where(d => d.Id == "BLITE001"), d => Assert.Equal(DiagnosticSeverity.Error, d.Severity));

        // No CS0152 (duplicate case label) or any other error in the generated code.
        Assert.Empty(compileErrors);
    }
}
