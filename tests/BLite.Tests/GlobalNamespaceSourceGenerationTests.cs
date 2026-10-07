using BLite.Bson;
using BLite.Core;
using BLite.SourceGenerators;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.Diagnostics;
using Xunit;

namespace BLite.Tests
{
    /// <summary>
    /// Runs <see cref="MapperGenerator"/> directly through
    /// <see cref="CSharpGeneratorDriver"/> against a compilation whose
    /// DbContext, entity and nested type are all declared in the global
    /// namespace. Before the global-namespace fix these runs crashed with an
    /// invalid hint-name character (CS8785: '&lt;global namespace&gt;'), so every
    /// assertion here fails on the unfixed generator.
    /// </summary>
    public class GlobalNamespaceSourceGenerationTests
    {
        private const string SourcePath = "/tmp/GlobalNsSource.cs";

        private const string GlobalNsSource = """
            using BLite.Bson;
            using BLite.Core;
            using BLite.Core.Collections;

            public partial class DriverGlobalNsDbContext : DocumentDbContext
            {
                public DocumentCollection<ObjectId, DriverGlobalNsPerson> People { get; set; } = null!;
            }

            public class DriverGlobalNsPerson
            {
                public ObjectId Id { get; set; }
                public string Name { get; set; } = "";

                // Nested type declared inside a global-namespace entity.
                public MetricsInfo Metrics { get; set; } = new();

                public class MetricsInfo
                {
                    public int Score { get; set; }
                }
            }
            """;

        private static CSharpCompilation CreateCompilation() => CSharpCompilation.Create(
            "GlobalNsGenerationTests",
            new[] { CSharpSyntaxTree.ParseText(GlobalNsSource, path: SourcePath) },
            new[]
            {
                MetadataReference.CreateFromFile(typeof(object).Assembly.Location),
                // typeof(object) resolves to System.Private.CoreLib; generated code
                // needs System.Runtime for the forwarded primitive types.
                MetadataReference.CreateFromFile(Path.Combine(
                    Path.GetDirectoryName(typeof(object).Assembly.Location)!,
                    "System.Runtime.dll")),
                MetadataReference.CreateFromFile(typeof(System.Linq.Enumerable).Assembly.Location),
                MetadataReference.CreateFromFile(typeof(System.Collections.Generic.List<>).Assembly.Location),
                MetadataReference.CreateFromFile(typeof(System.Threading.Tasks.Task).Assembly.Location),
                MetadataReference.CreateFromFile(typeof(DocumentDbContext).Assembly.Location),
                MetadataReference.CreateFromFile(typeof(ObjectId).Assembly.Location),
            },
            new CSharpCompilationOptions(OutputKind.DynamicallyLinkedLibrary));

        private static (GeneratorDriverRunResult Run, CSharpCompilation Output) RunGenerator()
        {
            var compilation = CreateCompilation();
            GeneratorDriver driver = CSharpGeneratorDriver.Create(new MapperGenerator());
            driver = driver.RunGeneratorsAndUpdateCompilation(compilation, out var output, out _);
            return (driver.GetRunResult(), (CSharpCompilation)output);
        }

        private static string MappersSource(GeneratorDriverRunResult run)
        {
            var generated = run.Results.Single().GeneratedSources;
            Assert.NotEmpty(generated);
            return generated.Single(g => g.HintName.EndsWith(".Mappers.g.cs", StringComparison.Ordinal)).SourceText.ToString();
        }

        [Fact]
        public void Generator_Completes_Without_Failing_On_Global_Namespace_Types()
        {
            var (run, _) = RunGenerator();

            // The unfixed generator throws on the invalid '<' hint-name character,
            // surfacing as CS8785 ("Generator failed to generate source").
            Assert.DoesNotContain(run.Diagnostics, d => d.Id == "CS8785");
            Assert.DoesNotContain(run.Diagnostics, d => d.Severity == DiagnosticSeverity.Error);
        }

        [Fact]
        public void HintNames_Contain_No_GlobalNamespace_Marker_Or_Leading_Dot()
        {
            var (run, _) = RunGenerator();
            var generated = run.Results.Single().GeneratedSources;

            Assert.NotEmpty(generated);
            Assert.All(generated, g =>
            {
                Assert.DoesNotContain("<", g.HintName);
                Assert.DoesNotContain(">", g.HintName);
                Assert.False(g.HintName.StartsWith(".", StringComparison.Ordinal), $"leading dot in '{g.HintName}'");
            });
            Assert.Contains(generated, g =>
                g.HintName == "DriverGlobalNsDbContext_GlobalNsSource.Mappers.g.cs");
        }

        [Fact]
        public void Emitted_Source_Uses_Clean_Namespaces_And_No_Wrapper_For_The_Partial()
        {
            var (run, _) = RunGenerator();
            var text = MappersSource(run);

            Assert.DoesNotContain("<global namespace>", text);
            Assert.DoesNotContain("namespace .", text);
            // Mapper namespace drops the empty global prefix entirely.
            Assert.Contains("namespace DriverGlobalNsDbContext_GlobalNsSource_Mappers", text);
            // The InitializeCollections partial is emitted without a namespace wrapper.
            Assert.Contains("public partial class DriverGlobalNsDbContext", text);
            Assert.DoesNotContain("namespace DriverGlobalNsDbContext_GlobalNsSource_Mappers.DriverGlobalNsDbContext", text);
            // The nested type of the global-namespace entity gets a mapper.
            Assert.Contains("MetricsInfo", text);
        }

        [Fact]
        public void Generated_Source_Parses_And_Produces_No_New_Errors()
        {
            var (run, output) = RunGenerator();
            var generatedFilePaths = run.GeneratedTrees.Select(t => t.FilePath).ToHashSet();

            var errorsInGenerated = output.GetDiagnostics()
                .Where(d => d.Severity == DiagnosticSeverity.Error)
                .Where(d => d.Location.SourceTree is not null && generatedFilePaths.Contains(d.Location.SourceTree.FilePath));

            Assert.Empty(errorsInGenerated);
        }
    }
}
