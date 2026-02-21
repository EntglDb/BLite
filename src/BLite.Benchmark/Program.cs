using BenchmarkDotNet.Columns;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Exporters;
using BenchmarkDotNet.Exporters.Csv;
using BenchmarkDotNet.Reports;
using BenchmarkDotNet.Running;
using System.IO;

namespace BLite.Benchmark;

class Program
{
    static void Main(string[] args)
    {
        if (args.Length > 0 && args[0] == "manual")
        {
            ManualBenchmark.Run();
            return;
        }

        // Always write artifacts next to the .csproj, not in the bin/ output directory
        var projectDir = Path.GetFullPath(Path.Combine(AppContext.BaseDirectory, "..", "..", "..", ".."));
        var artifactsPath = Path.Combine(projectDir, "BenchmarkDotNet.Artifacts");

        var config = DefaultConfig.Instance
            .WithArtifactsPath(artifactsPath)
            .AddExporter(HtmlExporter.Default)
            .AddExporter(CsvExporter.Default)
            .AddExporter(MarkdownExporter.GitHub)
            .WithSummaryStyle(SummaryStyle.Default
                .WithRatioStyle(RatioStyle.Trend)
                .WithTimeUnit(Perfolizer.Horology.TimeUnit.Microsecond));

        BenchmarkSwitcher.FromAssembly(typeof(Program).Assembly).Run(args, config);
    }
}
