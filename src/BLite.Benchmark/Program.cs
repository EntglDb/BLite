using BenchmarkDotNet.Columns;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Exporters;
using BenchmarkDotNet.Reports;
using BenchmarkDotNet.Running;

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

        var config = DefaultConfig.Instance
            .AddExporter(HtmlExporter.Default)
            .WithSummaryStyle(SummaryStyle.Default
                .WithRatioStyle(RatioStyle.Trend)
                .WithTimeUnit(Perfolizer.Horology.TimeUnit.Microsecond));

        BenchmarkSwitcher.FromAssembly(typeof(Program).Assembly).Run(args, config);
    }
}
