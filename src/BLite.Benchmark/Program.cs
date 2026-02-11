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

        BenchmarkSwitcher.FromAssembly(typeof(Program).Assembly).Run(args);
    }
}
