```

BenchmarkDotNet v0.15.8, Windows 11 (10.0.22631.6345/23H2/2023Update/SunValley3)
13th Gen Intel Core i7-13800H 2.50GHz, 1 CPU, 20 logical and 14 physical cores
.NET SDK 10.0.102
  [Host]     : .NET 10.0.2 (10.0.2, 10.0.225.61305), X64 RyuJIT x86-64-v3
  DefaultJob : .NET 10.0.2 (10.0.2, 10.0.225.61305), X64 RyuJIT x86-64-v3


```
| Method                          | Job        | Toolchain              | Mean | Error | Ratio | RatioSD | Alloc Ratio |
|-------------------------------- |----------- |----------------------- |-----:|------:|------:|--------:|------------:|
| &#39;SQLite FindById (Deserialize)&#39; | DefaultJob | Default                |   NA |    NA |     ? |       ? |           ? |
| &#39;DocumentDb FindById&#39;           | DefaultJob | Default                |   NA |    NA |     ? |       ? |           ? |
|                                 |            |                        |      |       |       |         |             |
| &#39;SQLite FindById (Deserialize)&#39; | InProcess  | InProcessEmitToolchain |   NA |    NA |     ? |       ? |           ? |
| &#39;DocumentDb FindById&#39;           | InProcess  | InProcessEmitToolchain |   NA |    NA |     ? |       ? |           ? |

Benchmarks with issues:
  ReadBenchmarks.'SQLite FindById (Deserialize)': DefaultJob
  ReadBenchmarks.'DocumentDb FindById': DefaultJob
  ReadBenchmarks.'SQLite FindById (Deserialize)': InProcess(Toolchain=InProcessEmitToolchain)
  ReadBenchmarks.'DocumentDb FindById': InProcess(Toolchain=InProcessEmitToolchain)
