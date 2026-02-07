```

BenchmarkDotNet v0.15.8, Windows 11 (10.0.22631.6345/23H2/2023Update/SunValley3)
13th Gen Intel Core i7-13800H 2.50GHz, 1 CPU, 20 logical and 14 physical cores
.NET SDK 10.0.102
  [Host]     : .NET 10.0.2 (10.0.2, 10.0.225.61305), X64 RyuJIT x86-64-v3
  Job-CNUJVU : .NET 10.0.2 (10.0.2, 10.0.225.61305), X64 RyuJIT x86-64-v3

InvocationCount=1  UnrollFactor=1  

```
| Method                                         | Job        | Toolchain              | Mean | Error | Ratio | RatioSD | Alloc Ratio |
|----------------------------------------------- |----------- |----------------------- |-----:|------:|------:|--------:|------------:|
| &#39;SQLite Batch Insert (1000 items, 1 Txn)&#39;      | Job-CNUJVU | Default                |   NA |    NA |     ? |       ? |           ? |
| &#39;DocumentDb Insert (1000 items, No Batch API)&#39; | Job-CNUJVU | Default                |   NA |    NA |     ? |       ? |           ? |
|                                                |            |                        |      |       |       |         |             |
| &#39;SQLite Batch Insert (1000 items, 1 Txn)&#39;      | InProcess  | InProcessEmitToolchain |   NA |    NA |     ? |       ? |           ? |
| &#39;DocumentDb Insert (1000 items, No Batch API)&#39; | InProcess  | InProcessEmitToolchain |   NA |    NA |     ? |       ? |           ? |
|                                                |            |                        |      |       |       |         |             |
| &#39;SQLite Single Insert (AutoCommit)&#39;            | Job-CNUJVU | Default                |   NA |    NA |     ? |       ? |           ? |
| &#39;DocumentDb Single Insert&#39;                     | Job-CNUJVU | Default                |   NA |    NA |     ? |       ? |           ? |
|                                                |            |                        |      |       |       |         |             |
| &#39;SQLite Single Insert (AutoCommit)&#39;            | InProcess  | InProcessEmitToolchain |   NA |    NA |     ? |       ? |           ? |
| &#39;DocumentDb Single Insert&#39;                     | InProcess  | InProcessEmitToolchain |   NA |    NA |     ? |       ? |           ? |

Benchmarks with issues:
  InsertBenchmarks.'SQLite Batch Insert (1000 items, 1 Txn)': Job-CNUJVU(InvocationCount=1, UnrollFactor=1)
  InsertBenchmarks.'DocumentDb Insert (1000 items, No Batch API)': Job-CNUJVU(InvocationCount=1, UnrollFactor=1)
  InsertBenchmarks.'SQLite Batch Insert (1000 items, 1 Txn)': InProcess(Toolchain=InProcessEmitToolchain, InvocationCount=1, UnrollFactor=1)
  InsertBenchmarks.'DocumentDb Insert (1000 items, No Batch API)': InProcess(Toolchain=InProcessEmitToolchain, InvocationCount=1, UnrollFactor=1)
  InsertBenchmarks.'SQLite Single Insert (AutoCommit)': Job-CNUJVU(InvocationCount=1, UnrollFactor=1)
  InsertBenchmarks.'DocumentDb Single Insert': Job-CNUJVU(InvocationCount=1, UnrollFactor=1)
  InsertBenchmarks.'SQLite Single Insert (AutoCommit)': InProcess(Toolchain=InProcessEmitToolchain, InvocationCount=1, UnrollFactor=1)
  InsertBenchmarks.'DocumentDb Single Insert': InProcess(Toolchain=InProcessEmitToolchain, InvocationCount=1, UnrollFactor=1)
