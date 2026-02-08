```

BenchmarkDotNet v0.15.8, Windows 11 (10.0.22631.6345/23H2/2023Update/SunValley3)
13th Gen Intel Core i7-13800H 2.50GHz, 1 CPU, 20 logical and 14 physical cores
.NET SDK 10.0.102
  [Host] : .NET 10.0.2 (10.0.2, 10.0.225.61305), X64 RyuJIT x86-64-v3

Job=InProcess  Toolchain=InProcessEmitToolchain  InvocationCount=1  
UnrollFactor=1  

```
| Method                                                | Mean       | Error     | StdDev     | Ratio | RatioSD | Gen0       | Gen1      | Allocated    | Alloc Ratio |
|------------------------------------------------------ |-----------:|----------:|-----------:|------:|--------:|-----------:|----------:|-------------:|------------:|
| &#39;SQLite Batch Insert (1000 items, Forced Checkpoint)&#39; | 217.924 ms | 9.0230 ms | 25.5968 ms |     ? |       ? |  5000.0000 |         - |  65260.37 KB |           ? |
| &#39;SQLite Batch Insert (1000 items, 1 Txn)&#39;             | 141.985 ms | 3.3877 ms |  9.6654 ms |     ? |       ? |  5000.0000 |         - |  65262.37 KB |           ? |
| &#39;DocumentDb Batch Insert (1000 items, 1 Txn)&#39;         | 467.537 ms | 9.2539 ms | 18.0491 ms |     ? |       ? | 67000.0000 | 5000.0000 | 829190.13 KB |           ? |
|                                                       |            |           |            |       |         |            |           |              |             |
| &#39;SQLite Single Insert (AutoCommit)&#39;                   |   4.577 ms | 0.1464 ms |  0.4271 ms |  1.01 |    0.13 |          - |         - |     10.19 KB |        1.00 |
| &#39;DocumentDb Single Insert&#39;                            |   1.377 ms | 0.0327 ms |  0.0958 ms |  0.30 |    0.03 |          - |         - |    127.66 KB |       12.53 |
