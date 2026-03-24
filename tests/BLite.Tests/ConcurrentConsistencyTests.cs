using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using BLite.Bson;
using BLite.Core;
using BLite.Core.Storage;

namespace BLite.Tests;

/// <summary>
/// End-to-end concurrent write + read consistency tests.
///
/// Verifies that under heavy multi-threaded workloads (8+ threads)
/// with simultaneous writers and readers, the database remains fully
/// consistent: every committed document is present, no duplicates,
/// no corruption, correct values.
///
/// Design note: each writer thread uses its own collection to avoid
/// write-write conflicts on shared data pages. This mirrors the Server
/// layout's per-collection PageFile isolation and is the correct
/// concurrency model for BLite (concurrent I/O across collections).
/// </summary>
public class ConcurrentConsistencyTests : IDisposable
{
    private readonly string _dbPath;
    private readonly BLiteEngine _engine;

    public ConcurrentConsistencyTests()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"blite_consistency_{Guid.NewGuid()}.db");
        _engine = new BLiteEngine(_dbPath, PageFileConfig.Server(_dbPath));
    }

    public void Dispose()
    {
        _engine.Dispose();
        TryDeleteServerDb(_dbPath);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // 1. Concurrent inserts — 8 threads, each with own collection, verify all
    //    documents are present and correct after all commits.
    // ─────────────────────────────────────────────────────────────────────────

    [Fact]
    public async Task ConcurrentInsert_8Threads_AllDocumentsConsistent()
    {
        const int threadCount = 8;
        const int docsPerThread = 100;

        var allIds = new ConcurrentBag<(int thread, int index, BsonId id)>();

        var tasks = Enumerable.Range(0, threadCount).Select(t => Task.Run(async() =>
        {
            var collName = $"thread_{t}";
            using var session = _engine.OpenSession();
            var col = session.GetOrCreateCollection(collName);
            for (int i = 0; i < docsPerThread; i++)
            {
                var value = t * docsPerThread + i;
                var id = await col.InsertAsync(col.CreateDocument(["thread", "index", "value"], b => b
                    .AddInt32("thread", t)
                    .AddInt32("index", i)
                    .AddInt32("value", value)));
                allIds.Add((t, i, id));
            }
            await session.CommitAsync();
        })).ToArray();

        await Task.WhenAll(tasks);

        // Verify: total count matches
        var expectedTotal = threadCount * docsPerThread;
        Assert.Equal(expectedTotal, allIds.Count);

        // Verify: every document is retrievable with correct data
        using var readSession = _engine.OpenSession();
        foreach (var group in allIds.GroupBy(x => x.thread))
        {
            var readCol = readSession.GetOrCreateCollection($"thread_{group.Key}");
            Assert.Equal(docsPerThread, await readCol.CountAsync());

            foreach (var (thread, index, id) in group)
            {
                var doc = await readCol.FindByIdAsync(id);
                Assert.NotNull(doc);
                Assert.True(doc.TryGetInt32("thread", out var docThread));
                Assert.Equal(thread, docThread);
                Assert.True(doc.TryGetInt32("index", out var docIndex));
                Assert.Equal(index, docIndex);
                Assert.True(doc.TryGetInt32("value", out var docValue));
                Assert.Equal(thread * docsPerThread + index, docValue);
            }
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // 2. Concurrent reads while writing — readers see consistent seeded data
    // ─────────────────────────────────────────────────────────────────────────

    [Fact]
    public async Task ConcurrentInsertAndRead_8Threads_NoCorruption()
    {
        const int writerCount = 4;
        const int readerCount = 4;
        const int docsPerWriter = 100;
        const string readCollection = "seeded";

        // Pre-seed data for readers
        const int seedCount = 200;
        var seedIds = new BsonId[seedCount];
        {
            using var session = _engine.OpenSession();
            var col = session.GetOrCreateCollection(readCollection);
            for (int i = 0; i < seedCount; i++)
            {
                seedIds[i] = await col.InsertAsync(col.CreateDocument(["seed", "value"], b => b
                    .AddInt32("seed", 1)
                    .AddInt32("value", i)));
            }
            await session.CommitAsync();
        }

        var writeIds = new ConcurrentBag<(int writer, BsonId id)>();
        var readErrors = new ConcurrentBag<string>();
        var readSuccesses = new ConcurrentBag<int>();
        using var cts = new CancellationTokenSource();

        // Writers: each thread inserts into its own collection
        var writerTasks = Enumerable.Range(0, writerCount).Select(t => Task.Run(async () =>
        {
            using var session = _engine.OpenSession();
            var col = session.GetOrCreateCollection($"writer_{t}");
            for (int i = 0; i < docsPerWriter; i++)
            {
                var id = await col.InsertAsync(col.CreateDocument(["writer", "seq"], b => b
                    .AddInt32("writer", t)
                    .AddInt32("seq", i)));
                writeIds.Add((t, id));
            }
            await session.CommitAsync();
        })).ToArray();

        // Readers: continuously read seeded documents while writers are active
        var readerTasks = Enumerable.Range(0, readerCount).Select(r => Task.Run(async () =>
        {
            int reads = 0;
            while (!cts.Token.IsCancellationRequested)
            {
                using var session = _engine.OpenSession();
                var col = session.GetOrCreateCollection(readCollection);
                var idx = reads % seedCount;
                var doc = await col.FindByIdAsync(seedIds[idx]);
                if (doc == null)
                {
                    readErrors.Add($"Reader {r}: seeded doc {seedIds[idx]} not found on read #{reads}");
                    break;
                }
                if (!doc.TryGetInt32("value", out var val) || val != idx)
                {
                    readErrors.Add($"Reader {r}: expected value={idx}, got {val} for doc {seedIds[idx]}");
                    break;
                }
                reads++;
                if (reads >= 500) break;
            }
            readSuccesses.Add(reads);
        })).ToArray();

        await Task.WhenAll(writerTasks);
        cts.Cancel();
        await Task.WhenAll(readerTasks);

        // Assert no read errors
        Assert.True(readErrors.IsEmpty,
            $"Read errors detected:\n{string.Join("\n", readErrors)}");

        // Assert all readers did actual work
        Assert.All(readSuccesses, count => Assert.True(count > 0, "Reader completed 0 reads"));

        // Verify all written documents exist per collection
        using var verifySession = _engine.OpenSession();
        for (int t = 0; t < writerCount; t++)
        {
            var verifyCol = verifySession.GetOrCreateCollection($"writer_{t}");
            Assert.Equal(docsPerWriter, await verifyCol.CountAsync());
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // 3. High contention — 8 threads × 50 transactions × 5 docs = 2000 docs
    // ─────────────────────────────────────────────────────────────────────────

    [Fact]
    public async Task HighContention_ManySmallTransactions_ConsistentFinalState()
    {
        const int threadCount = 8;
        const int txnsPerThread = 50;
        const int docsPerTxn = 5;

        var allIds = new ConcurrentBag<(int thread, int txn, int doc, BsonId id)>();

        var tasks = Enumerable.Range(0, threadCount).Select(t => Task.Run(async () =>
        {
            var collName = $"contention_{t}";
            for (int txn = 0; txn < txnsPerThread; txn++)
            {
                using var session = _engine.OpenSession();
                var col = session.GetOrCreateCollection(collName);
                for (int d = 0; d < docsPerTxn; d++)
                {
                    var id = await col.InsertAsync(col.CreateDocument(["t", "tx", "d"], b => b
                        .AddInt32("t", t)
                        .AddInt32("tx", txn)
                        .AddInt32("d", d)));
                    allIds.Add((t, txn, d, id));
                }
                await session.CommitAsync();
            }
        })).ToArray();

        await Task.WhenAll(tasks);

        var expectedTotal = threadCount * txnsPerThread * docsPerTxn;
        Assert.Equal(expectedTotal, allIds.Count);

        // Full verification pass
        using var readSession = _engine.OpenSession();
        foreach (var group in allIds.GroupBy(x => x.thread))
        {
            var readCol = readSession.GetOrCreateCollection($"contention_{group.Key}");
            Assert.Equal(txnsPerThread * docsPerTxn, await readCol.CountAsync());

            foreach (var (thread, txn, doc, id) in group)
            {
                var found = await readCol.FindByIdAsync(id);
                Assert.NotNull(found);
                Assert.True(found.TryGetInt32("t", out var ft) && ft == thread);
                Assert.True(found.TryGetInt32("tx", out var ftx) && ftx == txn);
                Assert.True(found.TryGetInt32("d", out var fd) && fd == doc);
            }
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // 4. Mixed CRUD — insert, update, delete concurrently, verify integrity
    //    Each thread operates on its own pre-seeded collection.
    // ─────────────────────────────────────────────────────────────────────────

    [Fact]
    public async Task ConcurrentMixedCrud_DataIntegrity()
    {
        const int threadCount = 8;
        const int opsPerThread = 50;

        // Seed documents per thread
        var seedIdsByThread = new BsonId[threadCount][];
        for (int t = 0; t < threadCount; t++)
        {
            seedIdsByThread[t] = new BsonId[opsPerThread];
            using var session = _engine.OpenSession();
            var col = session.GetOrCreateCollection($"crud_{t}");
            for (int i = 0; i < opsPerThread; i++)
            {
                seedIdsByThread[t][i] = await col.InsertAsync(col.CreateDocument(["status", "v"], b => b
                    .AddString("status", "original")
                    .AddInt32("v", i)));
            }
            await session.CommitAsync();
        }

        var updatedIds = new ConcurrentBag<(int thread, BsonId id)>();
        var deletedIds = new ConcurrentBag<(int thread, BsonId id)>();
        var insertedIds = new ConcurrentBag<(int thread, BsonId id)>();

        var tasks = Enumerable.Range(0, threadCount).Select(t => Task.Run(async () =>
        {
            using var session = _engine.OpenSession();
            var col = session.GetOrCreateCollection($"crud_{t}");

            for (int i = 0; i < opsPerThread; i++)
            {
                var op = i % 3; // 0=update, 1=delete, 2=insert

                switch (op)
                {
                    case 0: // update
                        var updated = col.CreateDocument(["status", "v"], b => b
                            .AddString("status", "updated")
                            .AddInt32("v", i * 10));
                        if (await col.UpdateAsync(seedIdsByThread[t][i], updated))
                            updatedIds.Add((t, seedIdsByThread[t][i]));
                        break;

                    case 1: // delete
                        if (await col.DeleteAsync(seedIdsByThread[t][i]))
                            deletedIds.Add((t, seedIdsByThread[t][i]));
                        break;

                    case 2: // insert new
                        var id = await col.InsertAsync(col.CreateDocument(["status", "v"], b => b
                            .AddString("status", "new")
                            .AddInt32("v", i)));
                        insertedIds.Add((t, id));
                        break;
                }
            }
            await session.CommitAsync();
        })).ToArray();

        await Task.WhenAll(tasks);

        // Verify final state per thread
        using var verifySession = _engine.OpenSession();
        for (int t = 0; t < threadCount; t++)
        {
            var verifyCol = verifySession.GetOrCreateCollection($"crud_{t}");

            // Updated docs should have new values
            foreach (var (thread, id) in updatedIds.Where(x => x.thread == t))
            {
                var doc = await verifyCol.FindByIdAsync(id);
                Assert.NotNull(doc);
                Assert.True(doc.TryGetString("status", out var status));
                Assert.Equal("updated", status);
            }

            // Deleted docs should be gone
            foreach (var (thread, id) in deletedIds.Where(x => x.thread == t))
            {
                var doc = await verifyCol.FindByIdAsync(id);
                Assert.Null(doc);
            }

            // Inserted docs should exist
            foreach (var (thread, id) in insertedIds.Where(x => x.thread == t))
            {
                var doc = await verifyCol.FindByIdAsync(id);
                Assert.NotNull(doc);
                Assert.True(doc.TryGetString("status", out var status));
                Assert.Equal("new", status);
            }
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // 5. Async commit path — 8 threads via CommitAsync + concurrent readers
    // ─────────────────────────────────────────────────────────────────────────

    [Fact]
    public async Task ConcurrentAsyncCommit_WithReaders_AllConsistent()
    {
        const int writerCount = 8;
        const int docsPerWriter = 80;
        const string readCollection = "async_seed";

        // Pre-seed for readers
        const int seedCount = 100;
        var seedIds = new BsonId[seedCount];
        {
            using var session = _engine.OpenSession();
            var col = session.GetOrCreateCollection(readCollection);
            for (int i = 0; i < seedCount; i++)
            {
                seedIds[i] = await col.InsertAsync(col.CreateDocument(["w", "s"], b => b
                    .AddInt32("w", -1)
                    .AddInt32("s", i)));
            }
            await session.CommitAsync();
        }

        var allIds = new ConcurrentBag<(int writer, int seq, BsonId id)>();
        var readErrors = new ConcurrentBag<string>();
        using var cts = new CancellationTokenSource();

        // Writers: each thread uses its own collection, commits via async path
        var writerTasks = Enumerable.Range(0, writerCount).Select(w => Task.Run(async () =>
        {
            using var session = _engine.OpenSession();
            var col = session.GetOrCreateCollection($"async_{w}");
            for (int i = 0; i < docsPerWriter; i++)
            {
                var id = await col.InsertAsync(col.CreateDocument(["w", "s"], b => b
                    .AddInt32("w", w)
                    .AddInt32("s", i)));
                allIds.Add((w, i, id));
            }
            await session.CommitAsync();
        })).ToArray();

        // Readers: scan the seeded collection while writers are active
        var readerTasks = Enumerable.Range(0, 4).Select(_ => Task.Run(async () =>
        {
            while (!cts.Token.IsCancellationRequested)
            {
                using var session = _engine.OpenSession();
                var col = session.GetOrCreateCollection(readCollection);
                await foreach (var doc in col.FindAllAsync())
                {
                    if (!doc.TryGetInt32("w", out _) || !doc.TryGetInt32("s", out _))
                    {
                        readErrors.Add("Document missing expected fields during scan");
                        return;
                    }
                }
                Thread.Sleep(1);
            }
        })).ToArray();

        await Task.WhenAll(writerTasks);
        cts.Cancel();
        await Task.WhenAll(readerTasks);

        Assert.True(readErrors.IsEmpty,
            $"Read errors:\n{string.Join("\n", readErrors)}");

        var expectedTotal = writerCount * docsPerWriter;
        Assert.Equal(expectedTotal, allIds.Count);

        // Final full verification
        using var verifySession = _engine.OpenSession();
        foreach (var group in allIds.GroupBy(x => x.writer))
        {
            var verifyCol = verifySession.GetOrCreateCollection($"async_{group.Key}");
            Assert.Equal(docsPerWriter, await verifyCol.CountAsync());

            foreach (var (writer, seq, id) in group)
            {
                var doc = await verifyCol.FindByIdAsync(id);
                Assert.NotNull(doc);
                Assert.True(doc.TryGetInt32("w", out var w) && w == writer);
                Assert.True(doc.TryGetInt32("s", out var s) && s == seq);
            }
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Cleanup helper
    // ─────────────────────────────────────────────────────────────────────────

    private static void TryDeleteServerDb(string dbPath)
    {
        try { if (File.Exists(dbPath)) File.Delete(dbPath); } catch { }
        try
        {
            var idx = Path.ChangeExtension(dbPath, ".idx");
            if (File.Exists(idx)) File.Delete(idx);
        }
        catch { }

        var dir = Path.GetDirectoryName(dbPath) ?? ".";
        var name = Path.GetFileNameWithoutExtension(dbPath);
        try
        {
            var walFile = Path.Combine(dir, "wal", name + ".wal");
            if (File.Exists(walFile)) File.Delete(walFile);
        }
        catch { }
        try
        {
            var collDir = Path.Combine(dir, "collections", name);
            if (Directory.Exists(collDir)) Directory.Delete(collDir, true);
        }
        catch { }
    }
}
