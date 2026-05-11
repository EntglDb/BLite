using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using BLite.Bson;
using BLite.Core.Audit;
using BLite.Shared;
using Xunit;

namespace BLite.Tests.Audit;

/// <summary>
/// Demo <see cref="IBLiteAuditSink"/> implementation referenced from the spec.
/// Writes one human-readable line per emitted event to <see cref="Console.Out"/>.
/// Intentionally minimal — a real production sink would batch / buffer / async-dispatch.
/// </summary>
public sealed class ConsoleAuditSink : IBLiteAuditSink
{
    private readonly TextWriter _out;

    public ConsoleAuditSink() : this(Console.Out) { }

    public ConsoleAuditSink(TextWriter @out)
    {
        _out = @out ?? throw new ArgumentNullException(nameof(@out));
    }

    public void OnInsert(in InsertAuditEvent evt)
        => _out.WriteLine($"[BLite][Insert] txn={evt.TransactionId} coll={evt.CollectionName} size={evt.DocumentSizeBytes}B elapsed={evt.Elapsed.TotalMilliseconds:F3}ms");

    public void OnQuery(in QueryAuditEvent evt)
        => _out.WriteLine($"[BLite][Query]  coll={evt.CollectionName} strategy={evt.Strategy} index={evt.IndexName ?? "(none)"} count={evt.ResultCount} elapsed={evt.Elapsed.TotalMilliseconds:F3}ms");

    public void OnCommit(in CommitAuditEvent evt)
        => _out.WriteLine($"[BLite][Commit] txn={evt.TransactionId} pages={evt.PagesWritten} wal={evt.WalSizeBytes}B elapsed={evt.Elapsed.TotalMilliseconds:F3}ms");

    public void OnSlowOperation(in SlowOperationEvent evt)
        => _out.WriteLine($"[BLite][SLOW {evt.OperationType}] coll={evt.CollectionName} elapsed={evt.Elapsed.TotalMilliseconds:F3}ms detail={evt.Detail}");
}

/// <summary>Smoke test that wires <see cref="ConsoleAuditSink"/> into a context and checks lines are emitted.</summary>
public class ConsoleAuditSinkTests : IDisposable
{
    private readonly string _dbPath;

    public ConsoleAuditSinkTests()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"blite_console_{Guid.NewGuid()}.db");
    }

    public void Dispose()
    {
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
        var wal = Path.ChangeExtension(_dbPath, ".wal");
        if (File.Exists(wal)) File.Delete(wal);
    }

    [Fact]
    public async Task ConsoleAuditSink_Writes_One_Line_Per_Event_Kind()
    {
        // Capture into an in-memory writer rather than polluting test runner stdout.
        using var buffer = new StringWriter();
        var sink = new ConsoleAuditSink(buffer);
        var options = new BLiteAuditOptions
        {
            Sink = sink,
            SlowQueryThreshold = TimeSpan.FromTicks(1), // force slow path so all four lines appear
        };

        using (var db = new MinimalDbContext(_dbPath, options))
        {
            await db.Users.InsertAsync(new User { Name = "Demo", Age = 1 });
            _ = db.Users.AsQueryable().ToList();
        }

        var output = buffer.ToString();
        Assert.Contains("[BLite][Insert]", output);
        Assert.Contains("[BLite][Query]", output);
        Assert.Contains("[BLite][Commit]", output);
        Assert.Contains("[BLite][SLOW", output);
    }
}
