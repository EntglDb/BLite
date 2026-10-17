// The operation generator of the model-based store test, copied verbatim from
// tests/BLite.Tests/Oracles/StoreModel.cs so that the Consumer runs the same sequences as
// StoreModelTests and WalCrashTests without linking the test project (its runner verifies the
// indexes through LINQ, which is not AOT-safe). run.sh fails if the lines between the
// "verbatim" markers drift from the source.

namespace AotSmoke;

internal static class StoreModel
{
    // ── verbatim from StoreModel.cs: begin ──
    public const int IdPool = 120;
    private static readonly string[] s_codes = Enumerable.Range(0, 40).Select(i => $"c{i:00}").ToArray();
    private static readonly int[] s_payloadSizes = [0, 0, 20, 20, 300, 300, 2500, 2500, 18000];

    /// <summary>A seeded random sequence of <paramref name="steps"/> operations.</summary>
    public static List<StoreOp> Generate(int seed, int steps)
    {
        var rnd = new Random(seed);
        var ops = new List<StoreOp>(steps);
        var live = new HashSet<int>(); // the generator's own guess of which ids exist (to aim at them)

        StoreDoc Doc(int id) => new()
        {
            Id = id,
            Name = $"n{rnd.Next(1000)}",
            Count = rnd.Next(10),
            Code = rnd.Next(3) == 0 ? null : s_codes[rnd.Next(s_codes.Length)],
            Payload = new string((char)('a' + rnd.Next(26)), s_payloadSizes[rnd.Next(s_payloadSizes.Length)]),
        };
        int ExistingOrAny() => live.Count > 0 && rnd.Next(100) < 70 ? live.ElementAt(rnd.Next(live.Count)) : rnd.Next(1, IdPool + 1);

        for (int i = 0; i < steps; i++)
        {
            var roll = rnd.Next(100);
            if (roll < 22)
            {
                var d = Doc(rnd.Next(100) < 15 ? ExistingOrAny() : rnd.Next(1, IdPool + 1));
                ops.Add(new StoreOp(StoreOpKind.Insert, d.Id, d.Count, d.Code, d.Payload.Length, Docs: [d]));
                live.Add(d.Id);
            }
            else if (roll < 36)
            {
                var d = Doc(ExistingOrAny());
                ops.Add(new StoreOp(StoreOpKind.Update, d.Id, d.Count, d.Code, d.Payload.Length, Docs: [d]));
            }
            else if (roll < 48)
            {
                var id = ExistingOrAny();
                ops.Add(new StoreOp(StoreOpKind.Delete, id));
                live.Remove(id);
            }
            else if (roll < 56)
            {
                var d = Doc(ExistingOrAny());
                ops.Add(new StoreOp(StoreOpKind.Upsert, d.Id, d.Count, d.Code, d.Payload.Length, Docs: [d]));
                live.Add(d.Id);
            }
            else if (roll < 60)
            {
                var docs = Enumerable.Range(0, rnd.Next(2, 9)).Select(_ => Doc(rnd.Next(1, IdPool + 1))).DistinctBy(d => d.Id).ToArray();
                ops.Add(new StoreOp(StoreOpKind.InsertBulk, Docs: docs));
                foreach (var d in docs) live.Add(d.Id);
            }
            else if (roll < 63)
            {
                var ids = Enumerable.Range(0, rnd.Next(2, 9)).Select(_ => ExistingOrAny()).Distinct().ToArray();
                ops.Add(new StoreOp(StoreOpKind.DeleteBulk, Ids: ids));
                foreach (var id in ids) live.Remove(id);
            }
            else if (roll < 71) ops.Add(new StoreOp(StoreOpKind.FindById, ExistingOrAny()));
            else if (roll < 78) ops.Add(new StoreOp(StoreOpKind.Begin));
            else if (roll < 86) ops.Add(new StoreOp(StoreOpKind.Commit));
            else if (roll < 90) ops.Add(new StoreOp(StoreOpKind.Rollback));
            else if (roll < 94) ops.Add(new StoreOp(StoreOpKind.Checkpoint));
            else if (roll < 97) ops.Add(new StoreOp(StoreOpKind.Backup));
            else ops.Add(new StoreOp(StoreOpKind.Reopen));
        }
        return ops;
    }
    // ── verbatim from StoreModel.cs: end ──

    public static StoreDoc Clone(StoreDoc d) => new() { Id = d.Id, Name = d.Name, Count = d.Count, Code = d.Code, Payload = d.Payload };
}

// ── verbatim from StoreModel.cs: begin ──
internal enum StoreOpKind { Insert, Update, Delete, Upsert, InsertBulk, DeleteBulk, FindById, Begin, Commit, Rollback, Checkpoint, Backup, Reopen }

/// <summary>One step. Only inputs are recorded; what the step must do follows from the model at replay.</summary>
internal sealed record StoreOp(StoreOpKind Kind, int Id = 0, int Count = 0, string? Code = null, int PayloadLength = 0, int[]? Ids = null, StoreDoc[]? Docs = null)
{
    public override string ToString() => Kind switch
    {
        StoreOpKind.Insert or StoreOpKind.Update or StoreOpKind.Upsert => $"{Kind}(id={Id}, count={Count}, code={Code ?? "null"}, payload={PayloadLength} chars)",
        StoreOpKind.InsertBulk => $"InsertBulk({string.Join(", ", Docs!.Select(d => $"id={d.Id}/count={d.Count}/code={d.Code ?? "null"}/payload={d.Payload.Length}"))})",
        StoreOpKind.DeleteBulk => $"DeleteBulk(ids={string.Join(",", Ids!)})",
        StoreOpKind.FindById or StoreOpKind.Delete => $"{Kind}(id={Id})",
        _ => Kind.ToString(),
    };
}
// ── verbatim from StoreModel.cs: end ──
