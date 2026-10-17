using BLite.Core.Storage;

namespace AotSmoke;

// The calls of the documented AOT-unsafe APIs. Each line marked "aot-expect:" must get exactly the
// warning codes it names in the Native AOT publish log, located at this file and line. A new
// annotated API is one more call here with its marker; run.sh reads the markers from this file.

internal static partial class Program
{
    static partial void AddSections(List<(string Name, Func<Dump, string, Task> Run)> sections)
        => sections.Add(("LINQ and index creation (documented as not AOT-safe)", LinqAsync));

    private static async Task LinqAsync(Dump dump, string dir)
    {
        using var db = OpenTyped(Path.Combine(dir, "linq.db"), crypto: null);
        await db.Docs.InsertBulkAsync([new StoreDoc { Id = 1, Name = "a", Count = 4 }, new StoreDoc { Id = 2, Name = "b", Count = 2 }]);

        var many = db.Docs.AsQueryable().Where(d => d.Count > 3).ToList(); // aot-expect: IL2026 IL3050
        dump.Line($"AsQueryable().Where(Count > 3): [{string.Join(",", many.Select(d => d.Id))}]");

        var index = await db.Docs.CreateIndexAsync(d => d.Name, "idx_name"); // aot-expect: IL3050
        dump.Line($"CreateIndexAsync: {(index == null ? "null" : "created")}");

        using var watch = db.Docs.Watch().Subscribe(new NullObserver()); // aot-expect: IL2026
        dump.Line("typed Watch: subscribed");
    }

    private sealed class NullObserver : IObserver<BLite.Core.CDC.ChangeStreamEvent<int, StoreDoc>>
    {
        public void OnNext(BLite.Core.CDC.ChangeStreamEvent<int, StoreDoc> value) { }
        public void OnError(Exception error) { }
        public void OnCompleted() { }
    }
}
