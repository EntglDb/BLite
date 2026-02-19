# Reddit Post — r/dotnet

**Title:**
> BLite – I built an embedded document database for .NET from scratch after LiteDB went dark. Zero reflection, NativeAOT, now open source.

---

Hey r/dotnet,

This is going to be a bit of a story. Bear with me.

**The LiteDB problem**

I've been writing embedded software professionally for years. Local databases are a constant — devices that need to store data locally, fast, without a server. LiteDB was my go-to. It's a solid piece of work and the API is genuinely pleasant to use.

Then it went quiet. No updates, no responses to issues, PRs sitting open with no acknowledgment. I even tried contributing to keep things moving — nothing. At some point the maintainer just… disappeared. I don't say this to be harsh, open source is hard and life happens. But the practical result was that I had a dependency in multiple production projects that wasn't going anywhere.

And that started to hurt. NativeAOT is increasingly relevant in my work — trimmed binaries, fast startup, devices where every MB and every millisecond matters. LiteDB's reflection-heavy internals make it incompatible with that direction. The more I leaned into AOT in my projects, the more LiteDB became a liability.

**The decision to build from scratch**

I didn't take it lightly. Writing a database engine means touching things I had only ever seen academically — page-based file storage, write-ahead logs, B-Trees as actual code rather than textbook diagrams. But I was tired of waiting and more tired of compromising.

So I did it.

**What I built**

**BLite** is an embedded BSON document database for .NET. Zero reflection. Source generators do all the serialization work at compile time.

One goal I kept front and center: *from a class, you should be able to persist a document*. No ceremony, no separate mapping configuration, no boilerplate. You annotate, the generator handles the rest:

```csharp
[BCollection]
public class Customer
{
    public ObjectId? Id { get; set; }
    public string Name { get; set; }

    [BIndex]
    public string TaxCode { get; set; }

    public Address? Address { get; set; }
}
```

The API is DbContext-style (EF Core-familiar on purpose — no point in making people learn a new mental model):

```csharp
[BLiteContext]
public partial class AppDbContext : DocumentDbContext
{
    public DocumentCollection<Customer> Customers { get; private set; }
}

await using var db = new AppDbContext("myapp.db");
await db.Customers.InsertAsync(customer);
var found = await db.Customers.FindAsync(c => c.TaxCode == "12345");
```

**C-BSON: a small optimization I'm proud of**

While working on the serialization layer I noticed something obvious in retrospect: in a database, field *names* repeat constantly across millions of documents. Standard BSON stores the full key string every time. So I built **C-BSON** (Compressed BSON) — same binary format, but field names are compressed into a global dictionary and stored as short integer keys instead. The documents stay readable, the serialization stays fast, and the storage footprint shrinks noticeably on real-world data.

**What's in the box:**
- B-Tree, Hash, and R-Tree (spatial/geo) indexes
- Change Data Capture — subscribe to insert/update/delete events
- Vector search index (experimental)
- Full async API, ACID transactions
- Zero external dependencies

**Benchmarks (BenchmarkDotNet, .NET 10):** around 8x faster than SQLite on single inserts, ~2.4x faster serialization, 0 bytes allocated on the serialization path. Full benchmark setup is in the repo — please run it and challenge the numbers if something looks off.

**Why I took it seriously**

I use DDD and a Clean Architecture approach every day. The persistence layer is often "the necessary evil" — the thing that quietly shapes your domain because *the database doesn't support this feature*. I've seen that compromise too many times. I wanted to build something that stays out of the domain model's way, covers the features that actually matter to application developers, and has the test suite to back it up.

**Links:**
- GitHub: https://github.com/EntglDb/BLite
- NuGet: https://www.nuget.org/packages/BLite

If you've used LiteDB or switched away from it — I'd really like to hear your experience. And if you're currently stuck with it for the same reasons I was, give this a look.

Happy to answer questions on any design decision. There were plenty of them.
