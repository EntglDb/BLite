using BLite.Bson;
using BLite.Core.Storage;
using BLite.Shared;
using BLite.Tests;

// ─── Layout selection ─────────────────────────────────────────────────────
// Flags:
//   --multi-file          : server layout (separate WAL, index, per-collection files)
//   --layout=small|large  : override page size preset (default = Default / 16 KB)
//   (no flag)             : classic single-file embedded layout
var isMultiFile = args.Any(a => a.Equals("--multi-file", StringComparison.OrdinalIgnoreCase));
var layoutArg   = args.FirstOrDefault(a => a.StartsWith("--layout=", StringComparison.OrdinalIgnoreCase));
PageFileConfig basePreset = layoutArg?.Split('=')[1].ToLowerInvariant() switch
{
    "small" => PageFileConfig.Small,
    "large" => PageFileConfig.Large,
    _       => PageFileConfig.Default,
};

// ─── Setup ────────────────────────────────────────────────────────────────
var dbPath = Path.Combine(Path.GetTempPath(), "blie_demo", "blite_demo.db");
var config = isMultiFile
    ? PageFileConfig.Server(dbPath, basePreset)
    : basePreset;

// Remove any previous run's files (single-file and multi-file companions)
DeleteIfExists(dbPath);
DeleteIfExists(Path.ChangeExtension(dbPath, ".wal"));
DeleteIfExists(Path.ChangeExtension(dbPath, ".idx"));
var collDir = Path.Combine(Path.GetDirectoryName(dbPath)!, "collections",
    Path.GetFileNameWithoutExtension(dbPath));
if (Directory.Exists(collDir)) Directory.Delete(collDir, recursive: true);
var walDir = Path.Combine(Path.GetDirectoryName(dbPath)!, "wal");
if (isMultiFile && Directory.Exists(walDir)) Directory.Delete(walDir, recursive: true);

string layoutLabel = isMultiFile
    ? $"Multi-file server  (WAL + index + per-collection files, {basePreset.PageSize / 1024} KB pages)"
    : $"Single-file embedded  ({basePreset.PageSize / 1024} KB pages)";

Console.WriteLine("╔══════════════════════════════════════╗");
Console.WriteLine("║       BLite Demo Database Seeder     ║");
Console.WriteLine("╚══════════════════════════════════════╝");
Console.WriteLine($"Layout : {layoutLabel}");
Console.WriteLine($"Path   : {dbPath}");
Console.WriteLine();

var rng = new Random(42);

// ─── Helper lambdas ───────────────────────────────────────────────────────
void DeleteIfExists(string path) { if (File.Exists(path)) File.Delete(path); }
ObjectId NewId() => ObjectId.NewObjectId();

string Pick(params string[] values) => values[rng.Next(values.Length)];

string[] firstNames  = ["Alice", "Bob", "Charlie", "Diana", "Ethan", "Fiona",
                        "George", "Hannah", "Ivan", "Julia", "Kevin", "Laura"];
string[] lastNames   = ["Rossi", "Bianchi", "Ferrari", "Esposito", "Romano",
                        "Colombo", "Ricci", "Marino", "Greco", "Bruno"];
string[] cityNames   = ["Milano", "Roma", "Napoli", "Torino", "Firenze", "Bologna"];
string[] streetNames = ["Via Roma", "Corso Italia", "Via Garibaldi", "Piazza Duomo", "Via Manzoni"];
string[] zipCodes    = ["20121", "00100", "80100", "10100", "50100", "40100"];
string[] tags        = ["premium", "vip", "new", "active", "verified", "beta"];
string[] carriers    = ["DHL", "FedEx", "UPS", "GLS", "BRT"];
string[] statuses    = ["Pending", "Shipped", "Delivered", "Cancelled", "Processing"];
string[] currencies  = ["EUR", "USD", "GBP"];
string[] skuPrefixes = ["LAPTOP", "PHONE", "TABLET", "DESK", "CHAIR"];
string[] productTitles =
[
    "Laptop Pro", "Mechanical Keyboard", "4K Monitor", "Wireless Mouse",
    "USB-C Hub", "Webcam HD", "Noise-Cancelling Headphones", "SSD 1TB",
    "RAM 32GB", "Ergonomic Chair"
];

string RandName()  => $"{Pick(firstNames)} {Pick(lastNames)}";
string RandEmail(string name) => $"{name.Replace(" ", ".").ToLower()}@demo.it";
string RandPhone() => $"+39 3{rng.Next(10, 99)} {rng.Next(1_000_000, 9_999_999)}";
City    RandCity()    => new() { Name = Pick(cityNames), ZipCode = Pick(zipCodes) };
Address RandAddress() => new() { Street = $"{Pick(streetNames)} {rng.Next(1, 200)}", City = RandCity() };
float[] RandVector(int dims) =>
    Enumerable.Range(0, dims).Select(_ => (float)rng.NextDouble()).ToArray();

using var db = new TestDbContext(dbPath, config);

// ─── Users ────────────────────────────────────────────────────────────────
await SeedAsync("Users", async () =>
{
    foreach (var name in firstNames)
        await db.Users.InsertAsync(new User { Id = NewId(), Name = name, Age = rng.Next(20, 65) });
});

// ─── AnnotatedUsers ───────────────────────────────────────────────────────
await SeedAsync("AnnotatedUsers", async () =>
{
    for (int i = 0; i < 8; i++)
    {
        var name = RandName();
        await db.AnnotatedUsers.InsertAsync(new AnnotatedUser
        {
            Id       = NewId(),
            Name     = name[..Math.Min(name.Length, 50)],
            Age      = rng.Next(18, 70),
            Location = (rng.NextDouble() * 10 + 40, rng.NextDouble() * 10 + 10)
        });
    }
});

// ─── ComplexUsers ─────────────────────────────────────────────────────────
await SeedAsync("ComplexUsers", async () =>
{
    for (int i = 0; i < 6; i++)
        await db.ComplexUsers.InsertAsync(new ComplexUser
        {
            Id             = NewId(),
            Name           = RandName(),
            MainAddress    = RandAddress(),
            OtherAddresses = [RandAddress(), RandAddress()],
            Tags           = [Pick(tags), Pick(tags), Pick(tags)]
        });
});

// ─── People ───────────────────────────────────────────────────────────────
await SeedAsync("People", async () =>
{
    for (int i = 1; i <= 10; i++)
        await db.People.InsertAsync(new Person { Id = i, Name = RandName(), Age = rng.Next(18, 80) });
});

// ─── PeopleV2 ─────────────────────────────────────────────────────────────
await SeedAsync("PeopleV2", async () =>
{
    for (int i = 0; i < 8; i++)
        await db.PeopleV2.InsertAsync(new PersonV2 { Id = NewId(), Name = RandName(), Age = rng.Next(18, 80) });
});

// ─── PeopleWithContacts ───────────────────────────────────────────────────
await SeedAsync("PeopleWithContacts", async () =>
{
    for (int i = 0; i < 5; i++)
    {
        var name = RandName();
        await db.PeopleWithContacts.InsertAsync(new PersonWithContact
        {
            Id          = NewId(),
            Name        = name,
            MainContact = new ContactInfo { Id = i * 10 + 1, Email = RandEmail(name), Phone = RandPhone() },
            Contacts    = [new ContactInfo { Id = i * 10 + 2, Email = $"alt.{RandEmail(name)}", Phone = RandPhone() }]
        });
    }
});

// ─── Products ─────────────────────────────────────────────────────────────
await SeedAsync("Products", async () =>
{
    for (int i = 0; i < productTitles.Length; i++)
        await db.Products.InsertAsync(new Product
        {
            Id    = i + 1,
            Title = productTitles[i],
            Price = Math.Round((decimal)(rng.NextDouble() * 1500 + 9.99), 2)
        });
});

// ─── Orders ───────────────────────────────────────────────────────────────
await SeedAsync("Orders", async () =>
{
    for (int i = 1; i <= 8; i++)
        await db.Orders.InsertAsync(new Order
        {
            Id           = new OrderId($"ORD-{2025_000 + i:D6}"),
            CustomerName = RandName()
        });
});

// ─── CustomerOrders ───────────────────────────────────────────────────────
await SeedAsync("CustomerOrders", async () =>
{
    for (int i = 1; i <= 6; i++)
    {
        var lines = Enumerable.Range(1, rng.Next(2, 5)).Select(_ =>
        {
            var qty   = rng.Next(1, 10);
            var price = Math.Round((decimal)(rng.NextDouble() * 500 + 5), 2);
            return new OrderLine
            {
                Sku         = $"{Pick(skuPrefixes)}-{rng.Next(1000, 9999)}",
                ProductName = productTitles[rng.Next(productTitles.Length)],
                Quantity    = qty,
                UnitPrice   = price,
                Subtotal    = qty * price,
                Tags        = [Pick(tags), Pick(tags)]
            };
        }).ToList();

        var subtotal = lines.Sum(l => l.Subtotal);
        var tax      = Math.Round(subtotal * 0.22m, 2);
        var cust     = RandName();

        await db.CustomerOrders.InsertAsync(new CustomerOrder
        {
            Id          = $"CO-{i:D4}",
            OrderNumber = $"#{2025_000 + i}",
            PlacedAt    = DateTime.UtcNow.AddDays(-rng.Next(0, 180)),
            Status      = Pick(statuses),
            Currency    = Pick(currencies),
            Subtotal    = subtotal,
            TaxAmount   = tax,
            Total       = subtotal + tax,
            Customer = new CustomerContact
            {
                FullName       = cust,
                Email          = RandEmail(cust),
                Phone          = RandPhone(),
                BillingAddress = new PostalAddress
                {
                    Street  = RandAddress().Street,
                    City    = Pick(cityNames),
                    ZipCode = Pick(zipCodes),
                    Country = "Italy"
                }
            },
            Shipping = new ShippingInfo
            {
                Carrier           = Pick(carriers),
                TrackingNumber    = $"TRK{rng.Next(100_000_000, 999_999_999)}",
                Destination       = new PostalAddress
                {
                    Street  = RandAddress().Street,
                    City    = Pick(cityNames),
                    ZipCode = Pick(zipCodes),
                    Country = "Italy"
                },
                EstimatedDelivery = DateTime.UtcNow.AddDays(rng.Next(1, 10))
            },
            Lines = lines,
            Notes = [new OrderNote { Author = "System", Text = "Auto-seeded demo order.", CreatedAt = DateTime.UtcNow }],
            Tags  = [Pick(tags), Pick(tags)]
        });
    }
});

// ─── TestDocuments ────────────────────────────────────────────────────────
string[] categories = ["Electronics", "Books", "Clothing", "Food", "Sports"];
await SeedAsync("TestDocuments", async () =>
{
    for (int i = 0; i < 10; i++)
        await db.TestDocuments.InsertAsync(new TestDocument
        {
            Id       = NewId(),
            Category = Pick(categories),
            Amount   = rng.Next(1, 500),
            Name     = $"Doc-{i + 1:D3}"
        });
});

// ─── OrderDocuments ───────────────────────────────────────────────────────
await SeedAsync("OrderDocuments", async () =>
{
    for (int i = 0; i < 8; i++)
        await db.OrderDocuments.InsertAsync(new OrderDocument
        {
            Id       = NewId(),
            ItemName = productTitles[rng.Next(productTitles.Length)],
            Quantity = rng.Next(1, 20)
        });
});

// ─── ComplexDocuments ─────────────────────────────────────────────────────
await SeedAsync("ComplexDocuments", async () =>
{
    for (int i = 0; i < 5; i++)
        await db.ComplexDocuments.InsertAsync(new ComplexDocument
        {
            Id              = NewId(),
            Title           = $"Complex Document #{i + 1}",
            ShippingAddress = RandAddress(),
            Items = Enumerable.Range(1, rng.Next(2, 5))
                              .Select(_ => new OrderItem
                              {
                                  Name  = productTitles[rng.Next(productTitles.Length)],
                                  Price = rng.Next(5, 999)
                              }).ToList()
        });
});

// ─── AutoInitEntities ─────────────────────────────────────────────────────
await SeedAsync("AutoInitEntities", async () =>
{
    for (int i = 1; i <= 8; i++)
        await db.AutoInitEntities.InsertAsync(new AutoInitEntity { Id = i, Name = $"AutoEntity-{i}" });
});

// ─── IntEntities ──────────────────────────────────────────────────────────
await SeedAsync("IntEntities", async () =>
{
    for (int i = 1; i <= 8; i++)
        await db.IntEntities.InsertAsync(new IntEntity { Id = i, Name = $"IntEntity-{i}" });
});

// ─── StringEntities ───────────────────────────────────────────────────────
await SeedAsync("StringEntities", async () =>
{
    for (int i = 1; i <= 6; i++)
        await db.StringEntities.InsertAsync(new StringEntity { Id = $"str-{i:D3}", Value = $"Value for string key {i}" });
});

// ─── GuidEntities ─────────────────────────────────────────────────────────
await SeedAsync("GuidEntities", async () =>
{
    for (int i = 0; i < 6; i++)
        await db.GuidEntities.InsertAsync(new GuidEntity { Id = Guid.NewGuid(), Name = $"GuidEntity-{i + 1}" });
});

// ─── CustomKeyEntities ────────────────────────────────────────────────────
await SeedAsync("CustomKeyEntities", async () =>
{
    string[] codes = ["ALPHA", "BETA", "GAMMA", "DELTA", "EPSILON"];
    foreach (var code in codes)
        await db.CustomKeyEntities.InsertAsync(new CustomKeyEntity
        {
            Code        = code,
            Description = $"Description for {code}"
        });
});

// ─── AsyncDocs ────────────────────────────────────────────────────────────
await SeedAsync("AsyncDocs", async () =>
{
    for (int i = 1; i <= 8; i++)
        await db.AsyncDocs.InsertAsync(new AsyncDoc { Id = i, Name = $"AsyncDoc-{i}" });
});

// ─── SchemaUsers ──────────────────────────────────────────────────────────
await SeedAsync("SchemaUsers", async () =>
{
    for (int i = 1; i <= 6; i++)
        await db.SchemaUsers.InsertAsync(new SchemaUser { Id = i, Name = RandName(), Address = RandAddress() });
});

// ─── VectorItems ──────────────────────────────────────────────────────────
await SeedAsync("VectorItems", async () =>
{
    for (int i = 0; i < 8; i++)
        await db.VectorItems.InsertAsync(new VectorEntity
        {
            Id        = NewId(),
            Title     = $"Vector Item {i + 1}",
            Embedding = RandVector(3)
        });
});

// ─── GeoItems ─────────────────────────────────────────────────────────────
await SeedAsync("GeoItems", async () =>
{
    (string name, double lat, double lon)[] places =
    [
        ("Milano Centrale",  45.4855,  9.2049),
        ("Colosseo",         41.8902, 12.4922),
        ("Torre di Pisa",    43.7230, 10.3966),
        ("Canal Grande",     45.4408, 12.3155),
        ("Vesuvio",          40.8218, 14.4264),
        ("Gran Paradiso",    45.5173,  7.2469),
    ];
    foreach (var (name, lat, lon) in places)
        await db.GeoItems.InsertAsync(new GeoEntity { Id = NewId(), Name = name, Location = (lat, lon) });
});

// ─── DerivedEntities ──────────────────────────────────────────────────────
await SeedAsync("DerivedEntities", async () =>
{
    for (int i = 0; i < 5; i++)
        await db.DerivedEntities.InsertAsync(new DerivedEntity
        {
            Id          = NewId(),
            CreatedAt   = DateTime.UtcNow.AddDays(-rng.Next(1, 365)),
            Name        = $"DerivedEntity-{i + 1}",
            Description = $"Auto-seeded derived entity #{i + 1}"
        });
});

// ─── ComputedPropertyEntities ─────────────────────────────────────────────
await SeedAsync("ComputedPropertyEntities", async () =>
{
    for (int i = 0; i < 5; i++)
        await db.ComputedPropertyEntities.InsertAsync(new EntityWithComputedProperties
        {
            Id        = NewId(),
            FirstName = Pick(firstNames),
            LastName  = Pick(lastNames),
            BirthYear = rng.Next(1960, 2005)
        });
});

// ─── AdvancedCollectionEntities ───────────────────────────────────────────
await SeedAsync("AdvancedCollectionEntities", async () =>
{
    for (int i = 0; i < 4; i++)
        await db.AdvancedCollectionEntities.InsertAsync(new EntityWithAdvancedCollections
        {
            Id           = NewId(),
            Name         = $"AdvancedCollection-{i + 1}",
            Tags         = new HashSet<string> { Pick(tags), Pick(tags), Pick(tags) },
            Numbers      = new HashSet<int>   { rng.Next(1, 100), rng.Next(1, 100) },
            History      = new LinkedList<string>(["created", "updated", "reviewed"]),
            PendingItems = new Queue<string>(["task-a", "task-b"]),
            UndoStack    = new Stack<string>(["step-1", "step-2"]),
            Addresses    = [RandAddress(), RandAddress()]
        });
});

// ─── PrivateSetterEntities ────────────────────────────────────────────────
await SeedAsync("PrivateSetterEntities", async () =>
{
    for (int i = 0; i < 5; i++)
        await db.PrivateSetterEntities.InsertAsync(
            EntityWithPrivateSetters.Create(RandName(), rng.Next(20, 60)));
});

// ─── InitSetterEntities ───────────────────────────────────────────────────
await SeedAsync("InitSetterEntities", async () =>
{
    for (int i = 0; i < 5; i++)
        await db.InitSetterEntities.InsertAsync(new EntityWithInitSetters
        {
            Id        = NewId(),
            Name      = RandName(),
            Age       = rng.Next(18, 65),
            CreatedAt = DateTime.UtcNow.AddDays(-rng.Next(1, 200))
        });
});

// ─── Employees (hierarchy) ────────────────────────────────────────────────
await SeedAsync("Employees", async () =>
{
    var ceo  = new Employee { Id = NewId(), Name = "CEO",   Department = "Management" };
    var cto  = new Employee { Id = NewId(), Name = "CTO",   Department = "Engineering", ManagerId = ceo.Id };
    var cfo  = new Employee { Id = NewId(), Name = "CFO",   Department = "Finance",     ManagerId = ceo.Id };
    var dev1 = new Employee { Id = NewId(), Name = "Dev 1", Department = "Engineering", ManagerId = cto.Id };
    var dev2 = new Employee { Id = NewId(), Name = "Dev 2", Department = "Engineering", ManagerId = cto.Id };
    ceo.DirectReportIds = [cto.Id, cfo.Id];
    cto.DirectReportIds = [dev1.Id, dev2.Id];
    foreach (var e in new[] { ceo, cto, cfo, dev1, dev2 })
        await db.Employees.InsertAsync(e);
});

// ─── CategoryRefs & ProductRefs ───────────────────────────────────────────
await SeedAsync("CategoryRefs & ProductRefs", async () =>
{
    var prodRefs = Enumerable.Range(1, 6).Select(i => new ProductRef
    {
        Id    = NewId(),
        Name  = productTitles[i - 1],
        Price = Math.Round((decimal)(rng.NextDouble() * 999 + 9), 2)
    }).ToList();
    foreach (var p in prodRefs)
        await db.ProductRefs.InsertAsync(p);

    string[] catNames = ["Tech", "Office", "Audio", "Storage", "Accessories"];
    foreach (var cat in catNames)
    {
        var subset = prodRefs.OrderBy(_ => rng.Next()).Take(rng.Next(1, 4)).Select(p => p.Id).ToList();
        await db.CategoryRefs.InsertAsync(new CategoryRef
        {
            Id          = NewId(),
            Name        = cat,
            Description = $"Category: {cat}",
            ProductIds  = subset
        });
    }
});

// ─── MockCounters ─────────────────────────────────────────────────────────
await SeedAsync("MockCounters", async () =>
{
    string[] counterNames = ["page_views", "logins", "api_calls", "errors", "signups"];
    foreach (var (name, idx) in counterNames.Select((n, i) => (n, i)))
        await db.MockCounters.InsertAsync(
            new MockCounter($"CTR-{idx + 1:D3}") { Name = name, Value = rng.Next(0, 100_000) });
});

// ─── TemporalEntities ─────────────────────────────────────────────────────
await SeedAsync("TemporalEntities", async () =>
{
    for (int i = 0; i < 6; i++)
    {
        var birth = DateOnly.FromDateTime(
            DateTime.Today.AddYears(-rng.Next(20, 60)).AddDays(-rng.Next(0, 365)));
        await db.TemporalEntities.InsertAsync(new TemporalEntity
        {
            Id               = NewId(),
            Name             = RandName(),
            CreatedAt        = DateTime.UtcNow.AddSeconds(-rng.Next(0, 1_000_000)),
            UpdatedAt        = DateTimeOffset.UtcNow.AddMinutes(-rng.Next(0, 10_000)),
            LastAccessedAt   = rng.Next(2) == 0 ? DateTimeOffset.UtcNow.AddHours(-rng.Next(1, 48)) : null,
            Duration         = TimeSpan.FromMinutes(rng.Next(1, 480)),
            OptionalDuration = rng.Next(2) == 0 ? TimeSpan.FromSeconds(rng.Next(30, 3600)) : null,
            BirthDate        = birth,
            Anniversary      = rng.Next(2) == 0 ? birth.AddYears(rng.Next(1, 10)) : null,
            OpeningTime      = new TimeOnly(rng.Next(6, 10), 0),
            ClosingTime      = rng.Next(2) == 0 ? new TimeOnly(rng.Next(17, 22), 30) : null
        });
    }
});

// ─── EnumEntities ─────────────────────────────────────────────────────────
await SeedAsync("EnumEntities", async () =>
{
    var roles      = Enum.GetValues<UserRole>();
    var statuses2  = Enum.GetValues<OrderStatus>();
    var priorities = Enum.GetValues<Priority>();
    var actions    = Enum.GetValues<AuditAction>();
    Permissions[] perms = [Permissions.Read, Permissions.Read | Permissions.Write, Permissions.All];

    for (int i = 0; i < 6; i++)
        await db.EnumEntities.InsertAsync(new EnumEntity
        {
            Id               = NewId(),
            Role             = roles[rng.Next(roles.Length)],
            Status           = rng.Next(2) == 0 ? statuses2[rng.Next(statuses2.Length)] : null,
            Priority         = priorities[rng.Next(priorities.Length)],
            FallbackPriority = rng.Next(2) == 0 ? priorities[rng.Next(priorities.Length)] : null,
            LastAction       = actions[rng.Next(actions.Length)],
            Permissions      = perms[rng.Next(perms.Length)],
            AssignableRoles  = [roles[rng.Next(roles.Length)], roles[rng.Next(roles.Length)]],
            StatusHistory    = [statuses2[rng.Next(statuses2.Length)], statuses2[rng.Next(statuses2.Length)]],
            Label            = $"EnumEntity-{i + 1}"
        });
});

await db.SaveChangesAsync();

// ─── Summary ──────────────────────────────────────────────────────────────
Console.WriteLine("\n╔══════════════════════════════════════╗");
Console.WriteLine("║             Seeding Summary          ║");
Console.WriteLine("╚══════════════════════════════════════╝");
Console.WriteLine($"  Users                     : {await db.Users.CountAsync()}");
Console.WriteLine($"  AnnotatedUsers            : {await db.AnnotatedUsers.CountAsync()}");
Console.WriteLine($"  ComplexUsers              : {await db.ComplexUsers.CountAsync()}");
Console.WriteLine($"  People                    : {await db.People.CountAsync()}");
Console.WriteLine($"  PeopleV2                  : {await db.PeopleV2.CountAsync()}");
Console.WriteLine($"  PeopleWithContacts        : {await db.PeopleWithContacts.CountAsync()}");
Console.WriteLine($"  Products                  : {await db.Products.CountAsync()}");
Console.WriteLine($"  Orders                    : {await db.Orders.CountAsync()}");
Console.WriteLine($"  CustomerOrders            : {await db.CustomerOrders.CountAsync()}");
Console.WriteLine($"  TestDocuments             : {await db.TestDocuments.CountAsync()}");
Console.WriteLine($"  OrderDocuments            : {await db.OrderDocuments.CountAsync()}");
Console.WriteLine($"  ComplexDocuments          : {await db.ComplexDocuments.CountAsync()}");
Console.WriteLine($"  AutoInitEntities          : {await db.AutoInitEntities.CountAsync()}");
Console.WriteLine($"  IntEntities               : {await db.IntEntities.CountAsync()}");
Console.WriteLine($"  StringEntities            : {await db.StringEntities.CountAsync()}");
Console.WriteLine($"  GuidEntities              : {await db.GuidEntities.CountAsync()}");
Console.WriteLine($"  CustomKeyEntities         : {await db.CustomKeyEntities.CountAsync()}");
Console.WriteLine($"  AsyncDocs                 : {await db.AsyncDocs.CountAsync()}");
Console.WriteLine($"  SchemaUsers               : {await db.SchemaUsers.CountAsync()}");
Console.WriteLine($"  VectorItems               : {await db.VectorItems.CountAsync()}");
Console.WriteLine($"  GeoItems                  : {await db.GeoItems.CountAsync()}");
Console.WriteLine($"  DerivedEntities           : {await db.DerivedEntities.CountAsync()}");
Console.WriteLine($"  ComputedPropertyEntities  : {await db.ComputedPropertyEntities.CountAsync()}");
Console.WriteLine($"  AdvancedCollectionEntities: {await db.AdvancedCollectionEntities.CountAsync()}");
Console.WriteLine($"  PrivateSetterEntities     : {await db.PrivateSetterEntities.CountAsync()}");
Console.WriteLine($"  InitSetterEntities        : {await db.InitSetterEntities.CountAsync()}");
Console.WriteLine($"  Employees                 : {await db.Employees.CountAsync()}");
Console.WriteLine($"  CategoryRefs              : {await db.CategoryRefs.CountAsync()}");
Console.WriteLine($"  ProductRefs               : {await db.ProductRefs.CountAsync()}");
Console.WriteLine($"  MockCounters              : {await db.MockCounters.CountAsync()}");
Console.WriteLine($"  TemporalEntities          : {await db.TemporalEntities.CountAsync()}");
Console.WriteLine($"  EnumEntities              : {await db.EnumEntities.CountAsync()}");
Console.WriteLine($"\nDatabase file  : {dbPath}");
if (isMultiFile)
{
    Console.WriteLine($"Index file     : {Path.ChangeExtension(dbPath, ".idx")}");
    Console.WriteLine($"WAL file       : {config.WalPath}");
    Console.WriteLine($"Collections dir: {config.CollectionDataDirectory}");
}
Console.WriteLine("Done.");

db.Dispose();

// ─── Helper ───────────────────────────────────────────────────────────────
async Task SeedAsync(string collectionName, Func<Task> action)
{
    Console.Write($"  Seeding {collectionName}...");
    await action();
    Console.WriteLine(" OK");
}
