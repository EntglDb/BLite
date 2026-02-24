using BLite.Bson;
using BLite.Shared;
using BLite.Tests;

// ─── Setup ────────────────────────────────────────────────────────────────
var dbPath  = Path.Combine(Path.GetTempPath(), $"blite_demo.db");
var walPath = Path.ChangeExtension(dbPath, ".wal");

if (File.Exists(dbPath))  File.Delete(dbPath);
if (File.Exists(walPath)) File.Delete(walPath);

Console.WriteLine("╔══════════════════════════════════════╗");
Console.WriteLine("║       BLite Demo Database Seeder     ║");
Console.WriteLine("╚══════════════════════════════════════╝");
Console.WriteLine($"Path: {dbPath}\n");

var rng = new Random(42);

// ─── Helper lambdas ───────────────────────────────────────────────────────
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

using var db = new TestDbContext(dbPath);

// ─── Users ────────────────────────────────────────────────────────────────
Seed("Users", () =>
{
    foreach (var name in firstNames)
        db.Users.Insert(new User { Id = NewId(), Name = name, Age = rng.Next(20, 65) });
});

// ─── AnnotatedUsers ───────────────────────────────────────────────────────
Seed("AnnotatedUsers", () =>
{
    for (int i = 0; i < 8; i++)
    {
        var name = RandName();
        db.AnnotatedUsers.Insert(new AnnotatedUser
        {
            Id       = NewId(),
            Name     = name[..Math.Min(name.Length, 50)],
            Age      = rng.Next(18, 70),
            Location = (rng.NextDouble() * 10 + 40, rng.NextDouble() * 10 + 10)
        });
    }
});

// ─── ComplexUsers ─────────────────────────────────────────────────────────
Seed("ComplexUsers", () =>
{
    for (int i = 0; i < 6; i++)
        db.ComplexUsers.Insert(new ComplexUser
        {
            Id             = NewId(),
            Name           = RandName(),
            MainAddress    = RandAddress(),
            OtherAddresses = [RandAddress(), RandAddress()],
            Tags           = [Pick(tags), Pick(tags), Pick(tags)]
        });
});

// ─── People ───────────────────────────────────────────────────────────────
Seed("People", () =>
{
    for (int i = 1; i <= 10; i++)
        db.People.Insert(new Person { Id = i, Name = RandName(), Age = rng.Next(18, 80) });
});

// ─── PeopleV2 ─────────────────────────────────────────────────────────────
Seed("PeopleV2", () =>
{
    for (int i = 0; i < 8; i++)
        db.PeopleV2.Insert(new PersonV2 { Id = NewId(), Name = RandName(), Age = rng.Next(18, 80) });
});

// ─── PeopleWithContacts ───────────────────────────────────────────────────
Seed("PeopleWithContacts", () =>
{
    for (int i = 0; i < 5; i++)
    {
        var name = RandName();
        db.PeopleWithContacts.Insert(new PersonWithContact
        {
            Id          = NewId(),
            Name        = name,
            MainContact = new ContactInfo { Id = i * 10 + 1, Email = RandEmail(name), Phone = RandPhone() },
            Contacts    = [new ContactInfo { Id = i * 10 + 2, Email = $"alt.{RandEmail(name)}", Phone = RandPhone() }]
        });
    }
});

// ─── Products ─────────────────────────────────────────────────────────────
Seed("Products", () =>
{
    for (int i = 0; i < productTitles.Length; i++)
        db.Products.Insert(new Product
        {
            Id    = i + 1,
            Title = productTitles[i],
            Price = Math.Round((decimal)(rng.NextDouble() * 1500 + 9.99), 2)
        });
});

// ─── Orders ───────────────────────────────────────────────────────────────
Seed("Orders", () =>
{
    for (int i = 1; i <= 8; i++)
        db.Orders.Insert(new Order
        {
            Id           = new OrderId($"ORD-{2025_000 + i:D6}"),
            CustomerName = RandName()
        });
});

// ─── CustomerOrders ───────────────────────────────────────────────────────
Seed("CustomerOrders", () =>
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

        db.CustomerOrders.Insert(new CustomerOrder
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
Seed("TestDocuments", () =>
{
    for (int i = 0; i < 10; i++)
        db.TestDocuments.Insert(new TestDocument
        {
            Id       = NewId(),
            Category = Pick(categories),
            Amount   = rng.Next(1, 500),
            Name     = $"Doc-{i + 1:D3}"
        });
});

// ─── OrderDocuments ───────────────────────────────────────────────────────
Seed("OrderDocuments", () =>
{
    for (int i = 0; i < 8; i++)
        db.OrderDocuments.Insert(new OrderDocument
        {
            Id       = NewId(),
            ItemName = productTitles[rng.Next(productTitles.Length)],
            Quantity = rng.Next(1, 20)
        });
});

// ─── ComplexDocuments ─────────────────────────────────────────────────────
Seed("ComplexDocuments", () =>
{
    for (int i = 0; i < 5; i++)
        db.ComplexDocuments.Insert(new ComplexDocument
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
Seed("AutoInitEntities", () =>
{
    for (int i = 1; i <= 8; i++)
        db.AutoInitEntities.Insert(new AutoInitEntity { Id = i, Name = $"AutoEntity-{i}" });
});

// ─── IntEntities ──────────────────────────────────────────────────────────
Seed("IntEntities", () =>
{
    for (int i = 1; i <= 8; i++)
        db.IntEntities.Insert(new IntEntity { Id = i, Name = $"IntEntity-{i}" });
});

// ─── StringEntities ───────────────────────────────────────────────────────
Seed("StringEntities", () =>
{
    for (int i = 1; i <= 6; i++)
        db.StringEntities.Insert(new StringEntity { Id = $"str-{i:D3}", Value = $"Value for string key {i}" });
});

// ─── GuidEntities ─────────────────────────────────────────────────────────
Seed("GuidEntities", () =>
{
    for (int i = 0; i < 6; i++)
        db.GuidEntities.Insert(new GuidEntity { Id = Guid.NewGuid(), Name = $"GuidEntity-{i + 1}" });
});

// ─── CustomKeyEntities ────────────────────────────────────────────────────
Seed("CustomKeyEntities", () =>
{
    string[] codes = ["ALPHA", "BETA", "GAMMA", "DELTA", "EPSILON"];
    foreach (var code in codes)
        db.CustomKeyEntities.Insert(new CustomKeyEntity
        {
            Code        = code,
            Description = $"Description for {code}"
        });
});

// ─── AsyncDocs ────────────────────────────────────────────────────────────
Seed("AsyncDocs", () =>
{
    for (int i = 1; i <= 8; i++)
        db.AsyncDocs.Insert(new AsyncDoc { Id = i, Name = $"AsyncDoc-{i}" });
});

// ─── SchemaUsers ──────────────────────────────────────────────────────────
Seed("SchemaUsers", () =>
{
    for (int i = 1; i <= 6; i++)
        db.SchemaUsers.Insert(new SchemaUser { Id = i, Name = RandName(), Address = RandAddress() });
});

// ─── VectorItems ──────────────────────────────────────────────────────────
Seed("VectorItems", () =>
{
    for (int i = 0; i < 8; i++)
        db.VectorItems.Insert(new VectorEntity
        {
            Id        = NewId(),
            Title     = $"Vector Item {i + 1}",
            Embedding = RandVector(3)
        });
});

// ─── GeoItems ─────────────────────────────────────────────────────────────
Seed("GeoItems", () =>
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
        db.GeoItems.Insert(new GeoEntity { Id = NewId(), Name = name, Location = (lat, lon) });
});

// ─── DerivedEntities ──────────────────────────────────────────────────────
Seed("DerivedEntities", () =>
{
    for (int i = 0; i < 5; i++)
        db.DerivedEntities.Insert(new DerivedEntity
        {
            Id          = NewId(),
            CreatedAt   = DateTime.UtcNow.AddDays(-rng.Next(1, 365)),
            Name        = $"DerivedEntity-{i + 1}",
            Description = $"Auto-seeded derived entity #{i + 1}"
        });
});

// ─── ComputedPropertyEntities ─────────────────────────────────────────────
Seed("ComputedPropertyEntities", () =>
{
    for (int i = 0; i < 5; i++)
        db.ComputedPropertyEntities.Insert(new EntityWithComputedProperties
        {
            Id        = NewId(),
            FirstName = Pick(firstNames),
            LastName  = Pick(lastNames),
            BirthYear = rng.Next(1960, 2005)
        });
});

// ─── AdvancedCollectionEntities ───────────────────────────────────────────
Seed("AdvancedCollectionEntities", () =>
{
    for (int i = 0; i < 4; i++)
        db.AdvancedCollectionEntities.Insert(new EntityWithAdvancedCollections
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
Seed("PrivateSetterEntities", () =>
{
    for (int i = 0; i < 5; i++)
        db.PrivateSetterEntities.Insert(
            EntityWithPrivateSetters.Create(RandName(), rng.Next(20, 60)));
});

// ─── InitSetterEntities ───────────────────────────────────────────────────
Seed("InitSetterEntities", () =>
{
    for (int i = 0; i < 5; i++)
        db.InitSetterEntities.Insert(new EntityWithInitSetters
        {
            Id        = NewId(),
            Name      = RandName(),
            Age       = rng.Next(18, 65),
            CreatedAt = DateTime.UtcNow.AddDays(-rng.Next(1, 200))
        });
});

// ─── Employees (hierarchy) ────────────────────────────────────────────────
Seed("Employees", () =>
{
    var ceo  = new Employee { Id = NewId(), Name = "CEO",   Department = "Management" };
    var cto  = new Employee { Id = NewId(), Name = "CTO",   Department = "Engineering", ManagerId = ceo.Id };
    var cfo  = new Employee { Id = NewId(), Name = "CFO",   Department = "Finance",     ManagerId = ceo.Id };
    var dev1 = new Employee { Id = NewId(), Name = "Dev 1", Department = "Engineering", ManagerId = cto.Id };
    var dev2 = new Employee { Id = NewId(), Name = "Dev 2", Department = "Engineering", ManagerId = cto.Id };
    ceo.DirectReportIds = [cto.Id, cfo.Id];
    cto.DirectReportIds = [dev1.Id, dev2.Id];
    foreach (var e in new[] { ceo, cto, cfo, dev1, dev2 })
        db.Employees.Insert(e);
});

// ─── CategoryRefs & ProductRefs ───────────────────────────────────────────
Seed("CategoryRefs & ProductRefs", () =>
{
    var prodRefs = Enumerable.Range(1, 6).Select(i =>
    {
        var p = new ProductRef
        {
            Id    = NewId(),
            Name  = productTitles[i - 1],
            Price = Math.Round((decimal)(rng.NextDouble() * 999 + 9), 2)
        };
        db.ProductRefs.Insert(p);
        return p;
    }).ToList();

    string[] catNames = ["Tech", "Office", "Audio", "Storage", "Accessories"];
    foreach (var cat in catNames)
    {
        var subset = prodRefs.OrderBy(_ => rng.Next()).Take(rng.Next(1, 4)).Select(p => p.Id).ToList();
        db.CategoryRefs.Insert(new CategoryRef
        {
            Id          = NewId(),
            Name        = cat,
            Description = $"Category: {cat}",
            ProductIds  = subset
        });
    }
});

// ─── MockCounters ─────────────────────────────────────────────────────────
Seed("MockCounters", () =>
{
    string[] counterNames = ["page_views", "logins", "api_calls", "errors", "signups"];
    foreach (var (name, idx) in counterNames.Select((n, i) => (n, i)))
        db.MockCounters.Insert(
            new MockCounter($"CTR-{idx + 1:D3}") { Name = name, Value = rng.Next(0, 100_000) });
});

// ─── TemporalEntities ─────────────────────────────────────────────────────
Seed("TemporalEntities", () =>
{
    for (int i = 0; i < 6; i++)
    {
        var birth = DateOnly.FromDateTime(
            DateTime.Today.AddYears(-rng.Next(20, 60)).AddDays(-rng.Next(0, 365)));
        db.TemporalEntities.Insert(new TemporalEntity
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
Seed("EnumEntities", () =>
{
    var roles      = Enum.GetValues<UserRole>();
    var statuses2  = Enum.GetValues<OrderStatus>();
    var priorities = Enum.GetValues<Priority>();
    var actions    = Enum.GetValues<AuditAction>();
    Permissions[] perms = [Permissions.Read, Permissions.Read | Permissions.Write, Permissions.All];

    for (int i = 0; i < 6; i++)
        db.EnumEntities.Insert(new EnumEntity
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

db.SaveChanges();

// ─── Summary ──────────────────────────────────────────────────────────────
Console.WriteLine("\n╔══════════════════════════════════════╗");
Console.WriteLine("║             Seeding Summary          ║");
Console.WriteLine("╚══════════════════════════════════════╝");
Console.WriteLine($"  Users                     : {db.Users.Count()}");
Console.WriteLine($"  AnnotatedUsers            : {db.AnnotatedUsers.Count()}");
Console.WriteLine($"  ComplexUsers              : {db.ComplexUsers.Count()}");
Console.WriteLine($"  People                    : {db.People.Count()}");
Console.WriteLine($"  PeopleV2                  : {db.PeopleV2.Count()}");
Console.WriteLine($"  PeopleWithContacts        : {db.PeopleWithContacts.Count()}");
Console.WriteLine($"  Products                  : {db.Products.Count()}");
Console.WriteLine($"  Orders                    : {db.Orders.Count()}");
Console.WriteLine($"  CustomerOrders            : {db.CustomerOrders.Count()}");
Console.WriteLine($"  TestDocuments             : {db.TestDocuments.Count()}");
Console.WriteLine($"  OrderDocuments            : {db.OrderDocuments.Count()}");
Console.WriteLine($"  ComplexDocuments          : {db.ComplexDocuments.Count()}");
Console.WriteLine($"  AutoInitEntities          : {db.AutoInitEntities.Count()}");
Console.WriteLine($"  IntEntities               : {db.IntEntities.Count()}");
Console.WriteLine($"  StringEntities            : {db.StringEntities.Count()}");
Console.WriteLine($"  GuidEntities              : {db.GuidEntities.Count()}");
Console.WriteLine($"  CustomKeyEntities         : {db.CustomKeyEntities.Count()}");
Console.WriteLine($"  AsyncDocs                 : {db.AsyncDocs.Count()}");
Console.WriteLine($"  SchemaUsers               : {db.SchemaUsers.Count()}");
Console.WriteLine($"  VectorItems               : {db.VectorItems.Count()}");
Console.WriteLine($"  GeoItems                  : {db.GeoItems.Count()}");
Console.WriteLine($"  DerivedEntities           : {db.DerivedEntities.Count()}");
Console.WriteLine($"  ComputedPropertyEntities  : {db.ComputedPropertyEntities.Count()}");
Console.WriteLine($"  AdvancedCollectionEntities: {db.AdvancedCollectionEntities.Count()}");
Console.WriteLine($"  PrivateSetterEntities     : {db.PrivateSetterEntities.Count()}");
Console.WriteLine($"  InitSetterEntities        : {db.InitSetterEntities.Count()}");
Console.WriteLine($"  Employees                 : {db.Employees.Count()}");
Console.WriteLine($"  CategoryRefs              : {db.CategoryRefs.Count()}");
Console.WriteLine($"  ProductRefs               : {db.ProductRefs.Count()}");
Console.WriteLine($"  MockCounters              : {db.MockCounters.Count()}");
Console.WriteLine($"  TemporalEntities          : {db.TemporalEntities.Count()}");
Console.WriteLine($"  EnumEntities              : {db.EnumEntities.Count()}");
Console.WriteLine($"\nDatabase file: {dbPath}");
Console.WriteLine("Done.");

db.Dispose();

// ─── Helper ───────────────────────────────────────────────────────────────
void Seed(string collectionName, Action action)
{
    Console.Write($"  Seeding {collectionName}...");
    action();
    Console.WriteLine(" OK");
}
