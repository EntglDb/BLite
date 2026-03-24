using BLite.Bson;
using BLite.Core;
using BLite.Shared;
using System.Collections.Generic;

namespace BLite.Benchmark;

/// <summary>
/// Generates realistic CustomerOrder documents for benchmarking.
/// Each document contains:
///   - 2 nested objects (CustomerContact → PostalAddress, ShippingInfo → PostalAddress)
///   - 3 collections: Lines (5 items, each with 3 sub-tags), Notes (2 items), Tags (3 items)
/// Approximately 1–2 KB per document when BSON-serialised.
/// </summary>
public static class BenchmarkDataFactory
{
    private static readonly string[] Statuses  = ["pending", "confirmed", "shipped", "delivered"];
    private static readonly string[] Carriers  = ["DHL", "BRT", "GLS", "UPS"];
    private static readonly string[] Cities    = ["Milano", "Roma", "Torino", "Napoli", "Firenze"];

    public static CustomerOrder CreateOrder(int i) => new()
    {
        Id          = ObjectId.NewObjectId().ToString(),
        OrderNumber = $"ORD-{10000 + i:D6}",
        PlacedAt    = DateTime.UtcNow.AddDays(-(i % 365)),
        Status      = Statuses[i % 4],
        Currency    = "EUR",
        Subtotal    = 100m + i * 1.5m,
        TaxAmount   = (100m + i * 1.5m) * 0.22m,
        Total       = (100m + i * 1.5m) * 1.22m,
        Tags        = ["web", i % 2 == 0 ? "promo" : "standard", "b2c"],

        Customer = new CustomerContact
        {
            FullName = $"Cliente {i}",
            Email    = $"customer{i}@example.com",
            Phone    = $"+39 333 {i:D7}",
            BillingAddress = new PostalAddress
            {
                Street  = $"Via Roma {i}",
                City    = Cities[i % Cities.Length],
                ZipCode = $"{20100 + i % 100:D5}",
                Country = "IT"
            }
        },

        Shipping = new ShippingInfo
        {
            Carrier           = Carriers[i % Carriers.Length],
            TrackingNumber    = $"TRK{i:D10}",
            EstimatedDelivery = DateTime.UtcNow.AddDays(3 + i % 5),
            Destination = new PostalAddress
            {
                Street  = $"Via Spedizione {i}",
                City    = Cities[(i + 1) % Cities.Length],
                ZipCode = $"{00100 + i % 100:D5}",
                Country = "IT"
            }
        },

        Lines = Enumerable.Range(0, 5).Select(j => new OrderLine
        {
            Sku         = $"SKU-{i:D5}-{j:D3}",
            ProductName = $"Prodotto {j} (batch {i / 100})",
            Quantity    = j + 1,
            UnitPrice   = 20m + j * 5m,
            Subtotal    = (j + 1) * (20m + j * 5m),
            Tags        = ["cat-a", j % 2 == 0 ? "promo" : "regular", "in-stock"]
        }).ToList(),

        Notes =
        [
            new OrderNote { Author = "system",          Text = "Ordine confermato automaticamente.", CreatedAt = DateTime.UtcNow.AddMinutes(-10) },
            new OrderNote { Author = $"agent{i % 5}",  Text = "Verificato e approvato.",             CreatedAt = DateTime.UtcNow }
        ]
    };

    /// <summary>All BSON field names used by <see cref="CreateBsonDocument"/>. Register once per engine instance.</summary>
    public static readonly string[] BsonOrderFieldNames =
    [
        "_id", "ordernumber", "placedat", "status", "currency",
        "subtotal", "taxamount", "total", "tags",
        "customer", "fullname", "email", "phone", "billingaddress",
        "street", "city", "zipcode", "country",
        "shipping", "carrier", "trackingnumber", "estimateddelivery", "destination",
        "lines", "sku", "productname", "quantity", "unitprice",
        "notes", "author", "text", "createdat"
    ];

    /// <summary>
    /// Creates a <see cref="BsonDocument"/> equivalent to <see cref="CreateOrder"/> for the given index.
    /// The engine's key dictionary must be seeded with <see cref="BsonOrderFieldNames"/> before calling
    /// (use <c>engine.RegisterKeys(BsonOrderFieldNames)</c> or let this method register them on the first call).
    /// </summary>
    public static BsonDocument CreateBsonDocument(int i, BLiteEngine engine)
    {
        return engine.CreateDocument(BsonOrderFieldNames, b =>
        {
            b.AddId(BsonId.NewId(BsonIdType.ObjectId));
            b.AddString("ordernumber", $"ORD-{10000 + i:D6}");
            b.AddDateTime("placedat", DateTime.UtcNow.AddDays(-(i % 365)));
            b.AddString("status", Statuses[i % 4]);
            b.AddString("currency", "EUR");
            b.AddDecimal("subtotal", 100m + i * 1.5m);
            b.AddDecimal("taxamount", (100m + i * 1.5m) * 0.22m);
            b.AddDecimal("total", (100m + i * 1.5m) * 1.22m);
            b.Add("tags", BsonValue.FromArray(
                new List<BsonValue> { "web", i % 2 == 0 ? "promo" : "standard", "b2c" }));

            b.AddDocument("customer", c =>
            {
                c.AddString("fullname", $"Cliente {i}");
                c.AddString("email", $"customer{i}@example.com");
                c.AddString("phone", $"+39 333 {i:D7}");
                c.AddDocument("billingaddress", a =>
                {
                    a.AddString("street", $"Via Roma {i}");
                    a.AddString("city", Cities[i % Cities.Length]);
                    a.AddString("zipcode", $"{20100 + i % 100:D5}");
                    a.AddString("country", "IT");
                });
            });

            b.AddDocument("shipping", s =>
            {
                s.AddString("carrier", Carriers[i % Carriers.Length]);
                s.AddString("trackingnumber", $"TRK{i:D10}");
                s.AddDateTime("estimateddelivery", DateTime.UtcNow.AddDays(3 + i % 5));
                s.AddDocument("destination", a =>
                {
                    a.AddString("street", $"Via Spedizione {i}");
                    a.AddString("city", Cities[(i + 1) % Cities.Length]);
                    a.AddString("zipcode", $"{100 + i % 100:D5}");
                    a.AddString("country", "IT");
                });
            });

            // Lines: array of 5 subdocuments (same structure as OrderLine)
            var lines = new List<BsonValue>(5);
            for (int j = 0; j < 5; j++)
            {
                var lineDoc = engine.CreateDocument([], inner =>
                {
                    inner.AddString("sku", $"SKU-{i:D5}-{j:D3}");
                    inner.AddString("productname", $"Prodotto {j} (batch {i / 100})");
                    inner.AddInt32("quantity", j + 1);
                    inner.AddDecimal("unitprice", 20m + j * 5m);
                    inner.AddDecimal("subtotal", (j + 1) * (20m + j * 5m));
                    inner.Add("tags", BsonValue.FromArray(
                        new List<BsonValue> { "cat-a", j % 2 == 0 ? "promo" : "regular", "in-stock" }));
                });
                lines.Add(BsonValue.FromDocument(lineDoc));
            }
            b.Add("lines", BsonValue.FromArray(lines));

            // Notes: array of 2 subdocuments (same structure as OrderNote)
            var notes = new List<BsonValue>(2)
            {
                BsonValue.FromDocument(engine.CreateDocument([], n =>
                {
                    n.AddString("author", "system");
                    n.AddString("text", "Ordine confermato automaticamente.");
                    n.AddDateTime("createdat", DateTime.UtcNow.AddMinutes(-10));
                })),
                BsonValue.FromDocument(engine.CreateDocument([], n =>
                {
                    n.AddString("author", $"agent{i % 5}");
                    n.AddString("text", "Verificato e approvato.");
                    n.AddDateTime("createdat", DateTime.UtcNow);
                }))
            };
            b.Add("notes", BsonValue.FromArray(notes));
        });
    }
}
