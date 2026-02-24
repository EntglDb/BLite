using BLite.Bson;
using BLite.Shared;

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
}
