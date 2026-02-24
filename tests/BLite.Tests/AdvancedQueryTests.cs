using BLite.Bson;
using BLite.Shared;

namespace BLite.Tests
{
    public class AdvancedQueryTests : IDisposable
    {
        private readonly string _dbPath;
        private readonly TestDbContext _db;

        public AdvancedQueryTests()
        {
            _dbPath = Path.Combine(Path.GetTempPath(), $"blite_advanced_{Guid.NewGuid()}.db");
            _db = new TestDbContext(_dbPath);

            // Seed Data
            _db.TestDocuments.Insert(new TestDocument { Category = "A", Amount = 10, Name = "Item1" });
            _db.TestDocuments.Insert(new TestDocument { Category = "A", Amount = 20, Name = "Item2" });
            _db.TestDocuments.Insert(new TestDocument { Category = "B", Amount = 30, Name = "Item3" });
            _db.TestDocuments.Insert(new TestDocument { Category = "B", Amount = 40, Name = "Item4" });
            _db.TestDocuments.Insert(new TestDocument { Category = "C", Amount = 50, Name = "Item5" });
            _db.SaveChanges();
        }

        public void Dispose()
        {
            _db.Dispose();
            if (File.Exists(_dbPath)) File.Delete(_dbPath);
        }

        [Fact]
        public void GroupBy_Simple_Key_Works()
        {
            var groups = _db.TestDocuments.AsQueryable()
                .GroupBy(x => x.Category)
                .ToList();

            Assert.Equal(3, groups.Count);

            var groupA = groups.First(g => g.Key == "A");
            Assert.Equal(2, groupA.Count());
            Assert.Contains(groupA, x => x.Amount == 10);
            Assert.Contains(groupA, x => x.Amount == 20);

            var groupB = groups.First(g => g.Key == "B");
            Assert.Equal(2, groupB.Count());

            var groupC = groups.First(g => g.Key == "C");
            Assert.Single(groupC);
        }

        [Fact]
        public void GroupBy_With_Aggregation_Select()
        {
            var results = _db.TestDocuments.AsQueryable()
                .GroupBy(x => x.Category)
                .Select(g => new { Category = g.Key, Total = g.Sum(x => x.Amount) })
                .OrderBy(x => x.Category)
                .ToList();

            Assert.Equal(3, results.Count);
            Assert.Equal("A", results[0].Category);
            Assert.Equal(30, results[0].Total); // 10 + 20

            Assert.Equal("B", results[1].Category);
            Assert.Equal(70, results[1].Total); // 30 + 40

            Assert.Equal("C", results[2].Category);
            Assert.Equal(50, results[2].Total); // 50
        }

        [Fact]
        public void Aggregations_Direct_Works()
        {
            var query = _db.TestDocuments.AsQueryable();

            Assert.Equal(5, query.Count());
            Assert.Equal(150, query.Sum(x => x.Amount));
            Assert.Equal(30.0, query.Average(x => x.Amount));
            Assert.Equal(10, query.Min(x => x.Amount));
            Assert.Equal(50, query.Max(x => x.Amount));
        }

        [Fact]
        public void Aggregations_With_Predicate_Works()
        {
            var query = _db.TestDocuments.AsQueryable().Where(x => x.Category == "A");

            Assert.Equal(2, query.Count());
            Assert.Equal(30, query.Sum(x => x.Amount));
        }

        [Fact]
        public void Join_Works_InMemory()
        {
            // Create a second collection for joining
            _db.OrderDocuments.Insert(new OrderDocument { ItemName = "Item1", Quantity = 5 });
            _db.OrderDocuments.Insert(new OrderDocument { ItemName = "Item3", Quantity = 2 });
            _db.SaveChanges();

            var query = _db.TestDocuments.AsQueryable()
                .Join(_db.OrderDocuments.AsQueryable(),
                    doc => doc.Name,
                    order => order.ItemName,
                    (doc, order) => new { doc.Name, doc.Category, order.Quantity })
                .OrderBy(x => x.Name)
                .ToList();

            Assert.Equal(2, query.Count);

            Assert.Equal("Item1", query[0].Name);
            Assert.Equal("A", query[0].Category);
            Assert.Equal(5, query[0].Quantity);

            Assert.Equal("Item3", query[1].Name);
            Assert.Equal("B", query[1].Category);
            Assert.Equal(2, query[1].Quantity);
        }


        [Fact]
        public void Select_Project_Nested_Object()
        {
            var doc = new ComplexDocument
            {
                Id = ObjectId.NewObjectId(),
                Title = "Order1",
                ShippingAddress = new Address { City = new City { Name = "New York" }, Street = "5th Ave" },
                Items = new List<OrderItem>
                {
                    new OrderItem { Name = "Laptop", Price = 1000 },
                    new OrderItem { Name = "Mouse", Price = 50 }
                }
            };
            _db.ComplexDocuments.Insert(doc);
            _db.SaveChanges();

            var query = _db.ComplexDocuments.AsQueryable()
                .Select(x => x.ShippingAddress)
                .ToList();

            Assert.Single(query);
            Assert.Equal("New York", query[0].City.Name);
            Assert.Equal("5th Ave", query[0].Street);
        }

        [Fact]
        public void Select_Project_Nested_Field()
        {
            var doc = new ComplexDocument
            {
                Id = ObjectId.NewObjectId(),
                Title = "Order1",
                ShippingAddress = new Address { City = new City { Name = "New York" }, Street = "5th Ave" }
            };
            _db.ComplexDocuments.Insert(doc);
            _db.SaveChanges();

            var cities = _db.ComplexDocuments.AsQueryable()
                .Select(x => x.ShippingAddress.City.Name)
                .ToList();

            Assert.Single(cities);
            Assert.Equal("New York", cities[0]);
        }

        [Fact]
        public void Select_Anonymous_Complex()
        {
            BLite.Tests.TestDbContext_TestDbContext_Mappers.BLite_Shared_CityMapper cityMapper = new BLite.Tests.TestDbContext_TestDbContext_Mappers.BLite_Shared_CityMapper();
            var doc = new ComplexDocument
            {
                Id = ObjectId.NewObjectId(),
                Title = "Order1",
                ShippingAddress = new Address {  City = new City { Name = "New York" }, Street = "5th Ave" }
            };


            _db.ComplexDocuments.Insert(doc);
            _db.SaveChanges();

            var result = _db.ComplexDocuments.AsQueryable()
                .Select(x => new { x.Title, x.ShippingAddress.City })
                .ToList();

            Assert.Single(result);
            Assert.Equal("Order1", result[0].Title);
            Assert.Equal("New York", result[0].City.Name);
        }

        [Fact]
        public void Where_And_Select_Push_Down_Works()
        {
            // WHERE + SELECT: both predicates must be evaluated in a single BSON-level pass.
            // Only scalar fields — push-down should fire and return filtered projections.
            var result = _db.TestDocuments.AsQueryable()
                .Where(x => x.Category == "A")
                .Select(x => new { x.Name, x.Amount })
                .ToList();

            Assert.Equal(2, result.Count);
            Assert.Contains(result, r => r.Name == "Item1" && r.Amount == 10);
            Assert.Contains(result, r => r.Name == "Item2" && r.Amount == 20);
        }

        [Fact]
        public void Select_Project_Nested_Array_Of_Objects()
        {
            var doc = new ComplexDocument
            {
                Id = ObjectId.NewObjectId(),
                Title = "Order with Items",
                ShippingAddress = new Address { City = new City { Name = "Los Angeles" }, Street = "Hollywood Blvd" },
                Items = new List<OrderItem>
                {
                    new OrderItem { Name = "Laptop", Price = 1500 },
                    new OrderItem { Name = "Mouse", Price = 25 },
                    new OrderItem { Name = "Keyboard", Price = 75 }
                }
            };
            _db.ComplexDocuments.Insert(doc);
            _db.SaveChanges();

            // Retrieve the full document and verify Items array
            var retrieved = _db.ComplexDocuments.FindAll().First();

            Assert.Equal("Order with Items", retrieved.Title);
            Assert.Equal("Los Angeles", retrieved.ShippingAddress.City.Name);
            Assert.Equal("Hollywood Blvd", retrieved.ShippingAddress.Street);
            
            // Verify array of nested objects
            Assert.Equal(3, retrieved.Items.Count);
            Assert.Equal("Laptop", retrieved.Items[0].Name);
            Assert.Equal(1500, retrieved.Items[0].Price);
            Assert.Equal("Mouse", retrieved.Items[1].Name);
            Assert.Equal(25, retrieved.Items[1].Price);
            Assert.Equal("Keyboard", retrieved.Items[2].Name);
            Assert.Equal(75, retrieved.Items[2].Price);
        }
    }
}
