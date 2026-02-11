using Xunit;
using BLite.Core.Collections;
using BLite.Bson;
using System.Linq;
using System.Collections.Generic;
using BLite.Core.Indexing;
using BLite.Core.Storage;
using System;
using System.IO;

namespace BLite.Tests
{
    public class AdvancedQueryTests : IDisposable
    {
        private readonly string _dbPath;
        private readonly StorageEngine _storage;
        private readonly DocumentCollection<TestDocument> _collection;

        public class TestDocument
        {
            public ObjectId Id { get; set; }
            public string Category { get; set; } = string.Empty;
            public int Amount { get; set; }
            public string Name { get; set; } = string.Empty;
        }

        public AdvancedQueryTests()
        {
            _dbPath = Path.Combine(Path.GetTempPath(), $"blite_advanced_{Guid.NewGuid()}.db");
            _storage = new StorageEngine(_dbPath, PageFileConfig.Default);
            _collection = new DocumentCollection<TestDocument>(_storage, new GenericMapper<TestDocument>());

            // Seed Data
            _collection.Insert(new TestDocument { Category = "A", Amount = 10, Name = "Item1" });
            _collection.Insert(new TestDocument { Category = "A", Amount = 20, Name = "Item2" });
            _collection.Insert(new TestDocument { Category = "B", Amount = 30, Name = "Item3" });
            _collection.Insert(new TestDocument { Category = "B", Amount = 40, Name = "Item4" });
            _collection.Insert(new TestDocument { Category = "C", Amount = 50, Name = "Item5" });
        }

        public void Dispose()
        {
            _storage.Dispose();
            if (File.Exists(_dbPath)) File.Delete(_dbPath);
        }

        [Fact]
        public void GroupBy_Simple_Key_Works()
        {
            var groups = _collection.AsQueryable()
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
            var results = _collection.AsQueryable()
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
            var query = _collection.AsQueryable();

            Assert.Equal(5, query.Count());
            Assert.Equal(150, query.Sum(x => x.Amount));
            Assert.Equal(30.0, query.Average(x => x.Amount));
            Assert.Equal(10, query.Min(x => x.Amount));
            Assert.Equal(50, query.Max(x => x.Amount));
        }

        [Fact]
        public void Aggregations_With_Predicate_Works()
        {
            var query = _collection.AsQueryable().Where(x => x.Category == "A");

            Assert.Equal(2, query.Count());
            Assert.Equal(30, query.Sum(x => x.Amount));
        }

        [Fact]
        public void Join_Works_InMemory()
        {
            // Create a second collection for joining
            var ordersCollection = new DocumentCollection<OrderDocument>(_storage, new GenericMapper<OrderDocument>(), "orders");
            ordersCollection.Insert(new OrderDocument { ItemName = "Item1", Quantity = 5 });
            ordersCollection.Insert(new OrderDocument { ItemName = "Item3", Quantity = 2 });

            var query = _collection.AsQueryable()
                .Join(ordersCollection.AsQueryable(),
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

        public class OrderDocument
        {
            public ObjectId Id { get; set; }
            public string ItemName { get; set; } = string.Empty;
            public int Quantity { get; set; }
        }

        public class Address
        {
            public string City { get; set; } = string.Empty;
            public string Street { get; set; } = string.Empty;
        }

        public class OrderItem
        {
            public string Name { get; set; } = string.Empty;
            public int Price { get; set; }
        }

        public class ComplexDocument
        {
            public ObjectId Id { get; set; }
            public string Title { get; set; } = string.Empty;
            public Address ShippingAddress { get; set; } = new();
            public List<OrderItem> Items { get; set; } = new();
        }

        [Fact]
        public void Select_Project_Nested_Object()
        {
            using var storage = new StorageEngine(Path.GetTempFileName(), PageFileConfig.Default);
            var collection = new DocumentCollection<ComplexDocument>(storage, new ComplexMapper());
            
            var doc = new ComplexDocument
            {
                Title = "Order1",
                ShippingAddress = new Address { City = "New York", Street = "5th Ave" },
                Items = new List<OrderItem> 
                { 
                    new OrderItem { Name = "Laptop", Price = 1000 },
                    new OrderItem { Name = "Mouse", Price = 50 }
                }
            };
            collection.Insert(doc);

            var query = collection.AsQueryable()
                .Select(x => x.ShippingAddress)
                .ToList();

            Assert.Single(query);
            Assert.Equal("New York", query[0].City);
            Assert.Equal("5th Ave", query[0].Street);
        }

        [Fact]
        public void Select_Project_Nested_Field()
        {
            using var storage = new StorageEngine(Path.GetTempFileName(), PageFileConfig.Default);
            var collection = new DocumentCollection<ComplexDocument>(storage, new ComplexMapper());
            
            var doc = new ComplexDocument
            {
                Title = "Order1",
                ShippingAddress = new Address { City = "New York", Street = "5th Ave" }
            };
            collection.Insert(doc);

            var cities = collection.AsQueryable()
                .Select(x => x.ShippingAddress.City)
                .ToList();

            Assert.Single(cities);
            Assert.Equal("New York", cities[0]);
        }

        [Fact]
        public void Select_Anonymous_Complex()
        {
             using var storage = new StorageEngine(Path.GetTempFileName(), PageFileConfig.Default);
            var collection = new DocumentCollection<ComplexDocument>(storage, new ComplexMapper());
            
            var doc = new ComplexDocument
            {
                Title = "Order1",
                ShippingAddress = new Address { City = "New York", Street = "5th Ave" }
            };
            collection.Insert(doc);

            var result = collection.AsQueryable()
                .Select(x => new { Title = x.Title, City = x.ShippingAddress.City })
                .ToList();

            Assert.Single(result);
            Assert.Equal("Order1", result[0].Title);
            Assert.Equal("New York", result[0].City);
        }

       internal class ComplexMapper : IDocumentMapper<ComplexDocument>
        {
             public string CollectionName => "complex";
             public IEnumerable<string> UsedKeys => new[] { "_id", "title", "shippingaddress", "items", "city", "street", "name", "price", "0", "1" };
             public BsonSchema GetSchema() => new BsonSchema { Version = 1 };

             public ObjectId GetId(ComplexDocument entity) => entity.Id;
             public void SetId(ComplexDocument entity, ObjectId id) => entity.Id = id;
             
             public IndexKey ToIndexKey(ObjectId id) => new IndexKey(id.ToByteArray());
             public ObjectId FromIndexKey(IndexKey key) => new ObjectId(key.Data.ToArray());

             public int Serialize(ComplexDocument entity, BsonSpanWriter writer)
             {
                 var docStart = writer.BeginDocument();
                 writer.WriteObjectId("_id", entity.Id);
                 writer.WriteString("title", entity.Title);
                 
                 // Nested Address
                 if (entity.ShippingAddress != null)
                 {
                     var addrStart = writer.BeginDocument("shippingaddress");
                     writer.WriteString("city", entity.ShippingAddress.City);
                     writer.WriteString("street", entity.ShippingAddress.Street);
                     writer.EndDocument(addrStart);
                 }
                 else
                 {
                     writer.WriteNull("shippingaddress");
                 }

                 // List Items
                 if (entity.Items != null)
                 {
                     var arrayStart = writer.BeginArray("items");
                     for(int i=0; i<entity.Items.Count; i++)
                     {
                         var item = entity.Items[i];
                         // Array elements have index as key "0", "1", etc.
                         var itemStart = writer.BeginDocument(i.ToString());
                         writer.WriteString("name", item.Name);
                         writer.WriteInt32("price", item.Price);
                         writer.EndDocument(itemStart);
                     }
                     writer.EndArray(arrayStart);
                 }
                 else
                 {
                     writer.WriteNull("items");
                 }

                 writer.EndDocument(docStart);
                 return writer.Position;
             }

             public ComplexDocument Deserialize(BsonSpanReader reader)
             {
                 var entity = new ComplexDocument();
                 reader.ReadDocumentSize(); // Consume size

                 while (reader.Remaining > 1)
                 {
                     var type = reader.ReadBsonType();
                     if (type == BsonType.EndOfDocument) break;
                     var name = reader.ReadElementHeader();

                     if (name == "_id") entity.Id = reader.ReadObjectId();
                     else if (name == "title") entity.Title = reader.ReadString();
                     else if (name == "shippingaddress")
                     {
                         if (type == BsonType.Null) 
                         {
                             // reader.SkipValue(type);
                         }
                         else
                         {
                             entity.ShippingAddress = new Address();
                             reader.ReadDocumentSize();
                             while (reader.Remaining > 1) 
                             {
                                 var t = reader.ReadBsonType();
                                 if (t == BsonType.EndOfDocument) break;
                                 var n = reader.ReadElementHeader();
                                 if (n == "city") entity.ShippingAddress.City = reader.ReadString();
                                 else if (n == "street") entity.ShippingAddress.Street = reader.ReadString();
                                 else reader.SkipValue(t);
                             }
                         }
                     }
                     else if (name == "items")
                     {
                          if (type == BsonType.Null) {}
                          else
                          {
                              entity.Items = new List<OrderItem>();
                              reader.ReadDocumentSize(); // Array size
                              while (reader.Remaining > 1)
                              {
                                  var t = reader.ReadBsonType();
                                  if (t == BsonType.EndOfDocument) break;
                                  var n = reader.ReadElementHeader(); // Index "0", "1"...
                                  
                                  // For array elements (documents), we need to handle them carefully
                                  if (t == BsonType.Document)
                                  {
                                      var item = new OrderItem();
                                      reader.ReadDocumentSize(); // Item doc size
                                      while (reader.Remaining > 1)
                                      {
                                          var it = reader.ReadBsonType();
                                          if (it == BsonType.EndOfDocument) break;
                                          var iname = reader.ReadElementHeader();
                                          if (iname == "name") item.Name = reader.ReadString();
                                          else if (iname == "price") item.Price = reader.ReadInt32();
                                          else reader.SkipValue(it);
                                      }
                                      entity.Items.Add(item);
                                  }
                                  else
                                  {
                                      reader.SkipValue(t);
                                  }
                              }
                          }
                     }
                     else reader.SkipValue(type);
                 }
                 return entity;
             }
        }

        internal class GenericMapper<T> : IDocumentMapper<T> where T : class
        {
            public string CollectionName => typeof(T).Name.ToLower();

            public ObjectId GetId(T entity)
            {
                var prop = typeof(T).GetProperty("Id");
                return (ObjectId)prop!.GetValue(entity)!;
            }

            public void SetId(T entity, ObjectId id)
            {
                var prop = typeof(T).GetProperty("Id");
                prop!.SetValue(entity, id);
            }

            public BsonSchema GetSchema() => new BsonSchema { Version = 1 };

            public int Serialize(T entity, BsonSpanWriter writer)
            {
                var docStart = writer.BeginDocument();
                
                // Write Id
                var idProp = typeof(T).GetProperty("Id");
                var id = (ObjectId)idProp!.GetValue(entity)!;
                writer.WriteObjectId("_id", id);
                
                foreach (var prop in typeof(T).GetProperties())
                {
                    if (prop.Name == "Id") continue;
                    
                    var value = prop.GetValue(entity);
                    if (value == null)
                    {
                        writer.WriteNull(prop.Name.ToLower());
                        continue;
                    }

                    if (prop.PropertyType == typeof(string))
                        writer.WriteString(prop.Name.ToLower(), (string)value);
                    else if (prop.PropertyType == typeof(int))
                        writer.WriteInt32(prop.Name.ToLower(), (int)value);
                    else if (prop.PropertyType == typeof(double))
                        writer.WriteDouble(prop.Name.ToLower(), (double)value);
                }
                
                writer.EndDocument(docStart);
                return writer.Position;
            }

            public T Deserialize(BsonSpanReader reader)
            {
                var entity = Activator.CreateInstance<T>();
                reader.ReadDocumentSize(); // Consume size
                
                while (reader.Remaining > 1) // At least 1 byte for EOD
                {
                    var type = reader.ReadBsonType();
                    if (type == BsonType.EndOfDocument) break;
                    
                    var name = reader.ReadElementHeader();

                    if (name == "_id")
                    {
                        var id = reader.ReadObjectId();
                        SetId(entity, id);
                        continue;
                    }

                    var prop = typeof(T).GetProperty(name, System.Reflection.BindingFlags.IgnoreCase | System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Instance);
                    if (prop != null)
                    {
                        if (prop.PropertyType == typeof(string))
                            prop.SetValue(entity, reader.ReadString());
                        else if (prop.PropertyType == typeof(int))
                            prop.SetValue(entity, reader.ReadInt32());
                         else if (prop.PropertyType == typeof(double))
                            prop.SetValue(entity, reader.ReadDouble());
                        else
                            reader.SkipValue(type);
                    }
                    else
                    {
                        reader.SkipValue(type);
                    }
                }
                
                return entity;
            }
            
            public IEnumerable<string> UsedKeys 
            {
                get 
                {
                    yield return "_id";
                    foreach (var prop in typeof(T).GetProperties())
                    {
                        if (prop.Name != "Id")
                            yield return prop.Name.ToLower();
                    }
                }
            }

            public IndexKey ToIndexKey(ObjectId id)
            {
                return new IndexKey(id.ToByteArray());
            }

            public ObjectId FromIndexKey(IndexKey key)
            {
                 return new ObjectId(key.Data.ToArray());
            }
        }
    }
}
