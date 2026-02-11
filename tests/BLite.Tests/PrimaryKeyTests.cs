using BLite.Bson;
using BLite.Core;
using BLite.Core.Collections;
using BLite.Core.Metadata;
using System;
using System.Buffers;

namespace BLite.Tests;

public class PrimaryKeyTests : IDisposable
{
    private readonly string _dbPath = "primary_key_tests.db";

    public PrimaryKeyTests()
    {
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
    }

    public void Dispose()
    {
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
    }

    public class IntEntity
    {
        public int Id { get; set; }
        public string? Name { get; set; }
    }

    public class IntMapper : Int32MapperBase<IntEntity>
    {
        public override string CollectionName => "int_entities";
        public override int GetId(IntEntity entity) => entity.Id;
        public override void SetId(IntEntity entity, int id) => entity.Id = id;

        public override int Serialize(IntEntity entity, Span<byte> buffer)
        {
            var writer = new BsonSpanWriter(buffer);
            var sizePos = writer.BeginDocument();
            writer.WriteInt32("Id", entity.Id);
            writer.WriteString("Name", entity.Name ?? "");
            writer.EndDocument(sizePos);
            return writer.Position;
        }

        public override IntEntity Deserialize(ReadOnlySpan<byte> data)
        {
            var reader = new BsonSpanReader(data);
            var entity = new IntEntity();
            reader.ReadDocumentSize();
            while (reader.Position < data.Length)
            {
                var type = reader.ReadBsonType();
                if (type == BsonType.EndOfDocument)
                    break;
                var name = reader.ReadCString();
                switch (name)
                {
                    case "Id":
                        entity.Id = reader.ReadInt32();
                        break;
                    case "Name":
                        entity.Name = reader.ReadString();
                        break;
                    default:
                        reader.SkipValue(type);
                        break;
                }
            }
            return entity;
        }
    }

    public class StringEntity
    {
        public required string Id { get; set; }
        public string? Value { get; set; }
    }

    public class StringMapper : StringMapperBase<StringEntity>
    {
        public override string CollectionName => "string_entities";
        public override string GetId(StringEntity entity) => entity.Id;
        public override void SetId(StringEntity entity, string id) => entity.Id = id;

        public override int Serialize(StringEntity entity, Span<byte> buffer)
        {
            var writer = new BsonSpanWriter(buffer);
            var sizePos = writer.BeginDocument();
            writer.WriteString("Id", entity.Id);
            writer.WriteString("Value", entity.Value ?? "");
            writer.EndDocument(sizePos);
            return writer.Position;
        }

        public override StringEntity Deserialize(ReadOnlySpan<byte> data)
        {
            var reader = new BsonSpanReader(data);
            reader.ReadDocumentSize();
            string id = string.Empty;
            string value = string.Empty;
            while (reader.Position < data.Length)
            {
                var type = reader.ReadBsonType();
                if (type == BsonType.EndOfDocument)
                    break;
                var name = reader.ReadCString();
                switch (name)
                {
                    case "Id":
                        id = reader.ReadString();
                        break;
                    case "Value":
                        value = reader.ReadString();
                        break;
                    default:
                        reader.SkipValue(type);
                        break;
                }
            }
            return new StringEntity { Id = id, Value = value };
        }
    }

    public class GuidEntity
    {
        public Guid Id { get; set; }
        public string? Name { get; set; }
    }

    public class GuidMapper : GuidMapperBase<GuidEntity>
    {
        public override string CollectionName => "guid_entities";
        public override Guid GetId(GuidEntity entity) => entity.Id;
        public override void SetId(GuidEntity entity, Guid id) => entity.Id = id;

        public override int Serialize(GuidEntity entity, Span<byte> buffer)
        {
            var writer = new BsonSpanWriter(buffer);
            var sizePos = writer.BeginDocument();
            writer.WriteString("Id", entity.Id.ToString());
            writer.WriteString("Name", entity.Name ?? "");
            writer.EndDocument(sizePos);
            return writer.Position;
        }

        public override GuidEntity Deserialize(ReadOnlySpan<byte> data)
        {
            var reader = new BsonSpanReader(data);
            var entity = new GuidEntity();
            reader.ReadDocumentSize();
            while (reader.Position < data.Length)
            {
                var type = reader.ReadBsonType();
                if (type == BsonType.EndOfDocument)
                    break;
                var name = reader.ReadCString();
                switch (name)
                {
                    case "Id":
                        entity.Id = Guid.Parse(reader.ReadString());
                        break;
                    case "Name":
                        entity.Name = reader.ReadString();
                        break;
                    default:
                        reader.SkipValue(type);
                        break;
                }
            }
            return entity;
        }
    }

    public class PrimaryKeyDbContext : DocumentDbContext
    {
        public DocumentCollection<int, IntEntity> IntEntities { get; private set; }
        public DocumentCollection<string, StringEntity> StringEntities { get; private set; }
        public DocumentCollection<Guid, GuidEntity> GuidEntities { get; private set; }

        public PrimaryKeyDbContext(string path) : base(path)
        {
            IntEntities = CreateCollection(new IntMapper());
            StringEntities = CreateCollection(new StringMapper());
            GuidEntities = CreateCollection(new GuidMapper());
        }

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<IntEntity>().HasKey(e => e.Id);
            modelBuilder.Entity<StringEntity>().HasKey(e => e.Id);
            modelBuilder.Entity<GuidEntity>().HasKey(e => e.Id);
        }
    }

    [Fact]
    public void Test_Int_PrimaryKey()
    {
        using var db = new PrimaryKeyDbContext(_dbPath);

        var entity = new IntEntity { Id = 1, Name = "Test 1" };
        db.IntEntities.Insert(entity);

        var retrieved = db.IntEntities.FindById(1);
        Assert.NotNull(retrieved);
        Assert.Equal(1, retrieved.Id);
        Assert.Equal("Test 1", retrieved.Name);

        entity.Name = "Updated";
        db.IntEntities.Update(entity);

        retrieved = db.IntEntities.FindById(1);
        Assert.Equal("Updated", retrieved?.Name);

        db.IntEntities.Delete(1);
        Assert.Null(db.IntEntities.FindById(1));
    }

    [Fact]
    public void Test_String_PrimaryKey()
    {
        using var db = new PrimaryKeyDbContext(_dbPath);

        var entity = new StringEntity { Id = "key1", Value = "Value 1" };
        db.StringEntities.Insert(entity);

        var retrieved = db.StringEntities.FindById("key1");
        Assert.NotNull(retrieved);
        Assert.Equal("key1", retrieved.Id);
        Assert.Equal("Value 1", retrieved.Value);

        db.StringEntities.Delete("key1");
        Assert.Null(db.StringEntities.FindById("key1"));
    }

    [Fact]
    public void Test_Guid_PrimaryKey()
    {
        using var db = new PrimaryKeyDbContext(_dbPath);

        var id = Guid.NewGuid();
        var entity = new GuidEntity { Id = id, Name = "Guid Test" };
        db.GuidEntities.Insert(entity);

        var retrieved = db.GuidEntities.FindById(id);
        Assert.NotNull(retrieved);
        Assert.Equal(id, retrieved.Id);

        db.GuidEntities.Delete(id);
        Assert.Null(db.GuidEntities.FindById(id));
    }
}
