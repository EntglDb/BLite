// EntityBase<TId> — base class with Id property
using BLite.Core;
using BLite.Core.Collections;
using BLite.Core.Metadata;
using System.Security.Principal;

public interface IEntity<TId> where TId : notnull
{
    TId Id { get; init; }
}

public interface IPoEntity
{
    DateTime AddedAt { get; set; }
}

public abstract class EntityBase<TId> : IEntity<TId> where TId : notnull
{
    protected EntityBase(TId id)
    {
        ArgumentNullException.ThrowIfNull(id, "entity id");
        Id = id;
    }

    public virtual TId Id { get; init; }
}

// PoBase<TId> — persistence object base
public abstract class PoBase<TId>(TId id) : EntityBase<TId>(id), IPoEntity where TId : notnull
{
    public DateTime AddedAt { get; set; }
}

// DevicePo — the entity being queried
public class DevicePo(string id) : PoBase<string>(id)
{
    public required string Name { get; init; }
    public DeviceType Type { get; set; }
    public required string Identifier { get; init; }
    public DateTime LastSeen { get; set; }
    public required List<LinkedFolderPo> LinkedFolders { get; set; } = [];
}

public enum DeviceType
{
    Nick = 0,
    Android = 1,
    iOS = 2,
    Unknown = 99
}

public class LinkedFolderPo : PoBase<string>
{
    public LinkedFolderPo(string id) : base(id)
    {
    }
    public string Name { get; set; }
    public DeviceType Type { get; set; }
    public string DeviceId { get; set; }
}

public sealed partial class PhotoDeviceSourceBLiteDbContext : DocumentDbContext
{
    public DocumentCollection<string, DevicePo> Devices { get; set; } = null!;
    public DocumentCollection<string, LinkedFolderPo> LinkedFolders { get; set; } = null!;

    public PhotoDeviceSourceBLiteDbContext(string path) : base(path)
    {
        InitializeCollections();
    }

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<DevicePo>()
            .ToCollection("Device")
            .HasIndex(x => x.Id, unique: true)         // secondary index on primary key
            .HasIndex(x => x.Identifier, unique: true);

        modelBuilder.Entity<LinkedFolderPo>()
            .ToCollection("LinkedFolder")
            .HasIndex(x => x.Id, unique: true)
            .HasIndex(x => x.DeviceId);
    }
}


public class Program
{
    public static async Task Main()
    {
        var tempDbPath = Path.Combine(Path.GetTempPath(), "PhotoDeviceSourceBLiteDbContext");
        var ReadOnlyDb = new PhotoDeviceSourceBLiteDbContext(tempDbPath);

        // Seed data
        var linkedFolder = new LinkedFolderPo("linked-folder-1")
        {
            Name = "Linked Folder 1",
            Type = DeviceType.Android,
            DeviceId = "device1"
        };
        var device = new DevicePo("device1")
        {
            Name = "Device 1",
            Type = DeviceType.Android,
            Identifier = "device-identifier-1",
            LastSeen = DateTime.UtcNow,
            LinkedFolders = new List<LinkedFolderPo> { linkedFolder }
        };

        await ReadOnlyDb.Devices.InsertAsync(device);

        var ct = CancellationToken.None;

        // ✅ Data exists
        Console.WriteLine(ReadOnlyDb.Devices.CountAsync().Result);

        // ✅ Deserialization works — entity is correctly retrieved without predicate
        Console.WriteLine(ReadOnlyDb.Devices.AsQueryable().FirstOrDefaultAsync().Result);
        // → DevicePo [Id=CC3Rgw-i], Name="LEO-PC", Identifier="TKFGF6F6NHHDGFYFBV2FB0HJW2YJDFMZ8JAHQ00T3E3A10JEC5CG"

        // ❌ Direct equality on primary key — returns null
        Console.WriteLine(ReadOnlyDb.Devices.AsQueryable().FirstOrDefaultAsync(x => x.Id == "CC3Rgw-i").Result);
        // → null

        // ❌ Same result with a variable
        Console.WriteLine(ReadOnlyDb.Devices.AsQueryable().FirstOrDefaultAsync(x => x.Id == id, ct).Result);
        // → null   (id = "CC3Rgw-i")

        // ✅ Forcing full scan via ToLowerInvariant() — returns correct result
        Console.WriteLine(ReadOnlyDb.Devices.AsQueryable()
            .FirstOrDefaultAsync(x => x.Id.ToLowerInvariant() == "CC3Rgw-i".ToLowerInvariant(), ct).Result);
        // → DevicePo [Id=CC3Rgw-i]

        // ✅ Forcing full scan via ToUpperInvariant() — also works
        Console.WriteLine(ReadOnlyDb.Devices.AsQueryable()
            .FirstOrDefaultAsync(x => x.Id.ToUpperInvariant() == "CC3Rgw-i".ToUpperInvariant(), ct).Result;
        // → DevicePo [Id=CC3Rgw-i]

        // ✅ Forcing full scan via ToLower() — also works
        Console.WriteLine(ReadOnlyDb.Devices.AsQueryable()
            .FirstOrDefaultAsync(x => x.Id.ToLower() == "CC3Rgw-i".ToLower(), ct).Result);
    }
}
