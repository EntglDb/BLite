// EntityBase<TId> — base class with Id property
using BLite.Core.Query;
using FirstOrDefaultAyncIssueRepro;

public class Program
{
    public static async Task Main()
    {
        var tempDbPath = Path.Combine(Path.GetTempPath(), "PhotoDeviceSourceBLiteDbContext");
        var ReadOnlyDb = new PhotoDeviceSourceBLiteDbContext(tempDbPath);

        var id = "device1";

        // Seed data
        var linkedFolder = new LinkedFolderPo("linked-folder-1")
        {
            Name = "Linked Folder 1",
            Type = DeviceType.Android,
            DeviceId = id
        };
        var device = new DevicePo(id)
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
        Console.WriteLine(ReadOnlyDb.Devices.AsQueryable().FirstOrDefaultAsync().Result?.Id);
        // → DevicePo [Id=device1], Name="LEO-PC", Identifier="TKFGF6F6NHHDGFYFBV2FB0HJW2YJDFMZ8JAHQ00T3E3A10JEC5CG"

        // ❌ Direct equality on primary key — returns null
        Console.WriteLine(ReadOnlyDb.Devices.AsQueryable().FirstOrDefaultAsync(x => x.Id == "device1").Result?.Id);
        // → null

        // ❌ Same result with a variable
        Console.WriteLine(ReadOnlyDb.Devices.AsQueryable().FirstOrDefaultAsync(x => x.Id == id, ct).Result?.Id);
        // → null   (id = "device1")

        // ✅ Forcing full scan via ToLowerInvariant() — returns correct result
        Console.WriteLine(ReadOnlyDb.Devices.AsQueryable()
            .FirstOrDefaultAsync(x => x.Id.ToLowerInvariant() == "device1".ToLowerInvariant(), ct).Result?.Id);
        // → DevicePo [Id=device1]

        // ✅ Forcing full scan via ToUpperInvariant() — also works
        Console.WriteLine(ReadOnlyDb.Devices.AsQueryable()
            .FirstOrDefaultAsync(x => x.Id.ToUpperInvariant() == "device1".ToUpperInvariant(), ct).Result?.Id);
        // → DevicePo [Id=device1]

        // ✅ Forcing full scan via ToLower() — also works
        Console.WriteLine(ReadOnlyDb.Devices.AsQueryable()
            .FirstOrDefaultAsync(x => x.Id.ToLower() == "device1".ToLower(), ct).Result?.Id);
    }
}
