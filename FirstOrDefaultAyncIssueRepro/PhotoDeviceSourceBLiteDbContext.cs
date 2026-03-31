// EntityBase<TId> — base class with Id property
using BLite.Core;
using BLite.Core.Collections;
using BLite.Core.Metadata;

namespace FirstOrDefaultAyncIssueRepro;

public partial class PhotoDeviceSourceBLiteDbContext : DocumentDbContext
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
