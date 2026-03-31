namespace FirstOrDefaultAyncIssueRepro;

// EntityBase<TId> — base class with Id property
// DevicePo — the entity being queried
public class DevicePo(string id) : PoBase<string>(id)
{
    public required string Name { get; init; }
    public DeviceType Type { get; set; }
    public required string Identifier { get; init; }
    public DateTime LastSeen { get; set; }
    public required List<LinkedFolderPo> LinkedFolders { get; set; } = [];
}
