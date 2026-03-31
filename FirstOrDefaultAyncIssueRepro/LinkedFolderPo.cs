namespace FirstOrDefaultAyncIssueRepro;

// EntityBase<TId> — base class with Id property
public class LinkedFolderPo : PoBase<string>
{
    public LinkedFolderPo(string id) : base(id)
    {
    }
    public string Name { get; set; }
    public DeviceType Type { get; set; }
    public string DeviceId { get; set; }
}
