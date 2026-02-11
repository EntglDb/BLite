using System.Collections.Generic;

namespace BLite.Bson;

public partial class BsonSchema
{
    public string? Title { get; set; }
    public List<BsonField> Fields { get; } = new();
}
