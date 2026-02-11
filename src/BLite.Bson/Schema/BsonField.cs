using System.Collections.Generic;

namespace BLite.Bson;

public partial class BsonField
{
    public required string Name { get; init; }
    public BsonType Type { get; init; }
    public bool IsNullable { get; init; }
    public BsonSchema? NestedSchema { get; init; }
    public BsonType? ArrayItemType { get; init; }
}
