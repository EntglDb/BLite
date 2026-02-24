using BLite.Bson;

namespace BLite.Studio.ViewModels.Explorer;

/// <summary>
/// Represents a single document row in the paginated documents grid.
/// </summary>
public sealed class DocumentRowViewModel
{
    public BsonId BsonId     { get; init; }
    public string Id         { get; init; } = "";
    public string Content    { get; init; } = "";
    public int    SizeBytes  { get; init; }

    public string SizeDisplay => SizeBytes >= 1024
        ? $"{SizeBytes / 1024.0:F1} KB"
        : $"{SizeBytes} B";
}
