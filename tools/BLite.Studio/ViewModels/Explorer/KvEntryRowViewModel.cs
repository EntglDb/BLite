namespace BLite.Studio.ViewModels.Explorer;

public class KvEntryRowViewModel
{
    public string Key          { get; init; } = string.Empty;
    public int    SizeBytes    { get; init; }
    public string ValuePreview { get; init; } = string.Empty;

    public string SizeDisplay => SizeBytes < 1024
        ? $"{SizeBytes} B"
        : $"{SizeBytes / 1024.0:0.#} KB";
}
