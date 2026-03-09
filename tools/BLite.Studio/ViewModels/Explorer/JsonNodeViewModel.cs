using System.Collections.ObjectModel;
using CommunityToolkit.Mvvm.ComponentModel;

namespace BLite.Studio.ViewModels.Explorer;

public sealed partial class JsonNodeViewModel : ObservableObject
{
    /// <summary>Tracks whether this tree node is expanded in the TreeView.</summary>
    [ObservableProperty] private bool _isExpanded;

    /// <summary>Property name, array index label (e.g. "[0]"), or null for the root node.</summary>
    public string? Key { get; init; }

    /// <summary>
    /// Formatted value text:
    /// objects → "{ }", arrays → "[ n ]", strings → "\"value\"",
    /// numbers/bools/null → raw text.
    /// </summary>
    public string DisplayValue { get; init; } = "";

    /// <summary>"string" | "number" | "bool" | "null" | "object" | "array"</summary>
    public string TypeLabel { get; init; } = "";

    public ObservableCollection<JsonNodeViewModel> Children { get; } = [];

    // ── Convenience helpers used by the AXAML template ────────────────────────

    public bool IsContainer  => Children.Count > 0;
    public bool HasKey       => Key is not null;

    public bool IsString     => TypeLabel == "string";
    public bool IsNumber     => TypeLabel == "number";
    public bool IsBoolOrNull => TypeLabel is "bool" or "null";
}
