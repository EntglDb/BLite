using CommunityToolkit.Mvvm.ComponentModel;

namespace BLite.Studio.ViewModels.Explorer;

public enum SidebarItemKind { FieldKeys, Collection }

public partial class SidebarItemViewModel : ObservableObject
{
    public SidebarItemKind Kind    { get; init; }
    public string          Label   { get; init; } = string.Empty;

    /// <summary>Set only when Kind == Collection.</summary>
    public string? CollectionName  { get; init; }

    /// <summary>True when the collection is a TimeSeries collection.</summary>
    public bool IsTimeSeries { get; init; }

    [ObservableProperty]
    private bool _isSelected;
}
