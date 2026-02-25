using CommunityToolkit.Mvvm.ComponentModel;

namespace BLite.Studio.ViewModels.Explorer;

public sealed partial class VectorSourceFieldViewModel : ObservableObject
{
    [ObservableProperty] private string _path   = string.Empty;
    [ObservableProperty] private string? _prefix;
    [ObservableProperty] private string? _suffix;
}
