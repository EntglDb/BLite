using BLite.Bson;
using CommunityToolkit.Mvvm.ComponentModel;

namespace BLite.Studio.ViewModels.Explorer;

public sealed partial class SchemaFieldViewModel : ObservableObject
{
    [ObservableProperty] private string _name = string.Empty;
    [ObservableProperty] private BsonType _type = BsonType.String;
    [ObservableProperty] private bool _isNullable = true;
}
