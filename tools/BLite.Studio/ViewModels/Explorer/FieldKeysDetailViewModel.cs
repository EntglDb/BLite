using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using CommunityToolkit.Mvvm.ComponentModel;

namespace BLite.Studio.ViewModels.Explorer;

public partial class FieldKeysDetailViewModel : ObservableObject
{
    private readonly List<FieldKeyEntry> _allKeys;

    public FieldKeysDetailViewModel(IReadOnlyDictionary<string, ushort> keyMap)
    {
        _allKeys = keyMap
            .OrderBy(kv => kv.Key)
            .Select(kv => new FieldKeyEntry { Name = kv.Key, Id = kv.Value })
            .ToList();

        FilteredKeys = new ObservableCollection<FieldKeyEntry>(_allKeys);
    }

    public int TotalCount => _allKeys.Count;

    [ObservableProperty]
    private string _searchText = string.Empty;

    partial void OnSearchTextChanged(string value) => ApplyFilter();

    public ObservableCollection<FieldKeyEntry> FilteredKeys { get; }

    private void ApplyFilter()
    {
        FilteredKeys.Clear();
        var q = SearchText?.Trim() ?? string.Empty;
        var filtered = string.IsNullOrEmpty(q)
            ? _allKeys
            : _allKeys.Where(k => k.Name.Contains(q, System.StringComparison.OrdinalIgnoreCase));

        foreach (var k in filtered)
            FilteredKeys.Add(k);

        OnPropertyChanged(nameof(FilteredCount));
    }

    public int FilteredCount => FilteredKeys.Count;
}
