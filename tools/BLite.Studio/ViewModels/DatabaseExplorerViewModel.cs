using System.Collections.ObjectModel;
using System.Linq;
using BLite.Core;
using BLite.Studio.ViewModels.Explorer;
using CommunityToolkit.Mvvm.ComponentModel;
using CommunityToolkit.Mvvm.Input;

namespace BLite.Studio.ViewModels;

public partial class DatabaseExplorerViewModel : ObservableObject
{
    private readonly BLiteEngine _engine;

    public DatabaseExplorerViewModel(BLiteEngine engine, string dbPath)
    {
        _engine   = engine;
        DbPath    = dbPath;
        DbName    = System.IO.Path.GetFileName(dbPath);

        BuildSidebar();
    }

    public string DbPath { get; }
    public string DbName { get; }

    // ── Sidebar ───────────────────────────────────────────────────────────────
    public ObservableCollection<SidebarItemViewModel> SidebarItems { get; } = [];
    public ObservableCollection<SidebarItemViewModel> CollectionItems { get; } = [];

    [ObservableProperty]
    [NotifyPropertyChangedFor(nameof(DetailContent))]
    private SidebarItemViewModel? _selectedItem;

    partial void OnSelectedItemChanged(SidebarItemViewModel? value)
    {
        // Clear all IsSelected flags, then set the active one
        foreach (var item in SidebarItems)
            item.IsSelected = false;
        foreach (var item in CollectionItems)
            item.IsSelected = false;

        if (value is not null)
            value.IsSelected = true;

        RefreshDetail();
    }

    // ── Detail ────────────────────────────────────────────────────────────────
    [ObservableProperty]
    private object? _detailContent;

    // ── Commands ──────────────────────────────────────────────────────────────
    [RelayCommand]
    private void SelectItem(SidebarItemViewModel? item)
    {
        SelectedItem = item;
    }

    [RelayCommand]
    private void Refresh()
    {
        BuildSidebar();
        // Re-show detail for currently selected item
        RefreshDetail();
    }

    // ── Internal ──────────────────────────────────────────────────────────────
    private void BuildSidebar()
    {
        SidebarItems.Clear();
        CollectionItems.Clear();

        // "Field Keys" is always the first top-level item
        SidebarItems.Add(new SidebarItemViewModel
        {
            Kind  = SidebarItemKind.FieldKeys,
            Label = "Field Keys",
        });

        // Collections
        foreach (var name in _engine.ListCollections().OrderBy(n => n))
        {
            CollectionItems.Add(new SidebarItemViewModel
            {
                Kind           = SidebarItemKind.Collection,
                Label          = name,
                CollectionName = name,
            });
        }

        CollectionCount = CollectionItems.Count;
    }

    [ObservableProperty]
    [NotifyPropertyChangedFor(nameof(HasCollections))]
    private int _collectionCount;

    public bool HasCollections => CollectionCount > 0;

    private void RefreshDetail()
    {
        if (SelectedItem is null)
        {
            DetailContent = null;
            return;
        }

        DetailContent = SelectedItem.Kind switch
        {
            SidebarItemKind.FieldKeys  =>
                new FieldKeysDetailViewModel(_engine.GetKeyMap()),

            SidebarItemKind.Collection when SelectedItem.CollectionName is { } cn =>
                new CollectionDetailViewModel(cn, _engine),

            _ => null
        };
    }
}
