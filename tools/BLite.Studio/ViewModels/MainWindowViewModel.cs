using System;
using System.Collections.ObjectModel;
using System.IO;
using System.IO.MemoryMappedFiles;
using BLite.Core;
using BLite.Core.Storage;
using BLite.Studio.Services;
using CommunityToolkit.Mvvm.ComponentModel;
using CommunityToolkit.Mvvm.Input;

namespace BLite.Studio.ViewModels;

public enum PagePreset { Small, Default, Large }

public partial class MainWindowViewModel : ViewModelBase
{
    private readonly ConnectionHistoryService _history;

    public MainWindowViewModel()
    {
        _history = new ConnectionHistoryService();
        LoadRecentConnections();
    }

    // ── Recent connections ────────────────────────────────────────────────────
    public ObservableCollection<RecentConnectionViewModel> RecentConnections { get; } = [];

    [ObservableProperty]
    [NotifyPropertyChangedFor(nameof(HasRecentConnections))]
    private int _recentCount;

    public bool HasRecentConnections => RecentConnections.Count > 0;

    private void LoadRecentConnections()
    {
        RecentConnections.Clear();
        foreach (var item in _history.GetRecent())
            RecentConnections.Add(new RecentConnectionViewModel(item));

        RecentCount = RecentConnections.Count;   // triggers HasRecentConnections notify
    }

    // ── Database path ──────────────────────────────────────────────────────────
    [ObservableProperty]
    [NotifyCanExecuteChangedFor(nameof(OpenDatabaseCommand))]
    private string _databasePath = string.Empty;

    // ── Page config preset ────────────────────────────────────────────────────
    [ObservableProperty]
    private PagePreset _selectedPreset = PagePreset.Default;

    partial void OnSelectedPresetChanged(PagePreset value)
    {
        OnPropertyChanged(nameof(IsPresetSmall));
        OnPropertyChanged(nameof(IsPresetDefault));
        OnPropertyChanged(nameof(IsPresetLarge));
        OnPropertyChanged(nameof(PageSizeDisplay));
        OnPropertyChanged(nameof(GrowthBlockDisplay));
    }

    public bool IsPresetSmall
    {
        get => SelectedPreset == PagePreset.Small;
        set { if (value) SelectedPreset = PagePreset.Small; }
    }

    public bool IsPresetDefault
    {
        get => SelectedPreset == PagePreset.Default;
        set { if (value) SelectedPreset = PagePreset.Default; }
    }

    public bool IsPresetLarge
    {
        get => SelectedPreset == PagePreset.Large;
        set { if (value) SelectedPreset = PagePreset.Large; }
    }

    // ── Access mode ───────────────────────────────────────────────────────────
    [ObservableProperty]
    private bool _isReadWrite = true;

    [ObservableProperty]
    private bool _isReadOnly;

    partial void OnIsReadWriteChanged(bool value)
    {
        if (value) IsReadOnly = false;
        OnPropertyChanged(nameof(AccessModeDisplay));
    }

    partial void OnIsReadOnlyChanged(bool value)
    {
        if (value) IsReadWrite = false;
        OnPropertyChanged(nameof(AccessModeDisplay));
    }

    public string AccessModeDisplay => IsReadOnly ? "Solo lettura" : "Lettura/Scrittura";

    // ── Computed display helpers ──────────────────────────────────────────────
    public string PageSizeDisplay => SelectedPreset switch
    {
        PagePreset.Small  => "8 KB",
        PagePreset.Large  => "32 KB",
        _                 => "16 KB",
    };

    public string GrowthBlockDisplay => SelectedPreset switch
    {
        PagePreset.Small  => "512 KB",
        PagePreset.Large  => "2 MB",
        _                 => "1 MB",
    };

    public PageFileConfig CurrentConfig
    {
        get
        {
            var access = IsReadOnly
                ? MemoryMappedFileAccess.Read
                : MemoryMappedFileAccess.ReadWrite;

            return SelectedPreset switch
            {
                PagePreset.Small  => PageFileConfig.Small  with { Access = access },
                PagePreset.Large  => PageFileConfig.Large  with { Access = access },
                _                 => PageFileConfig.Default with { Access = access },
            };
        }
    }

    // ── Open state ────────────────────────────────────────────────────────────
    [ObservableProperty]
    private bool _isDatabaseOpen;

    [ObservableProperty]
    private string? _statusMessage;

    [ObservableProperty]
    private string? _openDatabaseName;

    private BLiteEngine? _engine;

    // ── Explorer ──────────────────────────────────────────────────────────────
    [ObservableProperty]
    private DatabaseExplorerViewModel? _explorer;

    // ── Commands ──────────────────────────────────────────────────────────────
    [RelayCommand(CanExecute = nameof(CanOpen))]
    private void OpenDatabase()
    {
        TryOpen(DatabasePath, (int)SelectedPreset, IsReadOnly);
    }

    private bool CanOpen() => !string.IsNullOrWhiteSpace(DatabasePath);

    [RelayCommand]
    private void OpenRecent(RecentConnectionViewModel recent)
    {
        // Pre-fill form fields with the stored settings
        DatabasePath   = recent.FilePath;
        SelectedPreset = (PagePreset)recent.PresetValue;
        IsReadOnly     = recent.IsReadOnly;
        IsReadWrite    = !recent.IsReadOnly;

        TryOpen(recent.FilePath, recent.PresetValue, recent.IsReadOnly);
    }

    [RelayCommand]
    private void RemoveRecent(RecentConnectionViewModel recent)
    {
        _history.Remove(recent.Id);
        RecentConnections.Remove(recent);
        RecentCount = RecentConnections.Count;
    }

    [RelayCommand]
    private void CloseDatabase()
    {
        _engine?.Dispose();
        _engine          = null;
        IsDatabaseOpen   = false;
        StatusMessage    = null;
        OpenDatabaseName = null;
        Explorer         = null;
    }

    // ── Internal helpers ──────────────────────────────────────────────────────
    private void TryOpen(string path, int presetValue, bool readOnly)
    {
        try
        {
            _engine?.Dispose();
            _engine = new BLiteEngine(path, CurrentConfig);

            // Persist to history
            _history.AddOrUpdate(path, presetValue, readOnly);
            LoadRecentConnections();

            OpenDatabaseName = Path.GetFileName(path);
            StatusMessage    = null;
            IsDatabaseOpen   = true;
            Explorer         = new DatabaseExplorerViewModel(_engine, path);
        }
        catch (Exception ex)
        {
            StatusMessage  = $"Errore: {ex.Message}";
            IsDatabaseOpen = false;
        }
    }
}
