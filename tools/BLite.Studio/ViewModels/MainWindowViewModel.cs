using System;
using System.Collections.ObjectModel;
using System.IO;
using System.IO.MemoryMappedFiles;
using System.Reflection;
using System.Threading.Tasks;
using Avalonia.Platform;
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
        _ = LoadRecentConnectionsAsync();
    }

    // ── App info (version + license) ──────────────────────────────────────────
    public string AppVersion
    {
        get
        {
            var v = Assembly.GetEntryAssembly()?.GetName().Version;
            return v is null ? "dev" : $"{v.Major}.{v.Minor}.{v.Build}";
        }
    }

    private static string? _licenseText;
    public string AppLicense => _licenseText ??= LoadLicense();

    private static string LoadLicense()
    {
        try
        {
            using var stream = AssetLoader.Open(new Uri("avares://BLite.Studio/Assets/LICENSE.txt"));
            using var reader = new StreamReader(stream);
            return reader.ReadToEnd();
        }
        catch
        {
            return "MIT License — © 2026 BLite Project";
        }
    }

    // ── Recent connections ────────────────────────────────────────────────────
    public ObservableCollection<RecentConnectionViewModel> RecentConnections { get; } = [];

    [ObservableProperty]
    [NotifyPropertyChangedFor(nameof(HasRecentConnections))]
    private int _recentCount;

    public bool HasRecentConnections => RecentConnections.Count > 0;

    private async Task LoadRecentConnectionsAsync()
    {
        try
        {
            var items = await _history.GetRecentAsync();
            RecentConnections.Clear();
            foreach (var item in items)
                RecentConnections.Add(new RecentConnectionViewModel(item));

            RecentCount = RecentConnections.Count;   // triggers HasRecentConnections notify
        }
        catch (Exception ex)
        {
            StatusMessage = $"Failed to load recent connections: {ex.Message}";
        }
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

    public string AccessModeDisplay => IsReadOnly ? "Read-only" : "Read/Write";

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
    private PageFileConfig? _openedConfig;

    /// <summary>True when the open database uses a multi-file (server) layout.</summary>
    public bool IsMultiFile =>
        _openedConfig.HasValue &&
        (_openedConfig.Value.CollectionDataDirectory != null ||
         _openedConfig.Value.IndexFilePath           != null ||
         _openedConfig.Value.WalPath                 != null);

    public string LayoutDisplay => IsMultiFile ? "Multi-file" : "Single-file";

    // ── Explorer ──────────────────────────────────────────────────────────────
    [ObservableProperty]
    private DatabaseExplorerViewModel? _explorer;

    // ── Commands ──────────────────────────────────────────────────────────────
    [RelayCommand(CanExecute = nameof(CanOpen))]
    private async Task OpenDatabase()
    {
        await TryOpen(DatabasePath, (int)SelectedPreset, IsReadOnly);
    }

    private bool CanOpen() => !string.IsNullOrWhiteSpace(DatabasePath);

    [RelayCommand]
    private async Task OpenRecent(RecentConnectionViewModel recent)
    {
        // Pre-fill form fields with the stored settings
        DatabasePath   = recent.FilePath;
        SelectedPreset = (PagePreset)recent.PresetValue;
        IsReadOnly     = recent.IsReadOnly;
        IsReadWrite    = !recent.IsReadOnly;

        await TryOpen(recent.FilePath, recent.PresetValue, recent.IsReadOnly);
    }

    [RelayCommand]
    private async Task RemoveRecent(RecentConnectionViewModel recent)
    {
        await _history.Remove(recent.Id);
        RecentConnections.Remove(recent);
        RecentCount = RecentConnections.Count;
    }

    [RelayCommand]
    private void CloseDatabase()
    {
        _engine?.Dispose();
        _engine          = null;
        _openedConfig    = null;
        IsDatabaseOpen   = false;
        StatusMessage    = null;
        OpenDatabaseName = null;
        Explorer         = null;
        OnPropertyChanged(nameof(IsMultiFile));
        OnPropertyChanged(nameof(LayoutDisplay));
    }

    // ── Internal helpers ──────────────────────────────────────────────────────
    private async Task TryOpen(string path, int presetValue, bool readOnly)
    {
        try
        {
            _engine?.Dispose();

            // Auto-detect page size and layout for existing files; fall back to user preset for new ones.
            var detected = PageFileConfig.DetectFromFile(path);
            var config   = detected ?? CurrentConfig;
            if (detected.HasValue)
            {
                var access = IsReadOnly
                    ? MemoryMappedFileAccess.Read
                    : MemoryMappedFileAccess.ReadWrite;
                config = detected.Value with { Access = access };
            }

            _engine       = new BLiteEngine(path, config);
            _openedConfig = config;

            // Sync the preset selector to reflect the actual page size of the opened database.
            SelectedPreset = config.PageSize switch
            {
                8192  => PagePreset.Small,
                32768 => PagePreset.Large,
                _     => PagePreset.Default,
            };

            // Persist to history
            await _history.AddOrUpdate(path, (int)SelectedPreset, readOnly, IsMultiFile);
            await LoadRecentConnectionsAsync();

            OpenDatabaseName = Path.GetFileName(path);
            StatusMessage    = null;
            IsDatabaseOpen   = true;
            Explorer         = new DatabaseExplorerViewModel(_engine, path);
            OnPropertyChanged(nameof(IsMultiFile));
            OnPropertyChanged(nameof(LayoutDisplay));
        }
        catch (Exception ex)
        {
            StatusMessage  = $"Errore: {ex.Message}";
            IsDatabaseOpen = false;
        }
    }
}
