using System;
using System.Collections.ObjectModel;
using System.Linq;
using System.Text;
using BLite.Core.KeyValue;
using CommunityToolkit.Mvvm.ComponentModel;
using CommunityToolkit.Mvvm.Input;

namespace BLite.Studio.ViewModels.Explorer;

public sealed partial class KvStoreDetailViewModel : ObservableObject
{
    private readonly IBLiteKvStore _kv;

    public KvStoreDetailViewModel(IBLiteKvStore kv)
    {
        _kv = kv;
        DoRefresh();
    }

    // ── Entry list ────────────────────────────────────────────────────────────
    public ObservableCollection<KvEntryRowViewModel> Entries { get; } = [];

    [ObservableProperty]
    [NotifyPropertyChangedFor(nameof(EntryCountDisplay))]
    private int _entryCount;

    public string EntryCountDisplay => $"{EntryCount} entr{(EntryCount == 1 ? "y" : "ies")}";

    [ObservableProperty] private string _scanPrefix = "";

    // ── Viewer / insert panel ─────────────────────────────────────────────────
    [ObservableProperty]
    [NotifyPropertyChangedFor(nameof(IsPanelOpen))]
    [NotifyPropertyChangedFor(nameof(PanelTitle))]
    private KvEntryRowViewModel? _selectedEntry;

    [ObservableProperty]
    [NotifyPropertyChangedFor(nameof(IsPanelOpen))]
    [NotifyPropertyChangedFor(nameof(PanelTitle))]
    private bool _isInsertMode;

    public bool   IsPanelOpen => SelectedEntry is not null || IsInsertMode;
    public string PanelTitle  => IsInsertMode ? "New entry" : (SelectedEntry?.Key ?? "");

    [ObservableProperty] private string _viewerValue = "";
    [ObservableProperty] private bool   _viewerIsHex;

    // ── Insert form ───────────────────────────────────────────────────────────
    [ObservableProperty] private string _newKey      = "";
    [ObservableProperty] private string _newValue    = "";
    [ObservableProperty] private bool   _hasNewTtl;
    [ObservableProperty] private double _newTtlHours = 1;

    // ── Status ────────────────────────────────────────────────────────────────
    [ObservableProperty]
    [NotifyPropertyChangedFor(nameof(StatusIsOk))]
    private string _statusMessage = "";

    [ObservableProperty]
    [NotifyPropertyChangedFor(nameof(StatusIsOk))]
    private bool _statusIsError;

    public bool StatusIsOk => !StatusIsError && !string.IsNullOrEmpty(StatusMessage);

    // ── Commands ──────────────────────────────────────────────────────────────
    [RelayCommand]
    private void Refresh()
    {
        StatusMessage = "";
        StatusIsError = false;
        DoRefresh();
    }

    [RelayCommand]
    private void OpenEntry(KvEntryRowViewModel? row)
    {
        if (row is null) return;
        if (SelectedEntry == row) { ClosePanel(); return; }

        IsInsertMode  = false;
        StatusMessage = "";
        StatusIsError = false;
        SelectedEntry = row;

        var bytes = _kv.Get(row.Key);
        if (bytes is null)
        {
            ViewerValue = "(not found or expired)";
            ViewerIsHex = false;
            return;
        }

        try
        {
            ViewerValue = Encoding.UTF8.GetString(bytes);
            ViewerIsHex = false;
        }
        catch
        {
            ViewerValue = Convert.ToHexString(bytes);
            ViewerIsHex = true;
        }
    }

    [RelayCommand]
    private void NewEntry()
    {
        SelectedEntry = null;
        IsInsertMode  = true;
        StatusMessage = "";
        StatusIsError = false;
        NewKey        = "";
        NewValue      = "";
        HasNewTtl     = false;
        NewTtlHours   = 1;
    }

    [RelayCommand]
    private void ClosePanel()
    {
        SelectedEntry = null;
        IsInsertMode  = false;
        ViewerValue   = "";
        StatusMessage = "";
    }

    [RelayCommand]
    private void SaveEntry()
    {
        StatusMessage = "";
        StatusIsError = false;
        var key = NewKey.Trim();
        if (string.IsNullOrEmpty(key))
        {
            StatusMessage = "Key is required.";
            StatusIsError = true;
            return;
        }
        try
        {
            var bytes = Encoding.UTF8.GetBytes(NewValue);
            var ttl   = HasNewTtl ? (TimeSpan?)TimeSpan.FromHours(NewTtlHours) : null;
            _kv.Set(key, bytes, ttl);
            StatusMessage = $"Entry '{key}' saved.";
            ClosePanel();
            DoRefresh();
        }
        catch (Exception ex)
        {
            StatusMessage = ex.Message;
            StatusIsError = true;
        }
    }

    [RelayCommand]
    private void DeleteEntry(KvEntryRowViewModel? row)
    {
        if (row is null) return;
        StatusMessage = "";
        StatusIsError = false;
        try
        {
            var ok = _kv.Delete(row.Key);
            StatusMessage = ok ? $"Entry '{row.Key}' deleted." : $"Entry '{row.Key}' not found.";
            StatusIsError = !ok;
            if (SelectedEntry == row) ClosePanel();
            DoRefresh();
        }
        catch (Exception ex)
        {
            StatusMessage = ex.Message;
            StatusIsError = true;
        }
    }

    [RelayCommand]
    private void PurgeExpired()
    {
        StatusMessage = "";
        StatusIsError = false;
        try
        {
            var removed = _kv.PurgeExpired();
            DoRefresh();
            StatusMessage = $"Purged {removed} expired entr{(removed == 1 ? "y" : "ies")}.";
        }
        catch (Exception ex)
        {
            StatusMessage = ex.Message;
            StatusIsError = true;
        }
    }

    // ── Internal ──────────────────────────────────────────────────────────────
    private void DoRefresh()
    {
        Entries.Clear();
        var prefix = ScanPrefix?.Trim() ?? "";
        foreach (var key in _kv.ScanKeys(prefix).OrderBy(k => k))
        {
            var value = _kv.Get(key);
            Entries.Add(new KvEntryRowViewModel
            {
                Key          = key,
                SizeBytes    = value?.Length ?? 0,
                ValuePreview = BuildPreview(value)
            });
        }
        EntryCount = Entries.Count;
    }

    private static string BuildPreview(byte[]? bytes)
    {
        if (bytes is null || bytes.Length == 0) return "(empty)";
        try
        {
            var text = Encoding.UTF8.GetString(bytes);
            return text.Length > 64 ? text[..64] + "…" : text;
        }
        catch
        {
            var hex = Convert.ToHexString(bytes);
            return (hex.Length > 64 ? hex[..64] + "…" : hex) + " (binary)";
        }
    }
}
