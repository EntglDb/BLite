using System;
using BLite.Bson;
using BLite.Studio.Models;

namespace BLite.Studio.ViewModels;

/// <summary>
/// Display wrapper for a <see cref="RecentConnection"/> document.
/// </summary>
public sealed class RecentConnectionViewModel
{
    public RecentConnectionViewModel(RecentConnection model)
    {
        Model        = model;
        Id           = model.Id;
        FilePath     = model.FilePath;
        DisplayName  = model.DisplayName;
        PresetValue  = model.PresetValue;
        IsReadOnly   = model.IsReadOnly;
        IsMultiFile  = model.IsMultiFile;
        IsEncrypted  = model.IsEncrypted;
        LastOpenedAt = model.LastOpenedAt;
    }

    public RecentConnection Model { get; }

    public ObjectId Id          { get; }
    public string   FilePath    { get; }
    public string   DisplayName { get; }
    public int      PresetValue { get; }
    public bool     IsReadOnly  { get; }
    public bool     IsMultiFile { get; }
    public bool     IsEncrypted { get; }
    public DateTimeOffset LastOpenedAt { get; }

    public string PresetLabel => PresetValue switch
    {
        0 => "Small",
        2 => "Large",
        _ => "Default",
    };

    public string AccessLabel => IsReadOnly ? "Read-only" : "Read/Write";

    public string EncryptedLabel => IsEncrypted ? "Encrypted" : string.Empty;

    /// <summary>Short human-readable timestamp ("oggi", "ieri", "3 giorni fa…")</summary>
    public string RelativeTime
    {
        get
        {
            var diff = DateTimeOffset.UtcNow - LastOpenedAt;
            return diff.TotalMinutes < 2  ? "now"
                 : diff.TotalHours   < 1  ? $"{(int)diff.TotalMinutes} min ago"
                 : diff.TotalHours   < 24 ? $"{(int)diff.TotalHours} h ago"
                 : diff.TotalDays    < 2  ? "yesterday"
                 : diff.TotalDays    < 30 ? $"{(int)diff.TotalDays} days ago"
                 : LastOpenedAt.LocalDateTime.ToString("dd/MM/yyyy");
        }
    }
}
