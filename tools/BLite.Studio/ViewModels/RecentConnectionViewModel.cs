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
        Model       = model;
        Id          = model.Id;
        FilePath    = model.FilePath;
        DisplayName = model.DisplayName;
        PresetValue = model.PresetValue;
        IsReadOnly  = model.IsReadOnly;
        LastOpenedAt = model.LastOpenedAt;
    }

    public RecentConnection Model { get; }

    public ObjectId Id          { get; }
    public string   FilePath    { get; }
    public string   DisplayName { get; }
    public int      PresetValue { get; }
    public bool     IsReadOnly  { get; }
    public DateTimeOffset LastOpenedAt { get; }

    public string PresetLabel => PresetValue switch
    {
        0 => "Small",
        2 => "Large",
        _ => "Default",
    };

    public string AccessLabel => IsReadOnly ? "Solo lettura" : "R/W";

    /// <summary>Short human-readable timestamp ("oggi", "ieri", "3 giorni fa…")</summary>
    public string RelativeTime
    {
        get
        {
            var diff = DateTimeOffset.UtcNow - LastOpenedAt;
            return diff.TotalMinutes < 2  ? "adesso"
                 : diff.TotalHours   < 1  ? $"{(int)diff.TotalMinutes} min fa"
                 : diff.TotalHours   < 24 ? $"{(int)diff.TotalHours} h fa"
                 : diff.TotalDays    < 2  ? "ieri"
                 : diff.TotalDays    < 30 ? $"{(int)diff.TotalDays} giorni fa"
                 : LastOpenedAt.LocalDateTime.ToString("dd/MM/yyyy");
        }
    }
}
