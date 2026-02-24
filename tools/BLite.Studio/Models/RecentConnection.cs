using System;
using BLite.Bson;

namespace BLite.Studio.Models;

/// <summary>
/// Stored in the internal BLite studio database.
/// Represents a previously-opened database connection.
/// </summary>
public class RecentConnection
{
    /// <summary>Document key — auto-generated on insert.</summary>
    public ObjectId Id { get; set; }

    /// <summary>Full path to the database file on disk.</summary>
    public string FilePath { get; set; } = string.Empty;

    /// <summary>File name only, for display purposes.</summary>
    public string DisplayName { get; set; } = string.Empty;

    /// <summary>
    /// Stored preset ordinal:
    ///   0 = Small (8 KB / 512 KB)
    ///   1 = Default (16 KB / 1 MB)
    ///   2 = Large (32 KB / 2 MB)
    /// </summary>
    public int PresetValue { get; set; } = 1;

    /// <summary>True when the database was opened in read-only mode.</summary>
    public bool IsReadOnly { get; set; }

    /// <summary>UTC timestamp of the last successful open.</summary>
    public DateTimeOffset LastOpenedAt { get; set; } = DateTimeOffset.UtcNow;
}
