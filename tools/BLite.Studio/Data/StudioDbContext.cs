using System;
using System.IO;
using BLite.Bson;
using BLite.Core;
using BLite.Core.Collections;
using BLite.Core.Metadata;
using BLite.Studio.Models;

namespace BLite.Studio.Data;

/// <summary>
/// Internal BLite database used by BLite.Studio to persist connection history.
/// Stored in: %APPDATA%\BLite.Studio\studio.db
/// </summary>
public partial class StudioDbContext : DocumentDbContext
{
    /// <summary>Persistent path for the internal studio database.</summary>
    public static string AppDbPath
    {
        get
        {
            var dir = Path.Combine(
                Environment.GetFolderPath(Environment.SpecialFolder.ApplicationData),
                "BLite.Studio");
            Directory.CreateDirectory(dir);
            return Path.Combine(dir, "studio.db");
        }
    }

    public DocumentCollection<ObjectId, RecentConnection> RecentConnections { get; set; } = null!;

    public StudioDbContext() : base(AppDbPath) { }

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<RecentConnection>()
            .ToCollection("recent_connections")
            .HasIndex(r => r.LastOpenedAt);
    }
}
