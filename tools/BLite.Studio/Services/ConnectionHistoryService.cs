using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using BLite.Bson;
using BLite.Studio.Data;
using BLite.Studio.Models;

namespace BLite.Studio.Services;

/// <summary>
/// Manages the recent-connections list backed by the internal BLite studio database.
/// </summary>
public sealed class ConnectionHistoryService : IDisposable
{
    private readonly StudioDbContext _db;

    public ConnectionHistoryService()
    {
        _db = new StudioDbContext();
    }

    /// <summary>
    /// Returns up to <paramref name="max"/> most-recently-opened connections,
    /// sorted newest-first.
    /// </summary>
    public List<RecentConnection> GetRecent(int max = 10)
    {
        return _db.RecentConnections
            .AsQueryable()
            .OrderByDescending(r => r.LastOpenedAt)
            .Take(max)
            .ToList();
    }

    /// <summary>
    /// Adds a new connection entry, or updates the timestamp if the same file path
    /// already exists in history.
    /// </summary>
    public void AddOrUpdate(string filePath, int presetValue, bool isReadOnly)
    {
        // Look for an existing record for this file path
        var existing = _db.RecentConnections
            .AsQueryable()
            .FirstOrDefault(r => r.FilePath == filePath);

        if (existing is not null)
        {
            existing.PresetValue  = presetValue;
            existing.IsReadOnly   = isReadOnly;
            existing.LastOpenedAt = DateTimeOffset.UtcNow;
            _db.RecentConnections.Update(existing);
        }
        else
        {
            _db.RecentConnections.Insert(new RecentConnection
            {
                FilePath      = filePath,
                DisplayName   = Path.GetFileName(filePath),
                PresetValue   = presetValue,
                IsReadOnly    = isReadOnly,
                LastOpenedAt  = DateTimeOffset.UtcNow,
            });
        }
        _db.SaveChanges();
    }

    /// <summary>
    /// Removes a connection entry by its document ID.
    /// </summary>
    public void Remove(ObjectId id)
    {
        _db.RecentConnections.Delete(id);
        _db.SaveChanges();
    }

    public void Dispose() => _db.Dispose();
}
