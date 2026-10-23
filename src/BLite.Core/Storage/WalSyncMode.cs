namespace BLite.Core.Storage;

/// <summary>
/// When the write-ahead log is synced to disk (<see cref="PageFileConfig.WalSync"/>), the trade-off
/// between commit latency and what survives a power loss or an OS crash. A crash of the process
/// alone loses nothing in either mode: the OS keeps what the WAL handed it.
/// </summary>
public enum WalSyncMode
{
    /// <summary>
    /// The default. Every commit (one per group-commit batch) syncs the WAL before it is
    /// acknowledged: an acknowledged commit survives a power loss. Like SQLite's
    /// <c>synchronous=FULL</c>.
    /// </summary>
    Full = 0,

    /// <summary>
    /// A commit is acknowledged once the WAL has been handed to the OS. The WAL is synced only
    /// before a checkpoint copies committed pages to the data file, before pages a commit stopped
    /// using are freed, and when it is truncated. A power loss can lose the most recent
    /// acknowledged commits, never more, and leaves a committed state. Like SQLite's
    /// <c>synchronous=NORMAL</c> in WAL mode.
    /// </summary>
    Normal = 1,
}
