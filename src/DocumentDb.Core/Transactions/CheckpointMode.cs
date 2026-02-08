namespace DocumentDb.Core.Transactions;

/// <summary>
/// Defines checkpoint modes for WAL (Write-Ahead Log) checkpointing.
/// Similar to SQLite's checkpoint strategies.
/// </summary>
public enum CheckpointMode
{
    /// <summary>
    /// Passive checkpoint: Non-blocking, best-effort transfer from WAL to database.
    /// Does not wait for readers or writers. May not checkpoint all frames.
    /// </summary>
    Passive = 0,
    
    /// <summary>
    /// Full checkpoint: Waits for concurrent readers/writers, then checkpoints all
    /// committed transactions from WAL to database. Blocks until complete.
    /// </summary>
    Full = 1,
    
    /// <summary>
    /// Truncate checkpoint: Same as Full, but also truncates the WAL file after
    /// successful checkpoint. Use this to reclaim disk space.
    /// </summary>
    Truncate = 2,
    
    /// <summary>
    /// Restart checkpoint: Truncates WAL and restarts with a new WAL file.
    /// Forces a fresh start. Most aggressive mode.
    /// </summary>
    Restart = 3
}
