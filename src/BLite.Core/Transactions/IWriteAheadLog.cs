using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace BLite.Core.Transactions;

/// <summary>
/// Abstraction over Write-Ahead Log implementations.
/// The default file-system implementation is <see cref="WriteAheadLog"/>.
/// Alternative implementations (e.g. <see cref="MemoryWriteAheadLog"/> for in-memory or
/// browser storage) can be provided to enable scenarios such as WASM and unit testing.
/// </summary>
public interface IWriteAheadLog : IDisposable
{
    /// <summary>Appends a BEGIN record for the given transaction.</summary>
    ValueTask WriteBeginRecordAsync(ulong transactionId, CancellationToken ct = default);

    /// <summary>Appends a COMMIT record for the given transaction.</summary>
    ValueTask WriteCommitRecordAsync(ulong transactionId, CancellationToken ct = default);

    /// <summary>Appends an ABORT record for the given transaction.</summary>
    ValueTask WriteAbortRecordAsync(ulong transactionId, CancellationToken ct = default);

    /// <summary>Appends a WRITE (data) record for the given transaction and page.</summary>
    ValueTask WriteDataRecordAsync(ulong transactionId, uint pageId, ReadOnlyMemory<byte> afterImage, CancellationToken ct = default);

    /// <summary>Flushes all pending WAL records to their durable destination.</summary>
    Task FlushAsync(CancellationToken ct = default);

    /// <summary>Returns the current byte size of the WAL (used to trigger checkpoint decisions).</summary>
    long GetCurrentSize();

    /// <summary>
    /// Truncates the WAL, discarding all records.
    /// Should only be called after a successful checkpoint has applied all committed writes.
    /// </summary>
    Task TruncateAsync(CancellationToken ct = default);

    /// <summary>
    /// Reads and returns all WAL records (used during crash recovery).
    /// Returns an empty list for implementations that do not persist records.
    /// </summary>
    List<WalRecord> ReadAll();
}
