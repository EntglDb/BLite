using System;
using System.Threading;
using System.Threading.Tasks;

namespace BLite.Core.Storage;

/// <summary>
/// Abstraction over page-based storage backends.
/// The default file-system implementation is <see cref="PageFile"/>.
/// Alternative implementations (e.g. <see cref="MemoryPageStorage"/> for in-memory or
/// browser storage) can be provided to enable scenarios such as WASM, unit testing,
/// and ephemeral databases.
/// </summary>
public interface IPageStorage : IDisposable
{
    /// <summary>Page size in bytes. All pages are the same fixed size.</summary>
    int PageSize { get; }

    /// <summary>Total number of pages ever allocated (not all may currently be in use).</summary>
    uint NextPageId { get; }

    /// <summary>Opens (or initialises) the storage. Must be called once before any I/O.</summary>
    void Open();

    /// <summary>
    /// Reads a full page by its ID into <paramref name="destination"/>.
    /// <paramref name="destination"/> must be at least <see cref="PageSize"/> bytes.
    /// </summary>
    void ReadPage(uint pageId, Span<byte> destination);

    /// <summary>
    /// Reads up to <paramref name="destination"/>.Length bytes from the start of a page.
    /// Use this to read only the page header without copying the entire page payload.
    /// <paramref name="destination"/> must not exceed <see cref="PageSize"/> bytes.
    /// </summary>
    void ReadPageHeader(uint pageId, Span<byte> destination);

    /// <summary>Reads a full page asynchronously into <paramref name="destination"/>.</summary>
    ValueTask ReadPageAsync(uint pageId, Memory<byte> destination, CancellationToken cancellationToken = default);

    /// <summary>
    /// Writes a page at the given ID from <paramref name="source"/>.
    /// <paramref name="source"/> must be at least <see cref="PageSize"/> bytes.
    /// </summary>
    void WritePage(uint pageId, ReadOnlySpan<byte> source);

    /// <summary>Allocates a new page (reusing a free page if one is available) and returns its ID.</summary>
    uint AllocatePage();

    /// <summary>Returns a page to the free list so it can be reused by future allocations.</summary>
    void FreePage(uint pageId);

    /// <summary>Flushes all pending writes to their durable destination (synchronous).</summary>
    void Flush();

    /// <summary>Flushes all pending writes to their durable destination (asynchronous).</summary>
    Task FlushAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Creates a consistent backup of this storage to <paramref name="destinationPath"/>.
    /// Throws <see cref="NotSupportedException"/> for backends that do not support
    /// file-based backup (e.g. <see cref="MemoryPageStorage"/>).
    /// </summary>
    Task BackupAsync(string destinationPath, CancellationToken cancellationToken = default);

    /// <summary>
    /// Shrinks the storage file by removing trailing free pages after a VACUUM pass.
    /// Implementations must flush all pending writes, unmap (if memory-mapped), truncate
    /// the underlying storage, and remap before returning.
    /// <para>
    /// Throws <see cref="NotSupportedException"/> for in-memory backends that have no
    /// file to truncate (e.g. <see cref="MemoryPageStorage"/>).
    /// </para>
    /// </summary>
    Task TruncateToMinimumAsync(CancellationToken cancellationToken = default);
}
