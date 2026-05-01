using System;

namespace BLite.Core.Encryption;

/// <summary>
/// Abstraction over the per-page encryption layer used by <see cref="BLite.Core.Storage.PageFile"/>.
/// <para>
/// The default no-op implementation is <see cref="NullCryptoProvider"/> (zero overhead).
/// The production implementation is <see cref="AesGcmCryptoProvider"/> (AES-256-GCM).
/// </para>
/// <para>
/// All state mutations (e.g. key material loaded from the file header) occur in
/// <see cref="LoadFromFileHeader"/> or <see cref="GetFileHeader"/>. After that point the
/// instance is effectively immutable and its <see cref="Encrypt"/> / <see cref="Decrypt"/>
/// methods may be called from multiple threads concurrently.
/// </para>
/// </summary>
public interface ICryptoProvider
{
    /// <summary>
    /// Number of additional bytes appended to each page on disk beyond the logical page
    /// size (e.g. 16 for the AES-GCM authentication tag, 0 for <see cref="NullCryptoProvider"/>).
    /// </summary>
    int PageOverhead { get; }

    /// <summary>
    /// Number of bytes written at the very start of each encrypted file as a per-file
    /// crypto header (0 for <see cref="NullCryptoProvider"/>, 64 for AES-GCM).
    /// </summary>
    int FileHeaderSize { get; }

    /// <summary>
    /// Encrypts a single in-memory page into a temporary on-disk buffer.
    /// </summary>
    /// <param name="pageId">Page ID (passed for context; implementations must not use it as a nonce).</param>
    /// <param name="plaintext">
    /// The in-memory page buffer. Must be exactly <c>LogicalPageSize</c> bytes.
    /// This span must <b>not</b> be modified by the implementation.
    /// </param>
    /// <param name="ciphertext">
    /// Output buffer. Must be exactly <c>LogicalPageSize + PageOverhead</c> bytes.
    /// </param>
    void Encrypt(uint pageId, ReadOnlySpan<byte> plaintext, Span<byte> ciphertext);

    /// <summary>
    /// Decrypts a single on-disk page buffer into the in-memory page destination.
    /// </summary>
    /// <param name="pageId">Page ID (passed for context; must match the value used during <see cref="Encrypt"/>).</param>
    /// <param name="ciphertext">
    /// The on-disk page buffer. Must be exactly <c>LogicalPageSize + PageOverhead</c> bytes.
    /// </param>
    /// <param name="plaintext">
    /// Output buffer. Must be exactly <c>LogicalPageSize</c> bytes.
    /// </param>
    void Decrypt(uint pageId, ReadOnlySpan<byte> ciphertext, Span<byte> plaintext);

    /// <summary>
    /// Writes the per-file crypto header into <paramref name="header"/>.
    /// Called once when a <b>new</b> encrypted file is created; generates fresh key
    /// material (e.g. random salt) and derives the encryption key.
    /// </summary>
    /// <param name="header">
    /// Output buffer. Must be exactly <see cref="FileHeaderSize"/> bytes.
    /// </param>
    void GetFileHeader(Span<byte> header);

    /// <summary>
    /// Parses the per-file crypto header and derives the encryption key.
    /// Called once when an <b>existing</b> encrypted file is opened.
    /// </summary>
    /// <param name="header">
    /// The raw bytes read from the start of the file.
    /// Must be exactly <see cref="FileHeaderSize"/> bytes.
    /// </param>
    void LoadFromFileHeader(ReadOnlySpan<byte> header);

    /// <summary>
    /// Creates a sibling provider for a different physical file that belongs to the
    /// same logical database, using the same underlying key material.
    /// </summary>
    /// <remarks>
    /// <para>
    /// File roles: 0 = main database, 1 = collection, 2 = index, 3 = WAL.
    /// </para>
    /// <para>
    /// For <see cref="AesGcmCryptoProvider"/> (PBKDF2 mode) the sibling derives a fresh
    /// key from the same passphrase and a new per-file random salt, guaranteeing that
    /// different files never share a key even when they contain pages with the same ID.
    /// </para>
    /// <para>
    /// For coordinator-managed providers (HKDF mode) the sibling is derived from the
    /// same master key via HKDF-SHA256 with a file-role-specific context label.
    /// </para>
    /// </remarks>
    /// <param name="fileRole">Role of the sibling file (0–3).</param>
    /// <param name="fileIndex">0-based index within that role (e.g. collection slot).</param>
    ICryptoProvider CreateSiblingProvider(byte fileRole, ushort fileIndex);
}
