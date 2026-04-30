using System;

namespace BLite.Core.Encryption;

/// <summary>
/// No-op <see cref="ICryptoProvider"/> implementation that stores data in plaintext.
/// Used as the default when encryption is not configured, ensuring zero overhead.
/// <para>
/// <see cref="PageOverhead"/> is 0 and <see cref="FileHeaderSize"/> is 0, so
/// <see cref="BLite.Core.Storage.PageFile"/> uses exactly the same file layout as in
/// versions before encryption was introduced — every existing test passes unchanged.
/// </para>
/// </summary>
public sealed class NullCryptoProvider : ICryptoProvider
{
    /// <summary>Shared singleton instance.</summary>
    public static readonly NullCryptoProvider Instance = new();

    /// <inheritdoc/>
    public int PageOverhead => 0;

    /// <inheritdoc/>
    public int FileHeaderSize => 0;

    /// <inheritdoc/>
    /// <remarks>Copies <paramref name="plaintext"/> to <paramref name="ciphertext"/> unchanged.</remarks>
    public void Encrypt(uint pageId, ReadOnlySpan<byte> plaintext, Span<byte> ciphertext)
        => plaintext.CopyTo(ciphertext);

    /// <inheritdoc/>
    /// <remarks>Copies <paramref name="ciphertext"/> to <paramref name="plaintext"/> unchanged.</remarks>
    public void Decrypt(uint pageId, ReadOnlySpan<byte> ciphertext, Span<byte> plaintext)
        => ciphertext.CopyTo(plaintext);

    /// <inheritdoc/>
    /// <remarks>No-op: <see cref="FileHeaderSize"/> is 0 so there is nothing to write.</remarks>
    public void GetFileHeader(Span<byte> header) { }

    /// <inheritdoc/>
    /// <remarks>No-op: <see cref="FileHeaderSize"/> is 0 so there is nothing to read.</remarks>
    public void LoadFromFileHeader(ReadOnlySpan<byte> header) { }
}
