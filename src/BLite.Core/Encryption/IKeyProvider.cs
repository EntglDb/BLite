using System;
using System.Threading;
using System.Threading.Tasks;

namespace BLite.Core.Encryption;

/// <summary>
/// Abstraction for external key management systems (Azure Key Vault, AWS KMS, HSM, etc.).
/// Implement this interface to integrate BLite encryption with your organisation's
/// centralized key management and key rotation policies.
/// </summary>
/// <remarks>
/// <para>
/// <see cref="GetKeyAsync"/> is called exactly once per <see cref="BLite.Core.BLiteEngine"/>
/// open, not on every page read or write.  The returned key must be exactly 32 bytes
/// (256 bits) for AES-256-GCM.
/// </para>
/// <para>
/// BLite does not implement its own key storage or key rotation.  Key rotation is a
/// separate concern and must be coordinated at the application level.
/// </para>
/// </remarks>
public interface IKeyProvider
{
    /// <summary>
    /// Returns the 32-byte (256-bit) master key for the specified database.
    /// </summary>
    /// <param name="databaseName">
    /// The logical database name (file name without extension), provided so that a single
    /// provider instance can manage keys for multiple databases.
    /// </param>
    /// <param name="ct">Cancellation token.</param>
    /// <returns>
    /// A <see cref="ReadOnlyMemory{T}"/> containing exactly 32 bytes of key material.
    /// The memory must remain valid until the <see cref="BLite.Core.BLiteEngine"/> has finished
    /// opening the database (i.e. until the awaited <c>GetKeyAsync</c> call returns and
    /// the engine constructor has completed its initialization).
    /// </returns>
    ValueTask<ReadOnlyMemory<byte>> GetKeyAsync(string databaseName, CancellationToken ct);

    /// <summary>
    /// Notifies the provider that key rotation has been requested for the specified database.
    /// </summary>
    /// <remarks>
    /// This method is a hook for the provider to update its internal state (e.g. increment
    /// a key version, invalidate cached material).  BLite itself does not perform the
    /// rotation — the host application is responsible for the actual re-encryption.
    /// </remarks>
    /// <param name="databaseName">The logical database name.</param>
    /// <param name="ct">Cancellation token.</param>
    ValueTask NotifyKeyRotationAsync(string databaseName, CancellationToken ct);
}
