using System.Threading;
using System.Threading.Tasks;

namespace BLite.Core.Encryption;

/// <summary>
/// Abstraction over the key-retrieval layer used by <see cref="BLite.Core.BLiteEngine"/>
/// migration helpers. Implementations may fetch the master key from a KMS, HSM, Azure Key Vault,
/// or any other secure store.
/// </summary>
/// <remarks>
/// <para>
/// The returned key must be exactly 32 bytes (256 bits) and is used as the input key
/// material for an <see cref="EncryptionCoordinator"/>.  The coordinator then derives a
/// unique per-file AES-256-GCM sub-key using HKDF-SHA256.
/// </para>
/// <para>
/// The caller is responsible for zeroing the returned byte array after use to minimise
/// the time sensitive key material resides in managed memory.
/// </para>
/// </remarks>
public interface IKeyProvider
{
    /// <summary>
    /// Returns the 32-byte master key for the database identified by
    /// <paramref name="databaseName"/>.
    /// </summary>
    /// <param name="databaseName">
    /// The name (not the full path) of the database file, without extension.
    /// Used to allow a single provider to manage keys for multiple databases.
    /// </param>
    /// <param name="ct">Cancellation token.</param>
    /// <returns>Exactly 32 bytes of key material.</returns>
    /// <exception cref="System.ArgumentException">
    /// The implementation should throw when the key is not 32 bytes.
    /// </exception>
    ValueTask<byte[]> GetKeyAsync(string databaseName, CancellationToken ct = default);
}
