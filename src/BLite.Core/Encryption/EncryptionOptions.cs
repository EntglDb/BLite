using System;

namespace BLite.Core.Encryption;

/// <summary>
/// Encryption algorithm used for data at rest.
/// </summary>
public enum EncryptionAlgorithm : byte
{
    /// <summary>AES-256-GCM authenticated encryption (default).</summary>
    AesGcm256 = 1
}

/// <summary>
/// Encryption configuration passed to <see cref="BLite.Core.BLiteEngineOptions"/>.
/// </summary>
/// <remarks>
/// <para>
/// Exactly one of <see cref="Passphrase"/> or <see cref="KeyProvider"/> must be set
/// when encryption is desired.  Both being set is a configuration error.
/// </para>
/// <para>
/// <b>Development use — <see cref="Passphrase"/>:</b><br/>
/// A passphrase is convenient for local development and testing.  The actual encryption
/// key is derived from the passphrase at open time using PBKDF2-SHA256 and a random
/// per-database salt stored in the file header.  A passphrase is <b>unsuitable for
/// production</b> because it cannot leverage centralized key management, audit trails,
/// or key rotation policies.
/// </para>
/// <para>
/// <b>Production use — <see cref="KeyProvider"/>:</b><br/>
/// Supply an <see cref="IKeyProvider"/> implementation to integrate with Azure Key Vault,
/// AWS KMS, an HSM, or any other external key management system.  The provider is called
/// once at open time; the returned 32-byte master key is passed to
/// <see cref="EncryptionCoordinator"/> which derives unique per-file sub-keys via
/// HKDF-SHA256.
/// </para>
/// </remarks>
public sealed class EncryptionOptions
{
    /// <summary>
    /// Passphrase for direct key derivation.
    /// <para>
    /// <b>Warning:</b> this option is convenient for development but unsuitable for
    /// production environments that require centralized key management and key rotation.
    /// Use <see cref="KeyProvider"/> for production deployments.
    /// </para>
    /// </summary>
    public string? Passphrase { get; init; }

    /// <summary>
    /// External key management provider for production environments.
    /// Called once at open time; must return a 32-byte master key.
    /// </summary>
    public IKeyProvider? KeyProvider { get; init; }

    /// <summary>
    /// Encryption algorithm. Defaults to <see cref="EncryptionAlgorithm.AesGcm256"/>.
    /// </summary>
    public EncryptionAlgorithm Algorithm { get; init; } = EncryptionAlgorithm.AesGcm256;

    /// <summary>
    /// Key-derivation function used when <see cref="Passphrase"/> is set.
    /// Defaults to <see cref="KdfAlgorithm.Pbkdf2Sha256"/>.
    /// </summary>
    public KdfAlgorithm Kdf { get; init; } = KdfAlgorithm.Pbkdf2Sha256;

    /// <summary>
    /// Number of PBKDF2 iterations when <see cref="Passphrase"/> is set.
    /// Higher values increase resistance to brute-force attacks at the cost of open time.
    /// Defaults to 100 000.
    /// </summary>
    public int KdfIterations { get; init; } = 100_000;
}
