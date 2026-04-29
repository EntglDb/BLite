using System;

namespace BLite.Core.Encryption;

/// <summary>
/// KDF algorithm used to derive the encryption key from the passphrase.
/// </summary>
public enum KdfAlgorithm : byte
{
    /// <summary>PBKDF2 with HMAC-SHA-256 (default, 100 000 iterations).</summary>
    Pbkdf2Sha256 = 1
}

/// <summary>
/// Configuration for the transparent encryption layer.
/// Pass an instance of this class to the <see cref="BLite.Core.BLiteEngine"/> constructor
/// to enable AES-256-GCM encryption at rest.
/// </summary>
public sealed class CryptoOptions
{
    /// <summary>
    /// Creates a new <see cref="CryptoOptions"/> with the supplied passphrase.
    /// </summary>
    /// <param name="passphrase">
    /// The user-supplied secret.  Must not be null or empty.
    /// The actual encryption key is derived from this value using PBKDF2-SHA256.
    /// </param>
    /// <param name="kdf">Key-derivation function (default: <see cref="KdfAlgorithm.Pbkdf2Sha256"/>).</param>
    /// <param name="iterations">
    /// Number of KDF iterations (default: 100 000).
    /// Higher values increase resistance to brute-force attacks at the cost of open time.
    /// </param>
    public CryptoOptions(string passphrase, KdfAlgorithm kdf = KdfAlgorithm.Pbkdf2Sha256, int iterations = 100_000)
    {
        if (string.IsNullOrEmpty(passphrase))
            throw new ArgumentNullException(nameof(passphrase));
        if (iterations < 1)
            throw new ArgumentOutOfRangeException(nameof(iterations), "Iterations must be at least 1.");

        Passphrase = passphrase;
        Kdf = kdf;
        Iterations = iterations;
    }

    /// <summary>The user-supplied passphrase.</summary>
    public string Passphrase { get; }

    /// <summary>Key-derivation function.</summary>
    public KdfAlgorithm Kdf { get; }

    /// <summary>Number of KDF iterations.</summary>
    public int Iterations { get; }
}
