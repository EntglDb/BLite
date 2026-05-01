using System;
using System.Security.Cryptography;
using System.Text;

namespace BLite.Core.Encryption;

/// <summary>
/// Key-derivation helpers used by <see cref="AesGcmCryptoProvider"/>.
/// </summary>
internal static class KeyDerivation
{
    // Output key length for AES-256: 32 bytes.
    private const int KeyLength = 32;

    /// <summary>
    /// Derives a 256-bit key from <paramref name="passphrase"/> and <paramref name="salt"/>
    /// using PBKDF2-HMAC-SHA256.
    /// </summary>
    /// <param name="passphrase">User-supplied secret.</param>
    /// <param name="salt">Random per-database salt (at least 16 bytes recommended).</param>
    /// <param name="iterations">Iteration count (e.g. 100 000).</param>
    /// <returns>A 32-byte derived key.</returns>
    public static byte[] DeriveKeyPbkdf2(string passphrase, ReadOnlySpan<byte> salt, int iterations)
    {
        var passphraseBytes = Encoding.UTF8.GetBytes(passphrase);
        try
        {
            return DeriveKeyPbkdf2(passphraseBytes, salt, iterations);
        }
        finally
        {
            // Zero the transient UTF8 copy so the secret does not linger on the GC heap.
            CryptographicOperations.ZeroMemory(passphraseBytes);
        }
    }

    /// <summary>
    /// Derives a 256-bit key from raw passphrase bytes (typically UTF-8) using PBKDF2-HMAC-SHA256.
    /// The caller is responsible for zeroing <paramref name="passphraseBytes"/> after the call.
    /// </summary>
    public static byte[] DeriveKeyPbkdf2(byte[] passphraseBytes, ReadOnlySpan<byte> salt, int iterations)
    {
        if (passphraseBytes is null) throw new ArgumentNullException(nameof(passphraseBytes));
        var saltArray = salt.ToArray();

#if NET6_0_OR_GREATER
        return Rfc2898DeriveBytes.Pbkdf2(passphraseBytes, saltArray, iterations, HashAlgorithmName.SHA256, KeyLength);
#else
        using var kdf = new Rfc2898DeriveBytes(passphraseBytes, saltArray, iterations, HashAlgorithmName.SHA256);
        return kdf.GetBytes(KeyLength);
#endif
    }

    /// <summary>
    /// HKDF extract-then-expand (RFC 5869) using HMAC-SHA256.
    /// Returns <paramref name="outputLength"/> bytes of pseudorandom key material.
    /// </summary>
    /// <param name="ikm">Input key material.</param>
    /// <param name="salt">Optional salt (use <see cref="ReadOnlySpan{T}.Empty"/> for no salt).</param>
    /// <param name="info">Context and application-specific information.</param>
    /// <param name="outputLength">Desired output length in bytes (max 255 * 32 for SHA-256).</param>
    /// <returns>Derived key material of the requested length.</returns>
    public static byte[] DeriveKeyHkdf(ReadOnlySpan<byte> ikm, ReadOnlySpan<byte> salt, ReadOnlySpan<byte> info, int outputLength)
    {
#if NET5_0_OR_GREATER
        var output = new byte[outputLength];
        HKDF.DeriveKey(HashAlgorithmName.SHA256, ikm, output, salt, info);
        return output;
#else
        // Manual HKDF (RFC 5869) for netstandard2.1
        // Step 1: Extract
        // RFC 5869 §2.2: if salt is not provided, use a string of HashLen zeros.
        // An all-zeros salt is not "no salt" — it is the RFC-mandated default that
        // preserves the HKDF security proof when the caller omits salt intentionally.
        var saltArray = salt.IsEmpty ? new byte[32] : salt.ToArray(); // default salt = 0x00 * HashLen
        byte[] prk;
        using (var hmac = new HMACSHA256(saltArray))
            prk = hmac.ComputeHash(ikm.ToArray());

        // Step 2: Expand
        var output = new byte[outputLength];
        var infoArray = info.ToArray();
        byte[] prev = Array.Empty<byte>();
        int generated = 0;
        byte counter = 1;
        while (generated < outputLength)
        {
            var chunk = new byte[prev.Length + infoArray.Length + 1];
            prev.CopyTo(chunk, 0);
            infoArray.CopyTo(chunk, prev.Length);
            chunk[chunk.Length - 1] = counter++;
            using var hmac = new HMACSHA256(prk);
            prev = hmac.ComputeHash(chunk);
            int toCopy = Math.Min(prev.Length, outputLength - generated);
            prev.AsSpan(0, toCopy).CopyTo(output.AsSpan(generated));
            generated += toCopy;
        }
        return output;
#endif
    }
}
