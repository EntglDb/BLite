// This partial class adds GDPR-inspection helpers to StorageEngine without modifying
// any file under src/BLite.Core/Storage/.  It accesses private members that are
// available to all partial-class parts within the same compilation unit.
namespace BLite.Core.Storage;

public sealed partial class StorageEngine
{
    /// <summary>
    /// <see langword="true"/> when the engine is configured with a non-null, non-NullCryptoProvider
    /// crypto provider — i.e. AES-GCM page encryption is active.
    /// </summary>
    internal bool IsEncryptionEnabled
        => _config.CryptoProvider != null &&
           _config.CryptoProvider is not Encryption.NullCryptoProvider;

    /// <summary>
    /// <see langword="true"/> when the engine is in multi-file (server) mode —
    /// each collection uses its own file.
    /// </summary>
    internal bool IsMultiFileMode => UsesSeparateCollectionFiles;

    /// <summary>
    /// The collection-data directory when the engine is in multi-file (server) mode,
    /// or <see langword="null"/> for single-file (embedded) mode.
    /// </summary>
    internal string? CollectionDataDirectory => _config.CollectionDataDirectory;
}
