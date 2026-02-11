using System;

namespace BLite.Core.Collections;

public readonly struct SchemaVersion
{
    public int Version { get; }
    public long Hash { get; }

    public SchemaVersion(int version, long hash)
    {
        Version = version;
        Hash = hash;
    }

    public override string ToString() => $"v{Version} (0x{Hash:X16})";
}
