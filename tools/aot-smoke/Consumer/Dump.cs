using System.Text;
using BLite.Bson;

namespace AotSmoke;

/// <summary>
/// The canonical text the Consumer writes: one section per feature, every line free of paths,
/// times and anything else that differs between two runs of the same build. LF line endings.
/// </summary>
internal sealed class Dump
{
    private readonly StringBuilder _sb = new();
    private readonly string _root;

    public Dump(string root) => _root = root;

    public void Section(string name) => _sb.Append("## ").Append(name).Append('\n');

    public void Line(string text) => _sb.Append(text.Replace(_root, "<tmp>")).Append('\n');

    public override string ToString() => _sb.ToString();

    public static string Describe(StoreDoc d)
        => $"{{id={d.Id} name={d.Name} count={d.Count} code={d.Code ?? "null"} payload={d.Payload.Length}{(d.Payload.Length > 0 ? "x" + d.Payload[0] : "")} fnv={Fnv(d.Payload):x8}}}";

    public static string Describe(BsonDocument d)
        => "{" + string.Join(" ", d.EnumerateFields().Select(f => $"{f.Name}={f.Value}")) + "}";

    public static string Hex(byte[]? bytes) => bytes == null ? "null" : Convert.ToHexString(bytes);

    public static uint Fnv(string s)
    {
        uint h = 2166136261;
        foreach (var c in s) { h ^= c; h *= 16777619; }
        return h;
    }
}
