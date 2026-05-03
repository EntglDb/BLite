using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using BLite.Bson;
using BLite.Core.CDC;

namespace BLite.Core.GDPR;

/// <summary>
/// Pure BSON projection helpers for CDC payload masking.
/// All methods return a <em>new</em> <see cref="BsonDocument"/>; the source document is
/// never mutated.
/// </summary>
internal static class PayloadMask
{
    /// <summary>
    /// Returns a new document containing only the fields whose names are in
    /// <paramref name="keep"/>. Field-name comparison is case-insensitive.
    /// </summary>
    public static BsonDocument Allowlist(
        BsonDocument doc,
        IReadOnlyList<string> keep,
        IReadOnlyDictionary<string, ushort> keyMap,
        ConcurrentDictionary<ushort, string> reverseKeyMap)
    {
        var keepSet = new HashSet<string>(keep, StringComparer.OrdinalIgnoreCase);
        var builder = new BsonDocumentBuilder(keyMap, reverseKeyMap);
        foreach (var (name, value) in doc.EnumerateFields())
        {
            if (keepSet.Contains(name))
                builder.Add(name, value);
        }
        return builder.Build();
    }

    /// <summary>
    /// Returns a new document with the fields whose names are in
    /// <paramref name="remove"/> stripped out. Field-name comparison is case-insensitive.
    /// </summary>
    public static BsonDocument Blocklist(
        BsonDocument doc,
        IReadOnlyList<string> remove,
        IReadOnlyDictionary<string, ushort> keyMap,
        ConcurrentDictionary<ushort, string> reverseKeyMap)
    {
        var removeSet = new HashSet<string>(remove, StringComparer.OrdinalIgnoreCase);
        var builder = new BsonDocumentBuilder(keyMap, reverseKeyMap);
        foreach (var (name, value) in doc.EnumerateFields())
        {
            if (!removeSet.Contains(name))
                builder.Add(name, value);
        }
        return builder.Build();
    }

    /// <summary>
    /// Returns a new document where each personal-data key is replaced by
    /// <paramref name="maskValue"/>, or removed entirely when
    /// <paramref name="maskValue"/>.IsNull is <see langword="true"/>.
    /// Field-name resolution maps <see cref="PersonalDataField.PropertyName"/> to the
    /// BSON field name via <c>ToLowerInvariant()</c> (the BLite convention for BSON keys).
    /// </summary>
    public static BsonDocument MaskPersonalData(
        BsonDocument doc,
        IReadOnlyList<PersonalDataField> fields,
        BsonValue maskValue,
        IReadOnlyDictionary<string, ushort> keyMap,
        ConcurrentDictionary<ushort, string> reverseKeyMap)
    {
        // Build a case-insensitive set of BSON field names to mask.
        var maskKeys = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        foreach (var f in fields)
            maskKeys.Add(f.PropertyName.ToLowerInvariant());

        var builder = new BsonDocumentBuilder(keyMap, reverseKeyMap);
        foreach (var (name, value) in doc.EnumerateFields())
        {
            if (maskKeys.Contains(name))
            {
                // Drop the key when mask value is null; otherwise replace.
                if (!maskValue.IsNull)
                    builder.Add(name, maskValue);
            }
            else
            {
                builder.Add(name, value);
            }
        }
        return builder.Build();
    }

    /// <summary>
    /// Applies the full four-step masking pipeline defined in WP2:
    /// <list type="number">
    ///   <item>If <see cref="WatchOptions.IncludeOnlyFields"/> is non-null → allowlist (rules 2–3 skipped).</item>
    ///   <item>If <see cref="WatchOptions.RevealPersonalData"/> is <see langword="false"/> and
    ///         <paramref name="personalDataFields"/> is non-empty → mask personal-data keys.</item>
    ///   <item>If <see cref="WatchOptions.ExcludeFields"/> is non-empty → blocklist.</item>
    ///   <item>Return the (possibly unchanged) document.</item>
    /// </list>
    /// Returns a new <see cref="BsonDocument"/>; the source is never mutated.
    /// </summary>
    public static BsonDocument Apply(
        BsonDocument doc,
        WatchOptions options,
        IReadOnlyList<PersonalDataField> personalDataFields,
        IReadOnlyDictionary<string, ushort> keyMap,
        ConcurrentDictionary<ushort, string> reverseKeyMap)
    {
        // Rule 1: IncludeOnlyFields allowlist wins — skip all other rules.
        if (options.IncludeOnlyFields != null)
            return Allowlist(doc, options.IncludeOnlyFields, keyMap, reverseKeyMap);

        var result = doc;

        // Rule 2: Mask personal-data fields (unless RevealPersonalData is true).
        if (!options.RevealPersonalData && personalDataFields.Count > 0)
            result = MaskPersonalData(result, personalDataFields, options.PersonalDataMaskValue, keyMap, reverseKeyMap);

        // Rule 3: Remove excluded fields.
        if (options.ExcludeFields.Count > 0)
            result = Blocklist(result, options.ExcludeFields, keyMap, reverseKeyMap);

        // Rule 4: Deliver.
        return result;
    }
}
