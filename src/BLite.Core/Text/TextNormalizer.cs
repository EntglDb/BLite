// BLite.Core — TextNormalizer
// Copyright (C) 2026 Luca Fabbri — AGPL-3.0
//
// Pre-processes raw document text before feeding it to a vector embedding model.
// Pipeline:
//   1. ASCII Folding   — NFD decomposition + strip combining diacritics (è→e, ö→o, ñ→n…)
//   2. Lowercasing     — locale-invariant ToLowerInvariant
//   3. Punctuation     — all non-alphanumeric, non-whitespace chars become spaces
//   4. Collapse        — multiple spaces → single space, trim
//
// The resulting string is safe to pass to any BERT-family tokenizer.

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using BLite.Bson;
using BLite.Core.Storage;

namespace BLite.Core.Text;

/// <summary>
/// Deterministic, language-agnostic text normalizer for vector embedding preparation.
/// All operations are pure and thread-safe (no shared mutable state).
/// </summary>
public static class TextNormalizer
{
    private static readonly Regex _whitespace = new(@"\s+", RegexOptions.Compiled);

    // ── Public API ─────────────────────────────────────────────────────────────

    /// <summary>
    /// Normalizes a string through the four-step pipeline:
    /// ASCII fold → lowercase → punctuation removal → whitespace collapse.
    /// Returns <see cref="string.Empty"/> for null or whitespace-only input.
    /// </summary>
    public static string Normalize(string? input)
    {
        if (string.IsNullOrWhiteSpace(input))
            return string.Empty;

        // NFD decomposes accented chars into base char + combining diacritic codepoints.
        var nfd = input.Normalize(NormalizationForm.FormD);

        var sb = new StringBuilder(nfd.Length);
        foreach (var c in nfd)
        {
            // Step 1: drop combining diacritics (ASCII fold)
            if (CharUnicodeInfo.GetUnicodeCategory(c) == UnicodeCategory.NonSpacingMark)
                continue;

            // Step 2 + 3: lowercase letters/digits pass through; everything else → space
            if (char.IsLetter(c))
                sb.Append(char.ToLowerInvariant(c));
            else if (char.IsDigit(c))
                sb.Append(c);
            else
                sb.Append(' '); // whitespace, punctuation, symbols all become a space
        }

        // Step 4: collapse multiple spaces, trim
        return _whitespace.Replace(sb.ToString(), " ").Trim();
    }

    /// <summary>
    /// Builds embedding-ready text from a BsonDocument using the VectorSource configuration.
    /// Each configured field value is normalised and emitted as <c>fieldpath:normalizedvalue</c>.
    /// Fields that produce empty text after normalisation are skipped.
    /// </summary>
    /// <returns>
    /// A comma-joined string such as <c>title:blazor database,content:fast nosql engine</c>,
    /// or <see cref="string.Empty"/> if no field yields non-empty text.
    /// </returns>
    public static string BuildEmbeddingText(BsonDocument document, VectorSourceConfig config)
    {
        if (document == null || config == null || config.Fields.Count == 0)
            return string.Empty;

        var parts = new List<string>(config.Fields.Count);
        foreach (var field in config.Fields)
        {
            if (!document.TryGetValue(field.Path, out var value))
                continue;

            var raw = BsonValueToString(value);
            if (string.IsNullOrWhiteSpace(raw))
                continue;

            var normalized = Normalize(raw);
            if (normalized.Length == 0)
                continue;

            parts.Add($"{field.Path}:{normalized}");
        }

        return string.Join(",", parts);
    }

    // ── Internals ──────────────────────────────────────────────────────────────

    private static string BsonValueToString(BsonValue value) => value.Type switch
    {
        BsonType.String   => value.AsString,
        BsonType.Int32    => value.AsInt32.ToString(),
        BsonType.Int64    => value.AsInt64.ToString(),
        BsonType.Double   => value.AsDouble.ToString("G"),
        BsonType.Boolean  => value.AsBoolean ? "true" : "false",
        BsonType.Array    => string.Join(" ", value.AsArray.Select(BsonValueToString)),
        BsonType.ObjectId => value.AsObjectId.ToString(),
        BsonType.DateTime => value.AsDateTime.ToString("O"),
        _                 => string.Empty
    };
}
