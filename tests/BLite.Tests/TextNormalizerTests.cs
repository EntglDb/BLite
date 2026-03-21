using BLite.Bson;
using BLite.Core.Storage;
using BLite.Core.Text;

namespace BLite.Tests;

/// <summary>
/// Unit tests for <see cref="TextNormalizer"/> — deterministic text pipeline:
/// ASCII fold → lowercase → punctuation → whitespace collapse.
/// </summary>
public class TextNormalizerTests
{
    // ── Null / empty guard ────────────────────────────────────────────────────

    [Fact]
    public void Normalize_Null_ReturnsEmpty()
    {
        Assert.Equal(string.Empty, TextNormalizer.Normalize(null));
    }

    [Fact]
    public void Normalize_EmptyString_ReturnsEmpty()
    {
        Assert.Equal(string.Empty, TextNormalizer.Normalize(""));
    }

    [Fact]
    public void Normalize_WhitespaceOnly_ReturnsEmpty()
    {
        Assert.Equal(string.Empty, TextNormalizer.Normalize("   \t\n  "));
    }

    // ── Lowercasing ───────────────────────────────────────────────────────────

    [Fact]
    public void Normalize_UppercaseInput_ReturnsLowercase()
    {
        Assert.Equal("hello world", TextNormalizer.Normalize("HELLO WORLD"));
    }

    [Fact]
    public void Normalize_MixedCase_ReturnsLowercase()
    {
        Assert.Equal("blazor database", TextNormalizer.Normalize("Blazor Database"));
    }

    // ── ASCII folding (diacritics) ────────────────────────────────────────────

    [Fact]
    public void Normalize_AccentedChars_StripsDiacritics()
    {
        Assert.Equal("hello world", TextNormalizer.Normalize("Héllo Wörld"));
    }

    [Fact]
    public void Normalize_SpanishAccents_StripsDiacritics()
    {
        Assert.Equal("nino canon", TextNormalizer.Normalize("niño cañón"));
    }

    [Fact]
    public void Normalize_FrenchAccents_StripsDiacritics()
    {
        Assert.Equal("ecole ete", TextNormalizer.Normalize("école été"));
    }

    // ── Punctuation removal ───────────────────────────────────────────────────

    [Fact]
    public void Normalize_Comma_ReplaceWithSpace()
    {
        Assert.Equal("hello world", TextNormalizer.Normalize("hello, world"));
    }

    [Fact]
    public void Normalize_Punctuation_BecomesSpaces()
    {
        Assert.Equal("hello world", TextNormalizer.Normalize("Hello, World!"));
    }

    [Fact]
    public void Normalize_MultipleSpecialChars_CollapsedToSingleSpace()
    {
        Assert.Equal("a b", TextNormalizer.Normalize("a!@#$b"));
    }

    [Fact]
    public void Normalize_HyphenBetweenWords_BecomesSpace()
    {
        Assert.Equal("open source", TextNormalizer.Normalize("open-source"));
    }

    // ── Whitespace collapsing ─────────────────────────────────────────────────

    [Fact]
    public void Normalize_MultipleSpaces_CollapsesToSingle()
    {
        Assert.Equal("multiple spaces here", TextNormalizer.Normalize("multiple   spaces    here"));
    }

    [Fact]
    public void Normalize_LeadingTrailingSpaces_AreTrimmed()
    {
        Assert.Equal("trimmed", TextNormalizer.Normalize("  trimmed  "));
    }

    [Fact]
    public void Normalize_TabsAndNewlines_CollapsedToSpace()
    {
        Assert.Equal("a b c", TextNormalizer.Normalize("a\t\tb\nc"));
    }

    // ── Digits ───────────────────────────────────────────────────────────────

    [Fact]
    public void Normalize_DigitsPreserved()
    {
        Assert.Equal("version 42", TextNormalizer.Normalize("Version 42"));
    }

    [Fact]
    public void Normalize_NumbersOnly_ReturnsAsIs()
    {
        Assert.Equal("12345", TextNormalizer.Normalize("12345"));
    }

    // ── Combined pipeline ─────────────────────────────────────────────────────

    [Fact]
    public void Normalize_FullPipeline_AllStepsApplied()
    {
        // Accented + uppercase + punctuation + extra spaces
        var result = TextNormalizer.Normalize("  Héllo,  WÖRLD!  ");
        Assert.Equal("hello world", result);
    }

    [Fact]
    public void Normalize_IsDeterministic()
    {
        const string input = "Blazor  DB — fast & reliable!";
        var r1 = TextNormalizer.Normalize(input);
        var r2 = TextNormalizer.Normalize(input);
        Assert.Equal(r1, r2);
    }

    // ── BuildEmbeddingText ────────────────────────────────────────────────────

    [Fact]
    public void BuildEmbeddingText_NullDocument_ReturnsEmpty()
    {
        var config = new VectorSourceConfig();
        config.Fields.Add(new VectorSourceField { Path = "title" });
        Assert.Equal(string.Empty, TextNormalizer.BuildEmbeddingText(null!, config));
    }

    [Fact]
    public void BuildEmbeddingText_NullConfig_ReturnsEmpty()
    {
        var doc = new BsonDocument(new byte[] { 5, 0, 0, 0, 0 }); // empty BSON doc
        Assert.Equal(string.Empty, TextNormalizer.BuildEmbeddingText(doc, null!));
    }

    [Fact]
    public void BuildEmbeddingText_EmptyFields_ReturnsEmpty()
    {
        var doc = new BsonDocument(new byte[] { 5, 0, 0, 0, 0 });
        var config = new VectorSourceConfig(); // no fields added
        Assert.Equal(string.Empty, TextNormalizer.BuildEmbeddingText(doc, config));
    }

    [Fact]
    public void BuildEmbeddingText_MissingField_IsSkipped()
    {
        // Document with no fields
        var doc = new BsonDocument(new byte[] { 5, 0, 0, 0, 0 });
        var config = new VectorSourceConfig();
        config.Fields.Add(new VectorSourceField { Path = "title" });
        // Missing field → empty result
        Assert.Equal(string.Empty, TextNormalizer.BuildEmbeddingText(doc, config));
    }
}
