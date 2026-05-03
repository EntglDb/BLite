using BLite.Core.GDPR;

namespace BLite.Tests.Gdpr;

/// <summary>
/// Unit tests for <see cref="PersonalDataAttribute"/>, <see cref="DataSensitivity"/>,
/// <see cref="PersonalDataField"/>, and <see cref="PersonalDataMetadataCache"/>.
/// </summary>
public class PersonalDataAttributeTests
{
    // ── DataSensitivity enum ──────────────────────────────────────────────────

    [Fact]
    public void DataSensitivity_HasThreeTiers()
    {
        Assert.Equal(1, (byte)DataSensitivity.Personal);
        Assert.Equal(2, (byte)DataSensitivity.Sensitive);
        Assert.Equal(3, (byte)DataSensitivity.Special);
    }

    // ── PersonalDataAttribute defaults ────────────────────────────────────────

    [Fact]
    public void PersonalDataAttribute_DefaultSensitivity_IsPersonal()
    {
        var attr = new PersonalDataAttribute();
        Assert.Equal(DataSensitivity.Personal, attr.Sensitivity);
    }

    [Fact]
    public void PersonalDataAttribute_DefaultIsTimestamp_IsFalse()
    {
        var attr = new PersonalDataAttribute();
        Assert.False(attr.IsTimestamp);
    }

    [Fact]
    public void PersonalDataAttribute_CanSetSensitivityToSpecial()
    {
        var attr = new PersonalDataAttribute { Sensitivity = DataSensitivity.Special };
        Assert.Equal(DataSensitivity.Special, attr.Sensitivity);
    }

    [Fact]
    public void PersonalDataAttribute_CanSetIsTimestampTrue()
    {
        var attr = new PersonalDataAttribute { IsTimestamp = true };
        Assert.True(attr.IsTimestamp);
    }

    // ── PersonalDataField record struct ────────────────────────────────────────

    [Fact]
    public void PersonalDataField_Equality_ByValue()
    {
        var f1 = new PersonalDataField("Email", DataSensitivity.Personal, false);
        var f2 = new PersonalDataField("Email", DataSensitivity.Personal, false);
        Assert.Equal(f1, f2);
    }

    [Fact]
    public void PersonalDataField_Inequality_WhenPropertyNameDiffers()
    {
        var f1 = new PersonalDataField("Email", DataSensitivity.Personal, false);
        var f2 = new PersonalDataField("Phone", DataSensitivity.Personal, false);
        Assert.NotEqual(f1, f2);
    }

    // ── PersonalDataMetadataCache reflection fallback ──────────────────────────

    [Fact]
    public void MetadataCache_ReturnsEmptyList_WhenNoPersonalDataAnnotations()
    {
        var fields = PersonalDataMetadataCache.Resolve(typeof(EntityWithNoAnnotations));
        Assert.NotNull(fields);
        Assert.Empty(fields);
    }

    [Fact]
    public void MetadataCache_ReturnsPersonalDataFields_WhenAnnotated()
    {
        var fields = PersonalDataMetadataCache.Resolve(typeof(AnnotatedEntity));
        Assert.NotNull(fields);
        Assert.Equal(2, fields.Count);
    }

    [Fact]
    public void MetadataCache_CorrectSensitivity_ForPersonalAndSpecial()
    {
        var fields = PersonalDataMetadataCache.Resolve(typeof(AnnotatedEntity));

        var emailField = fields.Single(f => f.PropertyName == "Email");
        Assert.Equal(DataSensitivity.Personal, emailField.Sensitivity);
        Assert.False(emailField.IsTimestamp);

        var religionField = fields.Single(f => f.PropertyName == "Religion");
        Assert.Equal(DataSensitivity.Special, religionField.Sensitivity);
        Assert.False(religionField.IsTimestamp);
    }

    [Fact]
    public void MetadataCache_IsTimestamp_DetectedCorrectly()
    {
        var fields = PersonalDataMetadataCache.Resolve(typeof(EntityWithTimestamp));
        var tsField = fields.Single(f => f.IsTimestamp);
        Assert.Equal("CreatedAt", tsField.PropertyName);
    }

    [Fact]
    public void MetadataCache_SensitiveFields_AreDistinct()
    {
        var fields = PersonalDataMetadataCache.Resolve(typeof(MixedSensitivityEntity));
        var personalCount  = fields.Count(f => f.Sensitivity == DataSensitivity.Personal);
        var sensitiveCount = fields.Count(f => f.Sensitivity == DataSensitivity.Sensitive);
        var specialCount   = fields.Count(f => f.Sensitivity == DataSensitivity.Special);
        Assert.Equal(1, personalCount);
        Assert.Equal(1, sensitiveCount);
        Assert.Equal(1, specialCount);
    }

    [Fact]
    public void MetadataCache_ResultsAreCached_SameInstance()
    {
        var first  = PersonalDataMetadataCache.Resolve(typeof(AnnotatedEntity));
        var second = PersonalDataMetadataCache.Resolve(typeof(AnnotatedEntity));
        Assert.Same(first, second);
    }

    [Fact]
    public void PersonalDataAttribute_IsInheritedOnSubclass()
    {
        var fields = PersonalDataMetadataCache.Resolve(typeof(DerivedAnnotatedEntity));
        // DerivedAnnotatedEntity inherits Email and Religion from AnnotatedEntity
        Assert.True(fields.Count >= 2, "Inherited [PersonalData] attributes should be discoverable.");
    }

    // ── Test model classes ────────────────────────────────────────────────────

    private sealed class EntityWithNoAnnotations
    {
        public int Id { get; set; }
        public string Name { get; set; } = "";
    }

    private class AnnotatedEntity
    {
        public int Id { get; set; }

        [PersonalData]
        public string Email { get; set; } = "";

        [PersonalData(Sensitivity = DataSensitivity.Special)]
        public string Religion { get; set; } = "";
    }

    private sealed class EntityWithTimestamp
    {
        public int Id { get; set; }

        [PersonalData(IsTimestamp = true)]
        public DateTime CreatedAt { get; set; }
    }

    private sealed class MixedSensitivityEntity
    {
        [PersonalData(Sensitivity = DataSensitivity.Personal)]
        public string Email { get; set; } = "";

        [PersonalData(Sensitivity = DataSensitivity.Sensitive)]
        public string Ssn { get; set; } = "";

        [PersonalData(Sensitivity = DataSensitivity.Special)]
        public string Religion { get; set; } = "";
    }

    private class DerivedAnnotatedEntity : AnnotatedEntity
    {
        public string Department { get; set; } = "";
    }
}
