using System.Linq.Expressions;
using BLite.Bson;
using BLite.Core.Query;
using BLite.Shared;

namespace BLite.Tests;

/// <summary>
/// Unit tests for <see cref="ProjectionAnalyzer"/>, <see cref="ProjectionAnalysis"/>,
/// and <see cref="FieldAccess"/> (all internal, accessible via InternalsVisibleTo).
///
/// Covers: ToBsonName, IsSupportedScalarType, and Analyze lambda analysis.
/// </summary>
public class ProjectionAnalyzerTests
{
    #region ToBsonName

    [Fact]
    public void ToBsonName_Id_Returns_Underscored_Id()
    {
        Assert.Equal("_id", ProjectionAnalyzer.ToBsonName("Id"));
    }

    [Fact]
    public void ToBsonName_OtherName_ReturnsLowercase()
    {
        Assert.Equal("name", ProjectionAnalyzer.ToBsonName("Name"));
    }

    [Fact]
    public void ToBsonName_CamelCase_ReturnsAllLower()
    {
        Assert.Equal("userid", ProjectionAnalyzer.ToBsonName("UserId"));
    }

    [Fact]
    public void ToBsonName_AlreadyLower_Unchanged()
    {
        Assert.Equal("email", ProjectionAnalyzer.ToBsonName("email"));
    }

    [Fact]
    public void ToBsonName_Id_CaseSensitive_OnlyExactMatchMapsToUnderscore()
    {
        // "id" (lowercase) is not "Id" → should NOT map to "_id"
        Assert.NotEqual("_id", ProjectionAnalyzer.ToBsonName("id"));
    }

    #endregion

    #region IsSupportedScalarType

    [Theory]
    [InlineData(typeof(int))]
    [InlineData(typeof(long))]
    [InlineData(typeof(double))]
    [InlineData(typeof(float))]
    [InlineData(typeof(bool))]
    [InlineData(typeof(byte))]
    [InlineData(typeof(char))]
    [InlineData(typeof(short))]
    [InlineData(typeof(uint))]
    [InlineData(typeof(ulong))]
    public void IsSupportedScalarType_PrimitiveTypes_ReturnsTrue(Type type)
    {
        Assert.True(ProjectionAnalyzer.IsSupportedScalarType(type));
    }

    [Theory]
    [InlineData(typeof(string))]
    [InlineData(typeof(decimal))]
    [InlineData(typeof(DateTime))]
    [InlineData(typeof(DateTimeOffset))]
    [InlineData(typeof(Guid))]
    [InlineData(typeof(TimeSpan))]
    public void IsSupportedScalarType_KnownScalarTypes_ReturnsTrue(Type type)
    {
        Assert.True(ProjectionAnalyzer.IsSupportedScalarType(type));
    }

    [Fact]
    public void IsSupportedScalarType_Enum_ReturnsTrue()
    {
        Assert.True(ProjectionAnalyzer.IsSupportedScalarType(typeof(DayOfWeek)));
    }

    [Fact]
    public void IsSupportedScalarType_NullableInt_ReturnsTrue()
    {
        Assert.True(ProjectionAnalyzer.IsSupportedScalarType(typeof(int?)));
    }

    [Fact]
    public void IsSupportedScalarType_NullableDecimal_ReturnsTrue()
    {
        Assert.True(ProjectionAnalyzer.IsSupportedScalarType(typeof(decimal?)));
    }

    [Fact]
    public void IsSupportedScalarType_NullableEnum_ReturnsTrue()
    {
        Assert.True(ProjectionAnalyzer.IsSupportedScalarType(typeof(DayOfWeek?)));
    }

    [Theory]
    [InlineData(typeof(object))]
    [InlineData(typeof(Address))]
    [InlineData(typeof(User))]
    public void IsSupportedScalarType_ComplexType_ReturnsFalse(Type type)
    {
        Assert.False(ProjectionAnalyzer.IsSupportedScalarType(type));
    }

    [Fact]
    public void IsSupportedScalarType_ListOfInt_ReturnsFalse()
    {
        Assert.False(ProjectionAnalyzer.IsSupportedScalarType(typeof(List<int>)));
    }

    [Fact]
    public void IsSupportedScalarType_ArrayOfString_ReturnsFalse()
    {
        Assert.False(ProjectionAnalyzer.IsSupportedScalarType(typeof(string[])));
    }

    #endregion

    #region Analyze — simple projections

    [Fact]
    public void Analyze_SingleStringProperty_IsSimple_OneField()
    {
        Expression<Func<User, string>> lambda = x => x.Name;
        var result = ProjectionAnalyzer.Analyze(lambda);

        Assert.True(result.IsSimple);
        Assert.Single(result.Fields);
        Assert.Equal("Name", result.Fields[0].PropertyName);
        Assert.Equal("name", result.Fields[0].BsonName);
        Assert.Equal(typeof(string), result.Fields[0].ClrType);
        Assert.Equal(0, result.Fields[0].Index);
    }

    [Fact]
    public void Analyze_IntProperty_IsSimple_OneField()
    {
        Expression<Func<User, int>> lambda = x => x.Age;
        var result = ProjectionAnalyzer.Analyze(lambda);

        Assert.True(result.IsSimple);
        Assert.Single(result.Fields);
        Assert.Equal("Age", result.Fields[0].PropertyName);
        Assert.Equal("age", result.Fields[0].BsonName);
    }

    [Fact]
    public void Analyze_IdProperty_BsonNameIs_UnderscoredId()
    {
        Expression<Func<User, ObjectId>> lambda = x => x.Id;
        var result = ProjectionAnalyzer.Analyze(lambda);

        Assert.True(result.IsSimple);
        Assert.Single(result.Fields);
        Assert.Equal("_id", result.Fields[0].BsonName);
    }

    [Fact]
    public void Analyze_TwoScalarProperties_IsSimple_TwoFields()
    {
        // Build expression tree manually: x => new ValueTuple<string,int>(x.Name, x.Age)
        var param = System.Linq.Expressions.Expression.Parameter(typeof(User), "x");
        var nameMember = System.Linq.Expressions.Expression.Property(param, nameof(User.Name));
        var ageMember  = System.Linq.Expressions.Expression.Property(param, nameof(User.Age));
        var ctor = typeof(ValueTuple<string, int>).GetConstructor(new[] { typeof(string), typeof(int) })!;
        var body = System.Linq.Expressions.Expression.New(ctor, nameMember, ageMember);
        var lambda = System.Linq.Expressions.Expression.Lambda(body, param);

        var result = ProjectionAnalyzer.Analyze(lambda);

        Assert.True(result.IsSimple);
        Assert.Equal(2, result.Fields.Length);
    }

    [Fact]
    public void Analyze_TwoProperties_FieldsHaveSequentialIndexes()
    {
        var param = System.Linq.Expressions.Expression.Parameter(typeof(User), "x");
        var nameMember = System.Linq.Expressions.Expression.Property(param, nameof(User.Name));
        var ageMember  = System.Linq.Expressions.Expression.Property(param, nameof(User.Age));
        var ctor = typeof(ValueTuple<string, int>).GetConstructor(new[] { typeof(string), typeof(int) })!;
        var body = System.Linq.Expressions.Expression.New(ctor, nameMember, ageMember);
        var lambda = System.Linq.Expressions.Expression.Lambda(body, param);

        var result = ProjectionAnalyzer.Analyze(lambda);

        Assert.Equal(0, result.Fields[0].Index);
        Assert.Equal(1, result.Fields[1].Index);
    }

    [Fact]
    public void Analyze_DuplicatePropertyAccess_Deduplicated()
    {
        // Same property used twice → only one entry
        var param = System.Linq.Expressions.Expression.Parameter(typeof(User), "x");
        var nameMember1 = System.Linq.Expressions.Expression.Property(param, nameof(User.Name));
        var nameMember2 = System.Linq.Expressions.Expression.Property(param, nameof(User.Name));
        var ctor = typeof(ValueTuple<string, string>).GetConstructor(new[] { typeof(string), typeof(string) })!;
        var body = System.Linq.Expressions.Expression.New(ctor, nameMember1, nameMember2);
        var lambda = System.Linq.Expressions.Expression.Lambda(body, param);

        var result = ProjectionAnalyzer.Analyze(lambda);

        Assert.True(result.IsSimple);
        Assert.Single(result.Fields); // deduplicated
    }

    #endregion

    #region Analyze — non-simple projections

    [Fact]
    public void Analyze_NestedPath_IsSimpleFalse()
    {
        Expression<Func<ComplexUser, string>> lambda = x => x.MainAddress.Street;
        var result = ProjectionAnalyzer.Analyze(lambda);

        Assert.False(result.IsSimple);
        Assert.Empty(result.Fields);
    }

    [Fact]
    public void Analyze_MethodCallOnProperty_IsSimpleFalse()
    {
        Expression<Func<User, string>> lambda = x => x.Name.ToUpper();
        var result = ProjectionAnalyzer.Analyze(lambda);

        Assert.False(result.IsSimple);
    }

    [Fact]
    public void Analyze_ComplexPropertyType_IsSimpleFalse()
    {
        // Address is not a supported scalar type → not simple
        Expression<Func<ComplexUser, Address>> lambda = x => x.MainAddress;
        var result = ProjectionAnalyzer.Analyze(lambda);

        Assert.False(result.IsSimple);
    }

    [Fact]
    public void Analyze_MultiParameterLambda_IsSimpleFalse()
    {
        // Lambda with >1 parameters → Analyze returns IsSimple=false immediately
        Expression<Func<User, User, string>> lambda = (x, y) => x.Name;
        var result = ProjectionAnalyzer.Analyze(lambda);

        Assert.False(result.IsSimple);
        Assert.Empty(result.Fields);
    }

    #endregion

    #region Analyze — FieldAccess structure

    [Fact]
    public void Analyze_FieldAccess_PropertyName_MatchesCSharpName()
    {
        Expression<Func<User, string>> lambda = x => x.Name;
        var result = ProjectionAnalyzer.Analyze(lambda);

        Assert.Equal("Name", result.Fields[0].PropertyName); // C# PascalCase
    }

    [Fact]
    public void Analyze_FieldAccess_BsonName_IsLowercase()
    {
        Expression<Func<User, int>> lambda = x => x.Age;
        var result = ProjectionAnalyzer.Analyze(lambda);

        Assert.Equal("age", result.Fields[0].BsonName); // always lower
    }

    [Fact]
    public void Analyze_FieldAccess_ClrType_Matches()
    {
        Expression<Func<User, string>> lambda = x => x.Name;
        var result = ProjectionAnalyzer.Analyze(lambda);

        Assert.Equal(typeof(string), result.Fields[0].ClrType);
    }

    #endregion
}
