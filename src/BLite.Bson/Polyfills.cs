// BLite.Bson — Compiler polyfills for netstandard2.1
// Provides BCL types required by C# 9+ / 10+ / 11+ language features
// (init, record, record struct, required) that are not built into netstandard2.1.

#if NETSTANDARD2_1

namespace System.Runtime.CompilerServices
{
    // Required by init-only setters (C# 9) and record types.
    internal static class IsExternalInit { }

    // Required by the `required` keyword on members (C# 11).
    [AttributeUsage(AttributeTargets.Class | AttributeTargets.Struct | AttributeTargets.Field | AttributeTargets.Property, AllowMultiple = false, Inherited = false)]
    internal sealed class RequiredMemberAttribute : Attribute { }

    // Required by the compiler when the `required` keyword or ref-fields are used (C# 11+).
    [AttributeUsage(AttributeTargets.All, AllowMultiple = true, Inherited = false)]
    internal sealed class CompilerFeatureRequiredAttribute : Attribute
    {
        public CompilerFeatureRequiredAttribute(string featureName) => FeatureName = featureName;
        public string FeatureName { get; }
        public const string RefStructs = nameof(RefStructs);
        public const string RequiredMembers = nameof(RequiredMembers);
    }
}

namespace System.Diagnostics.CodeAnalysis
{
    // Required on constructors that initialise all `required` members (C# 11).
    [AttributeUsage(AttributeTargets.Constructor, AllowMultiple = false, Inherited = false)]
    internal sealed class SetsRequiredMembersAttribute : Attribute { }
}

#endif
