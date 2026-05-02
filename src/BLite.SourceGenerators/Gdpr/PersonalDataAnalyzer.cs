using BLite.SourceGenerators.Helpers;
using BLite.SourceGenerators.Models;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp.Syntax;

namespace BLite.SourceGenerators.Gdpr
{
    /// <summary>
    /// Augments <c>EntityAnalyzer</c> to harvest <c>[PersonalData]</c> attribute declarations
    /// and <c>HasPersonalData(…)</c> fluent model-builder calls.
    /// Results are stored on <see cref="PropertyInfo.IsPersonalData"/> /
    /// <see cref="PropertyInfo.PersonalDataSensitivityValue"/> /
    /// <see cref="PropertyInfo.IsPersonalDataTimestamp"/>.
    /// </summary>
    internal static class PersonalDataAnalyzer
    {
        // ── Attribute path ────────────────────────────────────────────────────

        /// <summary>
        /// Checks whether <paramref name="prop"/> is annotated with <c>[PersonalData]</c>
        /// and, if so, fills the personal-data fields on <paramref name="propInfo"/>.
        /// Called from <c>EntityAnalyzer.AnalyzeProperties</c> immediately after each property is created.
        /// </summary>
        public static void HarvestFromProperty(IPropertySymbol prop, PropertyInfo propInfo)
        {
            var attr = AttributeHelper.GetAttribute(prop, BLiteConventions.PersonalDataAttribute);
            if (attr == null) return;

            propInfo.IsPersonalData = true;

            // Sensitivity: named arg "Sensitivity" (init property)
            // DataSensitivity enum: Personal=1, Sensitive=2, Special=3
            foreach (var namedArg in attr.NamedArguments)
            {
                if (namedArg.Key == "Sensitivity" && namedArg.Value.Value is byte sensitivityByte)
                {
                    propInfo.PersonalDataSensitivityValue = sensitivityByte;
                }
                else if (namedArg.Key == "Sensitivity" && namedArg.Value.Value is int sensitivityInt)
                {
                    propInfo.PersonalDataSensitivityValue = (byte)sensitivityInt;
                }
                else if (namedArg.Key == "IsTimestamp" && namedArg.Value.Value is bool isTs)
                {
                    propInfo.IsPersonalDataTimestamp = isTs;
                }
            }
        }

        // ── Fluent path (HasPersonalData in OnModelCreating) ──────────────────

        /// <summary>
        /// Scans the <c>OnModelCreating</c> method body for
        /// <c>.Property(x =&gt; x.Prop).HasPersonalData(…)</c> calls and marks the
        /// matching <see cref="PropertyInfo"/> entries in <paramref name="entities"/>.
        /// </summary>
        public static void HarvestFromModelBuilder(
            MethodDeclarationSyntax onModelCreating,
            System.Collections.Generic.IEnumerable<EntityInfo> entities,
            SemanticModel semanticModel)
        {
            var calls = SyntaxHelper.FindMethodInvocations(onModelCreating, BLiteConventions.HasPersonalDataMethodName);

            foreach (var call in calls)
            {
                // Resolve sensitivity and isTimestamp from arguments
                byte sensitivity = 1; // Personal
                bool isTimestamp = false;

                for (int i = 0; i < call.ArgumentList.Arguments.Count; i++)
                {
                    var arg = call.ArgumentList.Arguments[i];

                    // Named argument "sensitivity:" or first positional
                    bool isSensitivityArg = arg.NameColon?.Name?.Identifier.Text is "sensitivity" or "Sensitivity"
                        || (i == 0 && arg.NameColon == null);
                    bool isTimestampArg = arg.NameColon?.Name?.Identifier.Text is "isTimestamp" or "IsTimestamp"
                        || (i == 1 && arg.NameColon == null);

                    if (isSensitivityArg)
                    {
                        var cv = semanticModel.GetConstantValue(arg.Expression);
                        if (cv.HasValue && cv.Value is int s)
                            sensitivity = (byte)s;
                        else if (cv.HasValue && cv.Value is byte sb)
                            sensitivity = sb;
                    }

                    if (isTimestampArg)
                    {
                        var cv = semanticModel.GetConstantValue(arg.Expression);
                        if (cv.HasValue && cv.Value is bool b)
                            isTimestamp = b;
                    }
                }

                // Walk up the fluent chain: .HasPersonalData() → .Property(lambda) → .Entity<T>()
                // Chain shape: modelBuilder.Entity<T>()...Property(x => x.Prop).HasPersonalData(...)
                var propertyCall = (call.Expression as MemberAccessExpressionSyntax)?.Expression as InvocationExpressionSyntax;
                int chainDepth = 0;
                while (propertyCall != null && chainDepth < BLiteConventions.MaxFluentChainDepth)
                {
                    if (propertyCall.Expression is MemberAccessExpressionSyntax { Name: { Identifier: { Text: BLiteConventions.PropertyMethodName } } })
                        break;
                    propertyCall = (propertyCall.Expression as MemberAccessExpressionSyntax)?.Expression as InvocationExpressionSyntax;
                    chainDepth++;
                }
                if (propertyCall == null) continue;

                // Extract property name from the lambda argument of .Property(x => x.Prop)
                if (propertyCall.ArgumentList.Arguments.Count == 0) continue;
                var lambdaExpr = propertyCall.ArgumentList.Arguments[0].Expression;
                var propertyPath = SyntaxHelper.GetPropertyName(lambdaExpr);
                if (string.IsNullOrEmpty(propertyPath)) continue;

                // Walk up from .Property(...) to find .Entity<T>()
                var entityCall = (propertyCall.Expression as MemberAccessExpressionSyntax)?.Expression as InvocationExpressionSyntax;
                int entityDepth = 0;
                while (entityCall != null && entityDepth < BLiteConventions.MaxFluentChainDepth)
                {
                    if (entityCall.Expression is MemberAccessExpressionSyntax { Name: GenericNameSyntax { Identifier: { Text: BLiteConventions.EntityMethodName } } })
                        break;
                    entityCall = (entityCall.Expression as MemberAccessExpressionSyntax)?.Expression as InvocationExpressionSyntax;
                    entityDepth++;
                }
                if (entityCall == null) continue;

                // Resolve the entity type
                INamedTypeSymbol? entityType = null;
                if (entityCall.Expression is MemberAccessExpressionSyntax entityMember &&
                    entityMember.Name is GenericNameSyntax entityGenericName &&
                    entityGenericName.TypeArgumentList.Arguments.Count > 0)
                {
                    var typeArgSyntax = entityGenericName.TypeArgumentList.Arguments[0];
                    entityType = semanticModel.GetSymbolInfo(typeArgSyntax).Symbol as INamedTypeSymbol;
                }
                if (entityType == null) continue;

                var entityFullName = SyntaxHelper.GetFullName(entityType);
                foreach (var entity in entities)
                {
                    if (entity.FullTypeName != entityFullName) continue;
                    foreach (var prop in entity.Properties)
                    {
                        if (string.Equals(prop.Name, propertyPath, System.StringComparison.Ordinal))
                        {
                            prop.IsPersonalData = true;
                            prop.PersonalDataSensitivityValue = sensitivity;
                            prop.IsPersonalDataTimestamp = isTimestamp;
                        }
                    }
                }
            }
        }
    }
}
