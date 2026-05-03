using BLite.SourceGenerators.Models;
using System.Text;

namespace BLite.SourceGenerators.Gdpr
{
    /// <summary>
    /// Emits the <c>PersonalDataFields</c> and <c>PersonalDataTimestampField</c> static members
    /// on the already-generated mapper class for an entity.
    /// Called from <c>CodeGenerator.GenerateMapperClass</c> after the main mapper body is produced.
    /// </summary>
    internal static class PersonalDataEmitter
    {
        /// <summary>
        /// Appends the two personal-data static members to <paramref name="sb"/>, which must be
        /// positioned inside the (partial) mapper class body, before the closing <c>}</c>.
        /// </summary>
        public static void EmitPersonalDataMembers(StringBuilder sb, EntityInfo entity, bool isRoot = false)
        {
            var personalDataProps = new System.Collections.Generic.List<PropertyInfo>();
            string? timestampField = null;

            foreach (var prop in entity.Properties)
            {
                if (!prop.IsPersonalData) continue;
                personalDataProps.Add(prop);
                if (prop.IsPersonalDataTimestamp)
                    timestampField = prop.Name;
            }

            sb.AppendLine();
            sb.AppendLine("    // ── GDPR personal-data metadata (WP1) ──");

            // CollectionNameStatic: emitted only for root entities so InspectDatabase
            // can find PersonalDataFields by collection name without instantiating the mapper.
            if (isRoot)
                sb.AppendLine($"    public static string CollectionNameStatic => \"{entity.CollectionName}\";");

            // PersonalDataFields
            if (personalDataProps.Count == 0)
            {
                sb.AppendLine("    public static global::System.Collections.Generic.IReadOnlyList<");
                sb.AppendLine("        global::BLite.Core.GDPR.PersonalDataField> PersonalDataFields { get; } =");
                sb.AppendLine("        global::System.Array.Empty<global::BLite.Core.GDPR.PersonalDataField>();");
            }
            else
            {
                sb.AppendLine("    public static global::System.Collections.Generic.IReadOnlyList<");
                sb.AppendLine("        global::BLite.Core.GDPR.PersonalDataField> PersonalDataFields { get; } = new[]");
                sb.AppendLine("    {");
                foreach (var p in personalDataProps)
                {
                    var sensitivity = SensitivityName(p.PersonalDataSensitivityValue);
                    var isTs = p.IsPersonalDataTimestamp ? "true" : "false";
                    sb.AppendLine(
                        $"        new global::BLite.Core.GDPR.PersonalDataField(" +
                        $"\"{p.Name}\", " +
                        $"global::BLite.Core.GDPR.DataSensitivity.{sensitivity}, " +
                        $"IsTimestamp: {isTs}),");
                }
                sb.AppendLine("    };");
            }

            // PersonalDataTimestampField
            if (timestampField is null)
                sb.AppendLine("    public static string? PersonalDataTimestampField { get; } = null;");
            else
                sb.AppendLine($"    public static string? PersonalDataTimestampField {{ get; }} = \"{timestampField}\";");
        }

        private static string SensitivityName(byte value) => value switch
        {
            1 => "Personal",
            2 => "Sensitive",
            3 => "Special",
            _ => "Personal",
        };
    }
}
