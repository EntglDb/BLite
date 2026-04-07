using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using BLite.SourceGenerators.Models;
using BLite.SourceGenerators.Helpers;

namespace BLite.SourceGenerators
{
    public static class CodeGenerator
    {
        public static string GenerateMapperClass(EntityInfo entity, string mapperNamespace)
        {
            var sb = new StringBuilder();
            var mapperName = GetMapperName(entity.FullTypeName);
            var keyProp = entity.Properties.FirstOrDefault(p => p.IsKey);
            // Nested type mappers are never treated as root entities, even if they have a key property.
            // Their Id field is serialized using BsonFieldName (e.g. "id") rather than "_id".
            var isRoot = entity.IdProperty != null && !entity.IsNestedTypeMapper;
            
            // Class Declaration
            if (isRoot)
            {
                var baseClass = GetBaseMapperClass(keyProp, entity);
                // Ensure FullTypeName has global:: prefix if not already present (assuming FullTypeName is fully qualified)
                var entityType = $"global::{entity.FullTypeName}";
                sb.AppendLine($"    public class {mapperName} : global::BLite.Core.Collections.{baseClass}{entityType}>");
            }
            else
            {
                sb.AppendLine($"    public class {mapperName}");
            }
            
            sb.AppendLine($"    {{");
            
            // Converter instance for the primary key
            if (keyProp?.ConverterTypeName != null)
            {
                sb.AppendLine($"        private readonly global::{keyProp.ConverterTypeName} _idConverter = new();");
                sb.AppendLine();
            }

            // Converter instances for non-key properties that have a detected converter
            var nonKeyConverterProps = entity.Properties
                .Where(p => !p.IsKey && p.ConverterTypeName != null)
                .ToList();
            foreach (var prop in nonKeyConverterProps)
            {
                sb.AppendLine($"        private readonly global::{prop.ConverterTypeName} _converter_{prop.Name} = new();");
            }
            if (nonKeyConverterProps.Any())
            {
                sb.AppendLine();
            }

            // Generate accessors for private/init-only property setters.
            // NET8+: [UnsafeAccessor(UnsafeAccessorKind.Method)] — AOT-safe, trimmer-safe, zero-overhead, no Expression.Compile().
            // netstandard2.1 fallback: MethodInfo.Invoke via FindSetter — reflection, not AOT-safe but functional.
            // DeclaringTypeName is used as the UnsafeAccessor parameter type so inherited setters resolve correctly.
            var privateSetterProps = entity.Properties.Where(p => (!p.HasPublicSetter && p.HasAnySetter) || p.HasInitOnlySetter).ToList();
            if (privateSetterProps.Any())
            {
                sb.AppendLine($"        // Private/init-only property setter accessors.");
                sb.AppendLine($"        // NET8+: [UnsafeAccessor] — AOT-safe, no reflection.");
                sb.AppendLine($"        // netstandard2.1: MethodInfo.Invoke fallback — see AOT_LIMITATIONS.md.");
                foreach (var prop in privateSetterProps)
                {
                    var declaringType = $"global::{(string.IsNullOrEmpty(prop.DeclaringTypeName) ? entity.FullTypeName : prop.DeclaringTypeName)}";
                    var propType = QualifyType(prop.TypeName);
                    sb.AppendLine($"#if NET8_0_OR_GREATER");
                    sb.AppendLine($"        [global::System.Runtime.CompilerServices.UnsafeAccessor(");
                    sb.AppendLine($"            global::System.Runtime.CompilerServices.UnsafeAccessorKind.Method, Name = \"set_{prop.Name}\")]");
                    sb.AppendLine($"        private static extern void __UnsafeSetter_{prop.Name}({declaringType} obj, {propType} value);");
                    sb.AppendLine($"#else");
                    sb.AppendLine($"#pragma warning disable IL2075, IL2026, IL2111");
                    sb.AppendLine($"        private static readonly global::System.Reflection.MethodInfo? _mi_{prop.Name} = FindSetter<{declaringType}>(\"{prop.Name}\");");
                    sb.AppendLine($"#pragma warning restore IL2075, IL2026, IL2111");
                    sb.AppendLine($"#endif");
                }
                sb.AppendLine();

                // Emit FindSetter only for netstandard2.1 (NET8+ uses UnsafeAccessor)
                sb.AppendLine($"#if !NET8_0_OR_GREATER");
                sb.AppendLine($"#pragma warning disable IL2075, IL2026, IL2111");
                sb.AppendLine($"        [global::System.Diagnostics.CodeAnalysis.RequiresUnreferencedCode(\"MethodInfo lookup for private/init-only setters requires type metadata to be preserved.\")]");
                sb.AppendLine($"        private static global::System.Reflection.MethodInfo? FindSetter<T>(string propertyName)");
                sb.AppendLine($"        {{");
                sb.AppendLine($"            global::System.Type? t = typeof(T);");
                sb.AppendLine($"            while (t != null)");
                sb.AppendLine($"            {{");
                sb.AppendLine($"                var pi = t.GetProperty(propertyName,");
                sb.AppendLine($"                    global::System.Reflection.BindingFlags.DeclaredOnly |");
                sb.AppendLine($"                    global::System.Reflection.BindingFlags.Instance |");
                sb.AppendLine($"                    global::System.Reflection.BindingFlags.Public |");
                sb.AppendLine($"                    global::System.Reflection.BindingFlags.NonPublic);");
                sb.AppendLine($"                var setter = pi?.GetSetMethod(nonPublic: true);");
                sb.AppendLine($"                if (setter != null) return setter;");
                sb.AppendLine($"                t = t.BaseType;");
                sb.AppendLine($"            }}");
                sb.AppendLine($"            return null;");
                sb.AppendLine($"        }}");
                sb.AppendLine($"#pragma warning restore IL2075, IL2026, IL2111");
                sb.AppendLine($"#endif");
                sb.AppendLine();
            }

            // Generate field accessors for DDD private backing fields
            // Pattern: private readonly List<T> _items; + public IReadOnlyCollection<T> Items => _items.AsReadOnly()
            // NET8+: [UnsafeAccessor] — zero-overhead, AOT-safe, no reflection.
            // netstandard2.1 fallback: FieldInfo.SetValue (reflection, not AOT-safe but functional).
            var backingFieldProps = entity.Properties.Where(p => p.HasPrivateBackingFieldAccess && p.IsCollection).ToList();
            if (backingFieldProps.Any())
            {
                sb.AppendLine($"        // DDD private backing field accessors");
                foreach (var prop in backingFieldProps)
                {
                    var entityType = $"global::{entity.FullTypeName}";
                    var itemType = prop.IsCollectionItemNested ? $"global::{prop.NestedTypeFullName}" : prop.CollectionItemType;
                    var listType = $"global::System.Collections.Generic.List<{itemType}>";
                    // NET8+: UnsafeAccessor for the private field — AOT-safe, trimmer-safe.
                    sb.AppendLine($"#if NET8_0_OR_GREATER");
                    sb.AppendLine($"        [global::System.Runtime.CompilerServices.UnsafeAccessor(");
                    sb.AppendLine($"            global::System.Runtime.CompilerServices.UnsafeAccessorKind.Field, Name = \"{prop.BackingFieldName}\")]");
                    sb.AppendLine($"        private static extern ref {listType} __UnsafeField_{prop.Name}({entityType} obj);");
                    sb.AppendLine($"#else");
                    // netstandard2.1 fallback: FieldInfo.SetValue
                    sb.AppendLine($"        private static readonly global::System.Reflection.FieldInfo _fi_{prop.Name} =");
                    sb.AppendLine($"            typeof({entityType}).GetField(\"{prop.BackingFieldName}\", global::System.Reflection.BindingFlags.Instance | global::System.Reflection.BindingFlags.NonPublic)!;");
                    sb.AppendLine($"#endif");
                }
                sb.AppendLine();
            }

            // Non-public constructor accessor (NET8+: [UnsafeAccessor(Constructor)] — AOT-safe, no reflection).
            // Generated when the selected constructor is non-public (or when required members force
            // bypassing the `new T()` call-site syntax even for a public ctor).
            if (entity.SelectedConstructorParameters != null && !entity.SelectedConstructorIsPublic)
            {
                var entityType2 = $"global::{entity.FullTypeName}";
                var ctorParams = entity.SelectedConstructorParameters;
                // Build the parameter list for the UnsafeAccessor method signature.
                var paramList = string.Join(", ", ctorParams.Select(p => $"{QualifyType(p.TypeName)} {p.Name}"));
                sb.AppendLine($"        // Non-public constructor accessor.");
                sb.AppendLine($"        // NET8+: [UnsafeAccessor(Constructor)] — AOT-safe, bypasses visibility and CS9035.");
                sb.AppendLine($"        // netstandard2.1: Activator.CreateInstance — reflection, not AOT-safe but functional.");
                sb.AppendLine($"#if NET8_0_OR_GREATER");
                sb.AppendLine($"        [global::System.Runtime.CompilerServices.UnsafeAccessor(");
                sb.AppendLine($"            global::System.Runtime.CompilerServices.UnsafeAccessorKind.Constructor)]");
                sb.AppendLine($"        private static extern {entityType2} __CreateInstance({paramList});");
                sb.AppendLine($"#endif");
                sb.AppendLine();
            }

            // Collection Name (only for root)
            if (isRoot)
            {
                sb.AppendLine($"        public override string CollectionName => \"{entity.CollectionName}\";");
                sb.AppendLine();
            }
            else if (entity.Properties.All(p => !p.IsKey))
            {
                sb.AppendLine($"// #warning Entity '{entity.Name}' has no defined primary key. Mapper may not support all features.");
            }

            // Serialize Method
            GenerateSerializeMethod(sb, entity, isRoot, mapperNamespace);
            
            sb.AppendLine();

            // Deserialize Method
            GenerateDeserializeMethod(sb, entity, isRoot, mapperNamespace);

            if (isRoot)
            {
                sb.AppendLine();
                GenerateIdAccessors(sb, entity);
            }

            sb.AppendLine($"    }}");
            
            return sb.ToString();
        }

        private static void GenerateSerializeMethod(StringBuilder sb, EntityInfo entity, bool isRoot, string mapperNamespace)
        {
            var entityType = $"global::{entity.FullTypeName}";
            
            // Always generate SerializeFields (writes only fields, no document wrapper)
            // This is needed even for root entities, as they may be used as nested objects
            // Note: BsonSpanWriter is a ref struct, so it must be passed by ref
            sb.AppendLine($"        public void SerializeFields({entityType} entity, ref global::BLite.Bson.BsonSpanWriter writer)");
            sb.AppendLine($"        {{");
            GenerateFieldWritesCore(sb, entity, mapperNamespace);
            sb.AppendLine($"        }}");
            sb.AppendLine();
            
            // Generate Serialize method (with document wrapper)
            var methodSig = isRoot 
                ? $"public override int Serialize({entityType} entity, global::BLite.Bson.BsonSpanWriter writer)"
                : $"public int Serialize({entityType} entity, global::BLite.Bson.BsonSpanWriter writer)";

            sb.AppendLine($"        {methodSig}");
            sb.AppendLine($"        {{");
            // Use BeginDocumentWithOffsets for root entities only: writes a C-BSON v2 offset table
            // header that allows O(1) field seeks during non-indexed predicate evaluation.
            // Nested type mappers call SerializeFields directly (no wrapper) so they are excluded.
            if (isRoot)
                sb.AppendLine($"            var startingPos = writer.BeginDocumentWithOffsets(checked((byte){entity.Properties.Count}));");
            else
                sb.AppendLine($"            var startingPos = writer.BeginDocument();");
            sb.AppendLine();
            GenerateFieldWritesCore(sb, entity, mapperNamespace);
            sb.AppendLine();
            sb.AppendLine($"            writer.EndDocument(startingPos);");
            sb.AppendLine($"            return writer.Position;");
            sb.AppendLine($"        }}");
        }
        
        private static void GenerateFieldWritesCore(StringBuilder sb, EntityInfo entity, string mapperNamespace)
        {
            foreach (var prop in entity.Properties)
            {
                // Handle key property - serialize as "_id" regardless of property name
                if (prop.IsKey && !entity.IsNestedTypeMapper)
                {
                    if (prop.ConverterTypeName != null)
                    {
                        var providerProp = new PropertyInfo { TypeName = prop.ProviderTypeName ?? "string" };
                        var idWriteMethod = GetPrimitiveWriteMethod(providerProp, allowKey: true);
                        sb.AppendLine($"            writer.{idWriteMethod}(\"{BLiteConventions.BsonIdFieldName}\", _idConverter.ConvertToProvider(entity.{prop.Name}));");
                    }
                    else
                    {
                        var idWriteMethod = GetPrimitiveWriteMethod(prop, allowKey: true);
                        if (idWriteMethod != null)
                        {
                            sb.AppendLine($"            writer.{idWriteMethod}(\"{BLiteConventions.BsonIdFieldName}\", entity.{prop.Name});");
                        }
                        else
                        {
                            sb.AppendLine($"#warning Unsupported Id type for '{prop.Name}': {prop.TypeName}. Serialization of '{BLiteConventions.BsonIdFieldName}' will fail.");
                            sb.AppendLine($"            // Unsupported Id type: {prop.TypeName}");
                        }
                    }
                    continue;
                }
                
                GenerateValidation(sb, prop);
                GenerateWriteProperty(sb, prop, mapperNamespace);
            }
        }
            
        private static void GenerateValidation(StringBuilder sb, PropertyInfo prop)
        {
            var isString = prop.TypeName == "string" || prop.TypeName == "String";

            if (prop.IsRequired)
            {
                if (isString)
                {
                    sb.AppendLine($"            if (string.IsNullOrEmpty(entity.{prop.Name})) throw new global::System.ComponentModel.DataAnnotations.ValidationException(\"Property {prop.Name} is required.\");");
                }
                else if (prop.IsNullable)
                {
                    sb.AppendLine($"            if (entity.{prop.Name} == null) throw new global::System.ComponentModel.DataAnnotations.ValidationException(\"Property {prop.Name} is required.\");");
                }
            }

            if (prop.MaxLength.HasValue && isString)
            {
                sb.AppendLine($"            if ((entity.{prop.Name}?.Length ?? 0) > {prop.MaxLength}) throw new global::System.ComponentModel.DataAnnotations.ValidationException(\"Property {prop.Name} exceeds max length {prop.MaxLength}.\");");
            }
            if (prop.MinLength.HasValue && isString)
            {
                sb.AppendLine($"            if ((entity.{prop.Name}?.Length ?? 0) < {prop.MinLength}) throw new global::System.ComponentModel.DataAnnotations.ValidationException(\"Property {prop.Name} is below min length {prop.MinLength}.\");");
            }

            if (prop.RangeMin.HasValue || prop.RangeMax.HasValue)
            {
                var minStr = prop.RangeMin?.ToString(System.Globalization.CultureInfo.InvariantCulture) ?? "double.MinValue";
                var maxStr = prop.RangeMax?.ToString(System.Globalization.CultureInfo.InvariantCulture) ?? "double.MaxValue";
                sb.AppendLine($"            if ((double)entity.{prop.Name} < {minStr} || (double)entity.{prop.Name} > {maxStr}) throw new global::System.ComponentModel.DataAnnotations.ValidationException(\"Property {prop.Name} is outside range [{minStr}, {maxStr}].\");");
            }
        }

        private static void GenerateWriteProperty(StringBuilder sb, PropertyInfo prop, string mapperNamespace)
        {
            var fieldName = prop.BsonFieldName;
            
            if (prop.IsCollection)
            {
                // Add null check for nullable collections
                if (prop.IsNullable)
                {
                    sb.AppendLine($"            if (entity.{prop.Name} != null)");
                    sb.AppendLine($"            {{");
                }
                
                var arrayVar = $"{prop.Name.ToLower()}Array";
                var indent = prop.IsNullable ? "    " : "";
                sb.AppendLine($"            {indent}var {arrayVar}Pos = writer.BeginArray(\"{fieldName}\");");
                sb.AppendLine($"            {indent}var {prop.Name.ToLower()}Index = 0;");
                sb.AppendLine($"            {indent}foreach (var item in entity.{prop.Name})");
                sb.AppendLine($"            {indent}{{");
                 
                 
                 if (prop.IsCollectionItemNested)
                 {
                     sb.AppendLine($"            {indent}    // Nested Object in List");
                     var nestedMapperTypes = GetMapperName(prop.NestedTypeFullName!);
                     sb.AppendLine($"            {indent}    var {prop.Name.ToLower()}ItemMapper = new global::{mapperNamespace}.{nestedMapperTypes}();");
                     
                     sb.AppendLine($"            {indent}    var itemStartPos = writer.BeginArrayDocument({prop.Name.ToLower()}Index);");
                     sb.AppendLine($"            {indent}    {prop.Name.ToLower()}ItemMapper.SerializeFields(item, ref writer);");
                     sb.AppendLine($"            {indent}    writer.EndDocument(itemStartPos);");
                 }
                 else if (prop.IsCollectionItemEnum)
                 {
                     var arrayWrite = GetArrayWriteMethodForUnderlyingType(prop.CollectionItemEnumUnderlyingTypeName!);
                     sb.AppendLine($"            {indent}    writer.{arrayWrite}({prop.Name.ToLower()}Index, ({prop.CollectionItemEnumUnderlyingTypeName})item);");
                 }
                 else
                 {
                     // Simplified: pass a dummy PropertyInfo with the item type for primitive collection items
                     var dummyProp = new PropertyInfo { TypeName = prop.CollectionItemType! };
                     var writeMethod = GetPrimitiveWriteMethod(dummyProp);
                     if (writeMethod != null)
                     {
                        var arrayWriteMethod = ToArrayWriteMethod(writeMethod);
                        sb.AppendLine($"            {indent}    writer.{arrayWriteMethod}({prop.Name.ToLower()}Index, item);");
                     }
                 }
                 sb.AppendLine($"            {indent}    {prop.Name.ToLower()}Index++;");
                 
                 sb.AppendLine($"            {indent}}}");
                 sb.AppendLine($"            {indent}writer.EndArray({arrayVar}Pos);");
                 
                 // Close the null check if block
                 if (prop.IsNullable)
                 {
                     sb.AppendLine($"            }}");
                     sb.AppendLine($"            else");
                     sb.AppendLine($"            {{");
                     sb.AppendLine($"                writer.WriteNull(\"{fieldName}\");");
                     sb.AppendLine($"            }}");
                 }
            }
            else if (prop.IsDictionary)
            {
                // Encode Dictionary<string,V> as a BSON array of alternating key (string) / value pairs.
                // This avoids inserting runtime strings into the static key-map.
                // Layout: [0]=key(string), [1]=value, [2]=key, [3]=value, ...
                var indent = prop.IsNullable ? "    " : "";
                if (prop.IsNullable)
                {
                    sb.AppendLine($"            if (entity.{prop.Name} != null)");
                    sb.AppendLine($"            {{");
                }
                var dictArrVar = $"{prop.Name.ToLower()}DictArrPos";
                var dictIdxVar = $"{prop.Name.ToLower()}DictIdx";
                var dummyKeyProp   = new PropertyInfo { TypeName = prop.DictionaryKeyType   ?? "string" };
                var dummyValueProp = new PropertyInfo { TypeName = prop.DictionaryValueType ?? "string" };
                var primitiveKeyWriteMethod   = GetPrimitiveWriteMethod(dummyKeyProp);
                var primitiveValueWriteMethod = GetPrimitiveWriteMethod(dummyValueProp);
                sb.AppendLine($"            {indent}var {dictArrVar} = writer.BeginArray(\"{fieldName}\");");
                sb.AppendLine($"            {indent}var {dictIdxVar} = 0;");
                sb.AppendLine($"            {indent}foreach (var kvp in entity.{prop.Name})");
                sb.AppendLine($"            {indent}{{");
                if (primitiveKeyWriteMethod != null && primitiveValueWriteMethod != null)
                {
                    var arrayKeyWriteMethod = ToArrayWriteMethod(primitiveKeyWriteMethod);
                    var arrayValWriteMethod = ToArrayWriteMethod(primitiveValueWriteMethod);
                    sb.AppendLine($"            {indent}    writer.{arrayKeyWriteMethod}({dictIdxVar}++, kvp.Key);");
                    sb.AppendLine($"            {indent}    writer.{arrayValWriteMethod}({dictIdxVar}++, kvp.Value);");
                }
                else
                {
                    sb.AppendLine($"            {indent}    // Dictionary entry skipped: key type '{prop.DictionaryKeyType}' or value type '{prop.DictionaryValueType}' is not a supported primitive.");
                }
                sb.AppendLine($"            {indent}}}");
                sb.AppendLine($"            {indent}writer.EndArray({dictArrVar});");
                if (prop.IsNullable)
                {
                    sb.AppendLine($"            }}");
                    sb.AppendLine($"            else");
                    sb.AppendLine($"            {{");
                    sb.AppendLine($"                writer.WriteNull(\"{fieldName}\");");
                    sb.AppendLine($"            }}");
                }
            }
            else if (prop.IsNestedObject)
            {
                sb.AppendLine($"            if (entity.{prop.Name} != null)");
                sb.AppendLine($"            {{");
                sb.AppendLine($"                var {prop.Name.ToLower()}Pos = writer.BeginDocument(\"{fieldName}\");");
                var nestedMapperType = GetMapperName(prop.NestedTypeFullName!);
                sb.AppendLine($"                var {prop.Name.ToLower()}Mapper = new global::{mapperNamespace}.{nestedMapperType}();");
                sb.AppendLine($"                {prop.Name.ToLower()}Mapper.SerializeFields(entity.{prop.Name}, ref writer);");
                sb.AppendLine($"                writer.EndDocument({prop.Name.ToLower()}Pos);");
                sb.AppendLine($"            }}");
                sb.AppendLine($"            else");
                sb.AppendLine($"            {{");
                sb.AppendLine($"                writer.WriteNull(\"{fieldName}\");");
                sb.AppendLine($"            }}");
            }
            else if (prop.IsEnum)
            {
                var enumType = $"global::{prop.EnumFullTypeName}";
                var underlyingWrite = GetWriteMethodForUnderlyingType(prop.EnumUnderlyingTypeName!);
                
                if (prop.IsNullable)
                {
                    sb.AppendLine($"            if (entity.{prop.Name} != null)");
                    sb.AppendLine($"            {{");
                    sb.AppendLine($"                writer.{underlyingWrite}(\"{fieldName}\", ({prop.EnumUnderlyingTypeName})entity.{prop.Name}.Value);");
                    sb.AppendLine($"            }}");
                    sb.AppendLine($"            else");
                    sb.AppendLine($"            {{");
                    sb.AppendLine($"                writer.WriteNull(\"{fieldName}\");");
                    sb.AppendLine($"            }}");
                }
                else
                {
                    sb.AppendLine($"            writer.{underlyingWrite}(\"{fieldName}\", ({prop.EnumUnderlyingTypeName})entity.{prop.Name});");
                }
            }
                else
                {
                    var writeMethod = GetPrimitiveWriteMethod(prop, allowKey: false);
                    if (writeMethod != null)
                    {
                        if (prop.IsNullable || prop.TypeName == "string" || prop.TypeName == "String" || prop.TypeName == "byte[]")
                        {
                            sb.AppendLine($"            if (entity.{prop.Name} != null)");
                            sb.AppendLine($"            {{");
                            // For nullable value types (Nullable<T>, e.g. int?), use .Value to unwrap.
                            // Do NOT use .Value when the nullable annotation comes from an unconstrained T? generic
                            // parameter (type name has no trailing '?' but NullableAnnotation is set).
                            var isValueTypeNullable = prop.IsNullable && prop.TypeName.TrimEnd('?') != prop.TypeName && IsValueType(prop.TypeName);
                            var valueAccess = isValueTypeNullable 
                                ? $"entity.{prop.Name}.Value" 
                                : $"entity.{prop.Name}";
                            sb.AppendLine($"                writer.{writeMethod}(\"{fieldName}\", {valueAccess});");
                            sb.AppendLine($"            }}");
                            sb.AppendLine($"            else");
                            sb.AppendLine($"            {{");
                            sb.AppendLine($"                writer.WriteNull(\"{fieldName}\");");
                            sb.AppendLine($"            }}");
                        }
                        else
                        {
                            sb.AppendLine($"            writer.{writeMethod}(\"{fieldName}\", entity.{prop.Name});");
                        }
                    }
                    else if (prop.ConverterTypeName != null)
                    {
                        // Non-key property with a source-generator-detected converter.
                        var providerProp = new PropertyInfo { TypeName = prop.ProviderTypeName ?? "string" };
                        var converterWriteMethod = GetPrimitiveWriteMethod(providerProp, allowKey: false);
                        if (converterWriteMethod != null)
                        {
                            if (prop.IsNullable)
                            {
                                // For nullable properties, guard against null before calling ConvertToProvider.
                                // For nullable value types (Nullable<T>) use .Value; unconstrained T? generics don't need it.
                                var isValueTypeNullable = prop.TypeName.TrimEnd('?') != prop.TypeName && IsValueType(prop.TypeName);
                                var valueAccess = isValueTypeNullable
                                    ? $"entity.{prop.Name}.Value"
                                    : $"entity.{prop.Name}";
                                sb.AppendLine($"            if (entity.{prop.Name} != null)");
                                sb.AppendLine($"            {{");
                                sb.AppendLine($"                writer.{converterWriteMethod}(\"{fieldName}\", _converter_{prop.Name}.ConvertToProvider({valueAccess}));");
                                sb.AppendLine($"            }}");
                                sb.AppendLine($"            else");
                                sb.AppendLine($"            {{");
                                sb.AppendLine($"                writer.WriteNull(\"{fieldName}\");");
                                sb.AppendLine($"            }}");
                            }
                            else
                            {
                                sb.AppendLine($"            writer.{converterWriteMethod}(\"{fieldName}\", _converter_{prop.Name}.ConvertToProvider(entity.{prop.Name}));");
                            }
                        }
                        else
                        {
                            sb.AppendLine($"#warning Property '{prop.Name}': converter '{prop.ConverterTypeName}' has an unsupported provider type '{prop.ProviderTypeName}'. It will be skipped during serialization.");
                            sb.AppendLine($"            // Unsupported provider type: {prop.ProviderTypeName} for {prop.Name}");
                        }
                    }
                    else
                    {
                        sb.AppendLine($"#warning Property '{prop.Name}' of type '{prop.TypeName}' is not directly supported and has no converter. It will be skipped during serialization.");
                        sb.AppendLine($"            // Unsupported type: {prop.TypeName} for {prop.Name}");
                    }
                }
            }

        private static void GenerateDeserializeMethod(StringBuilder sb, EntityInfo entity, bool isRoot, string mapperNamespace)
        {
             var entityType = $"global::{entity.FullTypeName}";
             var needsReflection = entity.HasPrivateSetters
                 || entity.SelectedConstructorParameters == null  // GetUninitializedObject path
                 || !entity.SelectedConstructorIsPublic           // non-public ctor path
                 || (entity.SelectedConstructorParameters.Count == 0 && entity.HasPrivateOrNoConstructor);
             
             // Always generate a public Deserialize method that accepts ref (for nested/internal usage)
             GenerateDeserializeCore(sb, entity, entityType, needsReflection, mapperNamespace);
             
             // For root entities, also generate the override without ref that calls the ref version
             if (isRoot)
             {
                 sb.AppendLine();
                 sb.AppendLine($"        public override {entityType} Deserialize(global::BLite.Bson.BsonSpanReader reader)");
                 sb.AppendLine($"        {{");
                 sb.AppendLine($"            return Deserialize(ref reader);");
                 sb.AppendLine($"        }}");
             }
        }
        
        private static void GenerateDeserializeCore(StringBuilder sb, EntityInfo entity, string entityType, bool needsReflection, string mapperNamespace)
        {
            // Public method that always accepts ref for internal/nested usage
            sb.AppendLine($"        public {entityType} Deserialize(ref global::BLite.Bson.BsonSpanReader reader)");
            sb.AppendLine($"        {{");
            // Use object initializer if possible or constructor, but for now standard new()
            // To support required properties, we might need a different approach or verify if source generators can detect required.
            // For now, let's assume standard creation and property setting.
            // If required properties are present, compiling 'new T()' might fail if they aren't set in initializer.
            // Alternative: Deserialize into temporary variables then construct.
            
            // Declare temp variables for all properties
            foreach(var prop in entity.Properties)
            {
                var baseType = QualifyType(prop.TypeName.TrimEnd('?'));

                 // Handle collections init
                if (prop.IsCollection)
                {
                    var itemType = prop.CollectionItemType;
                    if (prop.IsCollectionItemNested) itemType = $"global::{prop.NestedTypeFullName}"; // Use full name with global::
                     sb.AppendLine($"            var {prop.Name.ToLower()} = new global::System.Collections.Generic.List<{itemType}>();");
                }
                else if (prop.IsDictionary)
                {
                    var keyT   = QualifyType(prop.DictionaryKeyType   ?? "string");
                    var valT   = QualifyType(prop.DictionaryValueType ?? "object");
                    if (prop.IsNullable)
                        sb.AppendLine($"            global::System.Collections.Generic.Dictionary<{keyT}, {valT}>? {prop.Name.ToLower()} = null;");
                    else
                        sb.AppendLine($"            var {prop.Name.ToLower()} = new global::System.Collections.Generic.Dictionary<{keyT}, {valT}>();");
                }
                else
                {
                    sb.AppendLine($"            {baseType}? {prop.Name.ToLower()} = default;");
                }
            }
            
            
            // Read document size and track boundaries
            sb.AppendLine($"            var docSize = reader.ReadDocumentSize();");
            sb.AppendLine($"            var docEndPos = reader.Position + docSize - {BLiteConventions.BsonDocumentSizeOverhead}; // -{BLiteConventions.BsonDocumentSizeOverhead} because size includes itself");
            sb.AppendLine();
            sb.AppendLine($"            while (reader.Position < docEndPos)");
            sb.AppendLine($"            {{");
            sb.AppendLine($"                var bsonType = reader.ReadBsonType();");
            sb.AppendLine($"                if (bsonType == global::BLite.Bson.BsonType.EndOfDocument) break;");
            sb.AppendLine();
            sb.AppendLine($"                var elementName = reader.ReadElementHeader();");
            sb.AppendLine($"                switch (elementName)");
            sb.AppendLine($"                {{");
            
            foreach (var prop in entity.Properties)
            {
                // Root entities use "_id" for the key field; nested type mappers use the BsonFieldName (e.g. "id").
                var caseName = (prop.IsKey && !entity.IsNestedTypeMapper) ? BLiteConventions.BsonIdFieldName : prop.BsonFieldName;
                sb.AppendLine($"                    case \"{caseName}\":");
                
                // Read Logic -> assign to local var
                GenerateReadPropertyToLocal(sb, prop, "bsonType", mapperNamespace);
                
                sb.AppendLine($"                        break;");
            }
            
            sb.AppendLine($"                    default:");
            sb.AppendLine($"                        reader.SkipValue(bsonType);");
            sb.AppendLine($"                        break;");
            sb.AppendLine($"                }}");
            sb.AppendLine($"            }}");
            sb.AppendLine();
            
            // ── Object construction ───────────────────────────────────────────────────
            if (entity.SelectedConstructorParameters == null)
            {
                // No viable constructor found — last resort: GetUninitializedObject.
                // Field initializers do NOT run. BLITE010 warning was already emitted by the analyzer.
                sb.AppendLine($"            // BLITE010: No viable constructor — using GetUninitializedObject (field initializers will NOT run).");
                sb.AppendLine($"            var entity = (global::{entity.FullTypeName})global::System.Runtime.CompilerServices.RuntimeHelpers.GetUninitializedObject(typeof(global::{entity.FullTypeName}));");
            }
            else if (entity.SelectedConstructorParameters.Count == 0)
            {
                // Parameterless constructor.
                if (entity.SelectedConstructorIsPublic)
                {
                    // public — straightforward new().
                    sb.AppendLine($"            var entity = new {entityType}();");
                }
                else
                {
                    // Non-public: NET8+ uses [UnsafeAccessor(Constructor)], netstandard2.1 uses Activator.
                    sb.AppendLine($"            // Non-public parameterless constructor.");
                    sb.AppendLine($"#if NET8_0_OR_GREATER");
                    sb.AppendLine($"            var entity = __CreateInstance();");
                    sb.AppendLine($"#else");
                    sb.AppendLine($"#pragma warning disable IL2072, IL2026");
                    sb.AppendLine($"            var entity = (global::{entity.FullTypeName})global::System.Activator.CreateInstance(typeof(global::{entity.FullTypeName}), nonPublic: true)!;");
                    sb.AppendLine($"#pragma warning restore IL2072, IL2026");
                    sb.AppendLine($"#endif");
                }
            }
            else
            {
                // N-param constructor: pass matched properties as arguments.
                var ctorArgs = string.Join(", ",
                    entity.SelectedConstructorParameters.Select(p =>
                    {
                        var varName = p.MatchedPropertyName.ToLower();
                        var isNullableValueType = p.IsNullable && p.TypeName.TrimEnd('?') != p.TypeName;
                        return isNullableValueType ? varName : $"{varName} ?? default!";
                    }));
                if (entity.SelectedConstructorIsPublic)
                {
                    sb.AppendLine($"            var entity = new {entityType}({ctorArgs});");
                }
                else
                {
                    sb.AppendLine($"            // Non-public N-param constructor.");
                    sb.AppendLine($"#if NET8_0_OR_GREATER");
                    sb.AppendLine($"            var entity = __CreateInstance({ctorArgs});");
                    sb.AppendLine($"#else");
                    sb.AppendLine($"#pragma warning disable IL2072, IL2026");
                    var argTypesArr = string.Join(", ", entity.SelectedConstructorParameters.Select(p => $"typeof({QualifyType(p.TypeName)})"));
                    var argsArr = string.Join(", ", entity.SelectedConstructorParameters.Select(p =>
                    {
                        var varName = p.MatchedPropertyName.ToLower();
                        var isNullableValueType = p.IsNullable && p.TypeName.TrimEnd('?') != p.TypeName;
                        return isNullableValueType ? varName : $"{varName} ?? default!";
                    }));
                    sb.AppendLine($"            var ctor__ = typeof(global::{entity.FullTypeName}).GetConstructor(");
                    sb.AppendLine($"                global::System.Reflection.BindingFlags.Instance | global::System.Reflection.BindingFlags.NonPublic | global::System.Reflection.BindingFlags.Public,");
                    sb.AppendLine($"                null, new global::System.Type[] {{ {argTypesArr} }}, null)!;");
                    sb.AppendLine($"            var entity = (global::{entity.FullTypeName})ctor__.Invoke(new object?[] {{ {argsArr} }});");
                    sb.AppendLine($"#pragma warning restore IL2072, IL2026");
                    sb.AppendLine($"#endif");
                }
            }
            sb.AppendLine();

            // ── Post-construction property assignment ─────────────────────────────────
            // For N-param ctors, only assign properties NOT already set via constructor.
            var ctorMatchedPropNames = entity.SelectedConstructorParameters != null
                ? new System.Collections.Generic.HashSet<string>(entity.SelectedConstructorParameters.Select(p => p.MatchedPropertyName))
                : new System.Collections.Generic.HashSet<string>();

            // Set remaining properties via setters.
            foreach (var prop in entity.Properties)
            {
                // Properties already initialised through the constructor don't need to be set again.
                if (ctorMatchedPropNames.Contains(prop.Name)) continue;

                var varName = prop.Name.ToLower();
                var propValue = varName;
                
                if (prop.IsCollection)
                {
                    // Convert to appropriate collection type
                    if (prop.IsArray)
                    {
                        propValue += ".ToArray()";
                    }
                    else if (prop.HasPrivateBackingFieldAccess)
                    {
                        // DDD backing field is List<T> — no conversion; assign temp List<T> directly to the field.
                    }
                    else if (prop.CollectionConcreteTypeName != null)
                    {
                        var concreteType = prop.CollectionConcreteTypeName;
                        var itemType = prop.IsCollectionItemNested ? $"global::{prop.NestedTypeFullName}" : prop.CollectionItemType;
                        
                        if (concreteType.Contains("HashSet"))
                            propValue = $"new global::System.Collections.Generic.HashSet<{itemType}>({propValue})";
                        else if (concreteType.Contains("ISet"))
                            propValue = $"new global::System.Collections.Generic.HashSet<{itemType}>({propValue})";
                        else if (concreteType.Contains("LinkedList"))
                            propValue = $"new global::System.Collections.Generic.LinkedList<{itemType}>({propValue})";
                        else if (concreteType.Contains("Queue"))
                            propValue = $"new global::System.Collections.Generic.Queue<{itemType}>({propValue})";
                        else if (concreteType.Contains("Stack"))
                            propValue = $"new global::System.Collections.Generic.Stack<{itemType}>({propValue})";
                        else if (concreteType.Contains("IReadOnlyList") || concreteType.Contains("IReadOnlyCollection"))
                            propValue += ".AsReadOnly()";
                    }
                }
                
                // Use appropriate setter
                if (prop.HasPrivateBackingFieldAccess && prop.IsCollection)
                {
                    // DDD pattern: write directly to the private backing field.
                    // NET8+: UnsafeAccessor ref-return — one instruction, AOT-safe.
                    // netstandard2.1: FieldInfo.SetValue fallback.
                    sb.AppendLine($"#if NET8_0_OR_GREATER");
                    sb.AppendLine($"            __UnsafeField_{prop.Name}(entity) = {propValue};");
                    sb.AppendLine($"#else");
                    sb.AppendLine($"            _fi_{prop.Name}.SetValue(entity, {propValue});");
                    sb.AppendLine($"#endif");
                }
                else if ((!prop.HasPublicSetter && prop.HasAnySetter) || prop.HasInitOnlySetter)
                {
                    // NET8+: call the [UnsafeAccessor] static extern setter — AOT-safe, no reflection.
                    // netstandard2.1: call via MethodInfo.Invoke — reflection, not AOT-safe but functional.
                    var isNullableValueType = prop.IsNullable && prop.TypeName.TrimEnd('?') != prop.TypeName;
                    var setterArg = isNullableValueType ? propValue : $"{propValue} ?? default!";
                    sb.AppendLine($"#if NET8_0_OR_GREATER");
                    sb.AppendLine($"            __UnsafeSetter_{prop.Name}(entity, {setterArg});");
                    sb.AppendLine($"#else");
                    sb.AppendLine($"            _mi_{prop.Name}?.Invoke(entity, new object?[] {{ {setterArg} }});");
                    sb.AppendLine($"#endif");
                }
                else
                {
                    // Direct property assignment
                    if (prop.IsNullable && prop.TypeName.TrimEnd('?') != prop.TypeName)
                        sb.AppendLine($"            entity.{prop.Name} = {propValue};");
                    else
                        sb.AppendLine($"            entity.{prop.Name} = {propValue} ?? default!;");
                }
            }
            sb.AppendLine();
            sb.AppendLine($"            return entity;");
            sb.AppendLine($"        }}");
        }

        private static void GenerateReadPropertyToLocal(StringBuilder sb, PropertyInfo prop, string bsonTypeVar, string mapperNamespace)
        {
             var localVar = prop.Name.ToLower();
             
             if (prop.IsCollection)
             {
                 var arrVar = prop.Name.ToLower();
                 // Handle null for nullable collections
                 if (prop.IsNullable)
                 {
                     sb.AppendLine($"                        if ({bsonTypeVar} == global::BLite.Bson.BsonType.Null) break;");
                 }
                 sb.AppendLine($"                        // Read Array {prop.Name}");
                 sb.AppendLine($"                        var {arrVar}ArrSize = reader.ReadDocumentSize();");
                 sb.AppendLine($"                        var {arrVar}ArrEndPos = reader.Position + {arrVar}ArrSize - {BLiteConventions.BsonDocumentSizeOverhead};");
                 sb.AppendLine($"                        while (reader.Position < {arrVar}ArrEndPos)");
                 sb.AppendLine($"                        {{");
                 sb.AppendLine($"                            var itemType = reader.ReadBsonType();");
                 sb.AppendLine($"                            if (itemType == global::BLite.Bson.BsonType.EndOfDocument) break;");
                 sb.AppendLine($"                            reader.SkipArrayKey();");
                 
                 if (prop.IsCollectionItemNested)
                 {
                     var nestedMapperTypes = GetMapperName(prop.NestedTypeFullName!);
                     sb.AppendLine($"                            var {prop.Name.ToLower()}ItemMapper = new global::{mapperNamespace}.{nestedMapperTypes}();");
                     sb.AppendLine($"                            var item = {prop.Name.ToLower()}ItemMapper.Deserialize(ref reader);");
                     sb.AppendLine($"                            {localVar}.Add(item);");
                 }
                 else if (prop.IsCollectionItemEnum)
                 {
                     var enumItemType = $"global::{prop.CollectionItemEnumFullTypeName}";
                     var underlyingRead = GetReadMethodForUnderlyingType(prop.CollectionItemEnumUnderlyingTypeName!);
                     sb.AppendLine($"                            var item = ({enumItemType})reader.{underlyingRead}();");
                     sb.AppendLine($"                            {localVar}.Add(item);");
                 }
                 else
                 {
                     var readMethod = GetPrimitiveReadMethod(new PropertyInfo { TypeName = prop.CollectionItemType! });
                      if (readMethod != null)
                      {
                          var cast = (prop.CollectionItemType == "float" || prop.CollectionItemType == "Single") ? "(float)" : "";
                          var readArgs = IsCoercedReadMethod(readMethod) ? "(itemType)" : "()";
                          sb.AppendLine($"                            var item = {cast}reader.{readMethod}{readArgs};");
                          sb.AppendLine($"                            {localVar}.Add(item);");
                      }
                      else
                      {
                          sb.AppendLine($"                            reader.SkipValue(itemType);");
                      }
                 }
                 sb.AppendLine($"                        }}");
             }
             else if (prop.IsKey && prop.ConverterTypeName != null)
             {
                 var providerProp = new PropertyInfo { TypeName = prop.ProviderTypeName ?? "string" };
                 var readMethod = GetPrimitiveReadMethod(providerProp);
                 var providerReadArgs = IsCoercedReadMethod(readMethod) ? $"({bsonTypeVar})" : "()";
                 sb.AppendLine($"                        {localVar} = _idConverter.ConvertFromProvider(reader.{readMethod}{providerReadArgs});");
             }
             else if (prop.IsDictionary)
             {
                 // Dictionary encoded as BSON array: [0]=key, [1]=value, [2]=key, [3]=value, ...
                 if (prop.IsNullable)
                 {
                     sb.AppendLine($"                        if ({bsonTypeVar} == global::BLite.Bson.BsonType.Null)");
                     sb.AppendLine($"                        {{");
                     sb.AppendLine($"                            {localVar} = default;");
                     sb.AppendLine($"                            break;");
                     sb.AppendLine($"                        }}");
                 }
                 var keyT2   = QualifyType(prop.DictionaryKeyType   ?? "string");
                 var valT2   = QualifyType(prop.DictionaryValueType ?? "object");
                 sb.AppendLine($"                        {localVar} = new global::System.Collections.Generic.Dictionary<{keyT2}, {valT2}>();");
                 sb.AppendLine($"                        var {localVar}ArrSize = reader.ReadDocumentSize();");
                 sb.AppendLine($"                        var {localVar}ArrEnd  = reader.Position + {localVar}ArrSize - {BLiteConventions.BsonDocumentSizeOverhead};");
                 sb.AppendLine($"                        while (reader.Position < {localVar}ArrEnd)");
                 sb.AppendLine($"                        {{");
                 // Read key element
                 sb.AppendLine($"                            var {localVar}KeyBsonType = reader.ReadBsonType();");
                 sb.AppendLine($"                            if ({localVar}KeyBsonType == global::BLite.Bson.BsonType.EndOfDocument) break;");
                 sb.AppendLine($"                            reader.SkipArrayKey();");
                 sb.AppendLine($"                            var {localVar}Key = reader.ReadString();");
                 // Read value element
                 sb.AppendLine($"                            var {localVar}ValBsonType = reader.ReadBsonType();");
                 sb.AppendLine($"                            if ({localVar}ValBsonType == global::BLite.Bson.BsonType.EndOfDocument) break;");
                 sb.AppendLine($"                            reader.SkipArrayKey();");
                 var dictValProp2 = new PropertyInfo { TypeName = prop.DictionaryValueType ?? "string" };
                 var dictReadMethod2 = GetPrimitiveReadMethod(dictValProp2);
                 if (dictReadMethod2 != null)
                 {
                     var castDict2 = (prop.DictionaryValueType == "float" || prop.DictionaryValueType == "Single") ? "(float)" : "";
                     var dictReadArgs2 = IsCoercedReadMethod(dictReadMethod2) ? $"({localVar}ValBsonType)" : "()";
                     sb.AppendLine($"                            if ({localVar}ValBsonType == global::BLite.Bson.BsonType.Null)");
                     sb.AppendLine($"                            {{");
                     sb.AppendLine($"                                {localVar}[{localVar}Key] = default;");
                     sb.AppendLine($"                            }}");
                     sb.AppendLine($"                            else");
                     sb.AppendLine($"                            {{");
                     sb.AppendLine($"                                {localVar}[{localVar}Key] = {castDict2}reader.{dictReadMethod2}{dictReadArgs2};");
                     sb.AppendLine($"                            }}");
                 }
                 else
                 {
                     sb.AppendLine($"                            reader.SkipValue({localVar}ValBsonType);");
                 }
                 sb.AppendLine($"                        }}");
             }
             else if (prop.IsNestedObject)
             {
                 sb.AppendLine($"                        if ({bsonTypeVar} == global::BLite.Bson.BsonType.Null)");
                 sb.AppendLine($"                        {{");
                 sb.AppendLine($"                            {localVar} = null;");
                 sb.AppendLine($"                        }}");
                 sb.AppendLine($"                        else");
                 sb.AppendLine($"                        {{");
                 var nestedMapperType = GetMapperName(prop.NestedTypeFullName!);
                 sb.AppendLine($"                            var {prop.Name.ToLower()}Mapper = new global::{mapperNamespace}.{nestedMapperType}();");
                 sb.AppendLine($"                            {localVar} = {prop.Name.ToLower()}Mapper.Deserialize(ref reader);");
                 sb.AppendLine($"                        }}");
             }
             else if (prop.IsEnum)
             {
                 var enumType = $"global::{prop.EnumFullTypeName}";
                 var underlyingRead = GetReadMethodForUnderlyingType(prop.EnumUnderlyingTypeName!);
                 
                 if (prop.IsNullable)
                 {
                     sb.AppendLine($"                        if ({bsonTypeVar} == global::BLite.Bson.BsonType.Null)");
                     sb.AppendLine($"                        {{");
                     sb.AppendLine($"                            {localVar} = null;");
                     sb.AppendLine($"                        }}");
                     sb.AppendLine($"                        else");
                     sb.AppendLine($"                        {{");
                     sb.AppendLine($"                            {localVar} = ({enumType})reader.{underlyingRead}();");
                     sb.AppendLine($"                        }}");
                 }
                 else
                 {
                     sb.AppendLine($"                        {localVar} = ({enumType})reader.{underlyingRead}();");
                 }
             }
             else
             {
                 var readMethod = GetPrimitiveReadMethod(prop);
                 if (readMethod != null)
                 {
                    if (readMethod == "ReadBinary")
                    {
                        // ReadBinary returns ReadOnlySpan<byte> — must call .ToArray() to materialise
                        if (prop.IsNullable || prop.TypeName == "byte[]")
                        {
                            sb.AppendLine($"                        if ({bsonTypeVar} == global::BLite.Bson.BsonType.Null)");
                            sb.AppendLine($"                        {{");
                            sb.AppendLine($"                            {localVar} = null;");
                            sb.AppendLine($"                        }}");
                            sb.AppendLine($"                        else");
                            sb.AppendLine($"                        {{");
                            sb.AppendLine($"                            {localVar} = reader.ReadBinary(out _).ToArray();");
                            sb.AppendLine($"                        }}");
                        }
                        else
                        {
                            sb.AppendLine($"                        {localVar} = reader.ReadBinary(out _).ToArray();");
                        }
                    }
                    else
                    {
                    var cast = (prop.TypeName == "float" || prop.TypeName == "Single") ? "(float)" : "";
                    
                    // Handle nullable types and reference types (string) - check for null in BSON stream
                    if (prop.IsNullable || prop.TypeName == "string" || prop.TypeName == "String")
                    {
                        sb.AppendLine($"                        if ({bsonTypeVar} == global::BLite.Bson.BsonType.Null)");
                        sb.AppendLine($"                        {{");
                        sb.AppendLine($"                            {localVar} = null;");
                        sb.AppendLine($"                        }}");
                        sb.AppendLine($"                        else");
                        sb.AppendLine($"                        {{");
                        var nullableReadArgs = IsCoercedReadMethod(readMethod) ? $"({bsonTypeVar})" : "()";
                        sb.AppendLine($"                            {localVar} = {cast}reader.{readMethod}{nullableReadArgs};");
                        sb.AppendLine($"                        }}");
                    }
                    else
                    {
                        var readArgs = IsCoercedReadMethod(readMethod) ? $"({bsonTypeVar})" : "()";
                        sb.AppendLine($"                        {localVar} = {cast}reader.{readMethod}{readArgs};");
                    }
                    }
                 }
                 else if (prop.ConverterTypeName != null)
                 {
                     // Non-key property with a source-generator-detected converter.
                     var providerProp = new PropertyInfo { TypeName = prop.ProviderTypeName ?? "string" };
                     var converterReadMethod = GetPrimitiveReadMethod(providerProp);
                     if (converterReadMethod != null)
                     {
                         var converterReadArgs = IsCoercedReadMethod(converterReadMethod) ? $"({bsonTypeVar})" : "()";
                         if (prop.IsNullable)
                         {
                             // Guard against BSON null so ConvertFromProvider is never called with a missing value.
                             sb.AppendLine($"                        if ({bsonTypeVar} == global::BLite.Bson.BsonType.Null)");
                             sb.AppendLine($"                        {{");
                             sb.AppendLine($"                            {localVar} = null;");
                             sb.AppendLine($"                        }}");
                             sb.AppendLine($"                        else");
                             sb.AppendLine($"                        {{");
                             sb.AppendLine($"                            {localVar} = _converter_{prop.Name}.ConvertFromProvider(reader.{converterReadMethod}{converterReadArgs});");
                             sb.AppendLine($"                        }}");
                         }
                         else
                         {
                             sb.AppendLine($"                        {localVar} = _converter_{prop.Name}.ConvertFromProvider(reader.{converterReadMethod}{converterReadArgs});");
                         }
                     }
                     else
                     {
                         sb.AppendLine($"                        reader.SkipValue({bsonTypeVar});");
                     }
                 }
                 else
                 {
                     sb.AppendLine($"                        reader.SkipValue({bsonTypeVar});");
                 }
             }
        }

        public static string GetMapperName(string fullTypeName)
        {
             if (string.IsNullOrEmpty(fullTypeName)) return "Unknown" + BLiteConventions.MapperClassSuffix;
             // Remove global:: prefix
             var cleanName = fullTypeName.Replace("global::", "");
             // Escape any existing underscores first to prevent collisions between different
             // type name patterns (e.g. "NS.Foo_Bar" vs "NS.Foo.Bar" must produce different names).
             cleanName = cleanName.Replace("_", "__");
             // Replace namespace separators (dots), nested class markers (+), colons, and generic
             // type argument delimiters (<, >, ',', ' ') with underscores / empty string.
             return cleanName.Replace(".", "_").Replace("+", "_").Replace(":", "_")
                            .Replace("<", "_").Replace(">", "").Replace(",", "_").Replace(" ", "")
                            + BLiteConventions.MapperClassSuffix;
        }

        private static void GenerateIdAccessors(StringBuilder sb, EntityInfo entity)
        {
            var keyProp = entity.Properties.FirstOrDefault(p => p.IsKey);
            
            // Use CollectionIdTypeFullName if available (from DocumentCollection<TId, T> declaration)
            string keyType;
            if (!string.IsNullOrEmpty(entity.CollectionIdTypeFullName))
            {
                // Remove "global::" prefix if present
                keyType = entity.CollectionIdTypeFullName!.Replace("global::", "");
            }
            else
            {
                keyType = keyProp?.TypeName ?? "ObjectId";
            }
            
            // Normalize keyType - remove nullable suffix for the methods
            // We expect Id to have a value during serialization/deserialization
            keyType = keyType.TrimEnd('?');
            
            // Normalize keyType
             switch (keyType)
            {
                case "Int32": keyType = "int"; break;
                case "Int64": keyType = "long"; break;
                case "String": keyType = "string"; break;
                case "Double": keyType = "double"; break;
                case "Boolean": keyType = "bool"; break;
                case "Decimal": keyType = "decimal"; break;
                case "Guid": keyType = "global::System.Guid"; break;
                case "DateTime": keyType = "global::System.DateTime"; break;
                case "ObjectId": keyType = "global::BLite.Bson.ObjectId"; break;
            }

            var entityType = $"global::{entity.FullTypeName}";
            var qualifiedKeyType = keyType.StartsWith("global::") ? keyType : (keyProp?.ConverterTypeName != null ? $"global::{keyProp.TypeName.TrimEnd('?')}" : keyType);
            
            var propName = keyProp?.Name ?? "Id";
            
            // GetId can return nullable if the property is nullable, but we add ! to assert non-null
            // This helps catch bugs where entities are created without an Id
            if (keyProp?.IsNullable == true)
            {
                sb.AppendLine($"        public override {qualifiedKeyType} GetId({entityType} entity) => entity.{propName}!;");
            }
            else
            {
                sb.AppendLine($"        public override {qualifiedKeyType} GetId({entityType} entity) => entity.{propName};");
            }
            
            // If the ID property has a private or init-only setter, use the AOT-safe accessor
            if (entity.HasPrivateSetters && keyProp != null && (!keyProp.HasPublicSetter || keyProp.HasInitOnlySetter))
            {
                sb.AppendLine($"        public override void SetId({entityType} entity, {qualifiedKeyType} id)");
                sb.AppendLine($"        {{");
                sb.AppendLine($"#if NET8_0_OR_GREATER");
                sb.AppendLine($"            __UnsafeSetter_{propName}(entity, id);");
                sb.AppendLine($"#else");
                sb.AppendLine($"            _mi_{propName}?.Invoke(entity, new object?[] {{ id }});");
                sb.AppendLine($"#endif");
                sb.AppendLine($"        }}");
            }
            else
            {
                sb.AppendLine($"        public override void SetId({entityType} entity, {qualifiedKeyType} id) => entity.{propName} = id;");
            }

            if (keyProp?.ConverterTypeName != null)
            {
                var providerType = keyProp.ProviderTypeName ?? "string";
                // Normalize providerType
                switch (providerType)
                {
                    case "Int32": providerType = "int"; break;
                    case "Int64": providerType = "long"; break;
                    case "String": providerType = "string"; break;
                    case "Guid": providerType = "global::System.Guid"; break;
                    case "ObjectId": providerType = "global::BLite.Bson.ObjectId"; break;
                }

                sb.AppendLine();
                sb.AppendLine($"        public override global::BLite.Core.Indexing.IndexKey ToIndexKey({qualifiedKeyType} id) => ");
                sb.AppendLine($"            global::BLite.Core.Indexing.IndexKey.Create(_idConverter.ConvertToProvider(id));");
                sb.AppendLine();
                sb.AppendLine($"        public override {qualifiedKeyType} FromIndexKey(global::BLite.Core.Indexing.IndexKey key) => ");
                sb.AppendLine($"            _idConverter.ConvertFromProvider(key.As<{providerType}>());");
            }
        }

        private static string GetBaseMapperClass(PropertyInfo? keyProp, EntityInfo entity)
        {
            if (keyProp?.ConverterTypeName != null)
            {
                return $"DocumentMapperBase<global::{keyProp.TypeName}, ";
            }

            // Use CollectionIdTypeFullName if available (from DocumentCollection<TId, T> declaration)
            string keyType;
            if (!string.IsNullOrEmpty(entity.CollectionIdTypeFullName))
            {
                // Remove "global::" prefix if present
                keyType = entity.CollectionIdTypeFullName!.Replace("global::", "");
            }
            else
            {
                keyType = keyProp?.TypeName ?? "ObjectId";
            }
            
            // Normalize type by removing nullable suffix (?) for comparison
            // At serialization time, we expect the Id to always have a value
            var normalizedKeyType = keyType.TrimEnd('?');
            
            if (normalizedKeyType.EndsWith("Int32") || normalizedKeyType == "int") return "Int32MapperBase<";
            if (normalizedKeyType.EndsWith("Int64") || normalizedKeyType == "long") return "Int64MapperBase<";
            if (normalizedKeyType.EndsWith("String") || normalizedKeyType == "string") return "StringMapperBase<";
            if (normalizedKeyType.EndsWith("Guid")) return "GuidMapperBase<";
            if (normalizedKeyType.EndsWith("ObjectId")) return "ObjectIdMapperBase<";

            return "ObjectIdMapperBase<";
        }

        private static string? GetPrimitiveWriteMethod(PropertyInfo prop, bool allowKey = false)
        {
            var typeName = prop.TypeName;
            if (prop.ColumnTypeName == "point" || prop.ColumnTypeName == "coordinate" || prop.ColumnTypeName == "geopoint")
            {
                return "WriteCoordinates";
            }

            if (typeName.Contains("double") && typeName.Contains(",") && typeName.StartsWith("(") && typeName.EndsWith(")"))
            {
                return "WriteCoordinates";
            }

            var cleanType = typeName.TrimEnd('?').Trim();

            if (cleanType.EndsWith("Int32") || cleanType == "int") return "WriteInt32";
            if (cleanType.EndsWith("Int64") || cleanType == "long") return "WriteInt64";
            if (cleanType.EndsWith("String") || cleanType == "string") return "WriteString";
            if (cleanType.EndsWith("Boolean") || cleanType == "bool") return "WriteBoolean";
            if (cleanType.EndsWith("Single") || cleanType == "float") return "WriteDouble";
            if (cleanType.EndsWith("Double") || cleanType == "double") return "WriteDouble";
            if (cleanType.EndsWith("Decimal") || cleanType == "decimal") return "WriteDecimal128";
            if (cleanType.EndsWith("DateTime") && !cleanType.EndsWith("DateTimeOffset")) return "WriteDateTime";
            if (cleanType.EndsWith("DateTimeOffset")) return "WriteDateTimeOffset";
            if (cleanType.EndsWith("TimeSpan")) return "WriteTimeSpan";
            if (cleanType.EndsWith("DateOnly")) return "WriteDateOnly";
            if (cleanType.EndsWith("TimeOnly")) return "WriteTimeOnly";
            if (cleanType.EndsWith("Guid")) return "WriteGuid";
            if (cleanType.EndsWith("ObjectId")) return "WriteObjectId";
            if (cleanType == "byte[]") return "WriteBinary";

            return null;
        }
        
        private static string? GetPrimitiveReadMethod(PropertyInfo prop)
        {
            var typeName = prop.TypeName;
            if (prop.ColumnTypeName == "point" || prop.ColumnTypeName == "coordinate" || prop.ColumnTypeName == "geopoint")
            {
                return "ReadCoordinates";
            }

            if (typeName.Contains("double") && typeName.Contains(",") && typeName.StartsWith("(") && typeName.EndsWith(")"))
            {
                return "ReadCoordinates";
            }

            var cleanType = typeName.TrimEnd('?').Trim();

            if (cleanType.EndsWith("Int32") || cleanType == "int") return "ReadInt32Coerced";
            if (cleanType.EndsWith("Int64") || cleanType == "long") return "ReadInt64Coerced";
            if (cleanType.EndsWith("String") || cleanType == "string") return "ReadString";
            if (cleanType.EndsWith("Boolean") || cleanType == "bool") return "ReadBoolean";
            if (cleanType.EndsWith("Single") || cleanType == "float") return "ReadDoubleCoerced";
            if (cleanType.EndsWith("Double") || cleanType == "double") return "ReadDoubleCoerced";
            if (cleanType.EndsWith("Decimal") || cleanType == "decimal") return "ReadDecimal128";
            if (cleanType.EndsWith("DateTime") && !cleanType.EndsWith("DateTimeOffset")) return "ReadDateTime";
            if (cleanType.EndsWith("DateTimeOffset")) return "ReadDateTimeOffset";
            if (cleanType.EndsWith("TimeSpan")) return "ReadTimeSpan";
            if (cleanType.EndsWith("DateOnly")) return "ReadDateOnly";
            if (cleanType.EndsWith("TimeOnly")) return "ReadTimeOnly";
            if (cleanType.EndsWith("Guid")) return "ReadGuid";
            if (cleanType.EndsWith("ObjectId")) return "ReadObjectId";
            if (cleanType == "byte[]") return "ReadBinary";

            return null;
        }

        private static bool IsCoercedReadMethod(string? readMethod)
            => readMethod is "ReadInt32Coerced" or "ReadInt64Coerced" or "ReadDoubleCoerced";

        private static bool IsValueType(string typeName)
        {
            // Check if the type is a value type (struct) that requires .Value unwrapping when nullable
            // String is a reference type and doesn't need .Value
            var cleanType = typeName.TrimEnd('?').Trim();
            
            // Common value types
            if (cleanType.EndsWith("Int32") || cleanType == "int") return true;
            if (cleanType.EndsWith("Int64") || cleanType == "long") return true;
            if (cleanType.EndsWith("Boolean") || cleanType == "bool") return true;
            if (cleanType.EndsWith("Single") || cleanType == "float") return true;
            if (cleanType.EndsWith("Double") || cleanType == "double") return true;
            if (cleanType.EndsWith("Decimal") || cleanType == "decimal") return true;
            if (cleanType.EndsWith("DateTime")) return true;
            if (cleanType.EndsWith("DateTimeOffset")) return true;
            if (cleanType.EndsWith("TimeSpan")) return true;
            if (cleanType.EndsWith("DateOnly")) return true;
            if (cleanType.EndsWith("TimeOnly")) return true;
            if (cleanType.EndsWith("Guid")) return true;
            if (cleanType.EndsWith("ObjectId")) return true;
            
            // String and other reference types
            return false;
        }

        private static string QualifyType(string typeName)
        {
            if (string.IsNullOrEmpty(typeName)) return "object";
            if (typeName.StartsWith("global::")) return typeName;
            
            var isNullable = typeName.EndsWith("?");
            var baseType = typeName.TrimEnd('?').Trim();

            if (baseType.StartsWith("(") && baseType.EndsWith(")")) return typeName; // Tuple

            switch (baseType)
            {
                case "int":
                case "long":
                case "string":
                case "bool":
                case "double":
                case "float":
                case "decimal":
                case "byte":
                case "sbyte":
                case "short":
                case "ushort":
                case "uint":
                case "ulong":
                case "char":
                case "object":
                case "dynamic":
                case "void":
                case "byte[]":
                    return baseType + (isNullable ? "?" : "");
                case "Guid": return "global::System.Guid" + (isNullable ? "?" : "");
                case "DateTime": return "global::System.DateTime" + (isNullable ? "?" : "");
                case "DateTimeOffset": return "global::System.DateTimeOffset" + (isNullable ? "?" : "");
                case "TimeSpan": return "global::System.TimeSpan" + (isNullable ? "?" : "");
                case "DateOnly": return "global::System.DateOnly" + (isNullable ? "?" : "");
                case "TimeOnly": return "global::System.TimeOnly" + (isNullable ? "?" : "");
                case "ObjectId": return "global::BLite.Bson.ObjectId" + (isNullable ? "?" : "");
                default:
                    return $"global::{typeName}";
            }
        }

        private static bool IsPrimitive(string typeName)
        {
            var cleanType = typeName.TrimEnd('?').Trim();
            if (cleanType.StartsWith("(") && cleanType.EndsWith(")")) return true;

            switch (cleanType)
            {
                case "int":
                case "long":
                case "string":
                case "bool":
                case "double":
                case "float":
                case "decimal":
                case "byte":
                case "sbyte":
                case "short":
                case "ushort":
                case "uint":
                case "ulong":
                case "char":
                case "object":
                    return true;
                default:
                    return false;
            }
        }

        /// <summary>
        /// Maps an enum's underlying type name (e.g. "int", "byte", "long") to the
        /// corresponding BsonSpanWriter write method name.
        /// </summary>
        private static string GetWriteMethodForUnderlyingType(string underlyingType)
        {
            return underlyingType switch
            {
                "byte" or "sbyte" or "short" or "ushort" or "int" or "uint" => "WriteInt32",
                "long" or "ulong" => "WriteInt64",
                _ => "WriteInt32"
            };
        }

        /// <summary>
        /// Maps a BsonSpanWriter write method (e.g. "WriteString") to the corresponding
        /// array variant (e.g. "WriteArrayString") that uses a raw positional index
        /// instead of the key dictionary.
        /// </summary>
        private static string ToArrayWriteMethod(string writeMethod)
        {
            // "WriteString" -> "WriteArrayString", "WriteInt32" -> "WriteArrayInt32", etc.
            return BLiteConventions.WriteMethodPrefix + "Array" + writeMethod.Substring(BLiteConventions.WriteMethodPrefixLength);
        }

        /// <summary>
        /// Maps an enum's underlying type to the array-variant write method
        /// that uses a raw positional index.
        /// </summary>
        private static string GetArrayWriteMethodForUnderlyingType(string underlyingType)
        {
            return underlyingType switch
            {
                "byte" or "sbyte" or "short" or "ushort" or "int" or "uint" => "WriteArrayInt32",
                "long" or "ulong" => "WriteArrayInt64",
                _ => "WriteArrayInt32"
            };
        }

        /// <summary>
        /// Maps an enum's underlying type name to the corresponding BsonSpanReader read method name.
        /// </summary>
        private static string GetReadMethodForUnderlyingType(string underlyingType)
        {
            return underlyingType switch
            {
                "byte" or "sbyte" or "short" or "ushort" or "int" or "uint" => "ReadInt32",
                "long" or "ulong" => "ReadInt64",
                _ => "ReadInt32"
            };
        }

        // ══════════════════════════════════════════════════════════════════════
        // Filter class generation
        // ══════════════════════════════════════════════════════════════════════

        /// <summary>
        /// Generates a static <c>{Entity}Filter</c> class for the given entity.
        /// Returns <c>null</c> or empty when the entity has no filterable properties.
        ///
        /// For each serializable property the generator emits:
        /// <list type="bullet">
        ///   <item><description>
        ///     If the property has a B-Tree index: factory methods that return
        ///     <c>IndexQueryPlan</c> (B-Tree seek, falling back to
        ///     <c>BsonReaderPredicate</c> when the index is absent at runtime).
        ///   </description></item>
        ///   <item><description>
        ///     Otherwise: factory methods that return a <c>BsonReaderPredicate</c>
        ///     (BSON-level full scan, no reflection, no Expression.Compile).
        ///   </description></item>
        /// </list>
        /// </summary>
        public static string GenerateFilterClass(EntityInfo entity, string filterNamespace)
        {
            // Only generate for entities that have at least one non-key filterable property
            var filterableProps = entity.Properties
                .Where(p => !p.IsCollection && !p.IsNestedObject && !p.IsDictionary)
                .ToList();

            if (filterableProps.Count == 0)
                return string.Empty;

            var sb = new StringBuilder();
            sb.AppendLine("// <auto-generated/>");
            sb.AppendLine("#nullable enable");
            sb.AppendLine("using System;");
            sb.AppendLine("using System.Collections.Generic;");
            sb.AppendLine("using System.Linq;");
            sb.AppendLine("using BLite.Bson;");
            sb.AppendLine("using BLite.Core.Indexing;");
            sb.AppendLine("using BLite.Core.Query;");
            sb.AppendLine();
            sb.AppendLine($"namespace {filterNamespace}");
            sb.AppendLine("{");

            var filterClassName = $"{entity.Name}{BLiteConventions.FilterClassSuffix}";
            sb.AppendLine($"    /// <summary>");
            sb.AppendLine($"    /// AOT-safe, reflection-free typed filter factory for <see cref=\"global::{entity.FullTypeName}\"/>.");
            sb.AppendLine($"    /// Generated by BLite.SourceGenerators from the entity's property schema.");
            sb.AppendLine($"    /// Use <c>ScanAsync({filterClassName}.PropertyOp(value, collection.GetIndexes()))</c>");
            sb.AppendLine($"    /// to execute queries with optional B-Tree index acceleration.");
            sb.AppendLine($"    /// </summary>");
            sb.AppendLine($"    public static class {filterClassName}");
            sb.AppendLine($"    {{");

            // Build a set of indexed property paths for quick lookup
            var indexedPaths = new HashSet<string>(System.StringComparer.OrdinalIgnoreCase);
            foreach (var idx in entity.Indexes)
            {
                if (idx.PropertyPaths.Count == 1)
                    indexedPaths.Add(idx.PropertyPaths[0]);
            }

            foreach (var prop in filterableProps)
            {
                // Skip the primary key — it has its own lookup API (FindAsync)
                if (prop.IsKey) continue;

                bool isIndexed = indexedPaths.Contains(prop.Name);
                bool isString = prop.TypeName == "string" || prop.TypeName == "string?";
                bool isNullable = prop.IsNullable;

                // Determine the raw (non-nullable) CLR type name for value parameters
                var rawType = GetRawTypeName(prop.TypeName);
                var bsonField = prop.BsonFieldName;

                if (isIndexed)
                {
                    EmitIndexedPropertyMethods(sb, prop.Name, rawType, bsonField, isString, isNullable);
                }
                else
                {
                    EmitScanPropertyMethods(sb, prop.Name, rawType, bsonField, isString, isNullable);
                }
            }

            // Emit Resolve helper if there are indexed properties
            if (indexedPaths.Count > 0)
            {
                sb.AppendLine($"        // ── Internal index resolver ──────────────────────────────────────");
                sb.AppendLine($"        private static global::BLite.Core.Query.IndexPlanBuilder? Resolve(");
                sb.AppendLine($"            global::System.Collections.Generic.IEnumerable<global::BLite.Core.Indexing.CollectionIndexInfo> indexes,");
                sb.AppendLine($"            string propertyPath)");
                sb.AppendLine($"        {{");
                sb.AppendLine($"            foreach (var idx in indexes)");
                sb.AppendLine($"                if (idx.Type == global::BLite.Core.Indexing.IndexType.BTree &&");
                sb.AppendLine($"                    idx.PropertyPaths.Length == 1 &&");
                sb.AppendLine($"                    idx.PropertyPaths[0].Equals(propertyPath, global::System.StringComparison.OrdinalIgnoreCase))");
                sb.AppendLine($"                    return new global::BLite.Core.Query.IndexPlanBuilder(idx.Name);");
                sb.AppendLine($"            return null;");
                sb.AppendLine($"        }}");
            }

            sb.AppendLine($"    }}");
            sb.AppendLine($"}}");

            return sb.ToString();
        }

        /// <summary>
        /// Emits filter methods for an indexed property (returns <c>IndexQueryPlan</c>
        /// with fallback to <c>BsonReaderPredicate</c> when the index is absent).
        /// </summary>
        private static void EmitIndexedPropertyMethods(
            StringBuilder sb,
            string propName,
            string rawType,
            string bsonField,
            bool isString,
            bool isNullable)
        {
            var p = propName;
            var f = bsonField;

            sb.AppendLine($"        // ── {p}: indexed (B-Tree) ──────────────────────────────────────────");

            // Eq
            sb.AppendLine($"        public static global::BLite.Core.Query.IndexQueryPlan {p}Eq({rawType} v,");
            sb.AppendLine($"            global::System.Collections.Generic.IEnumerable<global::BLite.Core.Indexing.CollectionIndexInfo> indexes)");
            sb.AppendLine($"            => Resolve(indexes, \"{p}\")?.Exact(global::BLite.Core.Indexing.IndexKey.Create(v))");
            sb.AppendLine($"               ?? global::BLite.Core.Query.IndexQueryPlan.Scan(global::BLite.Core.Query.BsonPredicateBuilder.Eq(\"{f}\", v));");
            sb.AppendLine();

            // Gt
            sb.AppendLine($"        public static global::BLite.Core.Query.IndexQueryPlan {p}Gt({rawType} v,");
            sb.AppendLine($"            global::System.Collections.Generic.IEnumerable<global::BLite.Core.Indexing.CollectionIndexInfo> indexes)");
            sb.AppendLine($"            => Resolve(indexes, \"{p}\")");
            sb.AppendLine($"                   ?.Range(global::BLite.Core.Indexing.IndexKey.Create(v), global::BLite.Core.Indexing.IndexKey.MaxKey)");
            sb.AppendLine($"                    .And(global::BLite.Core.Query.BsonPredicateBuilder.Gt(\"{f}\", v))");
            sb.AppendLine($"               ?? global::BLite.Core.Query.IndexQueryPlan.Scan(global::BLite.Core.Query.BsonPredicateBuilder.Gt(\"{f}\", v));");
            sb.AppendLine();

            // Gte
            sb.AppendLine($"        public static global::BLite.Core.Query.IndexQueryPlan {p}Gte({rawType} v,");
            sb.AppendLine($"            global::System.Collections.Generic.IEnumerable<global::BLite.Core.Indexing.CollectionIndexInfo> indexes)");
            sb.AppendLine($"            => Resolve(indexes, \"{p}\")?.Range(global::BLite.Core.Indexing.IndexKey.Create(v), global::BLite.Core.Indexing.IndexKey.MaxKey)");
            sb.AppendLine($"               ?? global::BLite.Core.Query.IndexQueryPlan.Scan(global::BLite.Core.Query.BsonPredicateBuilder.Gte(\"{f}\", v));");
            sb.AppendLine();

            // Lt
            sb.AppendLine($"        public static global::BLite.Core.Query.IndexQueryPlan {p}Lt({rawType} v,");
            sb.AppendLine($"            global::System.Collections.Generic.IEnumerable<global::BLite.Core.Indexing.CollectionIndexInfo> indexes)");
            sb.AppendLine($"            => Resolve(indexes, \"{p}\")");
            sb.AppendLine($"                   ?.Range(global::BLite.Core.Indexing.IndexKey.MinKey, global::BLite.Core.Indexing.IndexKey.Create(v))");
            sb.AppendLine($"                    .And(global::BLite.Core.Query.BsonPredicateBuilder.Lt(\"{f}\", v))");
            sb.AppendLine($"               ?? global::BLite.Core.Query.IndexQueryPlan.Scan(global::BLite.Core.Query.BsonPredicateBuilder.Lt(\"{f}\", v));");
            sb.AppendLine();

            // Lte
            sb.AppendLine($"        public static global::BLite.Core.Query.IndexQueryPlan {p}Lte({rawType} v,");
            sb.AppendLine($"            global::System.Collections.Generic.IEnumerable<global::BLite.Core.Indexing.CollectionIndexInfo> indexes)");
            sb.AppendLine($"            => Resolve(indexes, \"{p}\")?.Range(global::BLite.Core.Indexing.IndexKey.MinKey, global::BLite.Core.Indexing.IndexKey.Create(v))");
            sb.AppendLine($"               ?? global::BLite.Core.Query.IndexQueryPlan.Scan(global::BLite.Core.Query.BsonPredicateBuilder.Lte(\"{f}\", v));");
            sb.AppendLine();

            // Between
            sb.AppendLine($"        public static global::BLite.Core.Query.IndexQueryPlan {p}Between({rawType} lo, {rawType} hi,");
            sb.AppendLine($"            global::System.Collections.Generic.IEnumerable<global::BLite.Core.Indexing.CollectionIndexInfo> indexes)");
            sb.AppendLine($"            => Resolve(indexes, \"{p}\")?.Range(global::BLite.Core.Indexing.IndexKey.Create(lo), global::BLite.Core.Indexing.IndexKey.Create(hi))");
            sb.AppendLine($"               ?? global::BLite.Core.Query.IndexQueryPlan.Scan(global::BLite.Core.Query.BsonPredicateBuilder.Between(\"{f}\", lo, hi));");
            sb.AppendLine();

            // In
            sb.AppendLine($"        public static global::BLite.Core.Query.IndexQueryPlan {p}In(global::System.Collections.Generic.IEnumerable<{rawType}> vs,");
            sb.AppendLine($"            global::System.Collections.Generic.IEnumerable<global::BLite.Core.Indexing.CollectionIndexInfo> indexes)");
            sb.AppendLine($"            => Resolve(indexes, \"{p}\")?.In(vs.Select(v => global::BLite.Core.Indexing.IndexKey.Create(v)))");
            sb.AppendLine($"               ?? global::BLite.Core.Query.IndexQueryPlan.Scan(global::BLite.Core.Query.BsonPredicateBuilder.In(\"{f}\", vs));");
            sb.AppendLine();

            if (isString)
            {
                // EmitStringPredicateMethods already includes IsNull/IsNotNull
                EmitStringPredicateMethods(sb, p, f);
            }
            else if (isNullable)
            {
                // For non-string nullable indexed types emit null checks separately
                EmitNullCheckMethods(sb, p, f);
            }
        }

        /// <summary>
        /// Emits filter methods for a non-indexed property (always returns <c>BsonReaderPredicate</c>).
        /// </summary>
        private static void EmitScanPropertyMethods(
            StringBuilder sb,
            string propName,
            string rawType,
            string bsonField,
            bool isString,
            bool isNullable)
        {
            var p = propName;
            var f = bsonField;

            sb.AppendLine($"        // ── {p}: unindexed (BSON scan) ──────────────────────────────────────");

            if (isString)
            {
                // EmitStringPredicateMethods includes Eq, Contains, StartsWith, EndsWith, IsNull, IsNotNull
                sb.AppendLine($"        public static global::BLite.Bson.BsonReaderPredicate {p}Eq(string v)");
                sb.AppendLine($"            => global::BLite.Core.Query.BsonPredicateBuilder.Eq(\"{f}\", v);");
                sb.AppendLine();
                EmitStringPredicateMethods(sb, p, f);
            }
            else
            {
                // For non-string nullable types: null checks first
                if (isNullable)
                {
                    EmitNullCheckMethods(sb, p, f);
                }

                // Eq
                sb.AppendLine($"        public static global::BLite.Bson.BsonReaderPredicate {p}Eq({rawType} v)");
                sb.AppendLine($"            => global::BLite.Core.Query.BsonPredicateBuilder.Eq(\"{f}\", v);");
                sb.AppendLine();

                // Gt/Gte/Lt/Lte/Between/In for numeric/comparable types
                sb.AppendLine($"        public static global::BLite.Bson.BsonReaderPredicate {p}Gt({rawType} v)");
                sb.AppendLine($"            => global::BLite.Core.Query.BsonPredicateBuilder.Gt(\"{f}\", v);");
                sb.AppendLine();

                sb.AppendLine($"        public static global::BLite.Bson.BsonReaderPredicate {p}Gte({rawType} v)");
                sb.AppendLine($"            => global::BLite.Core.Query.BsonPredicateBuilder.Gte(\"{f}\", v);");
                sb.AppendLine();

                sb.AppendLine($"        public static global::BLite.Bson.BsonReaderPredicate {p}Lt({rawType} v)");
                sb.AppendLine($"            => global::BLite.Core.Query.BsonPredicateBuilder.Lt(\"{f}\", v);");
                sb.AppendLine();

                sb.AppendLine($"        public static global::BLite.Bson.BsonReaderPredicate {p}Lte({rawType} v)");
                sb.AppendLine($"            => global::BLite.Core.Query.BsonPredicateBuilder.Lte(\"{f}\", v);");
                sb.AppendLine();

                sb.AppendLine($"        public static global::BLite.Bson.BsonReaderPredicate {p}Between({rawType} lo, {rawType} hi)");
                sb.AppendLine($"            => global::BLite.Core.Query.BsonPredicateBuilder.Between(\"{f}\", lo, hi);");
                sb.AppendLine();

                sb.AppendLine($"        public static global::BLite.Bson.BsonReaderPredicate {p}In(global::System.Collections.Generic.IEnumerable<{rawType}> vs)");
                sb.AppendLine($"            => global::BLite.Core.Query.BsonPredicateBuilder.In(\"{f}\", vs);");
                sb.AppendLine();
            }
        }

        private static void EmitStringPredicateMethods(StringBuilder sb, string propName, string bsonField)
        {
            sb.AppendLine($"        public static global::BLite.Bson.BsonReaderPredicate {propName}Contains(string v)");
            sb.AppendLine($"            => global::BLite.Core.Query.BsonPredicateBuilder.Contains(\"{bsonField}\", v);");
            sb.AppendLine();

            sb.AppendLine($"        public static global::BLite.Bson.BsonReaderPredicate {propName}StartsWith(string v)");
            sb.AppendLine($"            => global::BLite.Core.Query.BsonPredicateBuilder.StartsWith(\"{bsonField}\", v);");
            sb.AppendLine();

            sb.AppendLine($"        public static global::BLite.Bson.BsonReaderPredicate {propName}EndsWith(string v)");
            sb.AppendLine($"            => global::BLite.Core.Query.BsonPredicateBuilder.EndsWith(\"{bsonField}\", v);");
            sb.AppendLine();

            sb.AppendLine($"        public static global::BLite.Bson.BsonReaderPredicate {propName}IsNull()");
            sb.AppendLine($"            => global::BLite.Core.Query.BsonPredicateBuilder.IsNull(\"{bsonField}\");");
            sb.AppendLine();

            sb.AppendLine($"        public static global::BLite.Bson.BsonReaderPredicate {propName}IsNotNull()");
            sb.AppendLine($"            => global::BLite.Core.Query.BsonPredicateBuilder.IsNotNull(\"{bsonField}\");");
            sb.AppendLine();
        }

        private static void EmitNullCheckMethods(StringBuilder sb, string propName, string bsonField)
        {
            sb.AppendLine($"        public static global::BLite.Bson.BsonReaderPredicate {propName}IsNull()");
            sb.AppendLine($"            => global::BLite.Core.Query.BsonPredicateBuilder.IsNull(\"{bsonField}\");");
            sb.AppendLine();

            sb.AppendLine($"        public static global::BLite.Bson.BsonReaderPredicate {propName}HasValue()");
            sb.AppendLine($"            => global::BLite.Core.Query.BsonPredicateBuilder.IsNotNull(\"{bsonField}\");");
            sb.AppendLine();
        }

        /// <summary>
        /// Strips nullable annotation and returns the raw CLR type name suitable for
        /// use as a method parameter in the generated filter class.
        /// </summary>
        private static string GetRawTypeName(string typeName)
        {
            // Strip trailing '?' for nullable reference / value types
            var raw = typeName.TrimEnd('?');

            // Map common full-framework names to C# keywords where possible
            return raw switch
            {
                "System.Boolean" or "Boolean"   => "bool",
                "System.Int32"   or "Int32"     => "int",
                "System.Int64"   or "Int64"     => "long",
                "System.Double"  or "Double"    => "double",
                "System.Single"  or "Single"    => "float",
                "System.Decimal" or "Decimal"   => "decimal",
                "System.String"  or "String"    => "string",
                _ => raw
            };
        }
    }
}
