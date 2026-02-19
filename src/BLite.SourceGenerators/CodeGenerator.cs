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
            var isRoot = entity.IdProperty != null;
            
            // Class Declaration
            if (isRoot)
            {
                var baseClass = GetBaseMapperClass(keyProp);
                // Ensure FullTypeName has global:: prefix if not already present (assuming FullTypeName is fully qualified)
                var entityType = $"global::{entity.FullTypeName}";
                sb.AppendLine($"    public class {mapperName} : global::BLite.Core.Collections.{baseClass}{entityType}>");
            }
            else
            {
                sb.AppendLine($"    public class {mapperName}");
            }
            
            sb.AppendLine($"    {{");
            
            // Converter instance
            if (keyProp?.ConverterTypeName != null)
            {
                sb.AppendLine($"        private readonly global::{keyProp.ConverterTypeName} _idConverter = new();");
                sb.AppendLine();
            }

            // Generate static setters for private properties (Expression Trees)
            var privateSetterProps = entity.Properties.Where(p => (!p.HasPublicSetter && p.HasAnySetter) || p.HasInitOnlySetter).ToList();
            if (privateSetterProps.Any())
            {
                sb.AppendLine($"        // Cached Expression Tree setters for private properties");
                foreach (var prop in privateSetterProps)
                {
                    var entityType = $"global::{entity.FullTypeName}";
                    var propType = QualifyType(prop.TypeName);
                    sb.AppendLine($"        private static readonly global::System.Action<{entityType}, {propType}> _setter_{prop.Name} = CreateSetter<{entityType}, {propType}>(\"{prop.Name}\");");
                }
                sb.AppendLine();
                
                sb.AppendLine($"        private static global::System.Action<TObj, TVal> CreateSetter<TObj, TVal>(string propertyName)");
                sb.AppendLine($"        {{");
                sb.AppendLine($"            var param = global::System.Linq.Expressions.Expression.Parameter(typeof(TObj), \"obj\");");
                sb.AppendLine($"            var value = global::System.Linq.Expressions.Expression.Parameter(typeof(TVal), \"val\");");
                sb.AppendLine($"            var prop = global::System.Linq.Expressions.Expression.Property(param, propertyName);");
                sb.AppendLine($"            var assign = global::System.Linq.Expressions.Expression.Assign(prop, value);");
                sb.AppendLine($"            return global::System.Linq.Expressions.Expression.Lambda<global::System.Action<TObj, TVal>>(assign, param, value).Compile();");
                sb.AppendLine($"        }}");
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
            
            // For nested mappers, generate SerializeFields first (writes only fields, no document wrapper)
            // Note: BsonSpanWriter is a ref struct, so it must be passed by ref
            if (!isRoot)
            {
                sb.AppendLine($"        public void SerializeFields({entityType} entity, ref global::BLite.Bson.BsonSpanWriter writer)");
                sb.AppendLine($"        {{");
                GenerateFieldWritesCore(sb, entity, mapperNamespace);
                sb.AppendLine($"        }}");
                sb.AppendLine();
            }
            
            // Generate Serialize method (with document wrapper)
            var methodSig = isRoot 
                ? $"public override int Serialize({entityType} entity, global::BLite.Bson.BsonSpanWriter writer)"
                : $"public int Serialize({entityType} entity, global::BLite.Bson.BsonSpanWriter writer)";

            sb.AppendLine($"        {methodSig}");
            sb.AppendLine($"        {{");
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
                if (prop.IsKey)
                {
                    if (prop.ConverterTypeName != null)
                    {
                        var providerProp = new PropertyInfo { TypeName = prop.ProviderTypeName ?? "string" };
                        var idWriteMethod = GetPrimitiveWriteMethod(providerProp, allowKey: true);
                        sb.AppendLine($"            writer.{idWriteMethod}(\"_id\", _idConverter.ConvertToProvider(entity.{prop.Name}));");
                    }
                    else
                    {
                        var idWriteMethod = GetPrimitiveWriteMethod(prop, allowKey: true);
                        if (idWriteMethod != null)
                        {
                            sb.AppendLine($"            writer.{idWriteMethod}(\"_id\", entity.{prop.Name});");
                        }
                        else
                        {
                            sb.AppendLine($"#warning Unsupported Id type for '{prop.Name}': {prop.TypeName}. Serialization of '_id' will fail.");
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
                 var arrayVar = $"{prop.Name.ToLower()}Array";
                 sb.AppendLine($"            var {arrayVar}Pos = writer.BeginArray(\"{fieldName}\");");
                 sb.AppendLine($"            var {prop.Name.ToLower()}Index = 0;");
                 sb.AppendLine($"            foreach (var item in entity.{prop.Name})");
                 sb.AppendLine($"            {{");
                 
                 
                 if (prop.IsCollectionItemNested)
                 {
                     sb.AppendLine($"                // Nested Object in List");
                     var nestedMapperTypes = GetMapperName(prop.NestedTypeFullName!);
                     sb.AppendLine($"                var {prop.Name.ToLower()}ItemMapper = new global::{mapperNamespace}.{nestedMapperTypes}();");
                     
                     sb.AppendLine($"                var itemStartPos = writer.BeginDocument({prop.Name.ToLower()}Index.ToString());");
                     sb.AppendLine($"                {prop.Name.ToLower()}ItemMapper.SerializeFields(item, ref writer);");
                     sb.AppendLine($"                writer.EndDocument(itemStartPos);");
                 }
                 else
                 {
                     // Simplified: pass a dummy PropertyInfo with the item type for primitive collection items
                     var dummyProp = new PropertyInfo { TypeName = prop.CollectionItemType! };
                     var writeMethod = GetPrimitiveWriteMethod(dummyProp);
                     if (writeMethod != null)
                     {
                        sb.AppendLine($"                writer.{writeMethod}({prop.Name.ToLower()}Index.ToString(), item);");
                     }
                 }
                 sb.AppendLine($"                {prop.Name.ToLower()}Index++;");
                 
                 sb.AppendLine($"            }}");
                 sb.AppendLine($"            writer.EndArray({arrayVar}Pos);");
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
            else
            {
                var writeMethod = GetPrimitiveWriteMethod(prop, allowKey: false);
                if (writeMethod != null)
                {
                    if (prop.IsNullable || prop.TypeName == "string" || prop.TypeName == "String")
                    {
                        sb.AppendLine($"            if (entity.{prop.Name} != null)");
                        sb.AppendLine($"            {{");
                        sb.AppendLine($"                writer.{writeMethod}(\"{fieldName}\", entity.{prop.Name});");
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
             var needsReflection = entity.HasPrivateSetters || entity.HasPrivateOrNoConstructor;
             
             // Note: BsonSpanReader is a ref struct, so nested mappers must use ref
             var methodSig = isRoot 
                ? $"public override {entityType} Deserialize(global::BLite.Bson.BsonSpanReader reader)"
                : $"public {entityType} Deserialize(ref global::BLite.Bson.BsonSpanReader reader)";
                
            sb.AppendLine($"        {methodSig}");
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
                else
                {
                    sb.AppendLine($"            {baseType}? {prop.Name.ToLower()} = default;");
                }
            }
            
            
            // Read document size and track boundaries
            sb.AppendLine($"            var docSize = reader.ReadDocumentSize();");
            sb.AppendLine($"            var docEndPos = reader.Position + docSize - 4; // -4 because size includes itself");
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
                var caseName = prop.IsKey ? "_id" : prop.BsonFieldName;
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
            
            // Construct object - different approach if needs reflection
            if (needsReflection)
            {
                // Use GetUninitializedObject + Expression Trees for private setters
                sb.AppendLine($"            // Creating instance without calling constructor (has private members)");
                sb.AppendLine($"            var entity = (global::{entity.FullTypeName})global::System.Runtime.Serialization.FormatterServices.GetUninitializedObject(typeof(global::{entity.FullTypeName}));");
                sb.AppendLine();
                
                // Set properties using setters (Expression Trees for private, direct for public)
                foreach(var prop in entity.Properties)
                {
                    var varName = prop.Name.ToLower();
                    var propValue = varName;
                    
                    if (prop.IsCollection)
                    {
                        // Convert to appropriate collection type
                        if (prop.IsArray)
                        {
                            propValue += ".ToArray()";
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
                    if ((!prop.HasPublicSetter && prop.HasAnySetter) || prop.HasInitOnlySetter)
                    {
                        // Use Expression Tree setter (for private or init-only setters)
                        sb.AppendLine($"            _setter_{prop.Name}(entity, {propValue} ?? default!);");
                    }
                    else
                    {
                        // Direct property assignment
                        sb.AppendLine($"            entity.{prop.Name} = {propValue} ?? default!;");
                    }
                }
                sb.AppendLine();
                sb.AppendLine($"            return entity;");
            }
            else
            {
                // Standard object initializer approach
                sb.AppendLine($"            return new {entityType}");
                sb.AppendLine($"            {{");
                foreach(var prop in entity.Properties)
                {
                    var val = prop.Name.ToLower();
                    if (prop.IsCollection)
                    {
                        // Convert to appropriate collection type
                        if (prop.IsArray)
                        {
                            val += ".ToArray()";
                        }
                        else if (prop.CollectionConcreteTypeName != null)
                        {
                            var concreteType = prop.CollectionConcreteTypeName;
                            var itemType = prop.IsCollectionItemNested ? $"global::{prop.NestedTypeFullName}" : prop.CollectionItemType;
                            
                            // Check if it needs conversion from List
                            if (concreteType.Contains("HashSet"))
                            {
                                val = $"new global::System.Collections.Generic.HashSet<{itemType}>({val})";
                            }
                            else if (concreteType.Contains("ISet"))
                            {
                                val = $"new global::System.Collections.Generic.HashSet<{itemType}>({val})";
                            }
                            else if (concreteType.Contains("LinkedList"))
                            {
                                val = $"new global::System.Collections.Generic.LinkedList<{itemType}>({val})";
                            }
                            else if (concreteType.Contains("Queue"))
                            {
                                val = $"new global::System.Collections.Generic.Queue<{itemType}>({val})";
                            }
                            else if (concreteType.Contains("Stack"))
                            {
                                val = $"new global::System.Collections.Generic.Stack<{itemType}>({val})";
                            }
                            else if (concreteType.Contains("IReadOnlyList") || concreteType.Contains("IReadOnlyCollection"))
                            {
                                val += ".AsReadOnly()";
                            }
                            // Otherwise keep as List (works for List<T>, IList<T>, ICollection<T>, IEnumerable<T>)
                        }
                    }
                    sb.AppendLine($"                {prop.Name} = {val} ?? default!,");
                }
                sb.AppendLine($"            }};");
            }
            sb.AppendLine($"        }}");
        }

        private static void GenerateReadPropertyToLocal(StringBuilder sb, PropertyInfo prop, string bsonTypeVar, string mapperNamespace)
        {
             var localVar = prop.Name.ToLower();
             
             if (prop.IsCollection)
             {
                 var arrVar = prop.Name.ToLower();
                 sb.AppendLine($"                        // Read Array {prop.Name}");
                 sb.AppendLine($"                        var {arrVar}ArrSize = reader.ReadDocumentSize();");
                 sb.AppendLine($"                        var {arrVar}ArrEndPos = reader.Position + {arrVar}ArrSize - 4;");
                 sb.AppendLine($"                        while (reader.Position < {arrVar}ArrEndPos)");
                 sb.AppendLine($"                        {{");
                 sb.AppendLine($"                            var itemType = reader.ReadBsonType();");
                 sb.AppendLine($"                            if (itemType == global::BLite.Bson.BsonType.EndOfDocument) break;");
                 sb.AppendLine($"                            reader.ReadElementHeader(); // Skip index key");
                 
                 if (prop.IsCollectionItemNested)
                 {
                     var nestedMapperTypes = GetMapperName(prop.NestedTypeFullName!);
                     sb.AppendLine($"                            var {prop.Name.ToLower()}ItemMapper = new global::{mapperNamespace}.{nestedMapperTypes}();");
                     sb.AppendLine($"                            var item = {prop.Name.ToLower()}ItemMapper.Deserialize(ref reader);");
                     sb.AppendLine($"                            {localVar}.Add(item);");
                 }
                 else
                 {
                     var readMethod = GetPrimitiveReadMethod(new PropertyInfo { TypeName = prop.CollectionItemType! });
                      if (readMethod != null)
                      {
                          var cast = (prop.CollectionItemType == "float" || prop.CollectionItemType == "Single") ? "(float)" : "";
                          sb.AppendLine($"                            var item = {cast}reader.{readMethod}();");
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
                 sb.AppendLine($"                        {localVar} = _idConverter.ConvertFromProvider(reader.{readMethod}());");
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
             else
             {
                 var readMethod = GetPrimitiveReadMethod(prop);
                 if (readMethod != null)
                 {
                    var cast = (prop.TypeName == "float" || prop.TypeName == "Single") ? "(float)" : "";
                    sb.AppendLine($"                        {localVar} = {cast}reader.{readMethod}();");
                 }
                 else
                 {
                     sb.AppendLine($"                        reader.SkipValue({bsonTypeVar});");
                 }
             }
        }

        public static string GetMapperName(string fullTypeName)
        {
             if (string.IsNullOrEmpty(fullTypeName)) return "UnknownMapper";
             // Remove global:: prefix
             var cleanName = fullTypeName.Replace("global::", "");
             // Replace dots, plus (nested classes), and colons (global::) with underscores
             return cleanName.Replace(".", "_").Replace("+", "_").Replace(":", "_") + "Mapper";
        }

        private static void GenerateIdAccessors(StringBuilder sb, EntityInfo entity)
        {
            var keyProp = entity.Properties.FirstOrDefault(p => p.IsKey);
            var keyType = keyProp?.TypeName ?? "ObjectId";
            
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
            var qualifiedKeyType = keyType.StartsWith("global::") ? keyType : (keyProp?.ConverterTypeName != null ? $"global::{keyProp.TypeName}" : keyType);
            
            var propName = keyProp?.Name ?? "Id";
            sb.AppendLine($"        public override {qualifiedKeyType} GetId({entityType} entity) => entity.{propName};");
            
            // If the ID property has a private or init-only setter, use the compiled setter
            if (entity.HasPrivateSetters && keyProp != null && (!keyProp.HasPublicSetter || keyProp.HasInitOnlySetter))
            {
                sb.AppendLine($"        public override void SetId({entityType} entity, {qualifiedKeyType} id) => _setter_{propName}(entity, id);");
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

        private static string GetBaseMapperClass(PropertyInfo? keyProp)
        {
            if (keyProp?.ConverterTypeName != null)
            {
                return $"DocumentMapperBase<global::{keyProp.TypeName}, ";
            }

            var keyType = keyProp?.TypeName ?? "ObjectId";
            if (keyType.EndsWith("Int32") || keyType == "int") return "Int32MapperBase<";
            if (keyType.EndsWith("Int64") || keyType == "long") return "Int64MapperBase<";
            if (keyType.EndsWith("String") || keyType == "string") return "StringMapperBase<";
            if (keyType.EndsWith("Guid")) return "GuidMapperBase<";
            if (keyType.EndsWith("ObjectId")) return "ObjectIdMapperBase<";

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
            if (cleanType.EndsWith("DateTime")) return "WriteDateTime";
            if (cleanType.EndsWith("Guid")) return "WriteGuid";
            if (cleanType.EndsWith("ObjectId")) return "WriteObjectId";

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

            if (cleanType.EndsWith("Int32") || cleanType == "int") return "ReadInt32";
            if (cleanType.EndsWith("Int64") || cleanType == "long") return "ReadInt64";
            if (cleanType.EndsWith("String") || cleanType == "string") return "ReadString";
            if (cleanType.EndsWith("Boolean") || cleanType == "bool") return "ReadBoolean";
            if (typeName.EndsWith("Single") || typeName == "float") return "ReadDouble";
            if (typeName.EndsWith("Double") || typeName == "double") return "ReadDouble";
            if (typeName.EndsWith("Decimal") || typeName == "decimal") return "ReadDecimal128";
            if (typeName.EndsWith("DateTime")) return "ReadDateTime";
            if (typeName.EndsWith("Guid")) return "ReadGuid";
            if (typeName.EndsWith("ObjectId")) return "ReadObjectId";

            return null;
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
                    return baseType + (isNullable ? "?" : "");
                case "Guid": return "global::System.Guid" + (isNullable ? "?" : "");
                case "DateTime": return "global::System.DateTime" + (isNullable ? "?" : "");
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
    }
}
