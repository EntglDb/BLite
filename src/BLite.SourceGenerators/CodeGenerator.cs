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
            var isRoot = entity.IdProperty != null;
            
            // Class Declaration
            if (isRoot)
            {
                var keyProp = entity.Properties.FirstOrDefault(p => p.IsKey);
                var keyType = keyProp?.TypeName ?? "ObjectId";
                var baseClass = GetBaseMapperClass(keyType);
                // Ensure FullTypeName has global:: prefix if not already present (assuming FullTypeName is fully qualified)
                var entityType = $"global::{entity.FullTypeName}";
                sb.AppendLine($"    public class {mapperName} : global::BLite.Core.Collections.{baseClass}<{entityType}>");
            }
            else
            {
                sb.AppendLine($"    public class {mapperName}");
            }
            
            sb.AppendLine($"    {{");
            
            // Collection Name (only for root)
            if (isRoot)
            {
                sb.AppendLine($"        public override string CollectionName => \"{entity.CollectionName}\";");
                sb.AppendLine();
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
            var methodSig = isRoot 
                ? $"public override int Serialize({entityType} entity, global::BLite.Bson.BsonSpanWriter writer)"
                : $"public int Serialize({entityType} entity, global::BLite.Bson.BsonSpanWriter writer)";

            sb.AppendLine($"        {methodSig}");
            sb.AppendLine($"        {{");
            sb.AppendLine($"            var startingPos = writer.BeginDocument();");
            sb.AppendLine();
            
            foreach (var prop in entity.Properties)
            {
                if (prop.IsKey && prop.Name == "Id")
                {
                    // Use dynamic write method for Id based on its type
                    var idWriteMethod = GetPrimitiveWriteMethod(prop, allowKey: true);
                    if (idWriteMethod != null)
                    {
                        sb.AppendLine($"            writer.{idWriteMethod}(\"_id\", entity.Id);");
                    }
                    else
                    {
                         sb.AppendLine($"            // Unsupported Id type: {prop.TypeName}");
                    }
                    continue;
                }
                
                GenerateValidation(sb, prop);
                GenerateWriteProperty(sb, prop, mapperNamespace);
            }
            
            sb.AppendLine();
            sb.AppendLine($"            writer.EndDocument(startingPos);");
            sb.AppendLine($"            return writer.Position;");
            sb.AppendLine($"        }}");
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
                 var countSource = prop.IsArray ? "Length" : "Count";
                 sb.AppendLine($"            for (int i = 0; i < entity.{prop.Name}.{countSource}; i++)");
                 sb.AppendLine($"            {{");
                 sb.AppendLine($"                var item = entity.{prop.Name}[i];");
                 
                 if (prop.IsCollectionItemNested)
                 {
                     sb.AppendLine($"                // Nested Object in List");
                     var nestedMapperTypes = GetMapperName(prop.NestedTypeFullName!);
                     sb.AppendLine($"                var {prop.Name.ToLower()}ItemMapper = new global::{mapperNamespace}.{nestedMapperTypes}();");
                     
                     sb.AppendLine($"                var itemStartPos = writer.BeginDocument(i.ToString());");
                     sb.AppendLine($"                {prop.Name.ToLower()}ItemMapper.Serialize(item, writer);");
                     sb.AppendLine($"                writer.EndDocument(itemStartPos);");
                 }
                 else
                 {
                     // Simplified: pass a dummy PropertyInfo with the item type for primitive collection items
                     var dummyProp = new PropertyInfo { TypeName = prop.CollectionItemType! };
                     var writeMethod = GetPrimitiveWriteMethod(dummyProp);
                     if (writeMethod != null)
                     {
                        sb.AppendLine($"                writer.{writeMethod}(i.ToString(), item);");
                     }
                 }
                 
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
                sb.AppendLine($"                {prop.Name.ToLower()}Mapper.Serialize(entity.{prop.Name}, writer);");
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
                    sb.AppendLine($"            writer.{writeMethod}(\"{fieldName}\", entity.{prop.Name});");
                }
                else
                {
                    sb.AppendLine($"            // Unsupported type: {prop.TypeName} for {prop.Name}");
                }
            }
        }

        private static void GenerateDeserializeMethod(StringBuilder sb, EntityInfo entity, bool isRoot, string mapperNamespace)
        {
             var entityType = $"global::{entity.FullTypeName}";
             var methodSig = isRoot 
                ? $"public override {entityType} Deserialize(global::BLite.Bson.BsonSpanReader reader)"
                : $"public {entityType} Deserialize(global::BLite.Bson.BsonSpanReader reader)";
                
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
                var type = prop.IsKey ? (prop.TypeName == "ObjectId" ? "global::BLite.Bson.ObjectId" : prop.TypeName) : prop.TypeName;
                if (prop.TypeName == "Guid") type = "global::System.Guid";
                if (prop.TypeName == "DateTime") type = "global::System.DateTime";
                 // Handle collections init
                if (prop.IsCollection)
                {
                    var itemType = prop.CollectionItemType;
                    if (prop.IsCollectionItemNested) itemType = $"global::{prop.NestedTypeFullName}"; // Use full name with global::
                     sb.AppendLine($"            var {prop.Name.ToLower()} = new global::System.Collections.Generic.List<{itemType}>();");
                }
                else
                {
                    sb.AppendLine($"            {type} {prop.Name.ToLower()} = default;");
                }
            }
            
            sb.AppendLine($"            reader.ReadDocumentSize();");
            sb.AppendLine();
            sb.AppendLine($"            while (reader.Remaining > 0)");
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
            
            // Construct object using object initializer to satisfy 'required'
            sb.AppendLine($"            return new {entityType}");
            sb.AppendLine($"            {{");
            foreach(var prop in entity.Properties)
            {
                var val = prop.Name.ToLower();
                if (prop.IsArray) val += ".ToArray()"; // Simplified: convert list to array
                sb.AppendLine($"                {prop.Name} = {val},");
            }
            sb.AppendLine($"            }};");
            sb.AppendLine($"        }}");
        }

        private static void GenerateReadPropertyToLocal(StringBuilder sb, PropertyInfo prop, string bsonTypeVar, string mapperNamespace)
        {
             var localVar = prop.Name.ToLower();
             
             if (prop.IsCollection)
             {
                 sb.AppendLine($"                        // Read Array {prop.Name}");
                 sb.AppendLine($"                        reader.ReadDocumentSize();");
                 sb.AppendLine($"                        while (reader.Remaining > 0)");
                 sb.AppendLine($"                        {{");
                 sb.AppendLine($"                            var itemType = reader.ReadBsonType();");
                 sb.AppendLine($"                            if (itemType == global::BLite.Bson.BsonType.EndOfDocument) break;");
                 sb.AppendLine($"                            reader.ReadElementHeader(); // Skip index key");
                 
                 if (prop.IsCollectionItemNested)
                 {
                     var nestedMapperTypes = GetMapperName(prop.NestedTypeFullName!);
                     sb.AppendLine($"                            var {prop.Name.ToLower()}ItemMapper = new global::{mapperNamespace}.{nestedMapperTypes}();");
                     sb.AppendLine($"                            var item = {prop.Name.ToLower()}ItemMapper.Deserialize(reader);");
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
                 sb.AppendLine($"                            {localVar} = {prop.Name.ToLower()}Mapper.Deserialize(reader);");
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
            sb.AppendLine($"        public override {keyType} GetId({entityType} entity) => entity.Id;");
            sb.AppendLine($"        public override void SetId({entityType} entity, {keyType} id) => entity.Id = id;");
        }

        private static string GetBaseMapperClass(string keyType)
        {
            switch (keyType)
            {
                case "int": 
                case "Int32": return "Int32MapperBase";
                case "long": 
                case "Int64": return "Int64MapperBase";
                case "string": 
                case "String": return "StringMapperBase";
                case "Guid": return "GuidMapperBase";
                case "ObjectId": return "ObjectIdMapperBase";
                default: return "ObjectIdMapperBase"; 
            }
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
                // Likely a (double, double) tuple - use specialized WriteCoordinates
                return "WriteCoordinates";
            }

            switch (typeName)
            {
                case "int": 
                case "Int32": return "WriteInt32";
                case "long": 
                case "Int64": return "WriteInt64";
                case "string": 
                case "String": return "WriteString";
                case "bool": 
                case "Boolean": return "WriteBoolean";
                case "double": 
                case "Double": return "WriteDouble";
                case "float":
                case "Single": return "WriteDouble"; // Map float to double in BSON
                case "decimal": 
                case "Decimal": return "WriteDecimal128"; // Corrected Name
                case "DateTime": return "WriteDateTime";
                case "Guid": return "WriteGuid"; // We need to Ensure WriteGuid exists or map to String/Binary
                case "ObjectId": return allowKey ? "WriteObjectId" : "WriteObjectId"; 
                default: return null;
            }
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
                // Likely a (double, double) tuple - use specialized ReadCoordinates
                return "ReadCoordinates";
            }

             switch (typeName)
            {
                case "int": 
                case "Int32": return "ReadInt32";
                case "long": 
                case "Int64": return "ReadInt64";
                case "string": 
                case "String": return "ReadString";
                case "bool": 
                case "Boolean": return "ReadBoolean";
                case "double": 
                case "Double": return "ReadDouble";
                case "float":
                case "Single": return "ReadDouble"; // Map float to double in BSON
                case "decimal": 
                case "Decimal": return "ReadDecimal128"; // Corrected Name
                case "DateTime": return "ReadDateTime";
                case "Guid": return "ReadGuid"; // We need to Ensure ReadGuid exists
                case "ObjectId": return "ReadObjectId";
                default: return null;
            }
        }
    }
}
