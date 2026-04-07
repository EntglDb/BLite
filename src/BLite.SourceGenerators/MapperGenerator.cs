using System.Collections.Generic;
using System.Linq;
using System.Text;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using BLite.SourceGenerators.Helpers;
using BLite.SourceGenerators.Models;

namespace BLite.SourceGenerators
{
public class DbContextInfo
{
    public string ClassName { get; set; } = "";
    public string FullClassName => string.IsNullOrEmpty(Namespace) ? ClassName : $"{Namespace}.{ClassName}";
    public string Namespace { get; set; } = "";
    public string FilePath { get; set; } = "";
    public bool IsNested { get; set; }
    public bool IsPartial { get; set; }
    public bool HasBaseDbContext { get; set; } // True if inherits from another DbContext (not DocumentDbContext directly)
    public List<EntityInfo> Entities { get; set; } = new List<EntityInfo>();
    public Dictionary<string, NestedTypeInfo> GlobalNestedTypes { get; set; } = new Dictionary<string, NestedTypeInfo>();
    public List<BLiteDiagnostic> Diagnostics { get; } = new List<BLiteDiagnostic>();
}

public readonly struct BLiteDiagnostic
{
    public readonly string Id;
    public readonly string Message;
    public readonly bool IsError;
    public BLiteDiagnostic(string id, string message, bool isError) { Id = id; Message = message; IsError = isError; }
}

    [Generator]
    public class MapperGenerator : IIncrementalGenerator
    {
        // ── Diagnostic descriptors ─────────────────────────────────────────────
        private static readonly DiagnosticDescriptor DiagError = new DiagnosticDescriptor(
            "BLITE001", "BLite Generator Error", "{0}",
            "BLite.SourceGenerators", DiagnosticSeverity.Error, isEnabledByDefault: true);

        private static readonly DiagnosticDescriptor DiagWarning = new DiagnosticDescriptor(
            "BLITE002", "BLite Generator Warning", "{0}",
            "BLite.SourceGenerators", DiagnosticSeverity.Warning, isEnabledByDefault: true);

        private static readonly DiagnosticDescriptor DiagDuplicateCollection = new DiagnosticDescriptor(
            "BLITE003", "BLite Duplicate Collection Name", "{0}",
            "BLite.SourceGenerators", DiagnosticSeverity.Error, isEnabledByDefault: true);

        private static readonly DiagnosticDescriptor DiagUnresolvableConstructor = new DiagnosticDescriptor(
            "BLITE010", "BLite Unresolvable Constructor", "{0}",
            "BLite.SourceGenerators", DiagnosticSeverity.Warning, isEnabledByDefault: true);

        public void Initialize(IncrementalGeneratorInitializationContext context)
        {
            // Find all classes that inherit from DocumentDbContext
            var dbContextClasses = context.SyntaxProvider
                .CreateSyntaxProvider(
                    predicate: static (node, _) => IsPotentialDbContext(node),
                    transform: static (ctx, _) => GetDbContextInfo(ctx))
                .Where(static context => context is not null)
                .Collect()
                .SelectMany(static (contexts, _) => contexts.GroupBy(c => c!.FullClassName).Select(g => g.First())!);
            
            // Generate code for each DbContext
            context.RegisterSourceOutput(dbContextClasses, static (spc, dbContext) =>
            {
                if (dbContext == null) return;

                // ── Emit collected diagnostics ─────────────────────────────────
                foreach (var diag in dbContext.Diagnostics)
                {
                    var descriptor = diag.Id == "BLITE010" ? DiagUnresolvableConstructor
                                   : diag.Id == "BLITE003" ? DiagDuplicateCollection
                                   : diag.IsError ? DiagError
                                   : DiagWarning;
                    spc.ReportDiagnostic(Diagnostic.Create(descriptor, Location.None, diag.Message));
                }

                var sb = new StringBuilder();
                sb.AppendLine($"// Found DbContext: {dbContext.ClassName}");
                sb.AppendLine($"// BaseType: {(dbContext.HasBaseDbContext ? "inherits from another DbContext" : "inherits from DocumentDbContext directly")}");
                
                
                foreach (var entity in dbContext.Entities)
                {
                    // Aggregate nested types recursively
                    CollectNestedTypes(entity.NestedTypes, dbContext.GlobalNestedTypes);
                }

                // Collect namespaces
                var namespaces = new HashSet<string>
                {
                    "System",
                    "System.Collections.Generic",
                    "BLite.Bson",
                    "BLite.Core.Collections"
                };

                // Add Entity namespaces
                foreach (var entity in dbContext.Entities)
                {
                    if (!string.IsNullOrEmpty(entity.Namespace))
                        namespaces.Add(entity.Namespace);
                }
                foreach (var nested in dbContext.GlobalNestedTypes.Values)
                {
                    if (!string.IsNullOrEmpty(nested.Namespace))
                        namespaces.Add(nested.Namespace);
                }

                // Sanitize file path for name uniqueness
                var safeName = dbContext.ClassName;
                if (!string.IsNullOrEmpty(dbContext.FilePath))
                {
                    var fileName = System.IO.Path.GetFileNameWithoutExtension(dbContext.FilePath);
                    safeName += $"_{fileName}";
                }

                sb.AppendLine("// <auto-generated/>");
                sb.AppendLine("#nullable enable");
                foreach (var ns in namespaces.OrderBy(n => n))
                {
                    sb.AppendLine($"using {ns};");
                }
                sb.AppendLine();

                // Use safeName (Context + Filename) to avoid collisions
                var mapperNamespace = $"{dbContext.Namespace}.{safeName}_Mappers";
                sb.AppendLine($"namespace {mapperNamespace}");
                sb.AppendLine($"{{");

                var generatedMappers = new HashSet<string>();

                // Generate Entity Mappers
                foreach (var entity in dbContext.Entities)
                {
                    if (generatedMappers.Add(entity.FullTypeName))
                    {
                        sb.AppendLine(CodeGenerator.GenerateMapperClass(entity, mapperNamespace));
                    }
                }

                // Generate Nested Mappers
                foreach (var nested in dbContext.GlobalNestedTypes.Values)
                {
                    if (generatedMappers.Add(nested.FullTypeName))
                    {
                        var nestedEntity = new EntityInfo 
                        { 
                            Name = nested.Name, 
                            Namespace = nested.Namespace,
                            FullTypeName = nested.FullTypeName,
                            IsNestedTypeMapper = true,
                            HasPrivateOrNoConstructor = nested.HasPrivateOrNoConstructor,
                            HasPrivateSetters = nested.HasPrivateSetters,
                            SelectedConstructorParameters = nested.SelectedConstructorParameters,
                            SelectedConstructorIsPublic = nested.SelectedConstructorIsPublic,
                        };
                        nestedEntity.Properties.AddRange(nested.Properties);
                        
                        sb.AppendLine(CodeGenerator.GenerateMapperClass(nestedEntity, mapperNamespace));
                    }
                }

                sb.AppendLine($"}}");
                sb.AppendLine();

                // Partial DbContext for InitializeCollections (Only for top-level partial classes)
                if (!dbContext.IsNested && dbContext.IsPartial)
                {
                    sb.AppendLine($"namespace {dbContext.Namespace}");
                    sb.AppendLine($"{{");
                    sb.AppendLine($"    public partial class {dbContext.ClassName}");
                    sb.AppendLine($"    {{");
                    sb.AppendLine($"        protected override void InitializeCollections()");
                    sb.AppendLine($"        {{");
                    
                    // Call base.InitializeCollections() if this context inherits from another DbContext
                    if (dbContext.HasBaseDbContext)
                    {
                        sb.AppendLine($"            base.InitializeCollections();");
                    }
                    
                    foreach(var entity in dbContext.Entities)
                    {
                        if (!string.IsNullOrEmpty(entity.CollectionPropertyName))
                        {
                            var mapperName = $"global::{mapperNamespace}.{CodeGenerator.GetMapperName(entity.FullTypeName)}";
                            if (entity.CollectionPropertyIsInterface)
                            {
                                // Property is typed as IDocumentCollection<TId,T> — direct assignment
                                sb.AppendLine($"            this.{entity.CollectionPropertyName} = CreateCollection(new {mapperName}());");
                            }
                            else
                            {
                                // Property is typed as DocumentCollection<TId,T> — needs downcast
                                sb.AppendLine($"            this.{entity.CollectionPropertyName} = (global::BLite.Core.Collections.DocumentCollection<{entity.CollectionIdTypeFullName}, global::{entity.FullTypeName}>)(object)CreateCollection(new {mapperName}());");
                            }
                        }
                    }
                    
                    sb.AppendLine($"        }}");
                    sb.AppendLine();

                    // Generate Set<TId, T>() override
                    var collectionsWithProperties = dbContext.Entities
                        .Where(e => !string.IsNullOrEmpty(e.CollectionPropertyName) && !string.IsNullOrEmpty(e.CollectionIdTypeFullName))
                        .ToList();

                    if (collectionsWithProperties.Any())
                    {
                        sb.AppendLine($"        public override global::BLite.Core.Collections.IDocumentCollection<TId, T> Set<TId, T>()");
                        sb.AppendLine($"        {{");

                        foreach (var entity in collectionsWithProperties)
                        {
                            var entityTypeStr = $"global::{entity.FullTypeName}";
                            var idTypeStr = entity.CollectionIdTypeFullName;
                            sb.AppendLine($"            if (typeof(TId) == typeof({idTypeStr}) && typeof(T) == typeof({entityTypeStr}))");
                            sb.AppendLine($"                return (global::BLite.Core.Collections.IDocumentCollection<TId, T>)(object)this.{entity.CollectionPropertyName}!;");
                        }

                        if (dbContext.HasBaseDbContext)
                        {
                            sb.AppendLine($"            return base.Set<TId, T>();");
                        }
                        else
                        {
                            sb.AppendLine($"            throw new global::System.InvalidOperationException($\"No collection registered for entity type '{{typeof(T).Name}}' with key type '{{typeof(TId).Name}}'.\");");
                        }

                        sb.AppendLine($"        }}");
                    }

                    // Generate OpenTypedSession() factory and {ClassName}Session nested class
                    if (collectionsWithProperties.Any())
                    {
                        var sessionClassName = $"{dbContext.ClassName}Session";
                        sb.AppendLine();
                        sb.AppendLine($"        /// <summary>");
                        sb.AppendLine($"        /// Opens a typed session with an independent transaction context.");
                        sb.AppendLine($"        /// Every session's collections are bound to a dedicated <see cref=\"global::BLite.Core.BLiteSession\"/>,");
                        sb.AppendLine($"        /// enabling concurrent callers to run independent transactions on the same database.");
                        sb.AppendLine($"        /// </summary>");
                        var newKeyword = dbContext.HasBaseDbContext ? "new " : "";
                        sb.AppendLine($"        public {newKeyword}{sessionClassName} OpenTypedSession()");
                        sb.AppendLine($"        {{");
                        sb.AppendLine($"            if (_disposed) throw new global::System.ObjectDisposedException(GetType().Name);");
                        sb.AppendLine($"            return new {sessionClassName}(this);");
                        sb.AppendLine($"        }}");
                        sb.AppendLine();
                        sb.AppendLine($"        /// <summary>");
                        sb.AppendLine($"        /// Exposes all collections of <see cref=\"{dbContext.ClassName}\"/> each bound to an independent");
                        sb.AppendLine($"        /// transaction context. Obtain via <see cref=\"OpenTypedSession\"/> and dispose when done.");
                        sb.AppendLine($"        /// </summary>");
                        sb.AppendLine($"        public sealed class {sessionClassName} : global::System.IDisposable");
                        sb.AppendLine($"        {{");
                        sb.AppendLine($"            private readonly global::BLite.Core.BLiteSession _session;");
                        sb.AppendLine();
                        foreach (var entity in collectionsWithProperties)
                        {
                            var entityTypeStr = $"global::{entity.FullTypeName}";
                            var idTypeStr = entity.CollectionIdTypeFullName;
                            sb.AppendLine($"            public global::BLite.Core.Collections.IDocumentCollection<{idTypeStr}, {entityTypeStr}> {entity.CollectionPropertyName} {{ get; }}");
                        }
                        sb.AppendLine();
                        sb.AppendLine($"            internal {sessionClassName}({dbContext.ClassName} ctx)");
                        sb.AppendLine($"            {{");
                        sb.AppendLine($"                _session = ctx.OpenSession();");
                        foreach (var entity in collectionsWithProperties)
                        {
                            var mapperName = $"global::{mapperNamespace}.{CodeGenerator.GetMapperName(entity.FullTypeName)}";
                            sb.AppendLine($"                {entity.CollectionPropertyName} = ctx.CreateSessionCollection(new {mapperName}(), _session);");
                        }
                        sb.AppendLine($"            }}");
                        sb.AppendLine();
                        sb.AppendLine($"            /// <summary>Commits all pending changes in this session.</summary>");
                        sb.AppendLine($"            public void SaveChanges() => _session.CommitAsync().GetAwaiter().GetResult();");
                        sb.AppendLine($"            /// <summary>Asynchronously commits all pending changes in this session.</summary>");
                        sb.AppendLine($"            public global::System.Threading.Tasks.Task SaveChangesAsync(global::System.Threading.CancellationToken ct = default) => _session.CommitAsync(ct);");
                        sb.AppendLine($"            /// <summary>Begins a new transaction for this session or returns the active one.</summary>");
                        sb.AppendLine($"            public global::BLite.Core.Transactions.ITransaction BeginTransaction() => _session.BeginTransaction();");
                        sb.AppendLine($"            /// <summary>Rolls back the active transaction, discarding uncommitted changes.</summary>");
                        sb.AppendLine($"            public void Rollback() => _session.Rollback();");
                        sb.AppendLine($"            /// <summary>Disposes this session, rolling back any uncommitted transaction.</summary>");
                        sb.AppendLine($"            public void Dispose() => _session.Dispose();");
                        sb.AppendLine($"        }}");
                    }

                    sb.AppendLine($"    }}");
                    sb.AppendLine($"}}");
                }
                
                spc.AddSource($"{dbContext.Namespace}.{safeName}.Mappers.g.cs", sb.ToString());
            });

            // ── Filter pipeline: emit exactly one {Entity}Filter per unique entity type ──
            // This separate pipeline aggregates entities from ALL DbContext registrations and
            // groups by FullTypeName so that the same entity registered in multiple DbContexts
            // produces a single filter class rather than duplicate definitions.
            var allEntityFilters = dbContextClasses
                .SelectMany(static (DbContextInfo? ctx, System.Threading.CancellationToken _) =>
                {
                    if (ctx == null) return System.Collections.Immutable.ImmutableArray<EntityInfo>.Empty;
                    var result = new System.Collections.Generic.List<EntityInfo>();
                    foreach (var e in ctx.Entities)
                    {
                        if (!e.IsNestedTypeMapper && e.Properties.Count > 0)
                            result.Add(e);
                    }
                    return System.Collections.Immutable.ImmutableArray.CreateRange(result);
                })
                .Collect()
                .SelectMany(static (System.Collections.Immutable.ImmutableArray<EntityInfo> entities, System.Threading.CancellationToken _) =>
                    System.Collections.Immutable.ImmutableArray.CreateRange(
                        entities.GroupBy(e => e.FullTypeName).Select(g => g.First())));

            context.RegisterSourceOutput(allEntityFilters, static (spc, entity) =>
            {
                if (entity == null) return;

                // Filter class lives in the entity's own namespace to avoid name collisions
                // across entities that share the same short CLR name (e.g. ModuleA.Widget vs
                // ModuleB.Widget → each gets its own {ns}.Filters.WidgetFilter).
                var filterNamespace = string.IsNullOrEmpty(entity.Namespace)
                    ? "Filters"
                    : $"{entity.Namespace}.Filters";

                var filterSource = CodeGenerator.GenerateFilterClass(entity, filterNamespace);
                if (!string.IsNullOrEmpty(filterSource))
                {
                    // Use the same double-underscore encoding as GetMapperName so that e.g.
                    // BLite.Shared.Module_A.Gadget and BLite.Shared.Module.A_Gadget produce
                    // unique hint names.
                    var hintKey = CodeGenerator.GetMapperName(entity.FullTypeName)
                        .Replace(BLiteConventions.MapperClassSuffix, BLiteConventions.FilterClassSuffix);
                    spc.AddSource($"{hintKey}.g.cs", filterSource);
                }
            });

            // ── Second pipeline: [DocumentMapper] attribute on a standalone class ──
            var directMapperClasses = context.SyntaxProvider
                .CreateSyntaxProvider(
                    predicate: static (node, _) => IsPotentialDirectMapper(node),
                    transform: static (ctx, _) => GetDirectMapperInfo(ctx))
                .Where(static info => info is not null);

            context.RegisterSourceOutput(directMapperClasses, static (spc, entity) =>
            {
                if (entity == null) return;

                // Emit diagnostics collected during analysis
                foreach (var diag in entity.Diagnostics)
                {
                    var descriptor = diag.IsError ? DiagError : DiagWarning;
                    spc.ReportDiagnostic(Diagnostic.Create(descriptor, Location.None, diag.Message));
                }

                var mapperNamespace = string.IsNullOrEmpty(entity.Namespace)
                    ? "Mappers"
                    : $"{entity.Namespace}.Mappers";

                var namespaces = new HashSet<string>
                {
                    "System",
                    "System.Collections.Generic",
                    "BLite.Bson",
                    "BLite.Core.Collections"
                };
                if (!string.IsNullOrEmpty(entity.Namespace))
                    namespaces.Add(entity.Namespace);

                var nestedTypes = new Dictionary<string, NestedTypeInfo>();
                CollectNestedTypes(entity.NestedTypes, nestedTypes);
                foreach (var nested in nestedTypes.Values)
                {
                    if (!string.IsNullOrEmpty(nested.Namespace))
                        namespaces.Add(nested.Namespace);
                }

                var sb = new StringBuilder();
                sb.AppendLine("// <auto-generated/>");
                sb.AppendLine("#nullable enable");
                foreach (var ns in namespaces.OrderBy(n => n))
                    sb.AppendLine($"using {ns};");
                sb.AppendLine();
                sb.AppendLine($"namespace {mapperNamespace}");
                sb.AppendLine("{");

                var generatedMappers = new HashSet<string>();
                if (generatedMappers.Add(entity.FullTypeName))
                    sb.AppendLine(CodeGenerator.GenerateMapperClass(entity, mapperNamespace));

                foreach (var nested in nestedTypes.Values)
                {
                    if (generatedMappers.Add(nested.FullTypeName))
                    {
                        var nestedEntity = new EntityInfo
                        {
                            Name = nested.Name,
                            Namespace = nested.Namespace,
                            FullTypeName = nested.FullTypeName,
                        };
                        nestedEntity.Properties.AddRange(nested.Properties);
                        sb.AppendLine(CodeGenerator.GenerateMapperClass(nestedEntity, mapperNamespace));
                    }
                }

                sb.AppendLine("}");

                spc.AddSource($"{entity.FullTypeName}.Mapper.g.cs", sb.ToString());
            });
        }
        
        private static void CollectNestedTypes(Dictionary<string, NestedTypeInfo> source, Dictionary<string, NestedTypeInfo> target)
        {
            foreach (var kvp in source)
            {
                if (!target.ContainsKey(kvp.Value.FullTypeName))
                {
                    target[kvp.Value.FullTypeName] = kvp.Value;
                    CollectNestedTypes(kvp.Value.NestedTypes, target);
                }
            }
        }
        
        private static void PrintNestedTypes(StringBuilder sb, Dictionary<string, NestedTypeInfo> nestedTypes, string indent)
        {
            foreach(var nt in nestedTypes.Values)
            {
                sb.AppendLine($"//{indent}- {nt.Name} (Depth: {nt.Depth})");
                if (nt.Properties.Count > 0)
                {
                     // Print properties for nested type to be sure
                     foreach(var p in nt.Properties)
                     {
                        var flags = new List<string>();
                        if (p.IsCollection) flags.Add($"Collection<{p.CollectionItemType}>");
                        if (p.IsNestedObject) flags.Add($"Nested<{p.NestedTypeName}>");
                        var flagStr = flags.Any() ? $" [{string.Join(", ", flags)}]" : "";
                        sb.AppendLine($"//{indent}  - {p.Name}: {p.TypeName}{flagStr}");
                     }
                }
                
                if (nt.NestedTypes.Any())
                {
                    PrintNestedTypes(sb, nt.NestedTypes, indent + "  ");
                }
            }
        }
        
        private static bool IsPotentialDbContext(SyntaxNode node)
        {
            if (node.SyntaxTree.FilePath.EndsWith(BLiteConventions.GeneratedFileSuffix)) return false;

            return node is ClassDeclarationSyntax classDecl &&
                   classDecl.BaseList != null && 
                   classDecl.Identifier.Text.EndsWith(BLiteConventions.DbContextClassSuffix);
        }
        
        private static DbContextInfo? GetDbContextInfo(GeneratorSyntaxContext context)
        {
            var classDecl = (ClassDeclarationSyntax)context.Node;
            var semanticModel = context.SemanticModel;
            
            var classSymbol = semanticModel.GetDeclaredSymbol(classDecl) as INamedTypeSymbol;
            if (classSymbol == null) return null;
            
            if (!SyntaxHelper.InheritsFrom(classSymbol, BLiteConventions.DocumentDbContextBaseName))
                return null;
            
            // Check if this context inherits from another DbContext (not DocumentDbContext directly)
            var baseType = classSymbol.BaseType;
            bool hasBaseDbContext = baseType != null && 
                                    baseType.Name != BLiteConventions.DocumentDbContextBaseName && 
                                    SyntaxHelper.InheritsFrom(baseType, BLiteConventions.DocumentDbContextBaseName);
            
            var info = new DbContextInfo
            {
                ClassName = classSymbol.Name,
                Namespace = classSymbol.ContainingNamespace.ToDisplayString(),
                FilePath = classDecl.SyntaxTree.FilePath,
                IsNested = classSymbol.ContainingType != null,
                IsPartial = classDecl.Modifiers.Any(m => m.IsKind(Microsoft.CodeAnalysis.CSharp.SyntaxKind.PartialKeyword)),
                HasBaseDbContext = hasBaseDbContext
            };

            if (!info.IsPartial)
            {
                info.Diagnostics.Add(new BLiteDiagnostic(
                    "BLITE001",
                    $"'{classSymbol.Name}' must be declared 'partial'. " +
                    $"The generator cannot emit InitializeCollections() or Set<TId,T>() without the partial keyword.",
                    isError: true));
            }
            
            // Analyze OnModelCreating to find entities
            var onModelCreating = classDecl.Members
                .OfType<MethodDeclarationSyntax>()
                .FirstOrDefault(m => m.Identifier.Text == BLiteConventions.OnModelCreatingMethodName);
                
            if (onModelCreating != null)
            {
                var entityCalls = SyntaxHelper.FindMethodInvocations(onModelCreating, BLiteConventions.EntityMethodName);
                foreach (var call in entityCalls)
                {
                    // Resolve directly from the type argument syntax node via semantic model.
                    // Name-based lookups (GetSymbolsWithName / GetTypeByMetadataName with a simple
                    // name) fail for types defined in referenced assemblies.
                    INamedTypeSymbol? entityType = null;
                    string? syntaxTypeName = SyntaxHelper.GetGenericTypeArgument(call);

                    if (call.Expression is MemberAccessExpressionSyntax memberAccess &&
                        memberAccess.Name is GenericNameSyntax genericName &&
                        genericName.TypeArgumentList.Arguments.Count > 0)
                    {
                        var typeArgSyntax = genericName.TypeArgumentList.Arguments[0];
                        entityType = semanticModel.GetSymbolInfo(typeArgSyntax).Symbol as INamedTypeSymbol;
                    }

                    if (entityType == null)
                    {
                        info.Diagnostics.Add(new BLiteDiagnostic(
                            "BLITE001",
                            $"[{info.ClassName}] Entity<{syntaxTypeName ?? "?"}> could not be resolved via semantic model. " +
                            $"Ensure the type is accessible and the assembly is referenced. No mapper will be generated for it.",
                            isError: true));
                        continue;
                    }

                    // Check for duplicates
                    var fullTypeName = SyntaxHelper.GetFullName(entityType);
                    if (!info.Entities.Any(e => e.FullTypeName == fullTypeName))
                    {
                        EntityInfo entityInfo;
                        try
                        {
                            entityInfo = EntityAnalyzer.Analyze(entityType, semanticModel, info.Diagnostics);
                        }
                        catch (System.Exception ex)
                        {
                            info.Diagnostics.Add(new BLiteDiagnostic(
                                "BLITE001",
                                $"[{info.ClassName}] Exception analyzing entity '{entityType.Name}': {ex.Message}",
                                isError: true));
                            continue;
                        }

                        if (entityInfo.Properties.Count == 0)
                        {
                            info.Diagnostics.Add(new BLiteDiagnostic(
                                "BLITE002",
                                $"[{info.ClassName}] Entity '{entityType.Name}' has no serializable properties. " +
                                $"Ensure properties have at least a getter. Private setters are supported via Expression Trees.",
                                isError: false));
                        }

                        if (entityInfo.IdProperty == null)
                        {
                            info.Diagnostics.Add(new BLiteDiagnostic(
                                "BLITE001",
                                $"[{info.ClassName}] Entity '{entityType.Name}' has no primary key property. " +
                                $"Add [Key] attribute or ensure a property named 'Id' exists and is visible to the analyzer.",
                                isError: true));
                        }

                        info.Entities.Add(entityInfo);
                    }
                }
            }

            // Analyze OnModelCreating for ToCollection
            // Pattern: modelBuilder.Entity<T>().ToCollection("collectionName")
            // This updates the CollectionName on the entity info so the generated mapper uses the correct name.
            if (onModelCreating != null)
            {
                var toCollectionCalls = SyntaxHelper.FindMethodInvocations(onModelCreating, "ToCollection");
                foreach (var call in toCollectionCalls)
                {
                    // Extract the collection name string argument — accept literals and compile-time constants.
                    if (call.ArgumentList.Arguments.Count == 0) continue;
                    var nameArgExpr = call.ArgumentList.Arguments[0].Expression;
                    string? collectionName = null;
                    var constantValue = semanticModel.GetConstantValue(nameArgExpr);
                    if (constantValue.HasValue && constantValue.Value is string constStr)
                    {
                        collectionName = constStr;
                    }
                    if (string.IsNullOrEmpty(collectionName)) continue;

                    // Walk up the fluent chain to find Entity<T>()
                    if (call.Expression is Microsoft.CodeAnalysis.CSharp.Syntax.MemberAccessExpressionSyntax toCollectionMember)
                    {
                        var entityCall = SyntaxHelper.FindEntityCallInChain(toCollectionMember.Expression);
                        if (entityCall == null) continue;

                        // Resolve the entity type via semantic model
                        INamedTypeSymbol? entityType = null;
                        if (entityCall.Expression is Microsoft.CodeAnalysis.CSharp.Syntax.MemberAccessExpressionSyntax entityMemberAccess &&
                            entityMemberAccess.Name is Microsoft.CodeAnalysis.CSharp.Syntax.GenericNameSyntax entityGenericName &&
                            entityGenericName.TypeArgumentList.Arguments.Count > 0)
                        {
                            var typeArgSyntax = entityGenericName.TypeArgumentList.Arguments[0];
                            entityType = semanticModel.GetSymbolInfo(typeArgSyntax).Symbol as INamedTypeSymbol;
                        }
                        if (entityType == null) continue;

                        var fullTypeName = SyntaxHelper.GetFullName(entityType);
                        var entity = info.Entities.FirstOrDefault(e => e.FullTypeName == fullTypeName);
                        if (entity != null)
                        {
                            entity.CollectionName = collectionName!;
                        }
                    }
                }
            }

            // Analyze OnModelCreating for HasConversion
            // Pattern: modelBuilder.Entity<T>().Property(x => x.PropertyName).HasConversion<TConverter>()
            // This works for any property, including primary keys (Id)
            if (onModelCreating != null)
            {
                var conversionCalls = SyntaxHelper.FindMethodInvocations(onModelCreating, BLiteConventions.HasConversionMethodName);
                foreach (var call in conversionCalls)
                {
                    var converterName = SyntaxHelper.GetGenericTypeArgument(call);
                    if (converterName == null) continue;

                    // Trace back: .Property(x => x.PropertyName).HasConversion<T>()
                    if (call.Expression is MemberAccessExpressionSyntax { Expression: InvocationExpressionSyntax propertyCall } &&
                        propertyCall.Expression is MemberAccessExpressionSyntax { Name: IdentifierNameSyntax { Identifier: { Text: var propertyMethod } } } &&
                        propertyMethod == BLiteConventions.PropertyMethodName)
                    {
                        var propertyName = SyntaxHelper.GetPropertyName(propertyCall.ArgumentList.Arguments.FirstOrDefault()?.Expression);
                        if (propertyName == null) continue;

                        // Trace further back through the fluent chain to find Entity<T>().
                        // The pattern may include intermediate calls such as .ToCollection("..."),
                        // e.g. modelBuilder.Entity<T>().ToCollection("name").Property(...).HasConversion<TC>()
                        // propertyCall.Expression is a MemberAccessExpressionSyntax (the ".Property" part);
                        // its own Expression is the invocation that precedes ".Property".
                        var entityCallCandidate = (propertyCall.Expression as MemberAccessExpressionSyntax)?.Expression as InvocationExpressionSyntax;
                        int chainDepth = 0;
                        while (entityCallCandidate != null && chainDepth < BLiteConventions.MaxFluentChainDepth)
                        {
                            if (entityCallCandidate.Expression is MemberAccessExpressionSyntax { Name: GenericNameSyntax { Identifier: { Text: var methodText } } } &&
                                methodText == BLiteConventions.EntityMethodName)
                            {
                                break; // found Entity<T>()
                            }
                            // Step back one more level in the chain
                            entityCallCandidate = (entityCallCandidate.Expression as MemberAccessExpressionSyntax)?.Expression as InvocationExpressionSyntax;
                            chainDepth++;
                        }

                        if (entityCallCandidate != null)
                        {
                            var entityTypeName = SyntaxHelper.GetGenericTypeArgument(entityCallCandidate);
                            if (entityTypeName != null)
                            {
                                var entity = info.Entities.FirstOrDefault(e => e.Name == entityTypeName || e.FullTypeName.EndsWith("." + entityTypeName));
                                if (entity != null)
                                {
                                    var prop = entity.Properties.FirstOrDefault(p => p.Name == propertyName);
                                    if (prop != null)
                                    {
                                        // Resolve TProvider from ValueConverter<TModel, TProvider>
                                        var converterType = semanticModel.Compilation.GetTypeByMetadataName(converterName) ??
                                                           semanticModel.Compilation.GetSymbolsWithName(converterName).OfType<INamedTypeSymbol>().FirstOrDefault();

                                        prop.ConverterTypeName = converterType != null ? SyntaxHelper.GetFullName(converterType) : converterName;

                                        if (converterType != null && converterType.BaseType != null &&
                                            converterType.BaseType.Name == "ValueConverter" &&
                                            converterType.BaseType.TypeArguments.Length == 2)
                                        {
                                            prop.ProviderTypeName = converterType.BaseType.TypeArguments[1].Name;
                                        }
                                        else if (converterType != null)
                                        {
                                            // Fallback: search deeper in base types
                                            var converterBaseType = converterType.BaseType;
                                            while (converterBaseType != null)
                                            {
                                                if (converterBaseType.Name == BLiteConventions.ValueConverterBaseName && converterBaseType.TypeArguments.Length == BLiteConventions.DocumentCollectionTypeArgCount)
                                                {
                                                    prop.ProviderTypeName = converterBaseType.TypeArguments[1].Name;
                                                    break;
                                                }
                                                converterBaseType = converterBaseType.BaseType;
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }

            // Analyze OnModelCreating for HasIndex
            // Pattern: modelBuilder.Entity<T>()...HasIndex(x => x.PropertyName)
            //      or: modelBuilder.Entity<T>()...HasIndex(x => x.PropertyName, name: "idx_name")
            if (onModelCreating != null)
            {
                var hasIndexCalls = SyntaxHelper.FindMethodInvocations(onModelCreating, BLiteConventions.HasIndexMethodName);
                foreach (var call in hasIndexCalls)
                {
                    if (call.ArgumentList.Arguments.Count == 0) continue;

                    // Extract property path from the lambda argument.
                    var lambdaExpr = call.ArgumentList.Arguments[0].Expression;
                    var propertyPath = SyntaxHelper.GetPropertyName(lambdaExpr);
                    if (string.IsNullOrEmpty(propertyPath)) continue;

                    // Extract optional explicit index name (named arg "name:")
                    string? indexName = null;
                    foreach (var arg in call.ArgumentList.Arguments.Skip(1))
                    {
                        if (arg.NameColon?.Name?.Identifier.Text == "name")
                        {
                            var cv = semanticModel.GetConstantValue(arg.Expression);
                            if (cv.HasValue && cv.Value is string nameStr)
                                indexName = nameStr;
                        }
                    }

                    // Walk up the fluent chain to find Entity<T>()
                    var entityCallCandidate2 = (call.Expression as MemberAccessExpressionSyntax)?.Expression as InvocationExpressionSyntax;
                    int depth2 = 0;
                    while (entityCallCandidate2 != null && depth2 < BLiteConventions.MaxFluentChainDepth)
                    {
                        if (entityCallCandidate2.Expression is MemberAccessExpressionSyntax { Name: GenericNameSyntax { Identifier: { Text: var m2 } } } &&
                            m2 == BLiteConventions.EntityMethodName)
                            break;
                        entityCallCandidate2 = (entityCallCandidate2.Expression as MemberAccessExpressionSyntax)?.Expression as InvocationExpressionSyntax;
                        depth2++;
                    }
                    if (entityCallCandidate2 == null) continue;

                    // Resolve entity type via semantic model
                    INamedTypeSymbol? idxEntityType = null;
                    if (entityCallCandidate2.Expression is MemberAccessExpressionSyntax idxEntityMember &&
                        idxEntityMember.Name is GenericNameSyntax idxGenericName &&
                        idxGenericName.TypeArgumentList.Arguments.Count > 0)
                    {
                        var typeArgSyntax2 = idxGenericName.TypeArgumentList.Arguments[0];
                        idxEntityType = semanticModel.GetSymbolInfo(typeArgSyntax2).Symbol as INamedTypeSymbol;
                    }
                    if (idxEntityType == null) continue;

                    var fullTypeName2 = SyntaxHelper.GetFullName(idxEntityType);
                    var entityForIndex = info.Entities.FirstOrDefault(e => e.FullTypeName == fullTypeName2);
                    if (entityForIndex == null) continue;

                    // Avoid duplicating the same index
                    if (!entityForIndex.Indexes.Any(idx => idx.PropertyPaths.Count == 1 && idx.PropertyPaths[0] == propertyPath))
                    {
                        var indexInfo = new IndexInfo { Name = indexName };
                        indexInfo.PropertyPaths.Add(propertyPath!);
                        entityForIndex.Indexes.Add(indexInfo);
                    }
                }
            }

            // Analyze properties to find DocumentCollection<TId, TEntity> or IDocumentCollection<TId, TEntity>
            var properties = classSymbol.GetMembers().OfType<IPropertySymbol>();
            foreach (var prop in properties)
            {
                if (prop.Type is INamedTypeSymbol namedType &&
                    (namedType.OriginalDefinition.Name == BLiteConventions.DocumentCollectionTypeName ||
                     namedType.OriginalDefinition.Name == BLiteConventions.IDocumentCollectionTypeName))
                {
                    // Expecting 2 type arguments: TId, TEntity
                    if (namedType.TypeArguments.Length == BLiteConventions.DocumentCollectionTypeArgCount)
                    {
                        var entityType = namedType.TypeArguments[1];
                        var entityInfo = info.Entities.FirstOrDefault(e => e.FullTypeName == entityType.ToDisplayString());

                        // If found, update
                        if (entityInfo != null)
                        {
                            entityInfo.CollectionPropertyName = prop.Name;
                            entityInfo.CollectionIdTypeFullName = namedType.TypeArguments[0].ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat);
                            entityInfo.CollectionPropertyIsInterface = namedType.OriginalDefinition.Name == BLiteConventions.IDocumentCollectionTypeName;
                        }
                        else if (entityType is INamedTypeSymbol namedEntityType)
                        {
                            // Auto-discover: entity not registered via OnModelCreating — infer from property type.
                            // This allows DbContext subclasses to omit OnModelCreating for basic usage.
                            var fullTypeName = SyntaxHelper.GetFullName(namedEntityType);
                            var existing = info.Entities.FirstOrDefault(e => e.FullTypeName == fullTypeName);
                            if (existing == null)
                            {
                                EntityInfo discoveredEntity;
                                try
                                {
                                    discoveredEntity = EntityAnalyzer.Analyze(namedEntityType, semanticModel, info.Diagnostics);
                                }
                                catch (System.Exception ex)
                                {
                                    info.Diagnostics.Add(new BLiteDiagnostic(
                                        "BLITE001",
                                        $"[{info.ClassName}] Exception analyzing entity '{namedEntityType.Name}' (auto-discovered from property '{prop.Name}'): {ex.Message}",
                                        isError: true));
                                    continue;
                                }

                                if (discoveredEntity.IdProperty == null)
                                {
                                    info.Diagnostics.Add(new BLiteDiagnostic(
                                        "BLITE001",
                                        $"[{info.ClassName}] Entity '{namedEntityType.Name}' (auto-discovered from property '{prop.Name}') has no primary key property. " +
                                        $"Add [Key] attribute or ensure a property named 'Id' exists.",
                                        isError: true));
                                }

                                discoveredEntity.CollectionPropertyName = prop.Name;
                                discoveredEntity.CollectionIdTypeFullName = namedType.TypeArguments[0].ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat);
                                discoveredEntity.CollectionPropertyIsInterface = namedType.OriginalDefinition.Name == BLiteConventions.IDocumentCollectionTypeName;
                                info.Entities.Add(discoveredEntity);
                            }
                            else
                            {
                                existing.CollectionPropertyName = prop.Name;
                                existing.CollectionIdTypeFullName = namedType.TypeArguments[0].ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat);
                                existing.CollectionPropertyIsInterface = namedType.OriginalDefinition.Name == BLiteConventions.IDocumentCollectionTypeName;
                            }
                        }
                    }
                }
            }

            // Detect duplicate CollectionNames within the same DbContext — performed AFTER property-based
            // auto-discovery so that all root entities (including those inferred from DocumentCollection
            // properties) are included in the check.
            // Collection names are compared case-insensitively to match the core engine's behaviour.
            var collectionNameGroups = info.Entities
                .GroupBy(e => e.CollectionName, System.StringComparer.OrdinalIgnoreCase)
                .Where(g => g.Count() > 1);
            foreach (var group in collectionNameGroups)
            {
                var typeNames = string.Join(", ", group.Select(e => e.FullTypeName));
                info.Diagnostics.Add(new BLiteDiagnostic(
                    "BLITE003",
                    $"[{info.ClassName}] Multiple entity types map to the same collection name '{group.Key}': {typeNames}. " +
                    "Duplicate collection names cause data corruption at runtime. " +
                    "Assign a unique name using the [Table(\"name\")] attribute on the entity class, " +
                    "or call .ToCollection(\"unique_name\") in OnModelCreating.",
                    isError: true));
            }

            return info;
        }

        private static bool IsPotentialDirectMapper(SyntaxNode node)
        {
            if (node.SyntaxTree.FilePath.EndsWith(BLiteConventions.GeneratedFileSuffix)) return false;
            return node is ClassDeclarationSyntax classDecl && classDecl.AttributeLists.Count > 0;
        }

        private static EntityInfo? GetDirectMapperInfo(GeneratorSyntaxContext context)
        {
            var classDecl = (ClassDeclarationSyntax)context.Node;
            var semanticModel = context.SemanticModel;

            var classSymbol = semanticModel.GetDeclaredSymbol(classDecl) as INamedTypeSymbol;
            if (classSymbol == null) return null;

            var attr = AttributeHelper.GetAttribute(classSymbol, BLiteConventions.DocumentMapperAttributeName);
            if (attr == null) return null;

            var directDiagnostics = new System.Collections.Generic.List<BLiteDiagnostic>();
            var entity = EntityAnalyzer.Analyze(classSymbol, semanticModel, directDiagnostics);
            entity.Diagnostics.AddRange(directDiagnostics);

            // Override collection name from attribute constructor argument if provided
            if (attr.ConstructorArguments.Length > 0 &&
                attr.ConstructorArguments[0].Value is string collectionName &&
                !string.IsNullOrEmpty(collectionName))
            {
                entity.CollectionName = collectionName;
            }

            return entity;
        }
    }
}
