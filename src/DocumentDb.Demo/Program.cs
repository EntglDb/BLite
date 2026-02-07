using DocumentDb.Bson;
using DocumentDb.Core.Storage;

// Example 1: Creating and reading BSON documents
Console.WriteLine("=== DocumentDb Demo ===\n");

// 1. Create a BSON document using builder
var document = BsonDocument.Create(builder =>
{
    builder
        .AddObjectId("_id", ObjectId.NewObjectId())
        .AddString("name", "John Doe")
        .AddInt32("age", 30)
        .AddBoolean("active", true);
});

Console.WriteLine($"Document created: {document.Size} bytes");

// 2. Read fields from document
if (document.TryGetString("name", out var name))
    Console.WriteLine($"Name: {name}");

if (document.TryGetInt32("age", out var age))
    Console.WriteLine($"Age: {age}");

if (document.TryGetObjectId("_id", out var id))
    Console.WriteLine($"ID: {id}");

Console.WriteLine();

// 3. Manual BSON writing for zero-allocation scenarios
Span<byte> buffer = stackalloc byte[512];
var writer = new BsonSpanWriter(buffer);

var sizePos = writer.BeginDocument();
writer.WriteString("title", "High Performance BSON");
writer.WriteInt64("timestamp", DateTimeOffset.UtcNow.ToUnixTimeMilliseconds());
writer.WriteDouble("score", 98.5);
writer.EndDocument(sizePos);

Console.WriteLine($"Manual BSON document: {writer.Position} bytes");

// 4. Read it back
var reader = new BsonSpanReader(buffer[..writer.Position]);
varium = reader.ReadDocumentSize();
Console.WriteLine($"Reading document of {docSize} bytes...");

while (reader.Remaining > 1)
{
    var type = reader.ReadBsonType();
    if (type == BsonType.EndOfDocument)
        break;

    var fieldName = reader.ReadCString();
    
    switch (type)
    {
        case BsonType.String:
            Console.WriteLine($"  {fieldName}: {reader.ReadString()}");
            break;
        case BsonType.Int64:
            Console.WriteLine($"  {fieldName}: {reader.ReadInt64()}");
            break;
        case BsonType.Double:
            Console.WriteLine($"  {fieldName}: {reader.ReadDouble()}");
            break;
    }
}

Console.WriteLine();

// 5. Storage engine demo - page-based file
var config = PageFileConfig.Default;
var pageFile = new PageFile("demo.db", config);

Console.WriteLine($"Opening page file with {config.PageSize} byte pages...");
pageFile.Open();

// Allocate a page
var pageId = pageFile.AllocatePage();
Console.WriteLine($"Allocated page ID: {pageId}");

// Write a page header
Span<byte> pageBuffer = stackalloc byte[config.PageSize];
var header = new PageHeader
{
    PageId = pageId,
    PageType = PageType.Data,
    FreeBytes = (ushort)(config.PageSize - 32),
    NextPageId = 0,
    TransactionId = 1,
    Checksum = 0
};

header.WriteTo(pageBuffer);
pageFile.WritePage(pageId, pageBuffer);
Console.WriteLine($"Wrote page header (Type: {header.PageType})");

// Read it back
Span<byte> readBuffer = stackalloc byte[config.PageSize];
pageFile.ReadPage(pageId, readBuffer);
var readHeader = PageHeader.ReadFrom(readBuffer);

Console.WriteLine($"Read page header: PageId={readHeader.PageId}, Type={readHeader.PageType}, Free={readHeader.FreeBytes} bytes");

pageFile.Dispose();

Console.WriteLine("\n✅ Demo completed successfully!");
