# C-BSON: Compressed BSON Format

## What is C-BSON?

**C-BSON** (Compressed BSON) is BLite's optimized wire format that maintains full BSON type compatibility while achieving significant space savings through **field name compression**. This innovation reduces document size by 30-60% for typical schemas, improving both storage efficiency and I/O performance.

### The Problem with Standard BSON

Standard BSON stores field names as **null-terminated UTF-8 strings** in every document. Consider a typical user document:

```javascript
{
  "_id": ObjectId("..."),
  "email": "user@example.com",
  "created_at": ISODate("2026-02-12"),
  "last_login": ISODate("2026-02-12")
}
```

**Field Name Overhead:**
- `_id` → 4 bytes (3 chars + null terminator)
- `email` → 6 bytes
- `created_at` → 11 bytes
- `last_login` → 11 bytes

**Total overhead: 32 bytes** just for field names in a 4-field document.

### The C-BSON Solution: Key Compression

C-BSON replaces field names with **2-byte numeric IDs** via a schema-based dictionary:

```
Standard BSON:  [type][field_name\0][value]
C-BSON:         [type][field_id: ushort][value]
```

**Space Savings:**

| Field Name     | Standard BSON | C-BSON  | Savings |
|:---------------|:--------------|:--------|:--------|
| `_id`          | 4 bytes       | 2 bytes | 50%     |
| `email`        | 6 bytes       | 2 bytes | 67%     |
| `created_at`   | 11 bytes      | 2 bytes | 82%     |
| `last_login`   | 11 bytes      | 2 bytes | 82%     |

**Result:** The same 4-field document saves **24 bytes** per instance. For 1 million documents, that's **~23 MB saved**.

---

## Wire Format Specification

### Document Structure

```
┌────────────────────────────────────────────────┐
│  [4 bytes] Document Size (int32 little-endian)│
├────────────────────────────────────────────────┤
│  [Elements...]                                  │
│    ┌──────────────────────────────────────┐   │
│    │ [1 byte]  Type Code                  │   │
│    │ [2 bytes] Field ID (ushort)          │   │
│    │ [N bytes] Value (type-dependent)     │   │
│    └──────────────────────────────────────┘   │
│  [Repeat for each field]                       │
├────────────────────────────────────────────────┤
│  [1 byte] End of Document (0x00)               │
└────────────────────────────────────────────────┘
```

### Element Header Comparison

**Standard BSON Element Header:**
```
[1 byte: type code][N bytes: null-terminated UTF-8 string]
                    ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
                    Variable length: min 2 bytes, no max
```

**C-BSON Element Header:**
```
[1 byte: type code][2 bytes: field ID as ushort little-endian]
                    ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
                    Fixed length: exactly 2 bytes
```

### Type Codes

C-BSON uses **standard BSON type codes** for full compatibility:

| Code | Type        | Description                          |
|:-----|:------------|:-------------------------------------|
| 0x01 | Double      | 64-bit IEEE 754 floating point       |
| 0x02 | String      | UTF-8 string (int32 length + data + null) |
| 0x03 | Document    | Embedded document                    |
| 0x04 | Array       | Embedded array                       |
| 0x05 | Binary      | Binary data (subtype + length + data)|
| 0x07 | ObjectId    | 12-byte MongoDB-compatible ObjectId  |
| 0x08 | Boolean     | 1 byte (0x00 or 0x01)               |
| 0x09 | DateTime    | UTC milliseconds (int64)            |
| 0x10 | Int32       | 32-bit signed integer               |
| 0x12 | Int64       | 64-bit signed integer               |
| 0x13 | Decimal128  | 128-bit decimal (IEEE 754-2008)     |

---

## Schema-Based Key Mapping

### Bidirectional Dictionary

C-BSON requires a **schema-driven key mapping** maintained in memory:

**Writer Side:**
```csharp
ConcurrentDictionary<string, ushort> _keyMap;
// Example:
// "\_id" → 1
// "email" → 2
// "created_at" → 3
```

**Reader Side:**
```csharp
ConcurrentDictionary<ushort, string> _keys;
// Example:
// 1 → "\_id"
// 2 → "email"
// 3 → "created_at"
```

### Schema Generation

BLite automatically generates schemas from C# types using reflection:

```csharp
public class User
{
    public ObjectId Id { get; set; }
    public string Email { get; set; }
    public DateTime CreatedAt { get; set; }
}

// Generated schema:
// Field 1: "_id" (ObjectId)
// Field 2: "email" (String)
// Field 3: "created_at" (DateTime)
```

### Schema Storage

Schemas are stored in the **Page 1 (Collection Metadata)** and loaded into memory on database open:

```
┌─────────────────────────────────────────┐
│ [Schema Hash (long)]                    │
│ [Schema Version (int)]                  │
│ [Field Count (ushort)]                  │
├─────────────────────────────────────────┤
│ For each field:                         │
│   [Field ID (ushort)]                   │
│   [Field Name Length (byte)]            │
│   [Field Name UTF-8 bytes]              │
│   [BSON Type Code (byte)]               │
└─────────────────────────────────────────┘
```

---

## Implementation Details

### BsonSpanWriter (Serialization)

Zero-allocation writer using `Span<byte>`:

```csharp
public ref struct BsonSpanWriter
{
    private Span<byte> _buffer;
    private int _position;
    private readonly ConcurrentDictionary<string, ushort> _keyMap;

    public void WriteElementHeader(BsonType type, string name)
    {
        // Write type code
        _buffer[_position++] = (byte)type;

        // Lookup field ID in dictionary
        if (!_keyMap.TryGetValue(name, out var id))
            throw new InvalidOperationException($"Field '{name}' not in schema");

        // Write field ID (2 bytes, little-endian)
        BinaryPrimitives.WriteUInt16LittleEndian(_buffer.Slice(_position, 2), id);
        _position += 2;
    }
}
```

**Usage:**

```csharp
var keyMap = new ConcurrentDictionary<string, ushort>();
keyMap["_id"] = 1;
keyMap["name"] = 2;

Span<byte> buffer = stackalloc byte[1024];
var writer = new BsonSpanWriter(buffer, keyMap);

writer.WriteObjectId("_id", user.Id);
writer.WriteString("name", user.Name);
```

### BsonSpanReader (Deserialization)

Zero-allocation reader using `ReadOnlySpan<byte>`:

```csharp
public ref struct BsonSpanReader
{
    private ReadOnlySpan<byte> _buffer;
    private int _position;
    private readonly ConcurrentDictionary<ushort, string> _keys;

    public string ReadElementHeader()
    {
        // Read field ID (2 bytes, little-endian)
        var id = BinaryPrimitives.ReadUInt16LittleEndian(_buffer.Slice(_position, 2));
        _position += 2;

        // Reverse lookup in dictionary
        if (!_keys.TryGetValue(id, out var name))
            throw new InvalidOperationException($"Field ID {id} not in schema");

        return name;
    }
}
```

**Usage:**

```csharp
var keys = new ConcurrentDictionary<ushort, string>();
keys[1] = "_id";
keys[2] = "name";

var reader = new BsonSpanReader(bsonData, keys);
reader.ReadDocumentSize();

while (reader.Remaining > 0)
{
    var type = reader.ReadBsonType();
    if (type == BsonType.EndOfDocument) break;

    var fieldName = reader.ReadElementHeader(); // Returns "name" from ID
    // ... read value based on type
}
```

---

## Advanced Features

### Nested Documents

Nested documents recursively use the same C-BSON format with their own field mappings:

```csharp
public class User
{
    public ObjectId Id { get; set; }
    public Address HomeAddress { get; set; } // Nested
}

public class Address
{
    public string Street { get; set; }
    public string City { get; set; }
}

// Schema:
// User fields: 1="_id", 2="home_address"
// Address fields: 3="street", 4="city"
```

**Wire format for nested document:**
```
[0x03: Document]["home_address": 2]
  [nested_doc_size: 4 bytes]
  [0x02: String]["street": 3][value]
  [0x02: String]["city": 4][value]
  [0x00: End]
```

### Arrays

Arrays use numeric indices as field names, still compressed to 2-byte IDs:

```csharp
public class User
{
    public string[] Tags { get; set; }
}

// Schema includes numeric keys:
// "0" → 5, "1" → 6, "2" → 7, ...
```

**Wire format:**
```
[0x04: Array]["tags": 2]
  [array_size: 4 bytes]
  [0x02: String]["0": 5]["design"]
  [0x02: String]["1": 6]["dotnet"]
  [0x00: End]
```

### Geospatial Coordinates

C-BSON supports zero-allocation coordinate tuples via `[Column(TypeName="geopoint")]`:

```csharp
[Column(TypeName = "geopoint")]
public (double Lat, double Lon) Location { get; set; }
```

**Wire format:**
```
[0x04: Array]["location": field_id]
  [array_size: 4 bytes]
  [0x01: Double]["0": coord_0_id][8 bytes: latitude]
  [0x01: Double]["1": coord_1_id][8 bytes: longitude]
  [0x00: End]
```

This maps directly to R-Tree index structures without deserialization overhead.

---

## Performance Benefits

### Storage Efficiency

**Real-world example:** E-commerce product catalog

```csharp
public class Product
{
    public ObjectId Id { get; set; }              // "_id": 4 → 2 bytes
    public string Name { get; set; }             // "name": 5 → 2 bytes
    public decimal Price { get; set; }           // "price": 6 → 2 bytes
    public string Description { get; set; }      // "description": 12 → 2 bytes
    public string Category { get; set; }         // "category": 9 → 2 bytes
    public string[] Tags { get; set; }           // "tags": 5 → 2 bytes
    public DateTime CreatedAt { get; set; }      // "created_at": 11 → 2 bytes
    public DateTime UpdatedAt { get; set; }      // "updated_at": 11 → 2 bytes
}
```

**Field name overhead:**
- Standard BSON: 4+5+6+12+9+5+11+11 = **63 bytes**
- C-BSON: 2×8 = **16 bytes**
- **Savings: 47 bytes per document**

For 1 million products: **~45 MB saved** in field names alone.

### CPU Cache Efficiency

Smaller documents mean:
- **More documents fit in L1/L2/L3 cache**
- **Fewer cache misses during sequential scans**
- **Better prefetching** for range queries

### I/O Reduction

**Disk I/O:**
- 16KB page holds **more documents** → fewer page reads
- **Faster bulk inserts** → less data to write
- **Faster bulk reads** → less data to transfer from disk

**Network (future):**
- Smaller wire transfer for client/server scenarios
- Better replication throughput

---

## Hex Dump Examples

### Example 1: Simple User Document

**C# Object:**
```csharp
var user = new User
{
    Id = new ObjectId("65d3c2a1f4b8e9a2c3d4e5f6"),
    Name = "Alice",
    Age = 30
};
```

**C-BSON Wire Format (hex):**
```
20 00 00 00                 // Document size: 32 bytes
07 01 00                    // ObjectId, field 1 (_id)
  65 d3 c2 a1 f4 b8 e9 a2   // ObjectId bytes (12 total)
  c3 d4 e5 f6
02 02 00                    // String, field 2 (name)
  06 00 00 00               // String length: 6
  41 6c 69 63 65 00         // "Alice\0"
10 03 00                    // Int32, field 3 (age)
  1e 00 00 00               // Value: 30
00                          // End of document
```

### Example 2: Standard BSON Comparison

**Same document in standard BSON:**
```
2d 00 00 00                 // Document size: 45 bytes (+13 bytes)
07 5f 69 64 00              // "_id\0" (4 bytes)
  65 d3 c2 a1 f4 b8 e9 a2
  c3 d4 e5 f6
02 6e 61 6d 65 00           // "name\0" (5 bytes)
  06 00 00 00
  41 6c 69 63 65 00
10 61 67 65 00              // "age\0" (4 bytes)
  1e 00 00 00
00
```

**Comparison:**
- Standard BSON: 45 bytes
- C-BSON: 32 bytes
- **Reduction: 28% smaller**

### Example 3: Nested Document

**C# Object:**
```csharp
var user = new User
{
    Id = ObjectId.NewObjectId(),
    Address = new Address
    {
        Street = "123 Main St",
        City = "Springfield"
    }
};
```

**C-BSON Wire Format (partial, showing nested doc):**
```
... // document header
03 02 00                    // Document, field 2 (address)
  23 00 00 00               // Nested doc size: 35 bytes
  02 03 00                  // String, field 3 (street)
    0c 00 00 00             // Length: 12
    31 32 33 20 4d 61 69 6e // "123 Main St\0"
    20 53 74 00
  02 04 00                  // String, field 4 (city)
    0c 00 00 00             // Length: 12
    53 70 72 69 6e 67 66 69 // "Springfield\0"
    65 6c 64 00
  00                        // End of nested doc
...
```

---

## Technical Constraints

### Field ID Space

- **Type:** `ushort` (16-bit unsigned integer)
- **Range:** 0 to 65,535
- **Theoretical max:** 65,535 unique field names per schema hierarchy
- **Practical limit:** ~1,000 fields for optimal performance
- **Reserved IDs:** 0 is reserved (not used)

### Dictionary Overhead

**Memory footprint:**
- ~16 bytes per entry in `ConcurrentDictionary<string, ushort>`
- ~16 bytes per entry in `ConcurrentDictionary<ushort, string>`
- **Total:** ~32 bytes per unique field name

**Example:** A schema with 50 fields → **~1.6 KB** in-memory overhead (negligible).

### Schema Versioning

When a schema evolves (fields added/removed/renamed):

1. **New schema version** is created with incremented version number
2. **New field IDs** are assigned to new fields
3. **Old documents remain readable** with old schema
4. **Migration** can be applied lazily during read-modify-write cycles

**Schema hash** ensures consistency:
```csharp
long schemaHash = schema.GetHash(); // Hash of all field names and types
```

---

## Compatibility

### BSON Type Compatibility

C-BSON is **type-compatible** with standard BSON:
- ✅ Same type codes (0x01-0x13)
- ✅ Same value encoding (little-endian, IEEE 754, UTF-8)
- ✅ Same document structure (size prefix + elements + 0x00 terminator)
- ❌ **Different element header format** (field ID vs. field name)

### Migration from Standard BSON

**Strategy:**
1. Read standard BSON document
2. Extract field names and build schema
3. Assign field IDs based on schema
4. Re-serialize as C-BSON

**Future enhancement:** Hybrid reader capable of auto-detecting and reading both formats.

### Export to Standard BSON

For external tool compatibility (e.g., MongoDB Compass, Studio 3T):

```csharp
// Convert C-BSON → Standard BSON
public byte[] ToStandardBson(byte[] cbson, BsonSchema schema)
{
    var reader = new BsonSpanReader(cbson, schema.GetReverseKeyMap());
    var writer = new StandardBsonWriter(); // Uses string field names
    
    // Copy document element-by-element
    while (...)
    {
        var type = reader.ReadBsonType();
        var fieldName = reader.ReadElementHeader(); // ID → Name
        writer.WriteElementHeader(type, fieldName);  // Write name directly
        // ... copy value
    }
}
```

---

## Schema Evolution Strategies

### Adding Fields

**Backward compatible:** New fields get new IDs, old documents remain valid.

```csharp
// Version 1: User schema
// 1: "_id", 2: "name", 3: "email"

// Version 2: Add "phone"
// 1: "_id", 2: "name", 3: "email", 4: "phone"
```

Old documents:
- Missing field 4 → treated as `null` or default value
- No re-serialization required

### Removing Fields

**Forward compatible:** Removed field IDs are marked as deprecated.

```csharp
// Version 3: Remove "email" (field 3)
// Mark field 3 as deprecated in schema
```

New code:
- Ignores field 3 during deserialization
- Old documents with field 3 remain valid (data is skipped)

### Renaming Fields

**Breaking change:** Requires migration.

```csharp
// Version 4: Rename "phone" → "mobile_phone"

// Option 1: Lazy migration on read
if (doc.ContainsKey("phone"))
{
    doc["mobile_phone"] = doc["phone"];
    doc.Remove("phone");
    UpdateDocument(doc);
}

// Option 2: Batch migration script
foreach (var doc in collection.FindAll())
{
    if (doc.ContainsKey("phone"))
    {
        doc["mobile_phone"] = doc["phone"];
        doc.Remove("phone");
        collection.Update(doc);
    }
}
```

---

## Future Enhancements

### 1. Adaptive Key Width

Use **1 byte for field IDs** when schema has <256 fields:

```
Small schema flag: [1 bit in document header]
If set: field IDs are 1 byte (0-255)
Else: field IDs are 2 bytes (0-65535)
```

**Potential savings:** Additional 1 byte per field for small schemas.

### 2. Delta Compression

Store only **changed fields** in updates:

```
┌──────────────────────────────────────┐
│ [Base Document ID]                   │
│ [Changed Field IDs bitmap]           │
│ [Changed Field Values]               │
└──────────────────────────────────────┘
```

### 3. Column-Oriented Storage

Separate storage for each field:

```
Field 1 file: [all _id values]
Field 2 file: [all name values]
Field 3 file: [all email values]
```

Benefits:
- **Faster analytics** (read only needed columns)
- **Better compression** (similar data together)
- **Efficient projections** (SELECT name, email FROM ...)

### 4. Hybrid Format Support

Reader auto-detects C-BSON vs. Standard BSON:

```csharp
// Magic byte detection
if (firstElement[2] < 0x7F) // Likely field ID (< 127)
    return ReadCBSON();
else
    return ReadStandardBSON();
```

---

## Conclusion

C-BSON achieves **significant storage and performance improvements** while maintaining BSON's type system and flexibility:

- **30-60% smaller documents** via key compression
- **Zero-allocation** I/O with `Span<byte>`
- **Full BSON type compatibility**
- **Schema-based** for type safety and evolution

This format is the foundation of BLite's high-performance embedded database engine, enabling millions of documents to fit in memory and cache while minimizing disk I/O.

---

## References

- [BSON Specification v1.1](http://bsonspec.org/)
- [MongoDB BSON Types](https://www.mongodb.com/docs/manual/reference/bson-types/)
- [IEEE 754 Floating Point Standard](https://standards.ieee.org/standard/754-2019.html)
- [UTF-8 Encoding (RFC 3629)](https://tools.ietf.org/html/rfc3629)
