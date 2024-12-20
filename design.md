# RustDB: A Type-First Database Design

Hey there! 👋 This is my design doc for RustDB, a database that brings Rust's type system into SQL. I've always found it frustrating that databases treat types as an afterthought, so I decided to fix that. Here's how it works.

## Core Concept

The big idea here is simple: what if we could use Rust's type system directly in our database? Instead of being limited to whatever types our DB supports, we could define our own types right in the schema. Something like:

```sql
CREATE TYPE PhoneNumber {
    country_code: u16,
    area_code: Option<u16>,
    number: [u8; 8]  -- Fixed size array, exactly like Rust!
}

CREATE TABLE users {
    id: u64 @primary_key,
    phone: PhoneNumber @index(country_code),  -- Index on a field of our custom type
    preferences: Vec<String>  -- Dynamic arrays? Yes please!
}
```

## Type System

The type system is the heart of RustDB. Here's what we support:

### Primitive Types
- All the usuals: `u8`, `u16`, `u32`, `u64`, `i8`, `i16`, `i32`, `i64`, `f32`, `f64`, `bool`, `String`
- Fixed-size arrays: `[T; N]` 
- Dynamic arrays: `Vec<T>`
- Optional values: `Option<T>`
- Result type: `Result<T, E>`

### Custom Types
You can build your own types using:
- Structs (like the PhoneNumber example)
- Enums (yes, with data!)
- Generics (why not?)

Each custom type gets compiled down to an efficient binary representation. No more wasteful string storage for things that should be integers!

## Storage Engine

I'm going with a page-based storage system because it's battle-tested and works well. Each page is 4KB (might tune this later) and we memory-map the files for performance.

### Page Layout
```rust
struct Page {
    header: PageHeader,  // 64 bytes
    data: [u8; 4032],   // Rest of the 4KB page
}

struct PageHeader {
    page_id: u64,
    type_id: u32,       // What type of data is stored here
    free_space: u16,    // Where we can write next
    checksum: u32,      // For data integrity
    // ... other metadata
}
```

## Indexing

The index system is built around a custom B-tree implementation (because who doesn't love writing their own B-tree? 😅). The cool part is that it works with our custom types - you can index on any field that implements `Ord`.

### B-tree Design
- Order (B) = 6 (tunable)
- Nodes are page-aligned
- Support for composite keys
- Range scans
- Concurrent access (eventually...)

## Query Engine

The query engine is where it gets fun. We parse SQL-like syntax but with Rust types:

```sql
SELECT users.name, users.phone.country_code
FROM users
WHERE users.phone.area_code = Some(415)
AND users.preferences.contains("dark_mode");
```

### Query Planning
1. Parse the query into an AST
2. Figure out what indexes we can use
3. Generate an execution plan
4. Run it!

The planner is cost-based and uses statistics about our data to make good choices.

## Future Ideas

Things I want to add (help welcome!):
- [ ] MVCC for better concurrency
- [ ] Distributed queries
- [ ] More type system features (traits?)
- [ ] Query compilation to native code
- [ ] Time travel queries (because why not?)

## Implementation Notes

The code is organized into these main components:

```
src/
  ├── type_system/     -- Type definitions and validation
  ├── storage/         -- Page management and disk I/O
  ├── index/          -- B-tree and index management
  ├── query/          -- Query parsing and execution
  ├── buffer/         -- Buffer pool for caching
  └── main.rs         -- Server stuff
```

### Performance Goals
- Sub-millisecond point queries (with warm cache)
- 10K+ insertions/sec on modest hardware
- Support for databases up to 1TB

## Why This Matters

Current databases force you to think in terms of their type system. Want to store a phone number? Better make it a string or split it into columns. Want to store an enum? Hope you like VARCHAR!

RustDB lets you define your types naturally and stores them efficiently. It's like having Rust's type system in your database, which means:
1. Better data modeling
2. More efficient storage
3. Compile-time correctness checking
4. No more impedance mismatch

## Contributing

If you think this is cool, I'd love help! Check out the issues labeled "good first issue". Most pressing needs are:
- More efficient serialization for custom types
- Better query optimization
- Test coverage
- Documentation (always...)

## Testing Strategy

I'm big on testing (learned this the hard way). Here's the approach:
1. Unit tests for each component
2. Property-based testing for the type system
3. Integration tests with real data
4. Chaos testing for concurrent operations

## Implementation Plan

Phase 1 (Current):
- [x] Basic type system
- [x] Storage engine
- [x] Simple queries
- [ ] Single-node deployment

Phase 2:
- [ ] Advanced queries
- [ ] MVCC
- [ ] Better indexing
- [ ] Performance optimization

Phase 3:
- [ ] Distributed queries
- [ ] Replication
- [ ] Admin tools
- [ ] Production hardening

## Notes to Self

Things to not forget:
- Need to handle endianness in serialization
- Watch out for page splits in B-tree
- Add metrics everywhere
- Don't forget to write migrations
- Test with real workloads!

Let me know what you think! I'm especially interested in feedback on the type system design and storage layout. This is a big project but I think it could be really useful for folks who want better type safety in their databases.