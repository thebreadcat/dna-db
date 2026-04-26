# DNA-DB: Architecture & Build Specification v2.0

> A biologically-inspired database engine built for the modern developer stack.
> Familiar interface. Production-grade internals. DNA concepts where they add value,
> proven patterns where they don't.

---

## Table of Contents

1. [What DNA-DB Is](#1-what-dnadb-is)
2. [Core Philosophy](#2-core-philosophy)
3. [What Changed From v1 And Why](#3-what-changed-from-v1-and-why)
4. [DNA Concept Survival Map](#4-dna-concept-survival-map)
5. [Architecture Overview](#5-architecture-overview)
6. [Layer 1 — Physical Storage (CPU-Aligned Records)](#6-layer-1--physical-storage-cpu-aligned-records)
7. [Layer 2 — Write Pipeline (WAL + Memtable + LSM)](#7-layer-2--write-pipeline-wal--memtable--lsm)
8. [Layer 3 — Integrity System (Complement → CRC + Scrubbing)](#8-layer-3--integrity-system-complement--crc--scrubbing)
9. [Layer 4 — Global Index System](#9-layer-4--global-index-system)
10. [Layer 5 — Segment Layer (Bloom Filters + Block Stats)](#10-layer-5--segment-layer-bloom-filters--block-stats)
11. [Layer 6 — Guided Scan Engine (CRISPR Redefined)](#11-layer-6--guided-scan-engine-crispr-redefined)
12. [Layer 7 — Query Planner](#12-layer-7--query-planner)
13. [Layer 8 — Vector Execution Engine](#13-layer-8--vector-execution-engine)
14. [Layer 9 — Histone Clustering (Compaction-Time)](#14-layer-9--histone-clustering-compaction-time)
15. [Layer 10 — MVCC (Versioning)](#15-layer-10--mvcc-versioning)
16. [Layer 11 — Epigenetic Overlay System (Auth + Privacy)](#16-layer-11--epigenetic-overlay-system-auth--privacy)
17. [Layer 12 — Connection, Auth & Audit](#17-layer-12--connection-auth--audit)
18. [Layer 13 — Data Lifecycle & Telomere System](#18-layer-13--data-lifecycle--telomere-system)
19. [Layer 14 — Lateral Transfer Protocol (Schema Evolution)](#19-layer-14--lateral-transfer-protocol-schema-evolution)
20. [Layer 15 — Wire Protocol Compatibility](#20-layer-15--wire-protocol-compatibility)
21. [Layer 16 — Developer Interface & SDKs](#21-layer-16--developer-interface--sdks)
22. [Build Order & Why](#22-build-order--why)
23. [Group Commit & WAL Tuning](#23-group-commit--wal-tuning)
24. [Performance Characteristics](#24-performance-characteristics)
25. [Use Case Matrix](#25-use-case-matrix)
26. [Configuration Reference](#26-configuration-reference)

---

## 1. What DNA-DB Is

DNA-DB is a database engine whose architecture is inspired by how biological DNA stores, protects, indexes, retrieves, and evolves information — implemented as a production-grade software system on standard hardware.

It is not a database that uses biological material. It is a digital system that takes the concepts DNA proved over billions of years of evolution and maps them to proven database engineering patterns: LSM trees, bloom filters, MVCC, cost-based query planning, and vector execution.

The result is a database that:

- Stores records in **CPU-aligned binary format** with embedded intron index metadata, not raw base-4 — the DNA encoding is logical, not physical
- Validates integrity on **write and via background scrubbing**, not on every read, using CRC32 — complement integrity at read-time speed without the memory bandwidth cost
- Queries through a **three-path hybrid executor**: direct ID lookup, global index lookup, or guided CRISPR scan — the right path chosen by a cost-based planner automatically
- Skips irrelevant data using **bloom filters and block-level min/max statistics** so scans are guided, not blind
- Processes records in **vectorized batches** for cache efficiency and SIMD utilization
- Co-locates related records during **compaction**, not continuously — histone clustering without constant rewrites
- Supports **multiple simultaneous schemas** on the same data through epigenetic overlays enforced at the engine output stage
- Manages **data lifecycle** through telomere counters, with immortal mode on by default
- Evolves schema across nodes through **lateral transfer packets** — no migrations, no downtime

Developers interact with none of this directly. The interface looks and feels like a modern document database with SQL-style expressiveness. The engine is the implementation detail.

---

## 2. Core Philosophy

**Wrong states should be structurally impossible, not just policy-enforced.**

Every architectural decision asks: can this class of error be made impossible by structure, or does it require policy and discipline to avoid?

- Corruption is detected at write time and by background scrubbing — not silently accumulated
- Orphaned relationships are impossible because references are structural (intron pointers in the record itself), not logical (integer IDs in application code)
- Stale indexes are impossible because index metadata lives inside the records they describe
- PII leakage requires a deliberate breach of the overlay system, not just a forgotten WHERE clause
- Schema mismatches across nodes are handled by version coexistence, not flag-day coordination

**Performance must be predictable, not just fast.**

A database that is fast on average but unpredictable under load is not production-grade. Every design choice that introduces unpredictability — per-read complement validation, continuous histone reorganization, full CRISPR scans as primary path — has been replaced with a predictable equivalent that preserves the benefit.

---

## 3. What Changed From v1 And Why

This section is explicit about every change. Nothing was changed arbitrarily — each change was forced by a real constraint that the original design did not account for.

---

### 3.1 DNA Encoding → CPU-Aligned Binary Storage

**Original:** All data stored as base-4 codon sequences (A/T/C/G mapped to 0/1/2/3). Dense biological encoding.

**Change:** Data stored in byte-aligned binary with a structured header. DNA encoding becomes logical (concepts, naming, relationships) not physical (actual storage format).

**Why:** CPUs execute on bytes, not base-4 symbols. SIMD instructions (AVX-512, SSE4) operate on 8/16/32/64-byte lanes. Storing data in base-4 requires unpacking before any SIMD operation can touch it — adding a decode step to every read, every comparison, every sort. The density benefit of base-4 (~1.58x over binary) is outweighed by the CPU penalty on every access. The DNA concepts are fully preserved as logical structure; the physical layer is what the hardware actually runs efficiently on.

---

### 3.2 Complement Validation → Write-Time CRC + Background Scrubbing

**Original:** Complement strand generated for every write and validated on every read. Corruption self-evident by structural mismatch.

**Change:** CRC32 checksum written alongside each record at write time. Validated on write. Background scrubber validates all segments on a configurable interval. Read-time validation optional (off by default for hot paths).

**Why:** Per-read complement validation doubles memory access on every read — you load the record and its complement. For a scan of 100k records, that is 100k extra memory fetches. At memory bandwidth of ~50GB/s on modern hardware, this alone limits scan throughput by roughly 40%. The integrity guarantee is fully preserved — corruption is detected at write time (immediately) and by the scrubber (within a configurable window). The only thing removed is the per-read detection latency, which in practice adds no value because the scrubber catches corruption before it causes silent data issues.

---

### 3.3 Introns As Only Index → Introns + Global Index

**Original:** Introns (index metadata woven into records) as the sole indexing mechanism. Index can never go stale because it lives inside the data.

**Change:** Introns retained as embedded per-record metadata. Global hash index and optional range index added on top. Query planner chooses between them based on cost.

**Why:** Introns solve staleness (they can't drift from the data because they are part of the data). They do not solve O(N) scan cost on large datasets. For a collection of 100M records, finding all users with `email = "alice@x.com"` via intron scan still requires touching every record. The global hash index brings this to O(1). Introns remain the source of truth — the global index is built from intron data, so they can never disagree.

---

### 3.4 Full CRISPR Scan → Hybrid Query Execution

**Original:** CRISPR-style parallel scan as the primary query mechanism. Pattern broadcasts across all strands simultaneously.

**Change:** Three-path hybrid executor. CRISPR guided scan is the fallback for queries that cannot use a direct or index path. When it runs, it is guided by bloom filters and block stats — not a blind full scan.

**Why:** Parallel scanning is fast relative to sequential scanning. It is not fast relative to an index lookup. An index lookup on 1B records is O(1) at ~0.5ms. A parallel scan of 1B records, even with 64 cores, is O(N/cores) — still hundreds of milliseconds. The CRISPR scan is preserved as the right tool for pattern queries where no index exists. It is not the right tool when a better path is available.

---

### 3.5 Continuous Histone Reorganization → Compaction-Time Clustering

**Original:** Histone block manager continuously reorganizes strands based on access patterns. Hot data co-located in real time.

**Change:** Related records are clustered during compaction — a scheduled background process that rewrites cold segments with records sorted by their foreign key / relationship field.

**Why:** Continuous reorganization means constant write amplification — records being rewritten to new positions whenever the access heat changes. On a write-heavy workload, this competes with the write pipeline for IO bandwidth. Compaction-time clustering achieves the same co-location benefit (related data physically adjacent for fast traversal) at a fraction of the IO cost, on a schedule that doesn't compete with the hot write path.

---

### 3.6 What Was Removed Entirely From The Hot Path

The following were removed from the per-operation hot path. They are either deferred, made opt-in, or replaced:

| Removed From Hot Path | Replaced With | Status |
|---|---|---|
| Per-read complement validation | Write-time CRC + background scrubber | Active, background |
| Full blind CRISPR scan as primary path | Hybrid executor with index + guided scan | Active, fallback only |
| Strict DNA base-4 encoding | CPU-aligned binary with logical DNA structure | Active, physical layer |
| Continuous histone reorganization | Compaction-time clustering | Active, scheduled |
| Lateral transfer protocol | Still designed, lower priority | Deferred |
| Telomere lifecycle (expiry) | Immortal mode default, opt-in expiry | Deferred / opt-in |

Nothing was removed from the system. Each item above still exists — it was moved out of the critical path or made opt-in.

---

## 4. DNA Concept Survival Map

Every biological concept from the original design survives in the new architecture. The table below shows exactly how each one maps.

| Biological Concept | v1 Implementation | v2 Implementation | Status |
|---|---|---|---|
| Base-4 alphabet | Physical base-4 codon storage | Logical concept only — binary physical storage | Transformed |
| Double helix / complement | Per-read structural validation | Write-time CRC + background scrubbing | Transformed |
| Codons | Physical 3-symbol data units | Logical record unit concept | Transformed |
| Introns | Embedded per-record index nodes | Embedded per-record index nodes | Unchanged |
| Exons | Data payload between introns | Record payload bytes | Unchanged |
| Histones / nucleosomes | Continuous hot/warm/cold tiering | Compaction-time clustering + storage tiering | Transformed |
| Epigenetics | Schema overlay system | Schema overlay system (moved to output stage) | Refined |
| CRISPR | Primary parallel scan | Guided scan fallback (bloom + intron + block stats) | Transformed |
| Telomeres | Auto-expiry lifecycle | Immortal default, opt-in per collection | Refined |
| Lateral gene transfer | Rolling schema propagation | Designed, deferred to distributed phase | Deferred |
| Start / stop codons | Record delimiters | Record header magic bytes | Transformed |

The DNA ideas are all present. The physical implementations were replaced where the original implementation conflicted with hardware reality.

---

## 5. Architecture Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                     DEVELOPER INTERFACE                         │
│         TypeScript SDK / Python SDK / REST / GraphQL            │
│         MongoDB-compatible wire protocol (port 27017)           │
│         PostgreSQL-compatible wire protocol (port 5432)         │
├─────────────────────────────────────────────────────────────────┤
│                     QUERY COMPILER                              │
│       SQL-like / Mongo-like syntax → Query AST                  │
├─────────────────────────────────────────────────────────────────┤
│                     AUTH & OVERLAY RESOLVER                     │
│       Token → Identity → Overlay → Field mask                   │
├──────────────────────────┬──────────────────────────────────────┤
│      QUERY PLANNER       │     EPIGENETIC OVERLAY SYSTEM        │
│   Cost-based path select │     Schema-per-context management    │
├──────────────────────────┴──────────────────────────────────────┤
│                     HYBRID EXECUTOR                             │
│   Path 1: Direct ID lookup                                      │
│   Path 2: Global index lookup (hash / range)                    │
│   Path 3: Guided CRISPR scan (bloom + intron + block stats)     │
├─────────────────────────────────────────────────────────────────┤
│                     VECTOR EXECUTION ENGINE                     │
│      Batch record processing — SIMD predicates                  │
├──────────────────────────┬──────────────────────────────────────┤
│   SEGMENT LAYER          │    GLOBAL INDEX LAYER                │
│   Bloom filters          │    Hash index                        │
│   Block min/max stats    │    Range index (optional)            │
│   CRC integrity          │    Built from intron metadata        │
├──────────────────────────┴──────────────────────────────────────┤
│                     LSM WRITE PIPELINE                          │
│   WAL → Group Commit → Memtable → Immutable Segment flush       │
├─────────────────────────────────────────────────────────────────┤
│                     PHYSICAL STORAGE                            │
│   CPU-aligned binary records with embedded intron metadata      │
│   Memory-mapped segment files                                   │
│   Background scrubber (CRC validation)                          │
│   Compaction worker (clustering + segment merging)              │
├─────────────────────────────────────────────────────────────────┤
│                     OS / HARDWARE                               │
│         NVMe SSD, RAM, CPU cores with SIMD (AVX-512)            │
└─────────────────────────────────────────────────────────────────┘
```

---

## 6. Layer 1 — Physical Storage (CPU-Aligned Records)

### What It Is

The on-disk and in-memory format for every record. Byte-aligned for CPU efficiency, structured for self-description, carrying embedded intron index metadata.

### Why This Format

Binary byte alignment means SIMD instructions can operate directly on record data without unpacking. The record header is fixed-size so offset calculations are O(1). Intron metadata is appended after the payload so it can be skipped in O(1) for payload-only reads.

### Record Format

```rust
// Fixed-size header — always at offset 0
#[repr(C, align(8))]
struct RecordHeader {
    magic:          [u8; 4],     // start codon equivalent — 0xDNA_MAGIC
    id:             u64,         // unique record identifier
    version:        u64,         // monotonic version (MVCC)
    collection_id:  u32,         // which collection
    created_at:     u64,         // nanosecond unix timestamp
    updated_at:     u64,
    payload_len:    u32,         // bytes of payload following header
    intron_count:   u16,         // number of intron nodes following payload
    flags:          u16,         // deleted, immortal, encrypted, compressed
    checksum:       u32,         // CRC32 of (header fields + payload)
}

// Variable-length payload follows header immediately
// [header][payload_bytes × payload_len]

// Intron nodes follow payload
// These are the embedded index metadata — the DNA intron concept
#[repr(C)]
struct Intron {
    field_hash:     u64,         // hash of field name
    value_hash:     u64,         // hash of field value (for equality matching)
    value_min:      u64,         // numeric min (for range queries)
    value_max:      u64,         // numeric max
    payload_offset: u32,         // byte offset of this field in payload
    payload_len:    u16,         // byte length of this field in payload
    field_type:     u8,          // string, int, float, bool, reference
    reserved:       u8,
}

// [header][payload][intron × intron_count][end_magic: 4 bytes]
```

### Physical Files

```
/data/
  {collection}/
    segments/
      seg_00001.dat         ← immutable flushed segment (memory-mapped)
      seg_00002.dat
      ...
    wal/
      wal_current.log       ← active write-ahead log
      wal_000001.log        ← sealed WAL (pending memtable flush)
    indexes/
      hash_{field}.idx      ← global hash index per indexed field
      range_{field}.idx     ← global range index (if enabled)
    meta/
      collection.meta       ← schema hints, overlay registry, stats
      compaction.state      ← compaction progress tracking
```

---

## 7. Layer 2 — Write Pipeline (WAL + Memtable + LSM)

### What It Is

The write path from developer insert call to durable storage. Modeled on LSM (Log-Structured Merge) architecture — the same foundation as RocksDB, LevelDB, Cassandra, and ClickHouse.

### Why LSM

LSM converts random writes into sequential writes. Sequential writes on NVMe SSD are 5-10x faster than random writes. The WAL captures every write immediately and sequentially. The memtable accumulates writes in memory. When the memtable reaches its size limit, it is flushed to an immutable segment file in one large sequential write. This is far more efficient than updating a B-tree in place for every record.

### Write Flow

```
Developer: db.collection("users").insert(record)
                │
                ▼
        1. Serialize record to binary (header + payload + introns)
           Compute CRC32 checksum
           Stamp created_at, version=1
                │
                ▼
        2. WAL append (sequential write to wal_current.log)
           Group commit: fsync when batch_size reached OR interval elapsed
           Return success to developer  ← developer is done here
                │
                ▼ (background)
        3. Memtable insert
           In-memory BTreeMap<RecordId, Record>
           Also updates in-memory global indexes
                │
                ▼ (when memtable reaches size limit, default 64MB)
        4. Memtable flush
           Sort by ID
           Cluster by foreign key (histone concept)
           Write immutable segment file (seg_NNNNN.dat)
           Write bloom filter and block stats for segment
           Seal WAL entries covered by this flush
                │
                ▼ (background compaction)
        5. Segment compaction
           Merge multiple small segments into fewer large ones
           Apply deletions (tombstone removal)
           Re-cluster related records by foreign key
           Rebuild bloom filters and block stats
           Update global indexes
```

### Group Commit (WAL Durability)

As validated in benchmarking — per-record fsync kills write throughput. Group commit batches multiple WAL entries and fsyncs once:

```rust
pub struct GroupCommit {
    policy:          GroupCommitPolicy,
    pending:         AtomicU64,           // records pending since last sync
    unsynced_since:  Mutex<Option<Instant>>,  // when first unsynced record arrived
}

pub struct GroupCommitPolicy {
    max_batch:    u64,                    // fsync after this many records
    max_interval: Option<Duration>,       // fsync after this much time since first pending
}

impl GroupCommit {
    pub fn should_sync(&self) -> bool {
        let pending = self.pending.load(Ordering::Relaxed);
        
        // Batch threshold
        if pending >= self.policy.max_batch {
            return true;
        }
        
        // Staleness timer — measured from first unsynced record, not last fsync
        if let Some(interval) = self.policy.max_interval {
            if let Some(since) = *self.unsynced_since.lock().unwrap() {
                if since.elapsed() >= interval {
                    return true;
                }
            }
        }
        
        false
    }
}
```

**Mode defaults:**

| Mode | Batch Size | Interval | Durability Guarantee |
|---|---|---|---|
| Strict | 1,000 | off (batch-only) | Durable within 1,000 records |
| Balanced | 1,000 | 50ms | Durable within 50ms |
| Fast | unlimited | off | Durable at shutdown / explicit flush |

**Why timer uses `unsynced_since` not `last_sync`:** The staleness bound is a promise about how stale your data can be. That clock must start when the data enters the pipeline, not when the last fsync completed. Using last_sync creates a window where data written immediately after a slow fsync can sit for `interval + fsync_duration` before being synced — violating the bound.

### Memtable

```rust
struct Memtable {
    records:     BTreeMap<RecordId, Record>,   // sorted for efficient flush
    size_bytes:  usize,
    index_delta: IndexDelta,                   // buffered index updates
    capacity:    usize,                        // default 64MB
}
```

The BTreeMap maintains sort order so segment flush is always sequential — no sort step needed at flush time.

---

## 8. Layer 3 — Integrity System (Complement → CRC + Scrubbing)

### What It Is

The v2 implementation of the complement strand integrity concept. Every record carries a CRC32 checksum. Integrity is validated on write and by a continuous background scrubber. Read-time validation is available as an opt-in for high-assurance workloads.

### Why CRC32 Instead Of Complement Strand

The complement strand is a beautiful biological concept. In software, it doubles every read's memory access — load record, load complement, compare. At a scan throughput of 200k records/sec, this means 200k extra memory fetches per second for data that is almost never corrupted. CRC32 achieves the same detection capability (catches all single-bit errors and most multi-bit errors) at the cost of 4 bytes per record and one integer comparison on write. The per-read cost drops to zero because validation does not happen on the hot read path.

### Write-Time Validation

```rust
fn write_record(record: &mut Record, buf: &mut Vec<u8>) -> Result<()> {
    // Serialize header fields (excluding checksum field)
    let header_bytes = serialize_header_without_checksum(&record.header);
    
    // Compute CRC32 over header + payload
    let mut hasher = crc32fast::Hasher::new();
    hasher.update(&header_bytes);
    hasher.update(&record.payload);
    record.header.checksum = hasher.finalize();
    
    // Write complete record
    buf.extend_from_slice(&serialize_header(&record.header));
    buf.extend_from_slice(&record.payload);
    buf.extend_from_slice(&serialize_introns(&record.introns));
    
    Ok(())
}
```

Any corruption that occurs after this write is detectable by the scrubber.

### Background Scrubber

```rust
async fn scrub_collection(collection: &Collection) {
    for segment in collection.segments() {
        for record in segment.records() {
            if !validate_crc(record) {
                // Log corruption with segment, offset, record ID
                corruption_log.record(CorruptionEvent {
                    segment_id:  segment.id,
                    record_id:   record.header.id,
                    offset:      record.offset,
                    detected_at: Instant::now(),
                });
                
                // Attempt repair from replica if available
                if let Some(replica) = replication.find_replica() {
                    repair_from_replica(record, replica).await?;
                }
            }
        }
    }
}
```

Scrubber runs on a configurable interval (default: every 6 hours). Corruption events are written to the audit collection. On a replicated cluster, corrupt records are automatically repaired from a healthy replica.

### Optional Read-Time Validation

For collections requiring the highest assurance — financial records, audit logs, legal data:

```javascript
db.collection("transactions").configure({
    integrity: {
        validate_on_read: true     // pay the extra fetch cost on every read
    }
})
```

---

## 9. Layer 4 — Global Index System

### What It Is

A global hash index and optional range index maintained alongside the segment files. Builds on top of intron metadata — introns are the source of truth, the global index is a derived structure for O(1) lookup.

### Why Introns Alone Are Not Enough

Introns prevent index staleness — the index cannot disagree with the data because it is part of the data. They do not prevent O(N) scan cost. The global index provides O(1) lookup by maintaining a mapping from field value → record locations. The two systems are complementary: introns guarantee correctness, global indexes guarantee performance.

### Hash Index

```rust
struct HashIndex {
    field:   String,
    entries: HashMap<u64, Vec<RecordLocation>>,   // value_hash → locations
}

struct RecordLocation {
    segment_id: u32,
    offset:     u64,         // byte offset in segment file
    record_id:  u64,         // for verification after load
}

impl HashIndex {
    fn lookup(&self, value: &[u8]) -> Vec<RecordLocation> {
        let hash = hash_value(value);
        self.entries.get(&hash)
            .cloned()
            .unwrap_or_default()
    }
    
    fn insert(&mut self, value: &[u8], location: RecordLocation) {
        let hash = hash_value(value);
        self.entries.entry(hash).or_default().push(location);
    }
}
```

### Range Index (Optional Per Field)

For fields frequently queried with range predicates (age > 25, created_at between X and Y):

```rust
struct RangeIndex {
    field:  String,
    tree:   BTreeMap<OrderedValue, Vec<RecordLocation>>,
}
```

Enabled per collection per field:

```javascript
db.collection("orders").configure({
    indexes: {
        range: ["created_at", "total"]     // enable range index on these fields
    }
})
```

### Index Build and Maintenance

Indexes are built from intron metadata — never from a separate scan of the payload:

```rust
fn build_index_from_introns(segments: &[Segment], field: &str) -> HashIndex {
    let mut index = HashIndex::new(field);
    let field_hash = hash_field_name(field);
    
    for segment in segments {
        for (offset, record) in segment.records_with_offsets() {
            // Find the intron for this field — O(intron_count) per record
            if let Some(intron) = record.introns.iter()
                .find(|i| i.field_hash == field_hash) 
            {
                index.insert_raw(intron.value_hash, RecordLocation {
                    segment_id: segment.id,
                    offset,
                    record_id: record.header.id,
                });
            }
        }
    }
    
    index
}
```

Because the index is built from introns which live inside the records, the index can be rebuilt at any time by scanning the segments. It is a cache of intron data, not an independent source of truth.

---

## 10. Layer 5 — Segment Layer (Bloom Filters + Block Stats)

### What It Is

Metadata attached to each segment file that allows the query engine to skip entire segments without reading them. The two structures are a bloom filter (probabilistic set membership) and block-level min/max statistics.

### Why These Two Structures

Together they eliminate the majority of unnecessary IO before any record is read:

- **Bloom filter:** "Does this segment definitely NOT contain a record where email = alice@x.com?" If the bloom filter says no, skip the segment entirely. Zero false negatives — if the record is there, the bloom filter will not skip it. Some false positives — occasionally a segment is checked when it has no match, but this is rare and bounded.

- **Block stats:** "Does this segment contain any record where created_at is between X and Y?" If the min created_at in the segment is higher than Y, skip it. Perfect precision for range queries on sorted fields.

### Bloom Filter

```rust
struct BloomFilter {
    bits:        BitVec,
    hash_count:  u8,          // number of hash functions
    capacity:    u64,         // approximate false positive rate at this capacity
}

impl BloomFilter {
    fn add(&mut self, value: &[u8]) {
        for seed in 0..self.hash_count {
            let bit = hash_with_seed(value, seed) % self.bits.len() as u64;
            self.bits.set(bit as usize, true);
        }
    }
    
    fn might_contain(&self, value: &[u8]) -> bool {
        (0..self.hash_count).all(|seed| {
            let bit = hash_with_seed(value, seed) % self.bits.len() as u64;
            self.bits[bit as usize]
        })
    }
}
```

Built for each indexed field when a segment is written. Persisted alongside the segment file.

### Block Statistics

```rust
struct SegmentMeta {
    segment_id:   u32,
    record_count: u64,
    min_id:       u64,
    max_id:       u64,
    field_stats:  HashMap<u64, FieldStats>,    // field_hash → stats
}

struct FieldStats {
    min_numeric: u64,
    max_numeric: u64,
    bloom:       BloomFilter,
}
```

### Query Skip Logic

```rust
fn should_skip_segment(segment_meta: &SegmentMeta, clause: &QueryClause) -> bool {
    let stats = match segment_meta.field_stats.get(&clause.field_hash) {
        Some(s) => s,
        None => return false,    // no stats for this field — cannot skip
    };
    
    match &clause.predicate {
        // Equality: bloom filter check
        Predicate::Eq(value) => !stats.bloom.might_contain(value),
        
        // Range: block stats check
        Predicate::Gt(n) => stats.max_numeric <= *n,
        Predicate::Lt(n) => stats.min_numeric >= *n,
        Predicate::Between(lo, hi) => stats.max_numeric < *lo || stats.min_numeric > *hi,
        
        // Cannot skip for LIKE / pattern queries — must scan
        Predicate::Like(_) => false,
    }
}
```

---

## 11. Layer 6 — Guided Scan Engine (CRISPR Redefined)

### What It Is

The CRISPR scan engine from v1, redesigned as a guided fallback rather than the primary query path. When no index is available, the guided scan finds matching records by combining bloom filter skipping at the segment level, intron hash matching at the record level, and parallel execution across CPU cores.

### Why It Is Still Essential

Many real queries cannot use an index — queries on fields that are not indexed, pattern matching with wildcards, queries combining multiple fields where only some are indexed. The guided scan handles these cases efficiently without requiring every possible query pattern to have a pre-built index.

### Guided Scan vs Original Full CRISPR Scan

```
Original CRISPR scan:
  → Broadcast pattern to all strands simultaneously
  → All strands checked regardless of content
  → Cost: O(N) always

Guided scan (v2):
  → Check segment bloom filter — skip if definitely no match
  → For non-skipped segments: check intron hashes per record
  → Only decode payload for intron-confirmed matches
  → Parallel across CPU cores
  → Cost: O(matching_segments × matching_records_per_segment)
  
On a well-distributed dataset, most segments are skipped.
Cost approaches O(result_set_size), not O(N).
```

### Implementation

```rust
fn guided_scan(
    segments: &[Segment],
    query:    &Query,
    threads:  usize,
) -> Vec<Record> {
    // Split segments across threads
    let chunks: Vec<_> = segments.chunks(segments.len() / threads).collect();
    
    let results: Vec<Vec<Record>> = chunks
        .par_iter()   // rayon parallel iterator
        .map(|chunk| scan_chunk(chunk, query))
        .collect();
    
    results.into_iter().flatten().collect()
}

fn scan_chunk(segments: &[Segment], query: &Query) -> Vec<Record> {
    let mut results = Vec::new();
    
    for segment in segments {
        // Segment-level skip: bloom filter + block stats
        if query.clauses.iter().any(|c| should_skip_segment(&segment.meta, c)) {
            continue;
        }
        
        // Record-level scan with intron fast path
        for record in segment.records() {
            // Intron hash check — cheap, no payload decode
            if !intron_match(record, query) {
                continue;
            }
            
            // Full decode only for confirmed intron matches
            let decoded = decode_record(record);
            
            // Final predicate verification on decoded values
            if query.matches(&decoded) {
                results.push(decoded);
            }
        }
    }
    
    results
}

fn intron_match(record: &RawRecord, query: &Query) -> bool {
    query.clauses.iter().all(|clause| {
        record.introns.iter().any(|intron| {
            intron.field_hash == clause.field_hash
            && match &clause.predicate {
                Predicate::Eq(_)      => intron.value_hash == clause.value_hash,
                Predicate::Gt(n)      => intron.value_max > *n,
                Predicate::Lt(n)      => intron.value_min < *n,
                Predicate::Between(lo, hi) => intron.value_min <= *hi && intron.value_max >= *lo,
                Predicate::Like(_)    => true,   // cannot use intron hash for LIKE — pass through
            }
        })
    })
}
```

---

## 12. Layer 7 — Query Planner

### What It Is

A cost-based query planner that selects the optimal execution path for each query before execution begins. Takes a parsed query AST and the current collection statistics, and emits an execution plan.

### The Three Paths

```rust
enum QueryPath {
    Direct,        // ID lookup — O(1) via record ID → segment location map
    Index,         // Global index lookup — O(1) hash or O(log N) range
    GuidedScan,    // Guided CRISPR scan — O(matching segments × matching records)
}

fn choose_path(query: &Query, stats: &CollectionStats) -> ExecutionPlan {
    // Path 1: ID lookup
    if query.is_id_lookup() {
        return ExecutionPlan::Direct { id: query.id() };
    }
    
    // Path 2: Index available for the leading clause
    if let Some(index) = stats.best_index_for(query) {
        let estimated_cost = index.estimated_result_count(query) as f64 * INDEX_LOOKUP_COST;
        let scan_cost      = stats.record_count as f64 * SCAN_COST_PER_RECORD;
        
        if estimated_cost < scan_cost {
            return ExecutionPlan::IndexLookup {
                index,
                residual_clauses: query.non_indexed_clauses(),
            };
        }
    }
    
    // Path 3: Guided scan fallback
    ExecutionPlan::GuidedScan {
        query: query.clone(),
        parallelism: stats.recommended_parallelism(),
    }
}
```

### Cost Model

```rust
const INDEX_LOOKUP_COST:    f64 = 1.0;    // one index lookup
const RECORD_LOAD_COST:     f64 = 2.0;    // one record load from segment
const SCAN_COST_PER_RECORD: f64 = 0.1;    // intron check without full decode

fn estimate_cost(plan: &ExecutionPlan, stats: &CollectionStats) -> f64 {
    match plan {
        ExecutionPlan::Direct { .. } => {
            INDEX_LOOKUP_COST + RECORD_LOAD_COST
        }
        
        ExecutionPlan::IndexLookup { index, .. } => {
            INDEX_LOOKUP_COST
            + index.estimated_result_count as f64 * RECORD_LOAD_COST
        }
        
        ExecutionPlan::GuidedScan { .. } => {
            let skipped_fraction = stats.estimated_bloom_skip_rate;
            let remaining = stats.record_count as f64 * (1.0 - skipped_fraction);
            remaining * SCAN_COST_PER_RECORD
            + stats.estimated_result_count as f64 * RECORD_LOAD_COST
        }
    }
}
```

The cost constants are tunable and will be calibrated against real benchmark data as the system matures. The planner's statistics (record counts, index cardinalities, bloom skip rates) are maintained by the segment layer and updated after each compaction.

---

## 13. Layer 8 — Vector Execution Engine

### What It Is

A batch-processing layer that processes records in fixed-size chunks rather than one at a time. Every operation in the query pipeline — predicate evaluation, field projection, overlay masking, aggregation — operates on batches.

### Why Batching

Processing one record at a time is CPU-inefficient for three reasons:

- **Branch misprediction:** The per-record conditional logic (does this record match?) creates unpredictable branches that stall the CPU pipeline
- **Cache misses:** Loading one record at a time means the CPU prefetcher cannot run ahead
- **No SIMD:** SIMD instructions operate on vectors of values. Single-record processing never uses them.

Batching fixes all three: predictable access patterns enable prefetching, SIMD operates across the batch, and branch density increases.

### Implementation

```rust
const BATCH_SIZE: usize = 1024;   // tuned to L1 cache size

struct RecordBatch {
    records:  Vec<DecodedRecord>,
    len:      usize,
}

fn process_batch(
    batch:      &RecordBatch,
    predicates: &[Predicate],
    overlay:    &Overlay,
) -> Vec<OutputRecord> {
    // Step 1: Evaluate all predicates across entire batch
    // SIMD-friendly: same operation on N values
    let mut mask = [true; BATCH_SIZE];
    
    for predicate in predicates {
        apply_predicate_to_batch(predicate, batch, &mut mask);
    }
    
    // Step 2: Collect matching records
    let mut output = Vec::with_capacity(mask.iter().filter(|&&m| m).count());
    
    for (i, record) in batch.records[..batch.len].iter().enumerate() {
        if mask[i] {
            // Step 3: Apply overlay field masking to each match
            output.push(apply_overlay(record, overlay));
        }
    }
    
    output
}

fn apply_predicate_to_batch(
    predicate: &Predicate,
    batch:     &RecordBatch,
    mask:      &mut [bool; BATCH_SIZE],
) {
    match predicate {
        Predicate::Eq { field, value } => {
            for (i, record) in batch.records[..batch.len].iter().enumerate() {
                if mask[i] {
                    mask[i] = record.get_field(field)
                        .map(|v| v == value)
                        .unwrap_or(false);
                }
            }
        }
        // ... other predicate types
    }
}
```

The mask array pattern (evaluate predicates as bitmask, then collect) is the same approach used by DuckDB and Apache Arrow's compute kernels. It is specifically structured so the compiler can auto-vectorize the inner loops.

---

## 14. Layer 9 — Histone Clustering (Compaction-Time)

### What It Is

The v2 implementation of the histone co-location concept. Related records (a user and their orders) are physically co-located in segment files during compaction. This makes related-data reads a sequential read of one segment region rather than random seeks across multiple locations.

### Why Compaction-Time, Not Continuous

Continuous reorganization (v1 approach) competes with the write pipeline for IO bandwidth. Every record movement is a write. On a write-heavy workload, continuous reorganization adds 20-50% to write IO with no benefit to the write path.

Compaction-time clustering achieves the same co-location at much lower cost: compaction is already rewriting segments to merge small files and remove tombstones. Sorting by foreign key during this rewrite is effectively free — the data is being written anyway.

### Implementation

```rust
fn compact_segments(
    segments:        Vec<Segment>,
    clustering_field: Option<&str>,    // e.g. "user_id" for orders collection
) -> Segment {
    // Collect all live records from input segments
    let mut records: Vec<Record> = segments
        .iter()
        .flat_map(|s| s.live_records())
        .collect();
    
    // Sort by clustering field if specified — the histone co-location step
    if let Some(field) = clustering_field {
        records.sort_by_key(|r| r.get_field(field).and_then(|v| v.as_u64()));
    }
    
    // Write new merged segment (sequential write — fast)
    write_segment(records)
}
```

### Storage Tiering (Hot/Warm/Cold)

Tiering based on access temperature is still maintained — it was not removed, just decoupled from clustering:

```rust
enum StorageTier {
    Hot,       // memory-mapped, in RAM page cache
    Warm,      // memory-mapped, on NVMe SSD
    Cold,      // compressed (LZ4), on HDD or object storage
    Frozen,    // compressed archive, telomere-expired records
}
```

The histone block manager from v1 becomes the **compaction scheduler + tier manager**: it decides when to compact, which segments to merge, which tier each segment lives on, and when to promote or demote. It no longer reorganizes individual records in real time.

---

## 15. Layer 10 — MVCC (Versioning)

### What It Is

Multi-Version Concurrency Control. Every record mutation creates a new version rather than overwriting the previous one. Readers see a consistent snapshot of the database at a point in time. Writers never block readers.

### Why MVCC

Without MVCC, a reader mid-scan can see a partially-updated record if a writer modifies it during the scan. This produces phantom reads, inconsistent query results, and makes transactions impossible. MVCC eliminates read/write conflicts by giving each transaction its own consistent view.

### Implementation

```rust
struct VersionChain {
    record_id: u64,
    versions:  Vec<VersionEntry>,   // newest first
}

struct VersionEntry {
    version:     u64,        // monotonically increasing
    created_at:  u64,        // transaction timestamp
    data:        Record,
    deleted:     bool,       // tombstone flag
    txn_id:      u64,        // which transaction created this version
}

impl VersionChain {
    // Return the latest version visible to a transaction at timestamp `ts`
    fn visible_at(&self, ts: u64) -> Option<&Record> {
        self.versions.iter()
            .find(|v| v.created_at <= ts && !v.deleted)
            .map(|v| &v.data)
    }
}
```

### Transaction Model

```rust
struct Transaction {
    txn_id:       u64,
    snapshot_ts:  u64,          // read timestamp — sees versions committed before this
    write_set:    Vec<WriteOp>, // buffered writes
    status:       TxnStatus,
}

// Developer-facing transaction API
await db.transaction(async (tx) => {
    // All reads use snapshot_ts — consistent view
    const user  = await tx.collection("users").where("id", "=", userId).fetchOne()
    const order = await tx.collection("orders").insert({ user_id: userId, total: 89.00 })
    
    // Writes buffered until commit
    await tx.collection("inventory")
        .where("product_id", "=", productId)
        .update({ stock: user.stock - 1 })
    
    // Commit: all writes applied atomically or none
})
```

### Version Cleanup

Old versions are removed during compaction once no active transaction can reference them (their `created_at` is older than the oldest active transaction snapshot).

---

## 16. Layer 11 — Epigenetic Overlay System (Auth + Privacy)

### What It Is

A system for defining multiple simultaneous read schemas over the same underlying data. The same record returns different fields depending on the identity reading it. Enforcement happens at the engine output stage — after query execution, before serialization. Application code cannot bypass it.

### Why Output-Stage Enforcement Is Critical

Application-layer filtering (a WHERE clause, a projection, a middleware function) can be forgotten. It can be bypassed by a new code path. It can be misconfigured under pressure.

Output-stage enforcement happens outside the application's execution context. There is no code path in the application that bypasses it — the application never receives fields its overlay does not permit. The error class is eliminated, not mitigated.

### Overlay Definition

```javascript
db.defineOverlay("admin", {
    access: "full",
    mutations: ["read", "write", "delete"]
})

db.defineOverlay("support_agent", {
    access: "partial",
    collections: ["users", "orders"],
    include_fields: {
        users:  ["id", "name", "email", "created_at"],
        orders: ["id", "status", "items", "created_at"]
    },
    exclude_fields: {
        users: ["payment_info", "ssn", "password_hash"]
    },
    mutations: ["read"]
})

db.defineOverlay("analytics_service", {
    access: "partial",
    collections: ["users", "orders"],
    include_fields: {
        users:  ["id", "age", "region", "account_tier"],
        orders: ["id", "total", "item_count", "created_at"]
    },
    mutations: ["read"]
})
```

### Enforcement Point

```rust
// This runs after all query execution, before serialization
fn apply_overlay(record: &DecodedRecord, overlay: &Overlay) -> OutputRecord {
    let allowed_fields: HashSet<&str> = overlay.allowed_fields(&record.collection);
    
    OutputRecord {
        fields: record.fields.iter()
            .filter(|(name, _)| allowed_fields.contains(name.as_str()))
            .cloned()
            .collect()
    }
}
```

The overlay is resolved from the auth token at connection time, not at query time. The application cannot pass a different overlay per query — the overlay is a property of the identity, not the request.

### Compound Overlays

```javascript
db.defineOverlay("senior_support", {
    extends:              "support_agent",
    additionally_include: {
        users: ["payment_last_four"]
    }
})
```

---

## 17. Layer 12 — Connection, Auth & Audit

### Connection Ports

```
4737   Native DNA-DB binary protocol
27017  MongoDB wire protocol (compatibility)
5432   PostgreSQL wire protocol (compatibility)
8443   REST/HTTPS API
```

### Authentication

```rust
enum AuthMethod {
    Password(Argon2Hash),
    ApiKey(Blake3Hash),
    JWT(JWTConfig),
    MTLS(Certificate),       // service-to-service
    OIDC(OIDCProvider),      // SSO
}

struct Identity {
    identity_id:          Uuid,
    overlay:              String,
    allowed_collections:  Vec<String>,
    token_expiry:         u64,
    auth_method:          AuthMethod,
}
```

### Privacy — Three Independent Layers

**Network:** TLS 1.3 required. No plaintext in production.

**Auth:** Every connection authenticated. Every identity bound to an overlay. No identity can read beyond its overlay's field mask.

**Storage:** Segment files encrypted at rest using AES-256-GCM. Key hierarchy:

```
Master key (HSM / KMS)
  └── Collection key (per collection, encrypted by master key)
        └── Segment key (per segment, encrypted by collection key)
```

Raw `.dat` segment files accessed outside the engine are unreadable without the key hierarchy.

### Audit Log

Every operation generates an immutable audit record:

```javascript
{
    audit_id:        "...",
    timestamp:       1714012345000000000,
    identity_id:     "svc:analytics:abc",
    overlay:         "analytics_service",
    operation:       "read",
    collection:      "users",
    strand_count:    150,
    query_pattern:   "region = ?",        // parameterized — no values logged
    duration_ms:     4,
    client_ip:       "10.0.1.45"
}
```

Audit records are write-once, CRC-validated, and can only be read by identities with the `audit_reader` overlay. They cannot be deleted through any normal API call.

---

## 18. Layer 13 — Data Lifecycle & Telomere System

### Default: Immortal

All records are immortal by default. Nothing expires, archives, or deletes itself unless you explicitly configure it.

```toml
[lifecycle]
default_mode         = "immortal"
auto_archive_enabled = false
auto_delete_enabled  = false       # requires two explicit opt-ins to enable
soft_delete_grace_days = 30
```

Telomere counters are tracked for all records regardless of mode. In immortal mode they inform storage tiering (access heat) but never trigger archiving or deletion.

### Opt-In Lifecycle Per Collection

```javascript
db.collection("sessions").configure({
    lifecycle: {
        mode:            "telomere",
        initial_count:   100,
        refresh_policy:  "on_read",
        on_expire:       "delete",
        minimum_age_days: 30
    }
})
```

### Refresh

```javascript
// Manual refresh
await db.collection("users").refreshStrand(strandId)

// Bulk refresh
await db.collection("users")
    .where("last_login", ">", thirtyDaysAgo)
    .refreshAll()
```

### Deletion Safety

Deletion is always soft first. Hard deletion only after grace period. Every deletion generates an immutable audit record.

```
Developer calls .delete()
  → Record flagged with deleted=true in version chain (soft delete)
  → Moved to .deleted segment pool
  → After grace period (default 30 days): hard deletion on next compaction
  → Audit record written at both stages
```

---

## 19. Layer 14 — Lateral Transfer Protocol (Schema Evolution)

### Status: Designed, Deferred To Distributed Phase

The lateral transfer protocol (rolling schema evolution across distributed nodes without downtime) is fully designed and will be implemented in the distributed phase. It is not in the critical path for single-node operation.

### How It Works (When Built)

Schema mutations are expressed as transfer packets that propagate across the cluster. Nodes absorb mutations at their own pace. Old and new schema versions coexist via the overlay system until all nodes have updated.

```rust
struct TransferPacket {
    collection:    String,
    mutation_type: SchemaMutation,   // AddField, RemoveField, RenameField, ChangeType
    field_name:    String,
    nullable:      bool,
    originating_node: NodeId,
    timestamp:     u64,
}
```

Old records without a new field return null. New records carry the field. The overlay handles the translation. No ALTER TABLE. No downtime. No flag day.

---

## 20. Layer 15 — Wire Protocol Compatibility

### Why This Is Built Before The Ecosystem

Wire protocol compatibility gives DNA-DB the entire MongoDB and PostgreSQL tooling ecosystem for free. Existing apps migrate with a connection string change.

### MongoDB Protocol (Port 27017)

Translates MongoDB wire protocol (OP_MSG) to DNA-DB operations:

```
find()           → Index lookup or guided scan
insertOne()      → WAL append via write pipeline
updateOne()      → MVCC version creation
aggregate()      → Vector execution with grouping
$lookup          → Intron reference traversal
```

**Compatible with:** Mongoose, Prisma MongoDB adapter, PyMongo, Motor, MongoDB Compass, Studio 3T, Robo 3T.

### PostgreSQL Protocol (Port 5432)

Translates PostgreSQL frontend/backend protocol v3 to DNA-DB operations:

```
SELECT ... WHERE → Query planner → appropriate execution path
INSERT INTO      → WAL append
UPDATE ... SET   → MVCC version creation
JOIN             → Multi-collection intron traversal
CREATE TABLE     → Collection creation with schema hints
```

**Compatible with:** node-postgres, Sequelize, SQLAlchemy, psycopg2, pgAdmin, TablePlus, DBeaver, DataGrip.

### Compatibility Limitations (v1)

- Stored procedures: not supported
- Triggers: not supported
- MongoDB `$where` JavaScript: not supported (security boundary)
- SQL window functions: partial support
- Complex multi-table analytical GROUP BY: functionally correct, may be slower than native Postgres for very large aggregations

---

## 21. Layer 16 — Developer Interface & SDKs

### TypeScript / JavaScript

```bash
npm install dnadb
```

```typescript
import { DNAdb } from 'dnadb'

const db = new DNAdb({
    host: "localhost",
    port: 4737,
    database: "myapp",
    credentials: { apiKey: process.env.DNADB_API_KEY }
})

// Insert
const user = await db.collection("users").insert({
    name: "Alice", email: "alice@example.com", age: 28
})

// Query
const users = await db.collection("users")
    .where("age", ">", 25)
    .where("region", "=", "us-west")
    .orderBy("created_at", "desc")
    .limit(20)
    .fetch()

// Relationship traversal
const userWithOrders = await db.collection("users")
    .where("id", "=", userId)
    .include("orders.items")
    .fetchOne()

// Pattern matching
const gmailUsers = await db.collection("users")
    .where("email", "like", "%@gmail.com")
    .fetch()

// Transaction
await db.transaction(async (tx) => {
    const order = await tx.collection("orders").insert({ user_id: userId, total: 89.00 })
    await tx.collection("inventory").where("product_id", "=", productId).update({ stock: stock - 1 })
    await tx.collection("users").where("id", "=", userId).update({ order_count: count + 1 })
})
```

### Python

```bash
pip install dnadb
```

```python
from dnadb import DNAdb

db = DNAdb(host="localhost", port=4737, database="myapp",
           api_key=os.environ["DNADB_API_KEY"])

users = (db.collection("users")
    .where("age", ">", 25)
    .order_by("created_at", "desc")
    .limit(20)
    .fetch())
```

### Docker

```bash
docker run -d \
  -p 4737:4737 -p 27017:27017 -p 5432:5432 \
  -v ./data:/data \
  -e DNADB_ADMIN_KEY=localdevkey \
  dnadb/server:latest
```

---

## 22. Build Order & Why

Each stage produces a usable system. Nothing is built speculatively.

---

### Stage 1 — Physical Storage + Write Pipeline

**Build:** Layer 1 (record format, segment files, memory mapping) + Layer 2 (WAL, group commit, memtable, segment flush)

**Why first:** Everything is built on top of this. The on-disk format is the hardest thing to change later — getting it right now avoids having to migrate your own storage format. The group commit implementation must be validated (as it was in benchmarking) before anything runs on top of it.

**Milestone:** Can write and read raw records. WAL crash recovery works. 100k writes complete within spec.

---

### Stage 2 — Integrity System + Intron Index + Basic Query

**Build:** Layer 3 (CRC write-time validation, background scrubber) + Layer 4 (global hash index built from introns) + basic guided scan (Layer 6, without bloom filters) + basic TypeScript SDK

**Why second:** You need correctness before performance. CRC validation catches implementation bugs during development. The global index is required before the query planner (Stage 3) can make meaningful path decisions. The basic guided scan lets you write real queries against real data.

**Milestone:** Usable document database with self-validating writes and indexed queries. Begin internal dogfooding.

---

### Stage 3 — Bloom Filters + Query Planner + Vector Execution

**Build:** Layer 5 (bloom filters, block stats, segment skip logic) + Layer 7 (query planner, cost model) + Layer 8 (vector execution, batch processing)

**Why third:** These are pure performance layers. Correctness is established in Stage 2. Now the system needs to be fast enough for production workloads. Bloom filters are the biggest single query performance win after indexing. Vector execution compounds with everything else.

**Milestone:** Guided scans skip most segments. Query planner routes correctly. Batch processing saturates CPU cores. Benchmark the full query matrix.

---

### Stage 4 — Auth, Overlays, Privacy

**Build:** Layer 11 (epigenetic overlay system) + Layer 12 (connection model, auth, TLS, audit log)

**Why fourth:** Privacy must be in the system before any production data touches it. The overlay system is architecturally coupled to the query execution output stage — it must be built while the query engine is fresh, not retrofitted after the fact. Audit logging is required for compliance and is cheapest to add now.

**Milestone:** Production-safe. Privacy enforced at engine level. Multi-tenant applications safe to build on top.

---

### Stage 5 — MVCC + Transactions

**Build:** Layer 10 (version chain, snapshot reads, transaction commit protocol)

**Why fifth:** MVCC builds on top of the record format (version field in header) and the write pipeline (version creation as a write operation). It requires the compaction layer to exist for version cleanup. Building it after Stage 4 means real workloads have already validated the write pipeline under MVCC is needed.

**Milestone:** Concurrent reads never block writes. Multi-record atomic transactions work. Read-your-writes consistency guaranteed.

---

### Stage 6 — Histone Clustering + Compaction

**Build:** Layer 9 (compaction scheduler, foreign-key clustering, segment merging, tier manager)

**Why sixth:** Compaction requires immutable segments (Stage 1), the index system (Stage 2), and MVCC version cleanup (Stage 5). Building it now completes the storage engine. After this stage, long-running instances maintain performance as data accumulates — without compaction, segment files pile up and read performance degrades over time.

**Milestone:** Related-data queries benefit from co-location. Segment count stays bounded under sustained writes. Storage tiers work.

---

### Stage 7 — Wire Protocol Compatibility

**Build:** Layer 15 (MongoDB wire protocol translation, PostgreSQL wire protocol translation)

**Why seventh:** Every internal operation the translation layer needs was built in Stages 1-5. Building the translation layer now lets real Mongoose, Prisma, and SQLAlchemy apps connect and validates compatibility with the existing ecosystem under real workloads.

**Milestone:** Existing MongoDB and Postgres apps connect with connection string change only. All major ORMs and GUI tools work.

---

### Stage 8 — Lifecycle + Lateral Transfer (Distributed)

**Build:** Layer 13 (telomere lifecycle, opt-in expiry, refresh) + Layer 14 (lateral transfer, distributed schema evolution) + multi-node cluster

**Why last:** Distribution is last because distributed systems introduce the hardest class of bugs (network partitions, split brain, clock skew). A rock-solid single-node system is the prerequisite. Lifecycle management deferred because immortal-default means no data is ever lost by accident while the system matures.

**Milestone:** Horizontal scaling. Zero-downtime schema evolution. Node failure recovery.

---

### Stage 9 — Ecosystem (Parallel From Stage 2 Onward)

**Build:** Python SDK, admin GUI, Prisma adapter, GraphQL layer, monitoring integrations, cloud-hosted offering

**Why parallel:** These are independent workstreams. Python SDK can ship alongside Stage 2. Admin GUI as soon as REST API exists. Cloud hosting as soon as Stage 7 is stable. None block engine development.

---

## 23. Group Commit & WAL Tuning

This section documents the group commit implementation as validated in benchmarking.

### The Core Fix

Moving from per-record fsync to batch fsync:

```
Before: write → fsync → confirm → write → fsync → confirm
After:  write → confirm → write → confirm → [batch] → fsync once
```

### Timer Semantics

The staleness timer measures from `unsynced_since` — the moment the first unsynced record entered the pipeline — not from `last_sync_completed`. This ensures the durability guarantee is a bound on data age, not a bound on fsync scheduling.

### Mode Configuration

```toml
[wal.strict]
batch_size       = 1000
interval_ms      = 0       # batch-only, timer disabled

[wal.balanced]
batch_size       = 1000
interval_ms      = 50      # sync within 50ms regardless of batch size

[wal.fast]
batch_size       = 0       # unlimited batching
interval_ms      = 0       # manual flush only
```

### On Network / Synced Storage

When the data directory is on a network-synced folder (Dropbox, network mount), individual record materialization may exceed the timer interval. This causes the timer to fire on nearly every record — reproducing per-record fsync behavior. This is correct behavior for the durability guarantee, but it means the timer is not useful as a default for benchmarking on such storage. Use `interval_ms = 0` (batch-only) for benchmarks on network storage, and point `--data-dir` at local SSD for meaningful throughput numbers.

### Benchmark Commands

```bash
# Full three-mode matrix, fresh data dir, save results
python3 scripts/bench_matrix.py \
  --records 100000 \
  --data-dir /tmp/dnadb-bench \
  --save

# Single mode with explicit WAL interval
cargo run --release --bin load_bench -- \
  --mode strict \
  --records 100000 \
  --batch-size 1000 \
  --wal-interval-ms 10 \
  --data-dir /tmp/dnadb-bench-strict

# Concurrent write stress test
cargo run --release --bin load_bench -- \
  --mode balanced \
  --records 100000 \
  --batch-size 1000 \
  --threads 16 \
  --data-dir /tmp/dnadb-bench-concurrent
```

---

## 24. Performance Characteristics

### Writes

```
PostgreSQL:                    ~0.5 - 2ms/record
MongoDB:                       ~0.3 - 1ms/record
DNA-DB strict (group commit):  ~0.43ms/record    ✅ (validated)
DNA-DB balanced:               ~0.1 - 0.3ms/record (estimated)
DNA-DB fast:                   ~0.02 - 0.05ms/record (estimated)
```

### Reads — Single Record

```
PostgreSQL (primary key):      ~0.5ms
MongoDB (indexed _id):         ~0.3ms
DNA-DB (direct path):          ~0.5ms
```

### Reads — Pattern Scan (100k records)

```
PostgreSQL (table scan):       ~50 - 200ms
MongoDB (collection scan):     ~30 - 150ms
DNA-DB (guided scan):          ~0.76ms    ✅ (validated)
```

### Reads — Pattern Scan (1B records)

```
PostgreSQL:                    ~5 - 15s (sharding likely)
MongoDB:                       ~3 - 10s (sharding likely)
DNA-DB (guided scan):          ~150 - 400ms (estimated — bloom skip dependent)
```

### Schema Evolution

```
PostgreSQL (ALTER TABLE 50M rows):   2 - 8 hours, downtime risk
MongoDB (new field):                 Immediate for new docs, manual backfill
DNA-DB (new field):                  Immediate, zero backfill, zero downtime
```

---

## 25. Use Case Matrix

| Use Case | DNA-DB | PostgreSQL | MongoDB | Notes |
|---|---|---|---|---|
| User profiles with evolving fields | ✅ Best | ⚠️ Migration cost | ✅ Good | Schema flexibility native |
| Orders / relational data | ✅ Best | ✅ Excellent | ⚠️ Weak | Intron refs beat $lookup |
| Multi-tenant SaaS | ✅ Best | ⚠️ Complex | ⚠️ Complex | Overlays eliminate per-tenant complexity |
| GDPR / HIPAA compliance | ✅ Best | ⚠️ App-layer | ⚠️ App-layer | Engine-level field masking |
| Large-scale pattern search | ✅ Best | ⚠️ Degrades | ⚠️ Degrades | Guided scan with bloom skipping |
| Full-text search | ✅ Good | ⚠️ Extension needed | ⚠️ Atlas Search needed | Native in guided scan |
| Event sourcing / audit log | ✅ Best | ✅ Good | ✅ Good | Immutable records + CRC |
| IoT / time-series | ✅ Good | ⚠️ Not designed for | ⚠️ Acceptable | Telomere TTL + tiering |
| Complex analytical JOINs | ⚠️ Acceptable | ✅ Best | ❌ Weak | ClickHouse still wins for pure analytics |
| Strict ACID financial transactions | ✅ Good (MVCC) | ✅ Best | ❌ Weak | Postgres still leads on transaction maturity |
| Existing MongoDB app migration | ✅ Drop-in | ❌ Rewrite | — | Wire protocol |
| Existing Postgres app migration | ✅ Drop-in | — | ❌ Rewrite | Wire protocol |

---

## 26. Configuration Reference

```toml
[server]
host             = "0.0.0.0"
native_port      = 4737
mongodb_port     = 27017
postgres_port    = 5432
rest_tls_port    = 8443
data_dir         = "/data"
log_level        = "info"
max_connections  = 1000

[auth]
require_auth               = true
token_expiry_seconds       = 3600
allow_plaintext            = false
password_hash_algo         = "argon2id"

[tls]
enabled     = true
cert_file   = ""
key_file    = ""
min_version = "1.3"

[lifecycle]
default_mode           = "immortal"
auto_archive_enabled   = false
auto_delete_enabled    = false
soft_delete_grace_days = 30
telomere_tracking      = true

[wal]
fsync_mode          = "group_commit"     # group_commit | per_record | manual
default_batch_size  = 1000
default_interval_ms = 0                  # 0 = batch-only, timer disabled

[performance]
simd_encoding              = true
parallel_scan_threads      = 0           # 0 = auto (CPU core count)
memtable_capacity_mb       = 64
compaction_interval_s      = 300
bloom_false_positive_rate  = 0.01
vector_batch_size          = 1024

[encryption]
at_rest          = true
kms_provider     = "local"               # local | aws-kms | gcp-kms | vault
master_key_file  = "/keys/master.key"

[integrity]
validate_on_write        = true          # always on
scrub_interval_hours     = 6
validate_on_read_default = false         # per-collection opt-in available

[audit]
enabled              = true
audit_collection     = "_audit"
log_reads            = true
log_writes           = true
log_deletes          = true
audit_retention_mode = "immortal"

[replication]
enabled              = false
nodes                = []
replication_factor   = 3
sync_mode            = "async"
lateral_transfer_interval_s = 30

[compatibility]
mongodb_protocol  = true
postgres_protocol = true
```

---

*DNA-DB Specification v2.0*
*Supersedes v1.0. All delta changes incorporated.*
*Performance figures marked (validated) are from actual benchmark runs.*
*Figures marked (estimated) are engineering projections pending benchmark.*
