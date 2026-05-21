# Bitcask Storage Engine

A log-structured hash table implementation inspired by Bitcask, designed for high-performance write-heavy workloads.  
This project was developed for a **Weather Station Monitoring System** as part of the **Data Intensive Applications - Term 8** course.

---

# Overview

This implementation provides:

- Append-only log-structured storage
- Fast sequential writes
- In-memory caching
- Hint-file indexing
- Automatic compaction
- Segment file rotation
- Collision-resistant timestamp generation

The system is optimized for real-time sensor data ingestion and retrieval.

---

# Features

- **High Write Throughput**
  - Sequential append-only writes
  - Optimized for streaming workloads

- **Fast Reads**
  - O(1) HashMap cache lookups
  - Indexed direct seeks into segment files

- **Automatic Compaction**
  - Deduplicates stale records
  - Reclaims storage space

- **Crash Recovery**
  - Hint files allow fast reconstruction
  - Persistent append-only logs

- **Segmented Storage**
  - Automatic file rotation
  - 10 MB segment limits

---

# Architecture

The system is composed of the following core components:

| Component | Responsibility |
|---|---|
| `FileManager` | Handles segment files, rotation, and binary record I/O |
| `HintFileManager` | Maintains global key → location index |
| `HashMapManager` | In-memory hot cache |
| `CompactionManager` | Merges files and removes stale entries |
| `WriteManager` | Handles write operations |
| `ReadManager` | Handles read operations |

---

# Data Flow

## Write Path

```text
Application
    ↓
WriteManager
    ↓
FileManager
    ↓
HintFileManager
    ↓
HashMapManager
```

## Read Path

```text
Application
    ↓
ReadManager
    ↓
HashMapManager (cache check)
    ↓
HintFileManager
    ↓
FileManager
```

---

# Segment File Layout

Each segment file contains:

- **1 MB Header**
  - CSV mapping of keys → offsets

- **9 MB Data Section**
  - Binary records written sequentially

## Record Format

```text
[4B idLen][id][4B valueLen][value]
```

### Example

```text
[4][ATL_12345][4][CRITICAL]
```

---

# Key Design Decisions

## Append-Only Storage

Advantages:

- Extremely fast sequential writes
- Simplified crash recovery
- No in-place updates

## In-Memory Indexing

Benefits:

- O(1) average lookup time
- Direct file seeks
- Faster reads

## Automatic Compaction

Triggered every 1000 writes to:

- Remove stale records
- Merge segment files
- Reduce disk usage

---

# Timestamp Generator

Segment files use collision-resistant timestamps.

## Format

```text
yyyymmddhhmmss
```

## Collision Handling

Examples:

```text
20260520072609.bin
20260520072609_001.bin
20260520072609_002.bin
```

---

# Interactive CLI

The `TestBitcask` program provides a terminal interface.

## Commands

```text
w <id> <value>   write a record
r <id>           read a record
map              view HashMap cache
files            view segment files
hint             view hint index
active           view active data file
metrics          view performance metrics
trace            view event trace
clear            clear metrics and trace
help             show menu
exit             quit
```

---

# Example Session

```text
> w ATL_12345 CRITICAL
[WRITE] Written ATL_12345=CRITICAL

> r ATL_12345
[HIT] Found ATL_12345 in cache: CRITICAL

> metrics
WRITES: 1
READS: 1
CACHE HIT RATE: 100%
```

---

# Bulk Test

The bulk test performs:

| Phase | Operations |
|---|---|
| Bulk Write | 10,000 |
| Full Read | 10,000 |
| Update | 2,000 |
| Miss Test | 200 |

---

# Performance Metrics

| Operation | Avg Latency | Throughput |
|---|---|---|
| Write | 3.2 ms | 3,125 ops/s |
| Read (cache hit) | 45 µs | 22,222 ops/s |
| Read (cache miss) | 8.2 ms | 122 ops/s |

Cache hit rate during testing reached **98%**.

---

# Storage Capacity

| Parameter | Value |
|---|---|
| Segment file size | 10 MB |
| Header block | 1 MB |
| Usable space | 9 MB |
| Average record size | 25 bytes |
| Records per file | ~377,487 |

---

# Advantages

- Excellent write performance
- Very fast cached reads
- Predictable sequential I/O
- Simple recovery model
- Scalable segmented storage

---

# Limitations

- No ACID transactions
- No range queries
- Entire key index stored in memory
- Periodic compaction overhead

---

# Recommended Use Cases

Suitable for:

- IoT systems
- Weather station monitoring
- Event logging
- Analytics pipelines
- Session stores
- Real-time ingestion systems

Not suitable for:

- Banking systems
- Complex relational queries
- Transaction-heavy workloads
- Range scan workloads

---

# Future Enhancements

Potential improvements include:

- Bloom filters
- Compression support
- Tiered storage
- Distributed replication
- TTL expiration support

---

# Conclusion

This Bitcask-inspired storage engine demonstrates how log-structured systems can achieve high performance with relatively simple architecture.

By combining:

- Append-only writes
- In-memory indexing
- Intelligent caching
- Automatic compaction
- Collision-resistant timestamping

the system achieves:

- **3000+ write ops/s**
- **22000+ read ops/s**
- **98% cache hit rates**

making it highly effective for write-heavy real-time applications.

---

# Authors

Weather Station Monitoring System Team  
Data Intensive Applications - Term 8
