# Bitcask Tests Directory

This directory contains all test and client utilities for the Bitcask storage engine. The tests automatically reference the parent directory for the compiled Bitcask core libraries.

## Directory Structure

```
../                          # Parent: Bitcask core libraries
├── Bitcask.java
├── BitcaskClient.java
├── FileManager.java
├── HintFileManager.java
├── HashMapManager.java
├── WriteManager.java
├── ReadManager.java
├── CompactionManager.java
├── TimestampGenerator.java
└── [compiled .class files]

Tests/                       # This directory
├── BulkTest.java          # Load test (10,000 records)
├── TestBitcask.java       # Interactive CLI dashboard
├── bitcask_client.sh      # Bash wrapper for BitcaskClient
├── bitcask_client.py      # Python wrapper for BitcaskClient
└── README.md              # This file
```

## How to Run Tests

### 1. Compile (from parent directory)

```bash
cd ..
javac *.java
```

Or automatically via any test script (first-run only).

### 2. BulkTest — Load 10,000 Records

```bash
cd Tests
java BulkTest
```

Phases:
- Phase 1: BULK WRITE (10,000 records)
- Phase 2: FULL READ (all records)
- Phase 3: UPDATE PASS (20% overwrite)
- Phase 4: MISS TEST (200 unknown keys)
- Phase 5: SUMMARY (metrics report)

Output: Performance metrics, cache hit rates, file inventory

### 3. TestBitcask — Interactive CLI Dashboard

```bash
cd Tests
java TestBitcask
```

Interactive commands:
- `w <id> <value>` — Write record
- `r <id>` — Read record
- `map` — View HashMapManager cache
- `files` — View FileManager file list
- `hint` — View global.hint file
- `active` — View active data file
- `metrics` — View performance metrics
- `export` — Export all to CSV
- `query <key>` — Query specific key
- `bench <n>` — Run concurrent benchmark
- `trace` — View event trace
- `clear` — Clear trace and reset metrics
- `help` — Show menu
- `exit` — Quit

### 4. BitcaskClient — Standalone CLI

#### Via Bash Script (Unix/Linux/macOS):
```bash
cd Tests
./bitcask_client.sh --view-all
./bitcask_client.sh --view --key=ATL_12345
./bitcask_client.sh --concurrent --clients=100
```

#### Via Python Script (Cross-platform):
```bash
cd Tests
python bitcask_client.py --view-all
python bitcask_client.py --view --key=ATL_12345
python bitcask_client.py --concurrent --clients=50
```

#### Via Java directly (from parent directory):
```bash
cd ..
java BitcaskClient --view-all
java BitcaskClient --view --key=ATL_12345
java BitcaskClient --concurrent --clients=100
```

## BitcaskClient Commands

### `--view-all`
Exports all keys and their latest values to a CSV file.
- **Output file**: `yyyyMMddHHmmss.csv` (e.g., `20260520143542.csv`)
- **Format**: CSV with 2 columns (key, value)
- **Example**:
  ```bash
  ./bitcask_client.sh --view-all
  ✓ Exported 10000 entries to: 20260520143542.csv
  ```

### `--view --key=SOME_KEY`
Queries a specific key and prints the value to stdout.
- **Exit code**: 0 on success, 1 if key not found
- **Example**:
  ```bash
  ./bitcask_client.sh --view --key=ATL_12345
  CRITICAL
  ```

### `--concurrent --clients=N`
Runs N concurrent threads, each querying all keys and exporting to thread-specific CSV.
- **Output files**: `yyyyMMddHHmmss_thread_1.csv`, `yyyyMMddHHmmss_thread_2.csv`, etc.
- **Example**:
  ```bash
  ./bitcask_client.sh --concurrent --clients=50
  Starting 50 concurrent client threads...
  [Thread 1] Exported 10000/10000 entries → 20260520143542_thread_1.csv
  [Thread 2] Exported 10000/10000 entries → 20260520143542_thread_2.csv
  ...
  ✓ Completed 50 threads in 2345 ms
  ```

## Path Adjustments

These test files have been updated to reference the parent directory:
- **TestBitcask.java**: Updated to use `../Files/global.hint`
- **bitcask_client.sh**: Updated to compile and run from parent directory
- **bitcask_client.py**: Updated to compile and run from parent directory
- **BulkTest.java**: No path adjustments needed (uses FileManager)

When running from the Tests directory, the scripts automatically:
1. Change to the parent directory
2. Check if compilation is needed
3. Compile all .java files to .class files
4. Run the desired command

## Typical Workflow

```bash
# 1. Navigate to parent directory
cd Bitcask

# 2. Compile everything once
javac *.java

# 3. Run tests from Tests subdirectory
cd Tests

# Load test
java BulkTest

# Export to CSV
./bitcask_client.sh --view-all

# Interactive CLI
java TestBitcask
```

## Files Directory

The `../Files/` directory (created automatically by Bitcask) contains:
- Data segment files (`.bin` files with timestamp names like `20260520143542.bin`)
- Global hint file (`global.hint`)

This directory persists across test runs unless explicitly cleared.

## Notes

- All CSV files are created in the current working directory (Tests/)
- Timestamp format: `yyyyMMddHHmmss` (human-readable, sortable)
- CSV files include proper RFC 4180 escaping for special characters
- Concurrent benchmarks create separate CSV files per thread for detailed analysis

---

**Last Updated**: May 20, 2026
