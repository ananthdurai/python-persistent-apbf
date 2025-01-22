# Age-Partitioned Bloom Filter (APBF) in Python

The code is inspired from the [apbf](https://github.com/CrowdStrike/apbf) repository.

This repository contains a production-ready, thread-safe implementation of an Age-Partitioned Bloom Filter (APBF) in Python. It leverages:
* Apache Arrow for snapshot serialization
* Amazon S3 for storing and retrieving those snapshots
* mmh3 for fast MurmurHash3 hashing
* uv for additional functionality (depending on your project needs)

## Overview

A Bloom Filter is a probabilistic data structure used to test membership, potentially returning false positives but never false negatives. The Age-Partitioned design extends a standard Bloom Filter with multiple "generations," each stored in distinct slices. Older generations are periodically cleared (expired), which helps reduce false positives for stale data.

## Key Features
* **Age Partitioning**  
  Splits the filter into k + l "generations," allowing old data to be efficiently removed.
* **Capacity & Time-Based Refresh**
  * Capacity: When a generation reaches its capacity g items, it automatically shifts
  * Time: If an optional refresh interval is set, the filter also shifts after that interval has elapsed
* **Thread-Safe**  
  Uses a single threading.Lock to protect all operations, ensuring consistency
* **Snapshots & Restores**
  * Periodically serialize the filter state as an Arrow IPC file and store it in Amazon S3
  * Restore from that snapshot (e.g., during service restarts)
* **Periodic Automatic Snapshots**  
  Provides a background thread that saves snapshots at a set interval

## Reference

This implementation is based on the paper:

> Tang, Jun, Qi Huang, John L. Miller, Dan Pei, Haitao Zheng, and Ben Y. Zhao.  
> Age-Partitioned Bloom Filter: A Low Overhead, High Throughput Filter for Ephemeral Key-Value Caches.  
> USENIX Annual Technical Conference (ATC), 2013.

## Installation
1. Clone the repository:
```bash
git clone https://github.com/ananthdurai/python-persistent-apbf.git
cd python-persistent-apbf
```

2. Install dependencies:
   * We assume you have Python 3.9+
   * Install uv
   

```bash
uv sync
uv build
```

3. AWS Credentials (if storing snapshots in S3):
   * Make sure you have valid credentials. You can export them as environment variables:

```bash
export AWS_ACCESS_KEY_ID="YOUR-ACCESS-KEY"
export AWS_SECRET_ACCESS_KEY="YOUR-SECRET-KEY"
export AWS_DEFAULT_REGION="us-east-1"
```

Or you can provide them programmatically when calling the snapshot methods (see usage below).

## Quick Start Example

Below is a minimal usage example showing how to create and use the AgePartitionedBloomFilter class, including automatic snapshots to S3:

```python
from apbf import AgePartitionedBloomFilter

def main():
    # 1. Create a new Age-Partitioned Bloom Filter
    bf = AgePartitionedBloomFilter(k=2, l=2, g=100, refresh_interval=10.0)

    # 2. (Optional) start a background thread for periodic snapshots every 30 seconds
    bf.start_periodic_snapshot_thread(
        interval_sec=30.0,
        bucket="my-bucket",
        key="apbf_snapshot.arrow",
        access_key="YOUR-ACCESS-KEY",
        secret_key="YOUR-SECRET-KEY"
    )

    # 3. Insert items
    items = [b"apple", b"banana", b"carrot"]
    for item in items:
        bf.add(item)

    # 4. Query them
    print(bf.query(b"apple"))    # Likely True
    print(bf.query(b"unknown"))  # Likely False

    # 5. Manually snapshot to S3
    bf.snapshot_to_s3("my-bucket", "apbf_snapshot_now.arrow",
                      "YOUR-ACCESS-KEY", "YOUR-SECRET-KEY")

    # 6. Stop snapshot thread if you started it
    bf.stop_periodic_snapshot_thread()

    # 7. Re-initialize from S3
    bf2 = AgePartitionedBloomFilter.from_s3(
        bucket="my-bucket",
        key="apbf_snapshot.arrow",
        access_key="YOUR-ACCESS-KEY",
        secret_key="YOUR-SECRET-KEY"
    )

    # Verify that old items are still found
    print("bf2 query(b'banana'):", bf2.query(b"banana"))  # Should match original filter

if __name__ == "__main__":
    main()
```

## How It Works

### Age-Partitioning
* The filter is divided into k + l slices arranged in a ring
* k = number of hash slices per insertion
* l = extra slices to keep for older generations. Hence, there are (l+1) generations total

When a generation is "full" (g items have been inserted) or the optional time interval elapses, the filter shifts to the next generation:
* One slice is cleared (the oldest generation)
* A new generation becomes "active"

This constant turnover of slices prevents stale items from lingering forever in the filter.

### Bloom Filter Internals

We use MurmurHash3 to compute two 64-bit hashes for each item. The code then derives multiple bit positions (in multiple slices) using a double-hashing scheme. If those bits are all set, the item is possibly in the filter; if any bit is missing, the item is definitely not in the filter.

### Thread-Safety

A single threading.Lock enforces mutual exclusion between:
* Readers (queries)
* Writers (adds, shifts, snapshot creation)

This guarantees consistent state, though it also means queries block one another. If you need more concurrency, you could integrate a read/write lock from a third-party library (not included in the standard library).

### Snapshots & Apache Arrow
* We use PyArrow to serialize the filter's internal state (k, l, g, r, base, count, buffer) into an Arrow Table
* Then we write it to S3 as an IPC stream (lightweight binary format)
* Restoring is as simple as reading the Arrow IPC from S3, converting it back into a Python Snapshot, and creating a new filter from that snapshot

## API Reference

### Main Class
* **AgePartitionedBloomFilter(k, l, g, refresh_interval=0.0)**  
  Constructor
  * k (int): Number of hashes per item
  * l (int): Extra slices to keep. Total slices = k + l
  * g (int): Items per generation
  * refresh_interval (float in seconds): Optional time-based shifting interval. If zero, no automatic time-based shifting
* **add(item: bytes) -> None**  
  Insert an item into the filter
* **query(item: bytes) -> bool**  
  Check if item might be in the filter. Returns True (possible) or False (definitely not)
* **next_generation() -> None**  
  Manually force a generation shift
* **snapshot_to_s3(bucket, key, access_key, secret_key) -> None**  
  Take a snapshot and write it to S3
* **start_periodic_snapshot_thread(interval_sec, bucket, key, access_key, secret_key) -> None**  
  Launch a daemon thread that calls snapshot_to_s3(...) every interval_sec seconds
* **stop_periodic_snapshot_thread() -> None**  
  Signal the background snapshot thread to stop and wait for it to terminate
* **max_capacity() -> int**  
  Return the total capacity across all slices/generations, (l + 1) * g
* **calculate_false_positive_rate(k, l) -> float**  
  A static method returning a theoretical or approximate false positive rate

### Snapshot / Restore Methods
* **AgePartitionedBloomFilter.from_s3(bucket, key, access_key, secret_key) -> AgePartitionedBloomFilter**  
  Class method. Reads an Arrow IPC file from S3, reconstructs an internal Snapshot, and returns a new filter instance with that state
* **AgePartitionedBloomFilter.from_snapshot(snapshot: Snapshot) -> AgePartitionedBloomFilter**  
  Build a new filter instance from a Snapshot object (normally used internally for from_s3)

## Contributing
1. Fork the repo
2. Create a feature branch
3. Commit your changes
4. Open a Pull Request describing what you've changed and why


## Further Reading
1. [Bloom Filters: Wikipedia Article](https://en.wikipedia.org/wiki/Bloom_filter)
2. Age-Partitioned Bloom Filter:
   > Tang, Jun, Qi Huang, John L. Miller, Dan Pei, Haitao Zheng, and Ben Y. Zhao.  
   > Age-Partitioned Bloom Filter: A Low Overhead, High Throughput Filter for Ephemeral Key-Value Caches.  
   > USENIX ATC, 2013.
3. [Apache Arrow Documentation](https://arrow.apache.org/docs/)
4. [mmh3 (MurmurHash3 for Python)](https://pypi.org/project/mmh3/)

