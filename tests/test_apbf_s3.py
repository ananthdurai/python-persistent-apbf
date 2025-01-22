import pytest
import time
from unittest.mock import patch

from apbf.apbf_s3 import (
    AgePartitionedBloomFilter,
    Snapshot,
    snapshot_to_arrow,
    arrow_to_snapshot
)

@pytest.fixture
def basic_filter():
    """Basic APBF instance for testing"""
    return AgePartitionedBloomFilter(k=3, l=2, g=100)

@pytest.fixture
def timed_filter():
    """APBF instance with time-based refresh"""
    return AgePartitionedBloomFilter(k=3, l=2, g=100, refresh_interval=0.1)

def test_initialization():
    """Test filter initialization with valid and invalid parameters"""
    # Valid initialization
    apbf = AgePartitionedBloomFilter(k=3, l=2, g=100)
    assert apbf.k == 3
    assert apbf.l == 2
    assert apbf.h == 5  # k + l
    assert apbf.g == 100
    assert apbf.base == 1
    assert apbf.count == 0

    # Invalid parameters
    with pytest.raises(ValueError):
        AgePartitionedBloomFilter(k=0, l=2, g=100)
    with pytest.raises(ValueError):
        AgePartitionedBloomFilter(k=3, l=-1, g=100)
    with pytest.raises(ValueError):
        AgePartitionedBloomFilter(k=3, l=2, g=0)
    with pytest.raises(ValueError):
        AgePartitionedBloomFilter(k=3, l=2, g=100, refresh_interval=-1)

def test_basic_operations(basic_filter):
    """Test basic add and query operations"""
    item1 = b"test1"
    item2 = b"test2"
    
    # Add items
    basic_filter.add(item1)
    assert basic_filter.query(item1)
    assert not basic_filter.query(item2)
    
    basic_filter.add(item2)
    assert basic_filter.query(item1)
    assert basic_filter.query(item2)
    
    # Non-existent item
    assert not basic_filter.query(b"nonexistent")

def test_generation_shift(basic_filter):
    """Test manual generation shifting"""
    item = b"test_item"
    basic_filter.add(item)
    assert basic_filter.query(item)
    
    # Shift generation
    basic_filter.next_generation()
    # Item should still be queryable due to age partitioning
    assert basic_filter.query(item)

def test_capacity_limits(basic_filter):
    """Test behavior when reaching generation capacity"""
    # Fill up to capacity
    for i in range(basic_filter.g):
        basic_filter.add(str(i).encode())
    
    assert basic_filter.count == basic_filter.g
    
    # Adding one more should trigger automatic shift
    basic_filter.add(b"overflow")
    assert basic_filter.count == 1  # Count should reset

def test_time_based_refresh(timed_filter):
    """Test automatic time-based generation shifts"""
    item = b"test_item"
    timed_filter.add(item)
    assert timed_filter.query(item)
    
    # Wait for refresh interval
    time.sleep(0.15)  # Slightly longer than refresh_interval
    
    # Query should trigger refresh
    timed_filter.query(b"anything")
    assert timed_filter.base != 1  # Base should have shifted

def test_snapshot_serialization():
    """Test snapshot serialization and deserialization"""
    original = AgePartitionedBloomFilter(k=3, l=2, g=100)
    item = b"test_item"
    original.add(item)
    
    # Create snapshot
    snap = Snapshot(
        k=original.k,
        l=original.l,
        g=original.g,
        r=original.r,
        base=original.base,
        count=original.count,
        buffer=bytearray(original.buffer)
    )
    
    # Convert to Arrow and back
    table = snapshot_to_arrow(snap)
    restored_snap = arrow_to_snapshot(table)
    
    # Recreate filter from snapshot
    restored = AgePartitionedBloomFilter.from_snapshot(restored_snap)
    
    # Verify state
    assert restored.k == original.k
    assert restored.l == original.l
    assert restored.g == original.g
    assert restored.r == original.r
    assert restored.base == original.base
    assert restored.count == original.count
    assert restored.buffer == original.buffer
    assert restored.query(item)

@pytest.mark.asyncio
async def test_periodic_snapshot():
    """Test periodic snapshot functionality"""
    with patch('apbf.apbf_s3.write_snapshot_to_s3') as mock_write:
        apbf = AgePartitionedBloomFilter(k=3, l=2, g=100)
        
        # Start periodic snapshots
        apbf.start_periodic_snapshot_thread(
            interval_sec=0.1,
            bucket="test-bucket",
            key="test-key",
            access_key="test-access",
            secret_key="test-secret"
        )
        
        # Wait for at least one snapshot
        time.sleep(0.15)
        
        # Stop snapshots
        apbf.stop_periodic_snapshot_thread()
        
        # Verify that write_snapshot_to_s3 was called
        assert mock_write.called

def test_false_positive_rate():
    """Test false positive rate calculation"""
    rate = AgePartitionedBloomFilter.calculate_false_positive_rate(k=3, l=2)
    assert 0 <= rate <= 1  # Should be a valid probability
    
    with pytest.raises(ValueError):
        AgePartitionedBloomFilter.calculate_false_positive_rate(k=0, l=2)
    with pytest.raises(ValueError):
        AgePartitionedBloomFilter.calculate_false_positive_rate(k=3, l=0)

def test_thread_safety(basic_filter):
    """Test thread safety of add and query operations"""
    import threading
    import random
    
    items = [str(i).encode() for i in range(1000)]
    errors = []
    
    def worker():
        try:
            for _ in range(100):
                item = random.choice(items)
                basic_filter.add(item)
                basic_filter.query(item)
        except Exception as e:
            errors.append(e)
    
    threads = [threading.Thread(target=worker) for _ in range(10)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    
    assert not errors, f"Thread safety test failed with errors: {errors}"