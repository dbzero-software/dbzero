from dataclasses import dataclass
import dbzero as db0
from typing import Dict, List
from datetime import datetime
import pytest
import logging
import random

@db0.memo(no_default_tags = True)
@dataclass
class IndexContainer:
  index: db0.index

@db0.memo(singleton = True)
@dataclass
class IndexesSingleton:
  # wystawcy po numerze NIP
  indexes: List[IndexContainer]


@db0.memo(no_default_tags = True, no_cache=True)
@dataclass
class Value:
  index_number: int
  date: datetime
  value: str


def format_results(diffs):
    lines = []
    keys = list(diffs[0].keys())
    for key in keys:
        values = [diff[key] for diff in diffs]
        line = f"{key}: " + ", ".join([str(v) for v in values])
        lines.append(line)
    return "\n".join(lines)

@pytest.mark.stress_test
def test_io_operation_stability(db0_large_lang_cache_no_autocommit):
    numbers = set()
    print("Initializing test data...")
    numbers = list(numbers)
    indexes = IndexesSingleton(indexes=[])
    BYTES = "DB0"*2200
    diffs = []
    indexes_count = 250000
    for number in range(indexes_count):
        indexes.indexes.append(IndexContainer(index=db0.index()))

    # commit init
    print("Performing initial commit...")
    start = datetime.now()
    db0.commit()
    stop = datetime.now()

    initial_commit_time = (stop - start).seconds
    storage_stats = db0.get_storage_stats()
    min_commit_time = initial_commit_time
    max_commit_time = initial_commit_time
    print(f" Initial commit time seconds: {initial_commit_time}")
    print("Starting IO operation stability test...")

    for i in range(10):
        print(f" Iteration {i+1}/10")
        start = datetime.now()
        iterations = 100000
        # perform iteration
        for j in range(iterations):
            number = (i*iterations + j)%indexes_count
            index_container = indexes.indexes[number]
            now = datetime.now()
            new_value = Value(index_number=number, date=now, value=list_value)
            index_container.index.add(now, new_value)
        
        # calculate objects per second
        stop = datetime.now()
        seconds = (stop - start).miliseconds / 1000.0
        print(f" Objects per second: {iterations / seconds}")

        # commit changes
        start = datetime.now()
        db0.commit()
        stop = datetime.now()

        # measure commit time
        commit_time = (stop - start).seconds
        min_commit_time = min(min_commit_time, commit_time)
        max_commit_time = max(max_commit_time, commit_time)
        print(f" Commit time seconds: {commit_time}")

        storage_stats_after = db0.get_storage_stats()
        # get storage stats difference
        diff = {}
        for key in storage_stats_after:
            diff[key] = storage_stats_after[key] - storage_stats.get(key, 0)
        print(f" Storage stats diff: {diff}")
        diffs.append(diff)
        storage_stats = storage_stats_after

    results = format_results(diffs)
    print(f"IO Operation Stability Test Results:\n{results}")
    print(f"Min commit time: {min_commit_time} seconds")
    print(f"Max commit time: {max_commit_time} seconds")


@pytest.mark.stress_test
def test_big_cache_should_prevent_random_reads(db0_large_lang_cache_no_autocommit):
    numbers = set()
    print("Initializing test data...")
    storage_stats = db0.get_storage_stats()
    print(f"Random reads before test: {storage_stats['file_rand_read_ops']}")
    indexes = IndexesSingleton(indexes=[])
    storage_stats = db0.get_storage_stats()
    db0.commit()
    print(f"Random reads after singleton creation: {storage_stats['file_rand_read_ops']}")
    indexes_count = 250000
    for number in range(indexes_count):
        indexes.indexes.append(IndexContainer(index=db0.index()))
    db0.commit()
    storage_stats = db0.get_storage_stats()
    print(f"Random reads after indexes creation: {storage_stats['file_rand_read_ops']}")
    iterations = 100000
    # perform iteration
    BYTES = "DB0"*2200

    for i in range(4):
        for j in range(iterations):
            number = j
            index_container = indexes.indexes[number]
            now = datetime.now()
            new_value = Value(index_number=number, date=now, value=BYTES)
            index_container.index.add(now, new_value)
        db0.commit()
        storage_stats = db0.get_storage_stats()
        print(f"Random reads after {i} iteration: {storage_stats['file_rand_read_ops']}")
        assert storage_stats['file_rand_read_ops'] <= 3, "Too many random read operations detected!"