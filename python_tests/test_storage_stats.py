# SPDX-License-Identifier: LGPL-2.1-or-later
# Copyright (c) 2025 DBZero Software sp. z o.o.

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