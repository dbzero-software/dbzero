# SPDX-License-Identifier: LGPL-2.1-or-later
# Copyright (c) 2025 DBZero Software sp. z o.o.

import pytest
import dbzero as db0
from datetime import datetime
from .memo_test_types import MemoTestClass, DynamicDataClass
import random

    
@pytest.mark.stress_test
@pytest.mark.parametrize("db0_slab_size", [{"slab_size": 512 << 20, "autocommit": True, "autocommit_interval": 250}], indirect=True)
def test_create_random_objects_stress_test(db0_slab_size):
    def rand_string(max_len = 8192):
        import random
        import string
        actual_len = random.randint(1, max_len)
        return ''.join(random.choice(string.ascii_letters) for i in range(actual_len))

    def rand_dict(max_keys = 16):
        import random        
        key_count = random.randint(1, max_keys)
        result = {}
        for _ in range(key_count):
            result[rand_string(16)] = rand_string(128)
        return result

    def rand_list(max_items = 16):
        import random
        item_count = random.randint(1, max_items)
        result = []
        for _ in range(item_count):
            result.append(rand_string(128))            
        return result

    # Generate a random value which is either: string, or dict
    def rand_value():
        generators = [rand_string, rand_dict, rand_list, rand_list, rand_list]
        return random.choice(generators)()
    
    def read_value(value) -> int:
        if isinstance(value, str):
            return len(value)
        elif isinstance(value, db0.types.Dict):
            return sum(len(k) + read_value(v) for k, v in value.items())
        elif isinstance(value, db0.types.List):
            return sum(read_value(item) for item in value)
        else:
            raise ValueError("Unsupported value type")
    
    db0.set_cache_size(1 << 30)
    append_count = 50000
    # NOTE: in this version of test we reference objects from db0 list thus
    # they are not GC0 garbage collected
    buf = db0.list()
    total_bytes = 0
    count = 0
    read_count = 100
    report_bytes = 1024 * 1024
    rand_dram_io = 0
    rand_file_write_ops = 0
    bytes_written = 0
    for _ in range(append_count):
        with db0.atomic():
            buf.append(MemoTestClass(rand_value()))        
        total_bytes += len(buf[-1].value)
        count += 1
        if total_bytes > report_bytes:
            flush = datetime.now()
            print("*** next transaction ***")
            db0.commit()
            storage_stats = db0.get_storage_stats()
            print(f"Total bytes: {total_bytes}")
            print(f"Rand DRAM I/O ops: {storage_stats['dram_io_rand_ops'] - rand_dram_io}")
            print(f"Rand file write ops: {storage_stats['file_rand_write_ops'] - rand_file_write_ops}")
            print(f"File bytes written: {storage_stats['file_bytes_written'] - bytes_written}")
            print(f"Commit took: {datetime.now() - flush}\n")
            rand_dram_io = storage_stats["dram_io_rand_ops"]
            rand_file_write_ops = storage_stats["file_rand_write_ops"]
            bytes_written = storage_stats["file_bytes_written"]
            report_bytes += 1024 * 1024
        if count % 1000 == 0:
            print(f"Objects created: {count}")
        if count % 100 == 0:
            bytes_read = 0
            for _ in range(read_count):
                obj = random.choice(buf)
                bytes_read += read_value(obj.value)
            print(f"Read {read_count} objects, total bytes read: {bytes_read}")


@pytest.mark.stress_test
def test_create_random_gc0_objects_stress_test(db0_no_autocommit):
    def rand_string(max_len):
        import random
        import string
        actual_len = random.randint(1, max_len)
        return ''.join(random.choice(string.ascii_letters) for i in range(actual_len))
        
    append_count = 100000    
    buf = []
    total_bytes = 0
    count = 0
    report_bytes = 1024 * 1024
    rand_dram_io = 0
    rand_file_write_ops = 0    
    bytes_written = 0
    for _ in range(append_count):
        buf.append(MemoTestClass(rand_string(8192)))
        total_bytes += len(buf[-1].value)
        count += 1
        if total_bytes > report_bytes:
            flush = datetime.now()
            # NOTE: with each commit the size of GC0 is increasing due to large 
            # number of objects referenced only from python            
            db0.commit()
            storage_stats = db0.get_storage_stats()            
            print(f"Total bytes: {total_bytes}")
            print(f"Rand DRAM I/O ops: {storage_stats['dram_io_rand_ops'] - rand_dram_io}")
            print(f"Rand file write ops: {storage_stats['file_rand_write_ops'] - rand_file_write_ops}")
            print(f"File bytes written: {storage_stats['file_bytes_written'] - bytes_written}")
            print(f"Commit took: {datetime.now() - flush}")
            rand_dram_io = storage_stats["dram_io_rand_ops"]
            rand_file_write_ops = storage_stats["file_rand_write_ops"]
            bytes_written = storage_stats["file_bytes_written"]
            report_bytes += 1024 * 1024        
        if count % 1000 == 0:
            print(f"Objects created: {count}")
    # NOTE: on close, all objects are getting GC0 collected (dropped)

    
@pytest.mark.stress_test
def test_create_random_objects_with_short_members(db0_fixture):
    def rand_string(max_len):
        import random
        import string
        actual_len = random.randint(1, max_len)
        return ''.join(random.choice(string.ascii_letters) for i in range(actual_len))
    
    append_count = 100000
    buf = []
    for _ in range(append_count):
        buf.append(MemoTestClass(rand_string(32)))
