import pytest
import dbzero_ce as db0
from datetime import datetime
from .memo_test_types import MemoTestClass, DynamicDataClass

    
@pytest.mark.stress_test
# @pytest.mark.parametrize("db0_autocommit_fixture", [1], indirect=True)
def test_create_random_objects_stress_test(db0_no_autocommit):
    def rand_string(max_len):
        import random
        import string
        actual_len = random.randint(1, max_len)
        return ''.join(random.choice(string.ascii_letters) for i in range(actual_len))
    
    append_count = 100000
    # NOTE: in this version of test we reference objects from db0 list thus
    # they are not GC0 garbage collected
    buf = db0.list()
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
            pre_commit = datetime.now()
            print("*** next transaction ***")
            db0.commit()
            storage_stats = db0.get_storage_stats()
            print(f"Total bytes: {total_bytes}")
            print(f"Rand DRAM I/O ops: {storage_stats['dram_io_rand_ops'] - rand_dram_io}")
            print(f"Rand file write ops: {storage_stats['file_rand_write_ops'] - rand_file_write_ops}")
            print(f"File bytes written: {storage_stats['file_bytes_written'] - bytes_written}")
            print(f"Commit took: {datetime.now() - pre_commit}\n")
            rand_dram_io = storage_stats["dram_io_rand_ops"]
            rand_file_write_ops = storage_stats["file_rand_write_ops"]
            bytes_written = storage_stats["file_bytes_written"]
            report_bytes += 1024 * 1024
        if count % 1000 == 0:
            print(f"Objects created: {count}")


@pytest.mark.stress_test
# @pytest.mark.parametrize("db0_autocommit_fixture", [1], indirect=True)
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
            pre_commit = datetime.now()
            # NOTE: with each commit the size of GC0 is increasing due to large 
            # number of objects referenced only from python
            db0.commit()
            storage_stats = db0.get_storage_stats()
            print(f"Total bytes: {total_bytes}")
            print(f"Rand DRAM I/O ops: {storage_stats['dram_io_rand_ops'] - rand_dram_io}")
            print(f"Rand file write ops: {storage_stats['file_rand_write_ops'] - rand_file_write_ops}")
            print(f"File bytes written: {storage_stats['file_bytes_written'] - bytes_written}")
            print(f"Commit took: {datetime.now() - pre_commit}")
            rand_dram_io = storage_stats["dram_io_rand_ops"]
            rand_file_write_ops = storage_stats["file_rand_write_ops"]
            bytes_written = storage_stats["file_bytes_written"]
            report_bytes += 1024 * 1024
        if count % 1000 == 0:
            print(f"Objects created: {count}")

            
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
