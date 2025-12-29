# SPDX-License-Identifier: LGPL-2.1-or-later
# Copyright (c) 2025 DBZero Software sp. z o.o.

import random
import string
import datetime
import dbzero as db0


@db0.memo()
class TestObject:
    def __init__(self, id, value, timestamp):
        self.id = id
        self.value = value
        self.timestamp = timestamp

@db0.memo(singleton=True)
class TestDataSingleton:
    """Singleton containing various db0 collections for testing purposes"""
    
    def __init__(self):
        # List with mixed types (int, string, object)
        self.list = [
            1,
            "string_value",
            {"key": "value"},
            42,
            "another_string",
            {"nested": {"data": 123}},
            999,
            "third_string",
            {"id": 1, "name": "test"},
            100
        ]
        
        # Dict with 10 elements
        self.dict = {
            "key1": 1,
            "key2": "value2",
            "key3": [1, 2, 3],
            "key4": {"nested": "dict"},
            "key5": 42.5,
            "key6": True,
            "key7": None,
            "key8": "string_value",
            "key9": [4, 5, 6],
            "key10": {"id": 10}
        }
        
        # Set with 10 elements
        self.set = {
            1, 2, 3, 4, 5,
            "str1", "str2", "str3", "str4", "str5"
        }
        
        # Tuple with 10 elements
        self.tuple =(
            "a", "b", "c", "d", "e",
            1, 2, 3, 4, 5
        )
        
        # ByteArray with 10 bytes
        self.byte_array = db0.bytearray(b'\x00\x01\x02\x03\x04\x05\x06\x07\x08\x09')
        
        # Large list with 10k random objects
        self.large_list = []
        for i in range(10000):
            # Generate random string with random length (5-50 chars)
            random_length = random.randint(5, 50)
            random_string = ''.join(random.choices(
                string.ascii_letters + string.digits,
                k=random_length
            ))
            
            # Create object with random string value
            obj = TestObject(
                id=i,
                value=random_string,
                timestamp=datetime.datetime.now()
            )
            self.large_list.append(obj)


def create_test_data():
    """
    Create and return the TestDataSingleton with all test data.
    
    Usage example:
        import dbzero as db0
        from python_tests.create_test_data import create_test_data
        
        db0.init("/path/to/db")
        db0.open("test-prefix")
        
        test_data = create_test_data()
        
        print(f"List length: {len(test_data.list)}")
        print(f"Dict keys: {list(test_data.dict.keys())}")
        print(f"Large list length: {len(test_data.large_list)}")
        
        db0.close()
    """
    return TestDataSingleton()


if __name__ == "__main__":
    import os
    import shutil
    import argparse
    
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="Create test data in db0 database")
    parser.add_argument(
        "--out_dir",
        type=str,
        default=os.path.join(os.getcwd(), "db0-create-test-data"),
        help="Output directory for db0 database (default: db0-create-test-data)"
    )
    args = parser.parse_args()
    
    # Setup test database
    DB0_DIR = args.out_dir
    
    if os.path.exists(DB0_DIR):
        shutil.rmtree(DB0_DIR)
    os.mkdir(DB0_DIR)
    
    db0.init(DB0_DIR)
    db0.open("test-data-prefix")
    
    # Create test data
    print(f"Creating test data in: {DB0_DIR}")
    test_data = create_test_data()
    
    print(f"\nTest data created successfully!")
    print(f"  - list: {len(test_data.list)} elements")
    print(f"  - dict: {len(test_data.dict)} elements")
    print(f"  - set: {len(test_data.set)} elements")
    print(f"  - tuple: {len(test_data.tuple)} elements")
    print(f"  - byte_array: {len(test_data.byte_array)} bytes")
    print(f"  - large_list: {len(test_data.large_list)} elements")
    
    # Show some sample data
    print(f"\nSample from list: {test_data.list[:3]}")
    print(f"Sample from dict: {dict(list(test_data.dict.items())[:3])}")
    print(f"Sample from large_list: {test_data.large_list[0]}")
    
    db0.close()
    
    print(f"\nData persisted in: {DB0_DIR}")
    print("Done!")
