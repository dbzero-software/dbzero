# SPDX-License-Identifier: LGPL-2.1-or-later
# Copyright (c) 2025 DBZero Software sp. z o.o.

import os
import argparse
import dbzero as db0
from create_test_data import TestDataSingleton


def validate_test_data(test_data):
    """Validate that test data has the expected structure and content"""
    errors = []
    warnings = []
    
    # Validate list
    if not hasattr(test_data, 'list'):
        errors.append("Missing attribute: list")
    elif len(test_data.list) != 10:
        errors.append(f"list has {len(test_data.list)} elements, expected 10")
    else:
        print("list: 10 elements")
        # Check for mixed types
        has_int = any(isinstance(item, int) for item in test_data.list)
        has_str = any(isinstance(item, str) for item in test_data.list)
        has_dict = any(isinstance(item, dict) for item in test_data.list)
        if not (has_int and has_str and has_dict):
            warnings.append("list should contain mixed types (int, string, dict). ")
    
    # Validate dict
    if not hasattr(test_data, 'dict'):
        errors.append("Missing attribute: dict")
    elif len(test_data.dict) != 10:
        errors.append(f"dict has {len(test_data.dict)} elements, expected 10")
    else:
        print("dict: 10 elements")
        expected_keys = [f"key{i}" for i in range(1, 11)]
        missing_keys = [k for k in expected_keys if k not in test_data.dict]
        if missing_keys:
            warnings.append(f"dict missing expected keys: {missing_keys}")
    
    # Validate set
    if not hasattr(test_data, 'set'):
        errors.append("Missing attribute: set")
    elif len(test_data.set) != 10:
        errors.append(f"set has {len(test_data.set)} elements, expected 10")
    else:
        print("set: 10 elements")
    
    # Validate tuple
    if not hasattr(test_data, 'tuple'):
        errors.append("Missing attribute: tuple")
    elif len(test_data.tuple) != 10:
        errors.append(f"tuple has {len(test_data.tuple)} elements, expected 10")
    else:
        print("tuple: 10 elements")
    
    # Validate byte_array
    if not hasattr(test_data, 'byte_array'):
        errors.append("Missing attribute: byte_array")
    elif len(test_data.byte_array) != 10:
        errors.append(f"byte_array has {len(test_data.byte_array)} bytes, expected 10")
    else:
        print("byte_array: 10 bytes")
    
    # Validate large_list
    if not hasattr(test_data, 'large_list'):
        errors.append("Missing attribute: large_list")
    elif len(test_data.large_list) != 10000:
        errors.append(f"large_list has {len(test_data.large_list)} elements, expected 10000")
    else:
        print("large_list: 10000 elements")
        
        # Check first few objects in large_list
        sample_size = min(10, len(test_data.large_list))
        for i in range(sample_size):
            obj = test_data.large_list[i]
            if not hasattr(obj, 'id'):
                warnings.append(f"large_list[{i}] missing 'id' attribute")
            if not hasattr(obj, 'value'):
                warnings.append(f"large_list[{i}] missing 'value' attribute")
            elif not isinstance(obj.value, str):
                warnings.append(f"large_list[{i}].value is not a string")
            elif not (5 <= len(obj.value) <= 50):
                warnings.append(f"large_list[{i}].value length {len(obj.value)} not in range [5, 50]")
            if not hasattr(obj, 'timestamp'):
                warnings.append(f"large_list[{i}] missing 'timestamp' attribute")
        
        if not warnings or len([w for w in warnings if 'large_list' in w]) == 0:
            print(f"  Sample validation of first {sample_size} objects passed")
    
    return errors, warnings


if __name__ == "__main__":
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="Validate test data in db0 database")
    parser.add_argument(
        "--input_dir",
        type=str,
        required=True,
        help="Input directory containing db0 database to validate"
    )
    args = parser.parse_args()
    
    # Check if directory exists
    if not os.path.exists(args.input_dir):
        print(f"Error: Directory does not exist: {args.input_dir}")
        exit(1)
    
    if not os.path.isdir(args.input_dir):
        print(f"Error: Path is not a directory: {args.input_dir}")
        exit(1)
    
    print(f"Validating test data in: {args.input_dir}\n")
    
    # Initialize db0 and open the database
    try:
        db0.init(args.input_dir)
        db0.open("test-data-prefix")
    except Exception as e:
        print(f"Error opening database: {e}")
        exit(1)
    
    # Load the singleton
    try:
        test_data = TestDataSingleton()
    except Exception as e:
        print(f"Error loading TestDataSingleton: {e}")
        db0.close()
        exit(1)
    
    # Validate the data
    print("Validating collections:\n")
    errors, warnings = validate_test_data(test_data)
    
    # Display sample data
    print("\nSample data:")
    if hasattr(test_data, 'list') and len(test_data.list) > 0:
        print(f"  list[0:3]: {test_data.list[:3]}")
    
    if hasattr(test_data, 'dict') and len(test_data.dict) > 0:
        sample_dict = dict(list(test_data.dict.items())[:3])
        print(f"  dict (first 3): {sample_dict}")
    
    if hasattr(test_data, 'large_list') and len(test_data.large_list) > 0:
        obj = test_data.large_list[0]
        print(f"  large_list[0]: id={obj.id if hasattr(obj, 'id') else 'N/A'}, "
              f"value='{obj.value[:20] if hasattr(obj, 'value') else 'N/A'}...', "
              f"timestamp={obj.timestamp if hasattr(obj, 'timestamp') else 'N/A'}")
    
    # Close database
    db0.close()
    
    # Report results
    print("\n" + "="*50)
    if errors:
        print(f"\nVALIDATION FAILED with {len(errors)} error(s):")
        for error in errors:
            print(f"  - {error}")
        exit_code = 1
    else:
        print("\nVALIDATION PASSED - All required data present")
        exit_code = 0
    
    if warnings:
        print(f"\n{len(warnings)} warning(s):")
        for warning in warnings:
            print(f"  - {warning}")
    
    exit(exit_code)
