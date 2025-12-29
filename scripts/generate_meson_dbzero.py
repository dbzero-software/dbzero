#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-2.1-or-later
# Copyright (c) 2025 DBZero Software sp. z o.o.

"""
Simple script for generating meson.build file for dbzero Python package.
Based on the existing generate_meson.py script pattern.

Usage:
    python generate_meson_dbzero_simple.py [target_directory]
"""

import os
import sys


def get_python_files(directory):
    """Get all Python files in the dbzero subdirectory."""
    python_files = []
    dbzero_subdir = os.path.join(directory, "dbzero")
    
    if not os.path.exists(dbzero_subdir):
        print(f"Warning: dbzero subdirectory not found in {directory}")
        return python_files
    
    for entry in os.scandir(dbzero_subdir):
        if entry.is_file():
            name, ext = os.path.splitext(entry.name)
            if ext in ['.py', '.pyi']:
                # Use the format expected by meson (relative to package root)
                relative_path = f"dbzero/{entry.name}"
                python_files.append(relative_path)
    
    return sorted(python_files)


def generate_meson_build(target_dir):
    """Generate meson.build file in the target directory."""
    python_files = get_python_files(target_dir)
    
    if not python_files:
        print("No Python files found!")
        return False
    
    meson_path = os.path.join(target_dir, "meson.build")
    
    with open(meson_path, "w") as meson_file:
        # Write the python_sources array
        meson_file.write("python_sources = [\n")
        for file_path in python_files:
            meson_file.write(f"    '{file_path}',\n")
        meson_file.write("]\n\n")
        
        # Write the install configuration
        meson_file.write("py3_inst.install_sources(\n")
        meson_file.write("  python_sources,     \n")
        meson_file.write("    subdir: 'dbzero'\n")
        meson_file.write(")\n")
    
    print(f"Generated meson.build at: {meson_path}")
    return True


def main():
    """Main function."""
    if len(sys.argv) > 1:
        target_dir = sys.argv[1]
    else:
        # Default to dbzero directory relative to current working directory
        target_dir = os.path.join(os.getcwd(), "dbzero")
    
    if not os.path.exists(target_dir):
        print(f"Error: Directory {target_dir} does not exist")
        sys.exit(1)
    
    print(f"Generating meson.build for: {target_dir}")
    
    if generate_meson_build(target_dir):
        print("meson.build generation completed successfully!")
    else:
        print("meson.build generation failed!")
        sys.exit(1)


if __name__ == "__main__":
    main()