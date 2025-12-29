# SPDX-License-Identifier: LGPL-2.1-or-later
# Copyright (c) 2025 DBZero Software sp. z o.o.

import dbzero as db0
import argparse


def __main__():
    parser = argparse.ArgumentParser()
    parser.add_argument('--path', default=None, type=str, help="Location of .db0 file to be scanned")
    args = parser.parse_args()
    db0.init()    
    print(f"--- File {args.path} ---")
    dram_io_map = db0.get_dram_io_map(path=args.path)
    print(f"map size = {len(dram_io_map)}")
    print(dram_io_map)
    db0.close()
    
        
if __name__ == "__main__":
    __main__()
        