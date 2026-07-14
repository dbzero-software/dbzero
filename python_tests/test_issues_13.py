# SPDX-License-Identifier: LGPL-2.1-or-later
# Copyright (c) 2025 DBZero Software sp. z o.o.

import dbzero as db0
import pytest
from .conftest import DB0_DIR
from dataclasses import dataclass
import os
import random
import logging
from typing import Dict, List
import dbzero as db0


@db0.memo
@dataclass
class Issuer2:
  tax_id: int
  invoice_list: List
  invoice_index: db0.index

@db0.memo(singleton = True)
@dataclass
class INV_ROOT:
  # issuers by tax_id
  issuers: Dict[int, Issuer2]


@db0.memo(no_default_tags = True)
@dataclass
class Invoice2:
  nip_issuers: int
  issue_date: int
  data: bytes


@pytest.mark.stress_test
@pytest.mark.parametrize("db0_autocommit_fixture", [1500], indirect=True)
def test_commit_autocommit_race_issue(db0_autocommit_fixture):
    db0.set_cache_size(16 << 30)
    
    def get_random_ints():
        rand_ints_file = "rand_ints.txt"
        if os.path.exists(rand_ints_file):
            try:
                with open(rand_ints_file, 'r') as f:
                    content = f.read().strip()
                    if content:
                        random_ints = [int(x.strip()) for x in content.split(',') if x.strip()]
                        if random_ints:
                            print(f"Loaded {len(random_ints)} random integers from {rand_ints_file}")
                            return random_ints
            except Exception as e:
                print(f"Failed to load random integers from {rand_ints_file}: {e}")
        
        print("Generating 100000 random integers")
        result = [random.randint(1, 10000000) for _ in range(100000)]        
        return result
    
    random_ints = get_random_ints()
    
    def next_rand():
        while True:
            for ri in random_ints:
                yield ri
    
    rand_gen = next_rand()
    def get_random_nip(nips_set=set()):
        nip = next(rand_gen) + 100000000
        while nip in nips_set:
            nip = next(rand_gen) + 100000000
        nips_set.add(nip)
        return nip
    
    # create 250 k unique nip numbers
    nip_count = 50000
    nip_numbers = set()
    print("Generating 250000 unique NIP numbers")
    for i in range(nip_count):
        if i % 5000 == 0:
            print(f"Generated {i} NIP numbers so far")   
        get_random_nip(nip_numbers)
    nip_list = list(nip_numbers)
    
    inv_root = INV_ROOT(issuers={})    
    print(f"Created INV_ROOT singleton instance")
    print(f"Creating {len(nip_list)} issuers")
    for i, nip in enumerate(nip_list):
        if i % 5000 == 0:
            print(f"Created {i} issuers so far")
        new_issuer = Issuer2(tax_id=nip, invoice_list=[], invoice_index=db0.index())        
        inv_root.issuers[nip] = new_issuer
    
    RANDOM_BYTES = b'DB0'*22000
    total_size = 0
    count_of_objects = 0
    print("Starting benchmark loop")        
    count = 300000    
    db0.commit()
    
    def try_commit():
        if next(rand_gen) % 30000 < 5:
            print(f"*** next commit ***")
            db0.commit()
    
    while count > 0:
        count -= 1       
        # get random number between 0 and 100
        random_number = next(rand_gen) % 100
        # 90% chance to create small object, 10% chance to create large object
        if random_number < 90:
            data_size = next(rand_gen) % 1501 + 1
        else:
            data_size = next(rand_gen) % 56001 + 8000
        random_nip = nip_list[next(rand_gen) % len(nip_list)]
        random_issuer = inv_root.issuers[random_nip]
        try_commit()
        new_obj = Invoice2(nip_issuers=random_issuer.tax_id, issue_date=count, data=RANDOM_BYTES[:data_size])
        try_commit()
        random_issuer.invoice_list.append(new_obj)
        try_commit()
        random_issuer.invoice_index.add(count, new_obj)
        try_commit()
        count_of_objects += 1
        
        total_size += data_size
        if count % 5000 == 0:                
            stats = db0.get_storage_stats()
            # save report to csv file
            print(f"Objects count: {count_of_objects}, "
                f"Storage stats: {stats}")                
