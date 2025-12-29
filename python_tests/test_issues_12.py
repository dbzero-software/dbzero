# SPDX-License-Identifier: LGPL-2.1-or-later
# Copyright (c) 2025 DBZero Software sp. z o.o.

import dbzero as db0
import pytest
from .conftest import DB0_DIR
from datetime import datetime
from .memo_test_types import MemoBlob
from dataclasses import dataclass
import random
import time
from typing import Dict, List
import sys

@db0.memo
@dataclass
class Issuer:
  tax_id: int
  inv_list: List
  inv_index: db0.index


@db0.memo()
@dataclass
class Invoice:
  tax_id: int
  issue_dt: datetime
  data: bytes




@db0.memo(no_cache=True)
@dataclass
class InvoiceNoCache:
  tax_id: int
  issue_dt: datetime
  data: bytes

@db0.memo
@dataclass
class SimpleIssuer:
  inv_object: InvoiceNoCache
  inv_index: db0.index

def get_random_tax_id(tax_ids_set=set()):
    tax_id = random.randint(1000000000, 9999999999)
    while tax_id in tax_ids_set:
        tax_id = random.randint(1000000000, 9999999999)
    tax_ids_set.add(tax_id)
    return tax_id


@pytest.mark.stress_test
@pytest.mark.parametrize("db0_slab_size", [{"slab_size": 256 << 20, "autocommit": False}], indirect=True)
def test_no_cache_allocator_issue(db0_slab_size):
    db0.set_cache_size(8 << 30)
    # create 25 k unique tax_id numbers
    tax_id_count = 25000
    tax_id_numbers = set()
    print(f"Generating {tax_id_count} unique tax_id numbers")
    for i in range(tax_id_count):
        if i % 5000 == 0:
            print(f"Generated {i} tax_id numbers so far")   
        get_random_tax_id(tax_id_numbers)
    tax_id_list = list(tax_id_numbers)

    issuers = {}
    
    for i, tax_id in enumerate(tax_id_list):
        if i % 5000 == 0:
            print(f"Created {i} issuers so far")
        new_issuer = Issuer(tax_id=tax_id, inv_list=[], inv_index=db0.index())
        issuers[tax_id] = new_issuer
    
    execution_time = 45
    RANDOM_BYTES = b'DB0'*22000
    total_size = 0
    count_of_objects = 0
    new_objects = 0
    db0.commit()
    print("Starting benchmark loop")    
    last_report = time.perf_counter()
    start = last_report
    while True:
        # get random number between 0 and 100
        random_number = random.randint(0, 100)
        if random_number < 90:
            data_size = random.randint(500, 2000)
        else:
            data_size = random.randint(8000, 64000)
        
        random_tax_id = random.choice(tax_id_list)
        issuer = issuers[random_tax_id]
        invoice = Invoice(tax_id=issuer.tax_id, issue_dt=datetime.now(), data=RANDOM_BYTES[:data_size])
        issuer.inv_list.append(invoice)
        issuer.inv_index.add(datetime.now(), invoice)
        count_of_objects += 1
        new_objects += 1
        
        total_size += data_size
        # report every 3 seconds
        now = time.perf_counter()
        if (now - last_report) >= 3:     
            commit_start = time.perf_counter()
            db0.commit()
            commit_end = time.perf_counter()
            print(f"Commit time: {(commit_end - commit_start)} seconds")

            now = time.perf_counter()
            print(f"Objects / sec {float(new_objects) / (now - last_report)}, Total objects: {count_of_objects}, Total size: {total_size} bytes")
            print(db0.get_storage_stats())
            print(db0.get_lang_cache_stats())
            new_objects = 0    
            last_report = now
        
        if  (now - start) > execution_time:
            break
    
@pytest.mark.skip(reason="need to fix: issues/533")
def test_free_issue_with_index_and_no_cache_object(db0_fixture):
    index = db0.index()
    new_issuer = SimpleIssuer(inv_object=None, inv_index=index)
    db0.commit()
    RANDOM_BYTES = b'DB0'*5
    invoice = InvoiceNoCache(tax_id=None, issue_dt=datetime.now(), data=RANDOM_BYTES)
    new_issuer.inv_object = invoice
    new_issuer.inv_index.add(datetime.now(), invoice)
