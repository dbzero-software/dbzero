# SPDX-License-Identifier: LGPL-2.1-or-later
# Copyright (c) 2025 DBZero Software sp. z o.o.

from dbzero import db0
import os
import datetime

ORG_NAME = "division-by-zero"
PROJECT_NAME = "python-tests"
ENV = "dev"
DATA_PREFIX = f"/{ORG_NAME}/{PROJECT_NAME}/{ENV}/data"

config = {
    "prefix": DATA_PREFIX,
    "autocommit": True,
    "autocommit_interval": 3000,
    "cache_size": 4 << 30
}

@db0.memo(singleton = True, id = "my-singleton")
class Root:
    def __init__(self, value):
        print("*** Initializing MySingleton ***")
        self.value = value        
    
@db0.memo(id = "migration-test-class")
class MigrationTestClass():
    def __init__(self):
        self.int_value = 931
        self.str_value = "abdc"
        self.date_value = datetime.date(2023, 1, 1)
        self.list_value = []
        self.dict_value = {"a": 1, "b": 2}
        self.set_value = {}
    
        
def start():
    # Configure the dbzero connection without connecting yet
    db0.init(os.path.join(os.getcwd(), "app-data"), **config)
    db0.open(config["prefix"])
    root = Root([])
    for _ in range(10):
        root.value.append(MigrationTestClass())
    
    db0.close()
    
    
if __name__ == "__main__":
    start()
