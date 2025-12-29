# SPDX-License-Identifier: LGPL-2.1-or-later
# Copyright (c) 2025 DBZero Software sp. z o.o.

from dbzero import db0
import os

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
class MySingleton:
    def __init__(self, value):
        print("*** Initializing MySingleton ***")
        self.int_param = value
        self.str_param = str(value)
    
    
def start():
    # Configure the dbzero connection without connecting yet
    db0.init(os.path.join(os.getcwd(), "app-data") **config)
    db0.open(config["prefix"])
    obj = MySingleton(123)    
    db0.close()
    
    
if __name__ == "__main__":
    start()
