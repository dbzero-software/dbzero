#!/bin/bash
export PYTHONIOENCODING=utf8
export LD_PRELOAD=$(gcc -print-file-name=libasan.so)

python3 -m pytest -m 'stress_test' -c pytest.ini --capture=no "$@"
