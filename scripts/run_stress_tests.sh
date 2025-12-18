#!/bin/bash
export PYTHONIOENCODING=utf8
python3 -m pytest -m 'stress_test' -c pytest.ini --capture=no "$@"
