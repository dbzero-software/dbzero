#!/bin/bash
export PYTHONIOENCODING=utf8
python3 -m pytest -m 'not integration_test' -m 'not stress_test' -c pytest.ini --capture=no "$@" -vv
