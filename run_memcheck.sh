#!/bin/bash
export PYTHONIOENCODING=utf8
export G_SLICE=always-malloc 
export G_DEBUG=gc-friendly
valgrind -v --tool=memcheck --leak-check=full --num-callers=40 --log-file=valgrind.log python3 -m pytest -m 'not integration_test' -k='test_can_create_set_with_params' -c pytest.ini --capture=no "$@"
