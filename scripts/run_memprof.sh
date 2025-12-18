#!/bin/bash
export G_SLICE=always-malloc
export G_DEBUG=gc-friendly
# valgrind -v --tool=massif --log-file=masif.log ./build/debug/testsD.x --gtest_filter="*testMemoryUsageOverTime*"
valgrind -v --tool=massif --log-file=masif.log python3 -m pytest -m 'stress_test' -k='test_create_and_drop_simple_memo_objects' -c pytest.ini --capture=no "$@"

