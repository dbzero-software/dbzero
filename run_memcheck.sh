#!/bin/bash
export PYTHONIOENCODING=utf8
export G_SLICE=always-malloc 
export G_DEBUG=gc-friendly
#valgrind -v --tool=memcheck --leak-check=no --num-callers=250 --log-file=valgrind.log python3 -m pytest -m 'not integration_test' -m 'not stress_test' -c pytest.ini --capture=no "$@"
#valgrind -v --tool=memcheck --leak-check=no --num-callers=250 --log-file=valgrind.log python3 -m pytest -m 'stress_test' -c pytest.ini --capture=no "$@"
#valgrind -v --tool=memcheck --leak-check=full --num-callers=40 --log-file=valgrind.log python3 -m samples.explore --path='/src/zorch/app-data'
#valgrind -v --tool=memcheck --leak-check=no --num-callers=250 --log-file=valgrind.log python3 -m pytest -m 'stress_test' -k='test_no_cache_allocator_issue' -c pytest.ini --capture=no "$@"
valgrind -v --tool=memcheck --leak-check=no --num-callers=250 --log-file=valgrind.log python3 -m pytest -k='test_base_lock_usage_does_not_exceed_limits' -c pytest.ini --capture=no "$@"
