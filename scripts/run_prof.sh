export PYTHONIOENCODING=utf8
export G_SLICE=always-malloc 
export G_DEBUG=gc-friendly
valgrind --tool=callgrind --instr-atstart=no python3 -m pytest -m 'stress_test' -k='test_create_random_objects_stress_test' -c pytest.ini --capture=no "$@"
