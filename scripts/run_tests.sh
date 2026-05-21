#!/bin/bash
set -e

export PYTHONIOENCODING=utf8

pytest_args=()
parallel_args=()

while [[ $# -gt 0 ]]; do
    case "$1" in
        -j|--jobs)
            if [[ $# -lt 2 ]]; then
                echo "error: $1 requires a worker count" >&2
                exit 2
            fi
            parallel_args=(-n "$2")
            shift 2
            ;;
        --jobs=*)
            parallel_args=(-n "${1#--jobs=}")
            shift
            ;;
        *)
            pytest_args+=("$1")
            shift
            ;;
    esac
done

python3 -m pytest -m 'not integration_test' -m 'not stress_test' -c pytest.ini --capture=no "${parallel_args[@]}" "${pytest_args[@]}" -vv
