#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
build_type=release
test_binary=tests.x

case "${1:-}" in
    -d|--debug)
        build_type=debug
        test_binary=testsD.x
        shift
        ;;
    -r|--release)
        shift
        ;;
esac

if [[ ! -x "$repo_root/build/$build_type/$test_binary" ]]; then
    echo "Build the C++ tests first with ./scripts/build.sh -r -t or -d -t." >&2
    exit 1
fi

cd "$repo_root"
python_site_packages="$(uv run --no-sync python -c 'import sysconfig; print(sysconfig.get_paths()["purelib"])')"
export PYTHONPATH="$python_site_packages${PYTHONPATH:+:$PYTHONPATH}"

cd "$repo_root/build/$build_type"
exec "./$test_binary" "$@"
