#!/usr/bin/env bash
set -euo pipefail

show_help() {
    echo "Builds dbzero for the Claude development container"
    echo "Use: dbzero-build [options]"
    echo " -h, --help                   Shows this help screen."
    echo " -j, --jobs                   Threads number. Max by default."
    echo " -r, --release                Compile as release. Note: debug build is by default."
    echo " -s, --sanitize               Compile with sanitizers."
    echo " -p, --prefix                 Install build under the specified prefix."
    echo " -t, --tests                  Build tests."
    echo " -e, --disable_debug_exceptions      Disable debug exceptions."
    echo ""
    exit 0
}

if [[ ! -f scripts/generate_meson.py || ! -d dbzero ]]; then
    echo "Run dbzero-build from the repository root." >&2
    exit 1
fi

export CPLUS_INCLUDE_PATH="${CPLUS_INCLUDE_PATH:-}:/usr/include/python3.9/"

cores=$(grep -c ^processor /proc/cpuinfo)
build_type="debug"
sanitizer="false"
enable_debug_exceptions="true"
build_tests="false"

default_prefix="/usr/local"
if [[ "$(id -u)" -ne 0 ]]; then
    default_prefix="$HOME/.local"
fi
install_prefix="${DBZERO_INSTALL_PREFIX:-$default_prefix}"

temp=$(getopt -o hj:rtsep: --long help,jobs:,release,tests,sanitize,disable_debug_exceptions,prefix: -n 'dbzero-build' -- "$@")
if [[ $? -ne 0 ]]; then
    exit 1
fi
eval set -- "$temp"

while true; do
    case "$1" in
        -h|--help) show_help ;;
        -s|--sanitize) sanitizer="true" ; shift ;;
        -r|--release) build_type="release" ; shift ;;
        -t|--tests) build_tests="true" ; shift ;;
        -e|--disable_debug_exceptions) enable_debug_exceptions="false" ; shift ;;
        -p|--prefix) install_prefix="$2" ; shift 2 ;;
        -j|--jobs) cores="$2" ; shift 2 ;;
        --) shift ; break ;;
        *) echo "Argument parsing error: $1" >&2 ; exit 1 ;;
    esac
done

if [[ "$cores" -lt 1 ]]; then
    echo "Argument parsing error: Wrong jobs number: $cores" >&2
    exit 1
fi

python3 scripts/generate_meson.py ./src/dbzero/ core
python3 scripts/generate_meson_tests.py tests/
python3 scripts/generate_meson_dbzero.py dbzero/

mkdir -p build

build_dir="build/debug"
if [[ "$build_type" == "release" ]]; then
    build_dir="build/release"
fi

options=(
    "-Denable_debug_exceptions=$enable_debug_exceptions"
    "-Denable_sanitizers=$sanitizer"
    "-Dbuild_tests=$build_tests"
)

if [[ -f "$build_dir/meson-private/coredata.dat" ]]; then
    meson setup --reconfigure --prefix="$install_prefix" --buildtype="$build_type" "${options[@]}" "$build_dir"
else
    meson setup --prefix="$install_prefix" --buildtype="$build_type" "${options[@]}" "$build_dir"
fi

ninja -C "$build_dir" -j "$cores"
meson install -C "$build_dir"

cd dbzero
envsubst < dbzero/dbzero.template > dbzero/dbzero.py
dbzero-build-package --install
echo "$build_type"