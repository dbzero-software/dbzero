#!/bin/bash
cd $(cd "$(dirname "${BASH_SOURCE[0]}")" ; pwd -P)
set -e 
function show_help {
    echo "Builds dbzero"
    echo "Use: build.sh [options]"
    echo " -h, --help                   Shows this help screen."
    echo " -j, --jobs                   Threads number. Max by default."
    echo " -r, --release                Compile as release. Note: debug build is by default."
    echo " -s, --sanitize               Compile with sanitizers."
    echo " -i, --install                Install build in specified directory"
    echo " -e, --disable_debug_exceptions      Disable debug exceptions (exceptions and stack traces from c++ code)."
    echo ""
    exit 0;
}

export CPLUS_INCLUDE_PATH="$CPLUS_INCLUDE_PATH:/usr/include/python3.9/"
    
cores=`grep -c ^processor /proc/cpuinfo`
export build_type="debug"
install_dir=""
sanitizer="OFF"
enable_debug_exceptions="true"

TEMP=`getopt -o hj:rsie --long help,jobs:,release,sanitize,install,debug_exceptions -n 'build.sh' -- "$@"`
if [ ! $? -eq 0 ]; then
    exit
fi
eval set -- "$TEMP"

while true ; do
    case "$1" in
        -h|--help) show_help ; shift ;;
        -s|--sanitize) sanitizer="ON" ; shift ;;
        -r|--release) build_type="release" ; shift ;;
        -e|--disable_debug_exceptions) enable_debug_exceptions="false" ; shift ;;
        -j|--jobs)
            case "$2" in
                "") shift 2 ;;
                *) cores=$2 ; shift 2 ;;
            esac ;;
        --) shift ; break ;;
        *) echo "Argument parsing error: $1" ; exit 1 ;;
    esac
done

if [ $cores -lt 1 ]; then
    echo "Argument parsing error: Wrong jobs number: $cores"
    exit 1;
fi

# regenerate meson files:

python3 scripts/generate_meson.py ./src/dbzero/ core
python3 scripts/generate_meson_tests.py tests/
python3 scripts/generate_meson_dbzero.py dbzero/



mkdir -p build

if [ "$build_type" == "debug" ]; then
	meson setup --buildtype="debug"  -Denable_debug_exceptions=$enable_debug_exceptions build/debug
    cd build/debug
else
	meson setup --buildtype="release" -Denable_debug_exceptions=$enable_debug_exceptions build/release
    cd build/release
fi

ninja
meson install


cd ../..
# build package

cd dbzero

envsubst < dbzero/dbzero.template > dbzero/dbzero.py
./build_package.sh --install
echo $build_type
