#!/bin/bash
set -euo pipefail

function show_help {
    echo "Build dbzero python package in the ./build directory and optionally install it"
    echo "Use: build_package.sh [options]"
    echo " -h, --help   Shows this help screen."
    echo " --install    Install the package locally"
    exit 0
}

while true ; do
    case "${1-}" in
        -h|--help) show_help ; shift ;;
        --install) INSTALL="1" ; shift ;;
        --) shift ; break ;;
        *) break;;
    esac
done

package_root="$PWD"
rm -rf .build
trap 'rm -rf "$package_root/.build"' EXIT
mkdir -p .build/dbzero
find ./dbzero -maxdepth 1 -type f -exec cp {} ./.build/dbzero/ \;
cp setup.py .build/setup.py
cp pyproject.toml .build/pyproject.toml
cp LICENSE .build/LICENSE
cp README.md .build/README.md
cd .build
uv build --sdist
if [ "${INSTALL-}" ] ; then
    uv pip install "$(ls ./dist/*.tar.gz | sort | tail -n 1)"
fi

cd ..
rm -rf ./.build
