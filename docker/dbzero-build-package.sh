#!/usr/bin/env bash
set -euo pipefail

show_help() {
    echo "Build dbzero python package in the ./build directory and optionally install it"
    echo "Use: dbzero-build-package [options]"
    echo " -h, --help   Shows this help screen."
    echo " --install    Install the package locally"
    exit 0
}

if [[ ! -f setup.py || ! -d dbzero ]]; then
    echo "Run dbzero-build-package from the repository root." >&2
    exit 1
fi

install_package="false"
while [[ $# -gt 0 ]]; do
    case "$1" in
        -h|--help) show_help ;;
        --install) install_package="true" ; shift ;;
        --) shift ; break ;;
        *) break ;;
    esac
done

pip_install_args=()
if python3 -m pip install --help 2>/dev/null | grep -q -- "--break-system-packages"; then
    pip_install_args+=(--break-system-packages)
fi
if [[ "$(id -u)" -ne 0 && -z "${VIRTUAL_ENV:-}" ]]; then
    pip_install_args+=(--user)
fi

rm -rf ./.build
mkdir -p ./.build/dbzero
cp ./dbzero/* ./.build/dbzero
cp setup.py ./.build/setup.py
cp LICENSE ./.build/LICENSE
cp README.md ./.build/README.md

cd ./.build
python3 setup.py sdist

if [[ "$install_package" == "true" ]]; then
    python3 -m pip install "${pip_install_args[@]}" "$(ls ./dist/*.tar.gz | sort | tail -n 1)"
fi

cd ..
rm -rf ./.build