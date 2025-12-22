function show_help {
    echo "Build dbzero python package in the ./build directory and optionally install it"
    echo "Use: build_package.sh [options]"
    echo " -h, --help   Shows this help screen."
    echo " --install    Install the package locally"
    exit 0
}

while true ; do
    case "$1" in
        -h|--help) show_help ; shift ;;
        --install) INSTALL="1" ; shift ;;
        --) shift ; break ;;
        *) break;;
    esac
done

mkdir .build
mkdir .build/dbzero
cp ./dbzero/* ./.build/dbzero
cp setup.py .build/setup.py
cp LICENSE .build/LICENSE
cp README.md .build/README.md
cd .build
python3 setup.py sdist
install "$(ls dbzero_package/dist/*.whl | sort | tail -n 1)" --break-system-packages
# Get the current Python3 version
PYTHON3_VERSION=$(python3 -c 'import sys; print(f"{sys.version_info.minor}")')
if [ "${INSTALL}" ] ; then
    if [ "$PYTHON3_VERSION" -ge 11 ]; then
        pip3 install "$(ls ./dist/*.tar.gz | sort | tail -n 1)" --break-system-packages
    else
        pip3 install "$(ls ./dist/*.tar.gz | sort | tail -n 1)" 
    fi
fi

cd ..
rm -rf ./.build
