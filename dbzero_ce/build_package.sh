function show_help {
    echo "Build dbzero_ce python package in the ./build directory and optionally install it"
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
mkdir .build/dbzero_ce
cp ./dbzero_ce/* ./.build/dbzero_ce
cp setup.py .build/setup.py
cp LICENSE .build/LICENSE
cp README.md .build/README.md
cd .build
python3 setup.py sdist

if [ "${INSTALL}" ] ; then
    pip3 install ./dist/dbzero_ce-0.0.1.tar.gz
fi

cd ..
rm -rf ./.build
