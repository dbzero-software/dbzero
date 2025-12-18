rsync -r --delete --verbose --checksum --exclude=build --exclude=subprojects/ --exclude=.conan --exclude=.git --exclude=__pycache__/ --exclude=.pytest_cache/ /src/windows/ /src/dev/
./build.sh -r
if [ -n "$1" ]; then
    ./build/release/tests.x --gtest_filter="*$1*"
fi
