#! /bin/bash
function CheckRetCode() {
    if [ $1 -ne 0 ]; then       #parameter 1 is the return code
        if [ -n "$2" ]; then
            echo $2 >&2         #parameter 2 is the error message
        fi
        exit $1
    fi
}

sh_path=`readlink -f $(dirname "${BASH_SOURCE[0]}")`
mkdir -p $sh_path/build
CheckRetCode $? "Failed to create directory $sh_path/build"

pushd $sh_path/build

cmake $sh_path -DENCDB_DB_TYPE=NO_DB -DCMAKE_BUILD_TYPE=Debug -DENABLE_UNIT_TEST=on -DENABLE_COVERAGE=on && make -j
CheckRetCode $? "Build failure with NO_DB"

make -j test &&
ctest -T Coverage &&
gcovr --exclude-directories='encdb_kernel/src/extension' --exclude-directories='encdb_kernel/src/masking' -e '.*xxhash*' -r  ../ .
CheckRetCode $? "test failed, check test log"

cmake $sh_path -DENCDB_DB_TYPE=POLARDB_MYSQL -DCMAKE_BUILD_TYPE=Debug -DENABLE_UNIT_TEST=on -DENABLE_COVERAGE=on && make -j
CheckRetCode $? "Build failure with POLARDB_MYSQL"

make -j test &&
ctest -T Coverage &&
gcovr --exclude-directories='encdb_kernel/src/rule' -e '.*db_operation*' -e '.*xxhash*'  -r  ../ .
CheckRetCode $? "test failed, check test log"

if [ -e /dev/tdx-guest ]; then
    cmake $sh_path -DENCDB_DB_TYPE=NO_DB -DCMAKE_BUILD_TYPE=Debug -DENCDB_BUILD_WITH_TEE=ON  -DENABLE_UNIT_TEST=on -DENABLE_COVERAGE=on && make -j
    CheckRetCode $? "Build failure with NO_DB and ENCDB_BUILD_WITH_TEE=on"

    make -j test &&
    CheckRetCode $? "TDX RA test failure"
fi

popd

echo "Test Success."
exit 0
