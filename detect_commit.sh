#!/bin/bash

actual_dir=`pwd`
actual_dir=`realpath ${actual_dir}`

cd $1

dbz_branch=`git rev-parse --abbrev-ref HEAD`
dbz_commit=`git rev-parse HEAD`
dbz_remote="$(git config branch.`git name-rev --name-only HEAD`.remote)"
# if gitlab environment variable CI_COMMIT_REF_NAME is set, use it as the branch name,
# as git rev-parse will not work - HEAD is in detached state
if [[ -v BIGSENSECORE_GIT_BRANCH ]]; then
    echo "Detected GitLab branch " "${BIGSENSECORE_GIT_BRANCH}"
    dbz_branch="${BIGSENSECORE_GIT_BRANCH}"
fi

#convert
# ${dbz_remote} \t git@gitlab-01.itx.pl:something/something.git (fetch) 
#to
# git@gitlab-01.itx.pl:something/something.git

if [ ! "${dbz_remote}" = "" ]; then
    dbz_remote_url=`git remote -v | grep ${dbz_remote} | grep "(fetch)" | tr "\t" " " | cut -d" " -f2`
fi

conan_requres_cnt=1
conan_requres='{"", "", ""}'

#read content of conanfile.txt into variables and parse to C compatible array
if [ -f conanfile.txt ]; then
    conan_requres_cnt=$(cat conanfile.txt | sed -n '/\[requires\]/,/\[/p'  | head -n -1  | tail -n +2 | grep . | grep -v "#"  | wc -l) 
    conan_requres=$(cat conanfile.txt | sed -n '/\[requires\]/,/\[/p'  | grep -v "\[" | grep . | grep -v "#"  |  awk -F'[/@]' '{print "        {" "\""$1"\""", " "\""$2"\""", " "\""$3"/"$4"\"" "}""," }' | sed '$ s/.$//' )
fi

current_dir=`pwd`
current_dir=`realpath ${current_dir}`
dbz_date=`date`

echo " "
echo "dbzero git version: "
echo "${dbz_remote_url} ${dbz_remote} ${dbz_branch} ${dbz_commit}" 
echo " "

if [ -f src/dbzero/BuildInfo.cpp ]; then
    prev_dbz_commit=`cat src/dbzero/BuildInfo.cpp | grep "s_commit" | cut -d"=" -f2`
    if [ $prev_dbz_commit == "\"${dbz_commit}\";" ]; then
        cd ${actual_dir}
        exit 0
    fi
fi

rm -f src/dbzero/BuildInfo.cpp

echo "#include <dbzero/BuildInfo.h>"                                                        >> src/dbzero/BuildInfo.cpp
echo " "                                                                                    >> src/dbzero/BuildInfo.cpp
echo "namespace db0 {"                                                                      >> src/dbzero/BuildInfo.cpp
echo " "                                                                                    >> src/dbzero/BuildInfo.cpp
echo "    const char * const CompilationVersion::s_date=\"${dbz_date}\";"                    >> src/dbzero/BuildInfo.cpp
echo "    const char * const CompilationVersion::s_path=\"${current_dir}\";"                >> src/dbzero/BuildInfo.cpp
echo " "                                                                                    >> src/dbzero/BuildInfo.cpp
echo "    const char * const CompilationVersion::s_remote_url=\"${dbz_remote_url}\";"        >> src/dbzero/BuildInfo.cpp
echo "    const char * const CompilationVersion::s_remote_name=\"${dbz_remote}\";"           >> src/dbzero/BuildInfo.cpp
echo "    const char * const CompilationVersion::s_branch=\"${dbz_branch}\";"                >> src/dbzero/BuildInfo.cpp
echo "    const char * const CompilationVersion::s_commit=\"${dbz_commit}\";"                >> src/dbzero/BuildInfo.cpp
echo " "                                                                                    >> src/dbzero/BuildInfo.cpp
echo "    const char * const CompilationVersion::s_cxx_flags=\"${@:2}\";"                   >> src/dbzero/BuildInfo.cpp
echo "    const char * CompilationVersion::s_libraries[${conan_requres_cnt}+1][3]={"        >> src/dbzero/BuildInfo.cpp
echo "${conan_requres},"                                                                    >> src/dbzero/BuildInfo.cpp
echo "        {0, 0, 0}"                                                                    >> src/dbzero/BuildInfo.cpp
echo "    };"                                                                               >> src/dbzero/BuildInfo.cpp
echo " "                                                                                    >> src/dbzero/BuildInfo.cpp
echo "}"                                                                                    >> src/dbzero/BuildInfo.cpp
echo " "                                                                                    >> src/dbzero/BuildInfo.cpp

cd ${actual_dir}
exit 0

