#!/bin/bash

# If release a new version, modify these infos
VERSION_INFO="master"
VERSION_ADDR="https://github.com/baidu/tera"

BUILD_DATE_TIME=`date`
BUILD_HOSTNAME=`hostname`
BUILD_GCC_VERSION=`gcc --version | head -n 1`

gen_info_template_header ()
{
    echo "#include <iostream>"
    echo "#include \"version.h\""
    echo "extern const char kGitInfo[] = \"\\"
}


gen_info_template_foot ()
{
    echo "\";"
    echo "extern const char kBuildType[] = \"debug\";"
    echo "extern const char kBuildTime[] = \"$BUILD_DATE_TIME\";"
    echo "extern const char kBuilderName[] = \"$USER\";"
    echo "extern const char kHostName[] = \"$BUILD_HOSTNAME\";"
    echo "extern const char kCompiler[] = \"$BUILD_GCC_VERSION\";"
}

gen_info_print_template ()
{
    echo "void PrintSystemVersion() {"
    echo "    std::cout << \"=====  Version Info ===== \" << std::endl;"
    echo "    std::cout << \"Version: $VERSION_INFO\" << std::endl;"
    echo "    std::cout << \"Address: $VERSION_ADDR\" << std::endl;"
    echo "    std::cout << std::endl;"
    echo "    std::cout << \"=====  Git Info ===== \" << std::endl"
    echo "        << kGitInfo << std::endl;"
    echo "    std::cout << \"=====  Build Info ===== \" << std::endl;"
    echo "    std::cout << \"Build Type: \" << kBuildType << std::endl;"
    echo "    std::cout << \"Build Time: \" << kBuildTime << std::endl;"
    echo "    std::cout << \"Builder Name: \" << kBuilderName << std::endl;"
    echo "    std::cout << \"Build Host Name: \" << kHostName << std::endl;"
    echo "    std::cout << \"Build Compiler: \" << kCompiler << std::endl;"
    echo "    std::cout << std::endl;"
    echo "};"
}

TEMPLATE_HEADER_FILE=template_header.tmp
TEMPLATE_FOOT_FILE=template_foot.tmp
GIT_INFO_FILE=git_info.tmp
VERSION_CPP_FILE=src/version.cc

# generate template file
git remote -v | sed 's/$/&\\n\\/g' > $GIT_INFO_FILE
git log | head -n 3 | sed 's/$/&\\n\\/g' >> $GIT_INFO_FILE
gen_info_template_header > $TEMPLATE_HEADER_FILE
gen_info_template_foot > $TEMPLATE_FOOT_FILE
gen_info_print_template >> $TEMPLATE_FOOT_FILE

# generate version cpp
cat $TEMPLATE_HEADER_FILE $GIT_INFO_FILE $TEMPLATE_FOOT_FILE > $VERSION_CPP_FILE
rm -rf $TEMPLATE_HEADER_FILE $GIT_INFO_FILE $TEMPLATE_FOOT_FILE
