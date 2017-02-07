#!/bin/bash

# If release a new version, modify these infos
VERSION_INFO="0.4.6"
VERSION_ADDR="https://github.com/baidu/tera/archive/0.4.6.tar.gz"

BUILD_DATE_TIME=`date`
BUILD_HOSTNAME=`hostname`

gen_info_template_header ()
{
    echo "#include <iostream>"
    echo "#include <sstream>"
    echo "#include <string>"
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
    echo "extern const char kCompiler[] = __VERSION__;"
}

gen_info_print_template ()
{
    echo "std::string SystemVersionInfo() {"
    echo "    std::stringstream ss;"
    echo "    ss << \"=====  Version Info ===== \" << std::endl;"
    echo "    ss << \"Version: $VERSION_INFO\" << std::endl;"
    echo "    ss << \"Address: $VERSION_ADDR\" << std::endl;"
    echo "    ss << std::endl;"
    echo "    ss << \"=====  Git Info ===== \" << std::endl;"
    echo "    ss << kGitInfo << std::endl;"
    echo "    ss << \"=====  Build Info ===== \" << std::endl;"
    echo "    ss << \"Build Type: \" << kBuildType << std::endl;"
    echo "    ss << \"Build Time: \" << kBuildTime << std::endl;"
    echo "    ss << \"Builder Name: \" << kBuilderName << std::endl;"
    echo "    ss << \"Build Host Name: \" << kHostName << std::endl;"
    echo "    ss << \"Build Compiler: \" << kCompiler << std::endl;"
    echo "    return ss.str();"
    echo "};"
    echo "void PrintSystemVersion() {"
    echo "    std::cout << SystemVersionInfo() << std::endl;"
    echo "};"
}

TEMPLATE_HEADER_FILE=template_header.tmp
TEMPLATE_FOOT_FILE=template_foot.tmp
GIT_INFO_FILE=git_info.tmp
VERSION_CPP_FILE=src/version.cc

# generate template file
git log | head -n 6 | sed 's/$/&\\n\\/g' > $GIT_INFO_FILE
gen_info_template_header > $TEMPLATE_HEADER_FILE
gen_info_template_foot > $TEMPLATE_FOOT_FILE
gen_info_print_template >> $TEMPLATE_FOOT_FILE

# generate version cpp
cat $TEMPLATE_HEADER_FILE $GIT_INFO_FILE $TEMPLATE_FOOT_FILE > $VERSION_CPP_FILE
rm -rf $TEMPLATE_HEADER_FILE $GIT_INFO_FILE $TEMPLATE_FOOT_FILE
