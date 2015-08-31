// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#define private public

#include "utils/tprinter.h"
#include <glog/logging.h>
#include <gtest/gtest.h>

namespace tera {

class TPrinterTest : public ::testing::Test, public TPrinter {
public:
    TPrinterTest()
        : TPrinter(3, "No.", "year<int>", "avg<double>") {
    }
    ~TPrinterTest() {}
};

TEST_F(TPrinterTest, ParseColType) {
    string item, name;
    CellType type;
    item = "hello<int>";

    EXPECT_TRUE(TPrinter::ParseColType(item, &name, &type));
    VLOG(5) << name << " " << type;
    EXPECT_EQ(name, "hello");
    EXPECT_EQ(type, INT);

    item = "hello";
    EXPECT_FALSE(TPrinter::ParseColType(item, &name, &type));
}

TEST_F(TPrinterTest, NumToStr) {
    ASSERT_EQ("0", NumToStr(0));
    ASSERT_EQ("10", NumToStr(10));
    ASSERT_EQ("10K", NumToStr(10000));
    ASSERT_EQ("10P", NumToStr(10000000000000000ll));

    ASSERT_EQ("12.34K", NumToStr(12344));
    ASSERT_EQ("10.11P", NumToStr(10110000000000000ll));
}

TEST_F(TPrinterTest, AddRow) {
    ASSERT_TRUE(AddRow(3, "1", 2013, 1.234));
    ASSERT_TRUE(AddRow(3, "2", 2014, 500.0));

    ASSERT_EQ(2, (int)body_.size());
    ASSERT_EQ(3, (int)body_[0].size());
    ASSERT_EQ(body_[0][0].type, STRING);
    ASSERT_EQ(body_[0][1].type, INT);
    ASSERT_EQ(body_[0][1].value.i, 2013);
    ASSERT_EQ(3, (int)body_[1].size());
    ASSERT_EQ(body_[1][2].type, DOUBLE);
    ASSERT_EQ(body_[1][2].value.d, 500);
}

TEST_F(TPrinterTest, New) {
    ASSERT_EQ(3, (int)head_.size());
    ASSERT_EQ(STRING, head_[0].second);
    ASSERT_EQ(INT, head_[1].second);
    ASSERT_EQ(DOUBLE, head_[2].second);
}


/*
TEST_F(TPrinterTest, Print) {
    TPrinter t(3, "int", "double", "string");
}

TEST_F(TPrinterTest, ToString) {
    ASSERT_TRUE(AddRow(3, "No.", "date", "comment"));
    ASSERT_TRUE(AddRow(3, "3", "07/15/2014", "hello world"));
    std::vector<string> v;
    v.push_back("4");
    v.push_back("07/15/2014");
    v.push_back("hello baidu");
    ASSERT_TRUE(AddRow(v));

    ASSERT_TRUE(AddRow(3, "5", "07/16/2014", "hello tera"));

    string outstr = ToString();
    std::cout << outstr.size() << std::endl;
    std::cout << outstr;
}

TEST_F(TPrinterTest, RemoveSubString) {
    string input, substr;

    input = "www.baidu.com";
    substr = ".baidu.cn";
    ASSERT_EQ(RemoveSubString(input, substr), "www.baidu.com");

    input = "www.baidu.com";
    substr = ".baidu.com";
    ASSERT_EQ(RemoveSubString(input, substr), "www");

    input = "www.baidu.com-www.baidu.com";
    substr = ".baidu.com";
    ASSERT_EQ(RemoveSubString(input, substr), "www-www");
}
*/
} // namespace tera

int main(int argc, char** argv) {
    ::google::ParseCommandLineFlags(&argc, &argv, true);
    ::google::InitGoogleLogging(argv[0]);
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
