// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#define private public

#include "tprinter.h"
#include "gtest/gtest.h"

namespace tera {

class TPrinterTest : public ::testing::Test, public TPrinter {
public:
    TPrinterTest() : TPrinter(3) {}
    ~TPrinterTest() {}
};

TEST_F(TPrinterTest, Print) {
    ASSERT_TRUE(AddRow(3, "No.", "date", "comment"));
    ASSERT_TRUE(AddRow(3, "1", "07/15/2014", "hello world"));
    ASSERT_TRUE(AddRow(3, "2", "07/16/2014", "hello tera"));

    std::vector<int64_t> v;
    v.push_back(3);
    v.push_back(123456789);
    v.push_back(98765);
    ASSERT_TRUE(AddRow(v));

    Print();
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
} // namespace tera
