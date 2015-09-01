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
    int64_t i = 100;
    ASSERT_EQ("100", NumToStr(i));
    ASSERT_EQ("0", NumToStr(0));
    ASSERT_EQ("10", NumToStr(10));
    ASSERT_EQ("10K", NumToStr(10000));
    ASSERT_EQ("10P", NumToStr(10000000000000000ll));

    ASSERT_EQ("12.34K", NumToStr(12344));
    ASSERT_EQ("10.11P", NumToStr(10110000000000000ll));

    ASSERT_EQ("1", NumToStr(1.0));
    ASSERT_EQ("1.23", NumToStr(1.23));
    ASSERT_EQ("1.20", NumToStr(1.2));
}

TEST_F(TPrinterTest, AddRow) {
    // test varargs row
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

    ASSERT_FALSE(AddRow(4, "2", 2014, 500.0));
    ASSERT_FALSE(AddRow(1, "2", 2014, 500.0));

    // test int vector row
    std::vector<int64_t> vi(3, 9);
    ASSERT_TRUE(AddRow(vi));
    ASSERT_EQ(body_[2][0].type, INT);
    ASSERT_EQ(body_[2][1].type, INT);
    ASSERT_EQ(body_[2][2].value.i, 9);
    vi.resize(1);
    ASSERT_FALSE(AddRow(vi));

    // test string vector row
    std::vector<string> vs(3, "hello");
    ASSERT_TRUE(AddRow(vs));
    ASSERT_EQ(body_[3][0].type, STRING);
    ASSERT_EQ(body_[3][1].type, STRING);
    ASSERT_EQ(*body_[3][2].value.s, "hello");
    vs.resize(5);
    ASSERT_FALSE(AddRow(vs));
}

TEST_F(TPrinterTest, New) {
    ASSERT_EQ(3, (int)head_.size());
    ASSERT_EQ(STRING, head_[0].second);
    ASSERT_EQ(INT, head_[1].second);
    ASSERT_EQ(DOUBLE, head_[2].second);
}

TEST_F(TPrinterTest, ToString) {
    ASSERT_TRUE(AddRow(3, "1", 2013, 1.234));
    ASSERT_TRUE(AddRow(3, "2", 2014, 500));

    string outstr = ToString();
    LOG(ERROR) << outstr.size() << std::endl << outstr;
}

} // namespace tera

int main(int argc, char** argv) {
    ::google::ParseCommandLineFlags(&argc, &argv, true);
    ::google::InitGoogleLogging(argv[0]);
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
