// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#define private public

#include "scan_filter.h"

#include "thirdparty/gtest/gtest.h"

namespace tera {

ScanFilter::ScanFilter() {}

class ScanFilterTest : public ::testing::Test, public ScanFilter {
public:
    ScanFilterTest() {
        Init();
    }

    void Init() {
        Filter filter;
        filter.set_type(BinComp);
        filter.set_bin_comp_op(EQ);
        filter.set_field(ValueFilter);
        filter.set_content("cf1");
        filter.set_ref_value("10");
        _filter_list.add_filter()->CopyFrom(filter);

        filter.set_content("cf2");
        filter.set_bin_comp_op(GT);
        _filter_list.add_filter()->CopyFrom(filter);

        _filter_num = 2;
    }
};

TEST_F(ScanFilterTest, Check) {
    KeyValuePair kv;
    kv.set_column_family("cf1");
    kv.set_value("10");

    EXPECT_TRUE(Check(kv));
    EXPECT_FALSE(IsSuccess());

    kv.set_value("20");
    EXPECT_FALSE(Check(kv));
    EXPECT_FALSE(IsSuccess());

    kv.set_column_family("cf2");
    EXPECT_TRUE(Check(kv));
    EXPECT_TRUE(IsSuccess());
}

TEST_F(ScanFilterTest, GetAllCfs) {
    std::set<string> cf_set;
    GetAllCfs(&cf_set);
    EXPECT_EQ(cf_set.size(), 2);
    std::set<string>::iterator it = cf_set.begin();
    EXPECT_EQ(*it, "cf1");
    it++;
    EXPECT_EQ(*it, "cf2");
}

TEST_F(ScanFilterTest, BinCompCheck) {
    KeyValuePair kv;
    kv.set_column_family("cf1");
    kv.set_value("10");

    Filter filter;
    filter.set_type(BinComp);
    filter.set_bin_comp_op(EQ);
    filter.set_field(ValueFilter);
    filter.set_content("cf1");
    filter.set_ref_value("10");
    EXPECT_TRUE(BinCompCheck(kv, filter) > 0);

    kv.set_value("20");
    EXPECT_TRUE(BinCompCheck(kv, filter) < 0);

    kv.set_column_family("cf2");
    EXPECT_TRUE(BinCompCheck(kv, filter) == 0);
}

TEST_F(ScanFilterTest, DoBinCompCheck) {
    EXPECT_TRUE(DoBinCompCheck(EQ, "10", "10"));
    EXPECT_TRUE(DoBinCompCheck(NE, "10", "20"));
    EXPECT_TRUE(DoBinCompCheck(GT, "10", "00"));
    EXPECT_TRUE(DoBinCompCheck(GE, "10", "10"));
    EXPECT_TRUE(DoBinCompCheck(GE, "20", "10"));
    EXPECT_TRUE(DoBinCompCheck(LT, "00", "10"));
    EXPECT_TRUE(DoBinCompCheck(LE, "10", "10"));
    EXPECT_TRUE(DoBinCompCheck(LE, "00", "10"));

    EXPECT_FALSE(DoBinCompCheck(EQ, "00", "10"));
    EXPECT_FALSE(DoBinCompCheck(NE, "00", "00"));
    EXPECT_FALSE(DoBinCompCheck(GT, "00", "10"));
    EXPECT_FALSE(DoBinCompCheck(GT, "00", "00"));
    EXPECT_FALSE(DoBinCompCheck(GE, "00", "10"));
    EXPECT_FALSE(DoBinCompCheck(LT, "20", "10"));
    EXPECT_FALSE(DoBinCompCheck(LT, "10", "10"));
    EXPECT_FALSE(DoBinCompCheck(LE, "20", "10"));
}
} // namespace tera
