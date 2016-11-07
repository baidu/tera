// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#define private public

#include "scan_impl.h"

#include "gtest/gtest.h"

using std::string;

namespace tera {

class ScanDescImplTest : public ::testing::Test, public ScanDescImpl {
public:
    ScanDescImplTest() : ScanDescImpl("row") {
        CreateSchema();
        SetTableSchema(table_schema_);
    }

    ~ScanDescImplTest() {}

    void CreateSchema() {
        table_schema_.set_name("linkcache");
        LocalityGroupSchema* lg = table_schema_.add_locality_groups();
        lg->set_name("lg0");
        ColumnFamilySchema* cf = table_schema_.add_column_families();
        cf->set_name("cf0");
        cf->set_locality_group("lg0");
        cf->set_type("int32");

        cf = table_schema_.add_column_families();
        cf->set_name("cf1");
        cf->set_locality_group("lg0");
        cf->set_type("uint64");

        cf = table_schema_.add_column_families();
        cf->set_name("cf2");
        cf->set_locality_group("lg0");
        cf->set_type("binary");
    }

    const TableSchema& GetSchema() const {
        return table_schema_;
    }

private:
    TableSchema table_schema_;
};

TEST_F(ScanDescImplTest, GetCfType) {
    string cf_name, type;

    cf_name = "cf0";
    EXPECT_TRUE(GetCfType(cf_name, &type));
    EXPECT_EQ(type, "int32");

    cf_name = "cf2";
    EXPECT_TRUE(GetCfType(cf_name, &type));
    EXPECT_EQ(type, "binary");

    cf_name = "cf100";
    EXPECT_FALSE(GetCfType(cf_name, &type));
}

TEST_F(ScanDescImplTest, ParseValueCompareFilter) {
    string filter_str;
    Filter filter;

    EXPECT_FALSE(ParseValueCompareFilter(filter_str, NULL));

    filter_str = "qualifier==10";
    EXPECT_FALSE(ParseValueCompareFilter(filter_str, &filter));

    filter_str = "qualifier10";
    EXPECT_FALSE(ParseValueCompareFilter(filter_str, &filter));

    filter_str = "cf0==-10";
    EXPECT_TRUE(ParseValueCompareFilter(filter_str, &filter));
    EXPECT_EQ(filter.type(), BinComp);
    EXPECT_EQ(filter.bin_comp_op(), EQ);
    EXPECT_EQ(filter.field(), ValueFilter);
    EXPECT_EQ(filter.content(), "cf0");

    filter_str = "cf1>1";
    EXPECT_TRUE(ParseValueCompareFilter(filter_str, &filter));
    EXPECT_EQ(filter.bin_comp_op(), GT);

    filter_str = "cf2==hello";
    EXPECT_TRUE(ParseValueCompareFilter(filter_str, &filter));
    EXPECT_EQ(filter.bin_comp_op(), EQ);
    EXPECT_EQ(filter.ref_value(), "hello");
}

TEST_F(ScanDescImplTest, ParseSubFilterString) {
    // add more filter types
    string filter_str;
    Filter filter;

    filter_str = "qu";
    EXPECT_FALSE(ParseSubFilterString(filter_str, &filter));

    filter_str = "qual@ifier10";
    EXPECT_FALSE(ParseSubFilterString(filter_str, &filter));

    filter_str = "cf0 == -10";
    EXPECT_TRUE(ParseSubFilterString(filter_str, &filter));
    EXPECT_EQ(filter.type(), BinComp);
    EXPECT_EQ(filter.bin_comp_op(), EQ);
    EXPECT_EQ(filter.field(), ValueFilter);
    EXPECT_EQ(filter.content(), "cf0");

    filter_str = "cf1 > 1";
    EXPECT_TRUE(ParseSubFilterString(filter_str, &filter));
    EXPECT_EQ(filter.bin_comp_op(), GT);
}

TEST_F(ScanDescImplTest, ParseFilterString) {
    string filter_str;

    filter_str = "cf0 < 10 AND cf1 >100 AND cf2 == world";
    SetFilterString(filter_str);
    EXPECT_TRUE(ParseFilterString());
    EXPECT_EQ(filter_list_.filter_size(), 3);

    filter_str = "cf < 10 AND cf1 >100 AND cf2 == world";
    SetFilterString(filter_str);
    EXPECT_FALSE(ParseFilterString());

    filter_str = "cf0 < 10 OR cf1 >100 AND cf2 == world";
    SetFilterString(filter_str);
    EXPECT_FALSE(ParseFilterString());
}

} // namespace tera
