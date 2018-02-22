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

TEST_F(ScanDescImplTest, ParseValueCompareFilter) {
    string filter_str;
    Filter filter;

    EXPECT_FALSE(ParseValueCompareFilter(filter_str, NULL));

    filter_str = "qualifier==10";
    EXPECT_FALSE(ParseValueCompareFilter(filter_str, &filter));

    filter_str = "qualifier10";
    EXPECT_FALSE(ParseValueCompareFilter(filter_str, &filter));

    filter_str = "int64cf0==-10";
    EXPECT_TRUE(ParseValueCompareFilter(filter_str, &filter));
    EXPECT_EQ(filter.type(), BinComp);
    EXPECT_EQ(filter.bin_comp_op(), EQ);
    EXPECT_EQ(filter.field(), ValueFilter);
    EXPECT_EQ(filter.content(), "cf0");

    filter_str = "int64cf1>1";
    EXPECT_TRUE(ParseValueCompareFilter(filter_str, &filter));
    EXPECT_EQ(filter.bin_comp_op(), GT);

    filter_str = "cf2==hello";
    EXPECT_FALSE(ParseValueCompareFilter(filter_str, &filter));
}

TEST_F(ScanDescImplTest, ParseSubFilterString) {
    // add more filter types
    string filter_str;
    Filter filter;

    filter_str = "qu";
    EXPECT_FALSE(ParseSubFilterString(filter_str, &filter));

    filter_str = "qual@ifier10";
    EXPECT_FALSE(ParseSubFilterString(filter_str, &filter));

    filter_str = "int64cf0 == -10";
    EXPECT_TRUE(ParseSubFilterString(filter_str, &filter));
    EXPECT_EQ(filter.type(), BinComp);
    EXPECT_EQ(filter.bin_comp_op(), EQ);
    EXPECT_EQ(filter.field(), ValueFilter);
    EXPECT_EQ(filter.content(), "cf0");

    filter_str = "int64cf1 > 1";
    EXPECT_TRUE(ParseSubFilterString(filter_str, &filter));
    EXPECT_EQ(filter.bin_comp_op(), GT);
}
} // namespace tera
