// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "tera/sdk/sdk_utils.h"

#include "tera/sdk/scan_impl.h"
#include "thirdparty/gtest/gtest.h"

namespace tera {
namespace sdk {

TEST(SdkUtils, CheckName) {
    EXPECT_TRUE(CheckName("hel_lo"));
    EXPECT_TRUE(CheckName(""));
    EXPECT_FALSE(CheckName("0hel_lo"));
    EXPECT_FALSE(CheckName("h.el_lo"));
    EXPECT_FALSE(CheckName("h el_lo"));
    EXPECT_FALSE(CheckName("he#l_lo"));
}

TEST(SdkUtils, ParseCfNameType) {
    string in, name, type;

    in = "cf";
    ASSERT_TRUE(ParseCfNameType(in, &name, &type));
    ASSERT_TRUE(name == "cf");
    ASSERT_TRUE(type == "");
    ASSERT_TRUE(ParseCfNameType(in, NULL, &type));

    in = "";
    ASSERT_TRUE(ParseCfNameType(in, &name, &type));
    ASSERT_TRUE(name == "");
    ASSERT_TRUE(type == "");
    ASSERT_TRUE(ParseCfNameType(in, &name, NULL));

    in = "cf<int>";
    ASSERT_TRUE(ParseCfNameType(in, &name, &type));
    ASSERT_TRUE(name == "cf");
    ASSERT_TRUE(type == "int");
    ASSERT_TRUE(ParseCfNameType(in, NULL, NULL));

    in = "<int>";
    ASSERT_TRUE(ParseCfNameType(in, &name, &type));
    ASSERT_TRUE(name == "");
    ASSERT_TRUE(type == "int");

    in = "cf<";
    ASSERT_FALSE(ParseCfNameType(in, &name, &type));

    in = "cf1int>";
    ASSERT_FALSE(ParseCfNameType(in, &name, &type));

    in = "<>";
    ASSERT_FALSE(ParseCfNameType(in, &name, &type));
}

TEST(SdkUtils, CommaInBracket) {
    string test;

    test = "0123,{67,90,23},6,89,1{3,567,}01{345}789,12";
    EXPECT_TRUE(CommaInBracket(test, 8));
    EXPECT_TRUE(CommaInBracket(test, 13));
    EXPECT_TRUE(CommaInBracket(test, 23));
    EXPECT_TRUE(CommaInBracket(test, 27));
    EXPECT_TRUE(CommaInBracket(test, 34));

    EXPECT_FALSE(CommaInBracket(test, 2));
    EXPECT_FALSE(CommaInBracket(test, 4));
    EXPECT_FALSE(CommaInBracket(test, 15));
    EXPECT_FALSE(CommaInBracket(test, 20));
    EXPECT_FALSE(CommaInBracket(test, 37));
}

TEST(SdkUtils, SplitCfSchema) {
    string schema;
    std::vector<string> cfs;

    schema = "cf1";
    SplitCfSchema(schema, &cfs);
    EXPECT_EQ(cfs.size(), 1);

    schema = "cf1,cf2,cf3";
    SplitCfSchema(schema, &cfs);
    EXPECT_EQ(cfs.size(), 3);

    schema = "cf2{prop1,prop2}";
    SplitCfSchema(schema, &cfs);
    EXPECT_EQ(cfs.size(), 1);

    schema = "cf1,cf2{prop1,prop2},cf3{prop2}";
    SplitCfSchema(schema, &cfs);
    EXPECT_EQ(cfs.size(), 3);

    schema = "cf1{prop1,prop2,prop3},cf2,cf3{prop1,prop2,prop3}";
    SplitCfSchema(schema, &cfs);
    EXPECT_EQ(cfs.size(), 3);

    schema = "cf1{prop1,prop2,prop3},cf2{prop1,prop2,prop3},cf3";
    SplitCfSchema(schema, &cfs);
    EXPECT_EQ(cfs.size(), 3);

    schema = "cf1,cf2{prop1,prop2,prop3},cf3{prop1,prop2,prop3}";
    SplitCfSchema(schema, &cfs);
    EXPECT_EQ(cfs.size(), 3);

    schema = "cf1{prop1,prop2,prop3},cf2{prop1,prop2,prop3},cf3{prop1,prop2,prop3}";
    SplitCfSchema(schema, &cfs);
    EXPECT_EQ(cfs.size(), 3);
}

TEST(SdkUtils, ParseProperty) {
    string schema;
    PropertyList prop_list;
    string name;

    schema = "name{prop1,prop2=value2,prop3=value3}";
    ASSERT_TRUE(ParseProperty(schema, &name, &prop_list));
    ASSERT_TRUE(name == "name");
    ASSERT_EQ(prop_list.size(), 3);
    ASSERT_TRUE(prop_list[0].first == "prop1");
    ASSERT_TRUE(prop_list[0].second == "");
    ASSERT_TRUE(prop_list[1].first == "prop2");
    ASSERT_TRUE(prop_list[1].second == "value2");
    ASSERT_TRUE(prop_list[2].first == "prop3");
    ASSERT_TRUE(prop_list[2].second == "value3");

    schema = "{prop1,prop2=value2}";
    ASSERT_TRUE(ParseProperty(schema, &name, &prop_list));
    ASSERT_TRUE(name == "");
    ASSERT_EQ(prop_list.size(), 2);
    ASSERT_TRUE(prop_list[0].first == "prop1");
    ASSERT_TRUE(prop_list[0].second == "");
    ASSERT_TRUE(prop_list[1].first == "prop2");
    ASSERT_TRUE(prop_list[1].second == "value2");

    schema = "name";
    ASSERT_TRUE(ParseProperty(schema, &name, &prop_list));
    ASSERT_TRUE(name == "name");
    ASSERT_EQ(prop_list.size(), 0);

    schema = "";
    ASSERT_TRUE(ParseProperty(schema, &name, &prop_list));
    ASSERT_TRUE(name == "");
    ASSERT_EQ(prop_list.size(), 0);

    schema = "nameprop1,prop2=value2,prop3=value3}";
    ASSERT_FALSE(ParseProperty(schema, &name, &prop_list));

    schema = "name{prop1,pr'op2=value2,prop3=value3}";
    ASSERT_FALSE(ParseProperty(schema, &name, &prop_list));

    schema = "name{0prop1,prop2=value2,prop3=value3}";
    ASSERT_FALSE(ParseProperty(schema, &name, &prop_list));
}

TEST(SdkUtils, ParseScanSchema) {
    ScanDescriptor desc("row1");
    ScanDescImpl* impl;
    string schema;

    schema = "SELECT cf0,cf1:qu2";
    ASSERT_TRUE(ParseScanSchema(schema, &desc));
    impl = desc.GetImpl();
    ASSERT_EQ(impl->GetSizeofColumnFamilyList(), 2);
    ASSERT_TRUE(impl->GetFilterString() == "");

    schema = "SELECT cf0,cf1:qu2 WHERE cf0 < 10 AND cf1 > 23";
    ASSERT_TRUE(ParseScanSchema(schema, &desc));
    impl = desc.GetImpl();
    ASSERT_EQ(impl->GetSizeofColumnFamilyList(), 2);
    ASSERT_TRUE(impl->GetFilterString() == "cf0 < 10 AND cf1 > 23");
}

TEST(SdkUtils, BuildSchema) {
    string schema = "lg0:cf1,cf2|lg3:cf3,cf4,cf5";

    TableDescriptor table_desc("unittest");
    ParseSchema(schema, &table_desc);

    string schema_t;
    BuildSchema(&table_desc, &schema_t);
    EXPECT_TRUE(schema == schema_t);
}

TEST(SdkUtils, HasInvalidCharInSchema) {
    EXPECT_FALSE(HasInvalidCharInSchema(""));
    EXPECT_FALSE(HasInvalidCharInSchema("table:splitsize=3,lg0:compress=none"));

    EXPECT_TRUE(HasInvalidCharInSchema("\n \t`~!@#$%^&*()-+{}[]\\|;\"'.<>?/"));
    EXPECT_TRUE(HasInvalidCharInSchema("table:splitsize=3;lg0:compress=none"));
}

TEST(SdkUtils, PrefixType) {
    EXPECT_TRUE(PrefixType("compress") == "lg");
    EXPECT_TRUE(PrefixType("storage") == "lg");
    EXPECT_TRUE(PrefixType("blocksize") == "lg");
    EXPECT_TRUE(PrefixType("ttl") == "cf");
    EXPECT_TRUE(PrefixType("maxversions") == "cf");
    EXPECT_TRUE(PrefixType("minversions") == "cf");
    EXPECT_TRUE(PrefixType("diskquota") == "cf");
    EXPECT_TRUE(PrefixType("splitsize") == "unknown"); // only support lg && cf
    EXPECT_TRUE(PrefixType("anythingother") == "unknown");
}

TEST(SdkUtils, ParsePrefixPropertyValue) {
    string prefix;
    string property;
    string value;
    EXPECT_TRUE(ParsePrefixPropertyValue("lg123:compress=none", prefix, property, value));

    EXPECT_FALSE(ParsePrefixPropertyValue(":ttl=3", prefix, property, value));
    EXPECT_FALSE(ParsePrefixPropertyValue("cf123:=3", prefix, property, value));
    EXPECT_FALSE(ParsePrefixPropertyValue("cf123:ttl=", prefix, property, value));
    EXPECT_FALSE(ParsePrefixPropertyValue("ttl", prefix, property, value));
    EXPECT_FALSE(ParsePrefixPropertyValue("cf123:ttl", prefix, property, value));
    EXPECT_FALSE(ParsePrefixPropertyValue("cf123:ttl:3", prefix, property, value));
}

} // namespace sdk
} // namespace tera
