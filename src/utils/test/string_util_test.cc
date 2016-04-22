// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "utils/string_util.h"

#include <gtest/gtest.h>

namespace tera {

TEST(StringUtilTest, IsValidName) {
    ASSERT_FALSE(IsValidName(""));
    ASSERT_FALSE(IsValidName(std::string("\0", 1)));
    ASSERT_FALSE(IsValidName("\1"));

    ASSERT_FALSE(IsValidName(std::string(kNameLenMin - 1, 'a')));
    ASSERT_TRUE(IsValidName(std::string(kNameLenMin, 'a')));
    ASSERT_TRUE(IsValidName(std::string(kNameLenMin + 1, 'a')));

    ASSERT_TRUE(IsValidName(std::string(kNameLenMax - 1, 'a')));
    ASSERT_TRUE(IsValidName(std::string(kNameLenMax, 'a')));
    ASSERT_FALSE(IsValidName(std::string(kNameLenMax + 1, 'a')));

    ASSERT_FALSE(IsValidName("1abc"));
    ASSERT_FALSE(IsValidName("_1abc"));

    ASSERT_TRUE(IsValidName("a"));
    ASSERT_TRUE(IsValidName("A"));
    ASSERT_TRUE(IsValidName("abcDEFGz123_233000_"));

    ASSERT_FALSE(IsValidName("abcDEFGz123_233\1bac"));
    ASSERT_FALSE(IsValidName("a~`!@#$%^&*()_=+"));
    ASSERT_FALSE(IsValidName("a[{;:'\",<>/?\"'}]"));
}

TEST(StringUtilTest, IsValidCfName) {
    ASSERT_TRUE(IsValidColumnFamilyName(""));
    ASSERT_TRUE(IsValidColumnFamilyName(std::string(64 * 1024 - 1, 'a')));
    ASSERT_FALSE(IsValidColumnFamilyName(std::string(64 * 1024, 'a')));

    ASSERT_TRUE(IsValidColumnFamilyName("1"));
    ASSERT_TRUE(IsValidColumnFamilyName("cf0"));
    ASSERT_TRUE(IsValidColumnFamilyName("_1234567890-abcdefghijklmnopqrstuvwxyz:."));

    ASSERT_FALSE(IsValidColumnFamilyName("cf0\1"));
    ASSERT_FALSE(IsValidColumnFamilyName("cf0\2"));
}

TEST(StringUtilTest, RoundNumberToNDecimalPlaces) {
    ASSERT_EQ(RoundNumberToNDecimalPlaces(33, -1), "(null)");
    ASSERT_EQ(RoundNumberToNDecimalPlaces(33, 10), "(null)");

    ASSERT_EQ(RoundNumberToNDecimalPlaces(33, 0), "33");
    ASSERT_EQ(RoundNumberToNDecimalPlaces(33, 1), "33.0");
    ASSERT_EQ(RoundNumberToNDecimalPlaces(33, 2), "33.00");
    ASSERT_EQ(RoundNumberToNDecimalPlaces(33, 9), "33.000000000");

    ASSERT_EQ(RoundNumberToNDecimalPlaces(123456789.987654321, 0), "123456790");
    ASSERT_EQ(RoundNumberToNDecimalPlaces(123456789.987654321, 1), "123456790.0");
    ASSERT_EQ(RoundNumberToNDecimalPlaces(123456789.987654321, 2), "123456789.99");
    ASSERT_EQ(RoundNumberToNDecimalPlaces(123456789.987654321, 6), "123456789.987654");

    ASSERT_EQ(RoundNumberToNDecimalPlaces(0, 6), "0.000000");
    ASSERT_EQ(RoundNumberToNDecimalPlaces(0.1, 6), "0.100000");
    ASSERT_EQ(RoundNumberToNDecimalPlaces(0.01, 6), "0.010000");
    ASSERT_EQ(RoundNumberToNDecimalPlaces(0.000012345678, 6), "0.000012");
}

TEST(EditDistance, AllCase) {
    ASSERT_EQ(EditDistance("", ""), 0);
    ASSERT_EQ(EditDistance("", "a"), 1);
    ASSERT_EQ(EditDistance("a", ""), 1);
    ASSERT_EQ(EditDistance("ab", ""), 2);
    ASSERT_EQ(EditDistance("", "ab"), 2);

    ASSERT_EQ(EditDistance("a", "a"), 0);
    ASSERT_EQ(EditDistance("a", "b"), 1);

    ASSERT_EQ(EditDistance("ax", "axy"), 1); // insertion
    ASSERT_EQ(EditDistance("ax", "a"), 1);   // removal
    ASSERT_EQ(EditDistance("ax", "ay"), 1);  // substitution

    ASSERT_EQ(EditDistance("showschema", "show_schema"), 1);
    ASSERT_EQ(EditDistance("showschema", "showscheama"), 1);
    ASSERT_EQ(EditDistance("branch", "branc"), 1);
    ASSERT_EQ(EditDistance("update", "udpate"), 2);

    ASSERT_EQ(EditDistance("aaa", "bbb"), 3);
    ASSERT_EQ(EditDistance("aaa", "baa"), 1);
    ASSERT_EQ(EditDistance("abb", "acc"), 2);
    ASSERT_EQ(EditDistance("abc", "op"), 3);
    ASSERT_EQ(EditDistance("abc", "rstuvw"), 6);
}

}

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
