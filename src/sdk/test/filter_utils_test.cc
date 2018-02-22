// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "filter_utils.h"

#include "gtest/gtest.h"

namespace tera {

void PrintBytes(const std::string& c, int n) {
    fprintf(stderr, "-------------------");
    for (int i = 0; i < n; ++i) {
        fprintf(stderr, "%2x ", (unsigned char)c[i]);
    }
    fprintf(stderr, "\n");
}

TEST(FilterUtils, RemoveInvisibleChar) {
    string schema = "";
    schema = RemoveInvisibleChar(schema);
    EXPECT_TRUE(schema == "");

    schema = " ";
    schema = RemoveInvisibleChar(schema);
    EXPECT_TRUE(schema == "");

    schema = "a ";
    schema = RemoveInvisibleChar(schema);
    EXPECT_TRUE(schema == "a");

    schema = "a\n \t ";
    schema = RemoveInvisibleChar(schema);
    EXPECT_TRUE(schema == "a");
}

TEST(FilterUtils, DefaultValueConverter) {
    string in, type, out, out_p;

    EXPECT_FALSE(DefaultValueConverter("", "", NULL));

    in = "8";
    out_p = string("\x08\x0\x0\x0\x0\x0\x0\x0", 8);
    type = "int64";

    EXPECT_TRUE(DefaultValueConverter(in, type, &out));
    EXPECT_EQ(out, out_p);

    in = "-8";
    out_p = string("\xF8\xFF\xFF\xFF\xFF\xFF\xFF\xFF", 8);
    type = "int64";
    EXPECT_TRUE(DefaultValueConverter(in, type, &out));
    EXPECT_EQ(out, out_p);

    in = "-8";
    type = "string";
    EXPECT_FALSE(DefaultValueConverter(in, type, &out));

    type = "illegal";
    EXPECT_FALSE(DefaultValueConverter(in, type, &out));
}
} // namespace tera
