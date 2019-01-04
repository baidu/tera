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
}  // namespace tera
