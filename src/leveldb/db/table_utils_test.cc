// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "leveldb/table_utils.h"

#include "util/testharness.h"

namespace leveldb {

class TableUtilsTest {};

TEST(TableUtilsTest, GetSplitGapNumStringGeneral) {
    std::string cur_path = "tabletnode/ABCD/tablet0000/0/000065.sst";
    std::string real_path = "tabletnode/ABCD/tablet000/0/000065.sst";

    std::string id_str = GetSplitGapNumString(cur_path, real_path);
    ASSERT_EQ(id_str, "0");

    cur_path = "tabletnode/ABCD/tablet0001/0/000065.sst";
    real_path = "tabletnode/ABCD/tablet000/0/000065.sst";

    id_str = GetSplitGapNumString(cur_path, real_path);
    ASSERT_EQ(id_str, "1");
}

TEST(TableUtilsTest, GetSplitGapNumStringLogFile) {
    std::string cur_path = "tabletnode/ABCD/tablet0000/H000000000000321b.log";
    std::string real_path = "tabletnode/ABCD/tablet000/H000000000000321b.log";

    std::string id_str = GetSplitGapNumString(cur_path, real_path);
    ASSERT_EQ(id_str, "0");

    cur_path = "tabletnode/ABCD/tablet0001/H000000000000321b.log";
    real_path = "tabletnode/ABCD/tablet000/H000000000000321b.log";

    id_str = GetSplitGapNumString(cur_path, real_path);
    ASSERT_EQ(id_str, "1");
}

}

int main(int argc, char** argv) {
  return leveldb::test::RunAllTests();
}
