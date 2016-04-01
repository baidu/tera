// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include"leveldb/tera_key.h"
#include"leveldb/raw_key_operator.h"

#include <sys/time.h>
#include <iostream>

#include "util/testharness.h"

namespace leveldb {

class TeraKeyTest {};

void TestFunc(const RawKeyOperator* op) {
    TeraKey tk(op);
    ASSERT_TRUE(tk.empty());
    std::string key("row_key");
    std::string column("column");
    std::string qualifier("qualifier");
    int64_t timestamp = 0x0001020304050607;
    TeraKeyType type = TKT_VALUE;

    ASSERT_TRUE(tk.Encode(key, column, qualifier, timestamp, type));
    ASSERT_TRUE(!tk.empty());
    ASSERT_EQ(tk.key().ToString(), key);
    ASSERT_EQ(tk.column().ToString(), column);
    ASSERT_EQ(tk.qualifier().ToString(), qualifier);
    ASSERT_EQ(tk.timestamp(), timestamp);
    ASSERT_EQ(tk.type(), type);
    std::cout << tk.DebugString() << std::endl;

    std::string tera_key = tk.raw_key().ToString();
    TeraKey tk2(op);
    ASSERT_TRUE(tk2.Decode(tera_key));
    ASSERT_EQ(tk2.Compare(tk), 0);
    ASSERT_TRUE(!tk2.empty());
    ASSERT_EQ(tk2.key().ToString(), key);
    ASSERT_EQ(tk2.column().ToString(), column);
    ASSERT_EQ(tk2.qualifier().ToString(), qualifier);
    ASSERT_EQ(tk2.timestamp(), timestamp);
    ASSERT_EQ(tk2.type(), type);
    std::cout << tk2.DebugString() << std::endl;

    ASSERT_TRUE(tk.SameRow(tk2));
    ASSERT_TRUE(tk.SameColumn(tk2));
    ASSERT_TRUE(tk.SameQualifier(tk2));

    ASSERT_TRUE(tk2.Encode("haha", column, qualifier, 0, TKT_VALUE));
    ASSERT_LT(tk2.Compare(tk), 0);
    ASSERT_TRUE(!tk.SameRow(tk2));
    ASSERT_TRUE(!tk.SameColumn(tk2));
    ASSERT_TRUE(!tk.SameQualifier(tk2));
    std::cout << tk2.DebugString() << std::endl;

    ASSERT_TRUE(tk2.Encode(key, "hello", "world", 0, TKT_VALUE));
    ASSERT_GT(tk2.Compare(tk), 0);
    ASSERT_TRUE(tk.SameRow(tk2));
    ASSERT_TRUE(!tk.SameColumn(tk2));
    ASSERT_TRUE(!tk.SameQualifier(tk2));
    std::cout << tk2.DebugString() << std::endl;

    ASSERT_TRUE(tk2.Encode(key, column, "world", 0, TKT_VALUE));
    ASSERT_GT(tk2.Compare(tk), 0);
    ASSERT_TRUE(tk.SameRow(tk2));
    ASSERT_TRUE(tk.SameColumn(tk2));
    ASSERT_TRUE(!tk.SameQualifier(tk2));
    std::cout << tk2.DebugString() << std::endl;

    TeraKey tk3(tk);
    ASSERT_TRUE(tk.Encode("haha", "hello", "world", 0, TKT_VALUE));
    ASSERT_GT(tk3.Compare(tk), 0);
    ASSERT_TRUE(!tk3.empty());
    ASSERT_EQ(tk3.key().ToString(), key);
    ASSERT_EQ(tk3.column().ToString(), column);
    ASSERT_EQ(tk3.qualifier().ToString(), qualifier);
    ASSERT_EQ(tk3.timestamp(), timestamp);
    ASSERT_EQ(tk3.type(), type);
    std::cout << tk3.DebugString() << std::endl;
}

TEST(TeraKeyTest, Readable) {
    TestFunc(ReadableRawKeyOperator());
    TestFunc(BinaryRawKeyOperator());
}
}  // namespace leveldb

int main(int argc, char* argv[]) {
    return leveldb::test::RunAllTests();
}
