// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include"leveldb/raw_key_operator.h"

#include <sys/time.h>
#include <iostream>

#include "util/testharness.h"

namespace leveldb {

void print_bytes(const char* str, int len) {
    for (int i = 0; i < len; ++i) {
        printf("%x ", str[i]);
    }
    printf("\n");
}

int64_t get_micros() {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return static_cast<int64_t>(tv.tv_sec) * 1000000 + tv.tv_usec;
}

class RawKeyOperatorTest {};

TEST(RawKeyOperatorTest, ReadableEncodeTeraKey) {
    const RawKeyOperator* key_operator = ReadableRawKeyOperator();
    std::string key("row_key");
    std::string column("column");
    std::string qualifier("qualifier");
    int64_t timestamp = 0x0001020304050607;

    std::string tera_key1;
    std::string tera_key2;

    key_operator->EncodeTeraKey(key, column, qualifier, timestamp,
                                TKT_VALUE, &tera_key1);
    key_operator->EncodeTeraKey(key, column, qualifier, timestamp,
                                TKT_DEL, &tera_key2);

    size_t len = key.size() + column.size() + qualifier.size() + sizeof(timestamp) + 3;
    ASSERT_EQ(tera_key1.size(), len);

    std::string raw1("row_key\0column\0qualifier\0\xFE\xFD\xFC\xFB\xFA\xF9\xF8\x05", len);
    ASSERT_TRUE(tera_key1 == raw1);
//    print_bytes(tera_key1.data(), tera_key1.size());
//    print_bytes(raw1.data(), raw1.size());

    std::string raw2("row_key\0column\0qualifier\0\xFE\xFD\xFC\xFB\xFA\xF9\xF8\x01", len);
    ASSERT_TRUE(tera_key2 == raw2);

    ASSERT_TRUE(tera_key1.compare(tera_key2) > 0);
}

TEST(RawKeyOperatorTest, ReadableExtractTeraKey) {
    const RawKeyOperator* key_operator = ReadableRawKeyOperator();
    std::string tera_key1;
    std::string row_key1 = "row";
    std::string column1 = "column";
    std::string qualifier1 = "qualifier";
    int64_t timestamp1 = time(NULL);
    key_operator->EncodeTeraKey(row_key1, column1,qualifier1,
                              timestamp1, TKT_VALUE, &tera_key1);

    Slice row_key2;
    Slice column2;
    Slice qualifier2;
    int64_t timestamp2;
    TeraKeyType type2;
    ASSERT_TRUE(key_operator->ExtractTeraKey(tera_key1, &row_key2, &column2,
                                           &qualifier2, &timestamp2, &type2));

    ASSERT_EQ(row_key1, row_key2.ToString());
    ASSERT_EQ(column1, column2.ToString());
    ASSERT_EQ(qualifier1, qualifier2.ToString());
    ASSERT_EQ(timestamp1, timestamp2);
    ASSERT_EQ(type2, TKT_VALUE);
}

TEST(RawKeyOperatorTest, BinaryEncodeTeraKey) {
    const RawKeyOperator* key_operator = BinaryRawKeyOperator();
    std::string key("row_key");
    std::string column("column");
    std::string qualifier("qualifier");
    int64_t timestamp = 0x01020304050607;

    std::string tera_key1;
    std::string tera_key2;

    key_operator->EncodeTeraKey(key, column, qualifier, timestamp,
                                TKT_VALUE, &tera_key1);
    key_operator->EncodeTeraKey(key, column, qualifier, timestamp,
                                TKT_DEL, &tera_key2);

    size_t len = key.size() + column.size() + qualifier.size() + sizeof(timestamp) + 5;
    ASSERT_EQ(tera_key1.size(), len);

    std::string raw1("row_keycolumn\0qualifier\xFE\xFD\xFC\xFB\xFA\xF9\xF8\x5\x0\x7\x0\x9", len);
    ASSERT_TRUE(tera_key1 == raw1);
//    print_bytes(tera_key1.data(), tera_key1.size());
//    print_bytes(raw1.data(), raw1.size());

    std::string raw2("row_keycolumn\0qualifier\xFE\xFD\xFC\xFB\xFA\xF9\xF8\x01\x0\x7\x0\x9", len);
    ASSERT_TRUE(tera_key2 == raw2);

    ASSERT_TRUE(tera_key1.compare(tera_key2) > 0);

}

TEST(RawKeyOperatorTest, BinaryExtractTeraKey) {
    const RawKeyOperator* key_operator = BinaryRawKeyOperator();
    std::string tera_key1;
    std::string row_key1 = "row";
    std::string column1 = "column";
    std::string qualifier1 = "qualifier";
    key_operator->EncodeTeraKey(row_key1, column1,qualifier1,
                              0, TKT_VALUE, &tera_key1);

    Slice row_key2;
    Slice column2;
    Slice qualifier2;
    int64_t timestamp2;
    TeraKeyType type2;
    ASSERT_TRUE(key_operator->ExtractTeraKey(tera_key1, &row_key2, &column2,
                                           &qualifier2, &timestamp2, &type2));

    ASSERT_EQ(row_key1, row_key2.ToString());
    ASSERT_EQ(column1, column2.ToString());
    ASSERT_EQ(qualifier1, qualifier2.ToString());
    ASSERT_EQ(timestamp2, 0);
    ASSERT_EQ(type2, TKT_VALUE);
}

void GenTestString(int64_t len, std::string* output) {
    for (int i = 0; i < len; ++i) {
        output->append("a");
    }
}

TEST(RawKeyOperatorTest, TestBigRow) {
    const RawKeyOperator* key_operator = BinaryRawKeyOperator();
    std::string test_str_60K;
    GenTestString(60000, &test_str_60K);

    std::string tera_key1;
    std::string row_key1 = test_str_60K;
    std::string column1 = test_str_60K;
    std::string qualifier1 = test_str_60K;
    key_operator->EncodeTeraKey(row_key1, column1,qualifier1,
                                0, TKT_VALUE, &tera_key1);
    ASSERT_EQ(tera_key1.size(), 180013u);

    Slice row_key2;
    Slice column2;
    Slice qualifier2;
    int64_t timestamp2;
    TeraKeyType type2;
    ASSERT_TRUE(key_operator->ExtractTeraKey(tera_key1, &row_key2, &column2,
                                             &qualifier2, &timestamp2, &type2));

    ASSERT_EQ(row_key1, row_key2.ToString());
    ASSERT_EQ(column1, column2.ToString());
    ASSERT_EQ(qualifier1, qualifier2.ToString());
    ASSERT_EQ(timestamp2, 0);
    ASSERT_EQ(type2, TKT_VALUE);
}

TEST(RawKeyOperatorTest, Compare) {
    const RawKeyOperator* key_operator = BinaryRawKeyOperator();
    std::string tera_key1, tera_key2;
    std::string key1, key2;
    std::string column1, column2;
    std::string qualifier1, qualifier2;
    int64_t ts1, ts2;
    TeraKeyType type1, type2;

    key1 = "row";
    column1 = "column";
    qualifier1 = "qualifier";
    ts1 = 0;
    type1 = TKT_VALUE;
    key_operator->EncodeTeraKey(key1, column1, qualifier1, ts1, type1, &tera_key1);

    key2 = "row";
    column2 = "column";
    qualifier2 = "qualifier";
    ts2 = 0;
    type2 = TKT_VALUE;
    key_operator->EncodeTeraKey(key2, column2, qualifier2, ts2, type2, &tera_key2);
    ASSERT_EQ(key_operator->Compare(tera_key1, tera_key2), 0);

    key2 = "row1";
    column2 = "column";
    qualifier2 = "qualifier";
    ts2 = 0;
    type2 = TKT_VALUE;
    key_operator->EncodeTeraKey(key2, column2, qualifier2, ts2, type2, &tera_key2);
    ASSERT_LT(key_operator->Compare(tera_key1, tera_key2), 0);

    key2 = "ro";
    column2 = "column";
    qualifier2 = "qualifier";
    ts2 = 0;
    type2 = TKT_VALUE;
    key_operator->EncodeTeraKey(key2, column2, qualifier2, ts2, type2, &tera_key2);
    ASSERT_GT(key_operator->Compare(tera_key1, tera_key2), 0);

    key2 = "row";
    column2 = "columny";
    qualifier2 = "qualifier";
    ts2 = 0;
    type2 = TKT_VALUE;
    key_operator->EncodeTeraKey(key2, column2, qualifier2, ts2, type2, &tera_key2);
    ASSERT_LT(key_operator->Compare(tera_key1, tera_key2), 0);

    key2 = "row";
    column2 = "column";
    qualifier2 = "qualifierr";
    ts2 = 0;
    type2 = TKT_VALUE;
    key_operator->EncodeTeraKey(key2, column2, qualifier2, ts2, type2, &tera_key2);
    ASSERT_LT(key_operator->Compare(tera_key1, tera_key2), 0);

    key2 = "row";
    column2 = "column";
    qualifier2 = "qualifier";
    ts2 = 1;
    type2 = TKT_VALUE;
    key_operator->EncodeTeraKey(key2, column2, qualifier2, ts2, type2, &tera_key2);
    ASSERT_GT(key_operator->Compare(tera_key1, tera_key2), 0);

    key2 = "row";
    column2 = "column";
    qualifier2 = "qualifier";
    ts2 = 0;
    type2 = TKT_DEL;
    key_operator->EncodeTeraKey(key2, column2, qualifier2, ts2, type2, &tera_key2);
    ASSERT_GT(key_operator->Compare(tera_key1, tera_key2), 0);

    //
    type1 = TKT_DEL_COLUMN;
    key_operator->EncodeTeraKey(key1, column1, qualifier1, ts1, type1, &tera_key1);

    key2 = "row";
    column2 = "column";
    qualifier2 = "qualifier";
    ts2 = 0;
    type2 = TKT_VALUE;
    key_operator->EncodeTeraKey(key2, column2, qualifier2, ts2, type2, &tera_key2);
    ASSERT_LT(key_operator->Compare(tera_key1, tera_key2), 0);

    key2 = "row";
    column2 = "column";
    qualifier2 = "qualifier";
    ts2 = 1;
    type2 = TKT_VALUE;
    key_operator->EncodeTeraKey(key2, column2, qualifier2, ts2, type2, &tera_key2);
    ASSERT_GT(key_operator->Compare(tera_key1, tera_key2), 0);
}

void EncodeTeraKeyPerformanceTest(const RawKeyOperator* key_operator,
                                   const std::string& row,
                                   const std::string& col,
                                   const std::string& qual,
                                   int64_t ts,
                                   TeraKeyType type,
                                   const std::string& desc) {
    std::string tera_key;
    int64_t start = get_micros();
    for (int i = 0; i < 10000000; ++i) {
        key_operator->EncodeTeraKey(row, col, qual, ts, type, &tera_key);
    }
    int64_t end = get_micros();
    std::cout << "[Encode TeraKey Performance ("
        << desc << ")] cost: " << (end - start) / 1000 << "ms\n";
}

TEST(RawKeyOperatorTest, EncodeTeraKeyPerformace) {
    const RawKeyOperator* keyop_bin = BinaryRawKeyOperator();
    std::string tera_key, row, col, qual;
    int64_t ts;
    TeraKeyType type;
    row = "row";
    col = "col";
    qual = "qual";
    ts = 123456789;
    type = TKT_VALUE;

    EncodeTeraKeyPerformanceTest(keyop_bin, row, col, qual, ts, type, "binary short");

    row = "rowrowrowrowrowrowrowrowrowrowrowrowrowrowrowrowrowrowrowrow";
    col = "colcolcolcolcolcolcolcolcolcolcolcolcolcolcolcolcolcolcolcolcol";
    qual = "qualqualqualqualqualqualqualqualqualqualqualqualqualqualqualqual";
    EncodeTeraKeyPerformanceTest(keyop_bin, row, col, qual, ts, type, "binary long");
    EncodeTeraKeyPerformanceTest(keyop_bin, row, col, qual, ts, type, "binary long qualnull");
}

void ExtractTeraKeyPerformanceTest(const RawKeyOperator* key_operator,
                                   const std::string& key,
                                   const std::string& desc) {
    Slice row, col, qual;
    int64_t ts;
    TeraKeyType type;
    int64_t start = get_micros();
    for (int i = 0; i < 10000000; ++i) {
        key_operator->ExtractTeraKey(key, &row, &col, &qual, &ts, &type);
    }
    int64_t end = get_micros();
    std::cout << "[Extract TeraKey Performance ("
        << desc << ")] cost: " << (end - start) / 1000 << "ms\n";
}

TEST(RawKeyOperatorTest, ExtractTeraKeyPerformace) {
    const RawKeyOperator* keyop_bin = BinaryRawKeyOperator();
    std::string tera_key, row, col, qual;
    row = "row";
    col = "col";
    qual = "qual";
    keyop_bin->EncodeTeraKey(row, col, qual, 0, TKT_VALUE, &tera_key);
    ExtractTeraKeyPerformanceTest(keyop_bin, tera_key, "binary short");

    row = "rowrowrowrowrowrowrowrowrowrowrowrowrowrowrowrowrowrowrowrow";
    col = "colcolcolcolcolcolcolcolcolcolcolcolcolcolcolcolcolcolcolcolcol";
    qual = "qualqualqualqualqualqualqualqualqualqualqualqualqualqualqualqual";
    keyop_bin->EncodeTeraKey(row, col, qual, 0, TKT_VALUE, &tera_key);
    ExtractTeraKeyPerformanceTest(keyop_bin, tera_key, "binary long");

    keyop_bin->EncodeTeraKey(row, col, "", 0, TKT_VALUE, &tera_key);
    ExtractTeraKeyPerformanceTest(keyop_bin, tera_key, "binary long qualnull");
}

void ComparePerformanceTest(const RawKeyOperator* key_operator,
                     const std::string& key1,
                     const std::string& key2,
                     const std::string& desc) {
    int64_t start = get_micros();
    for (int i = 0; i < 10000000; ++i) {
        key_operator->Compare(key1, key2);
    }
    int64_t end = get_micros();
    std::cout << "[Compare Performance ("
        << desc << ")] cost: " << (end - start) / 1000 << "ms\n";
}

TEST(RawKeyOperatorTest, ComparePerformace) {
    const RawKeyOperator* keyop_bin = BinaryRawKeyOperator();
    const RawKeyOperator* keyop_read = ReadableRawKeyOperator();
    std::string tera_key1, tera_key2;
    std::string key1, key2;
    std::string column1, column2;
    std::string qualifier1, qualifier2;
    int64_t ts1, ts2;
    TeraKeyType type1;

    key1 = "rowrowrowrowrowrowrowrowrowrowrowrowrowrowrowrow";
    column1 = "columncolumncolumncolumn";
    qualifier1 = "qualifierqualifierqualifier";
    ts1 = 123456789;
    type1 = TKT_VALUE;
    key2 = "row";
    column2 = "column";
    qualifier2 = "qualifier";
    ts2 = 987654321;

    keyop_bin->EncodeTeraKey(key1, column1, qualifier1, ts1, type1, &tera_key1);
    keyop_bin->EncodeTeraKey(key2, column2, qualifier2, ts2, type1, &tera_key2);
    ComparePerformanceTest(keyop_bin, tera_key1, tera_key2, "binary long same none");

    keyop_bin->EncodeTeraKey(key1, column1, qualifier1, ts1, type1, &tera_key1);
    keyop_bin->EncodeTeraKey(key1, column2, qualifier2, ts2, type1, &tera_key2);
    ComparePerformanceTest(keyop_bin, tera_key1, tera_key2, "binary long same row");

    keyop_bin->EncodeTeraKey(key1, column1, qualifier1, ts1, type1, &tera_key1);
    keyop_bin->EncodeTeraKey(key1, column1, qualifier2, ts2, type1, &tera_key2);
    ComparePerformanceTest(keyop_bin, tera_key1, tera_key2, "binary long same row/col");

    keyop_bin->EncodeTeraKey(key1, column1, qualifier1, ts1, type1, &tera_key1);
    keyop_bin->EncodeTeraKey(key1, column1, qualifier1, ts2, type1, &tera_key2);
    ComparePerformanceTest(keyop_bin, tera_key1, tera_key2, "binary long same row/col/qu");

    keyop_bin->EncodeTeraKey(key1, column1, "", ts1, type1, &tera_key1);
    keyop_bin->EncodeTeraKey(key1, column1, "", ts2, type1, &tera_key2);
    ComparePerformanceTest(keyop_bin, tera_key1, tera_key2, "binary long same row/col/null");

    keyop_bin->EncodeTeraKey(key1, column1, qualifier1, ts1, type1, &tera_key1);
    keyop_bin->EncodeTeraKey(key1, column1, qualifier1, ts1, type1, &tera_key2);
    ComparePerformanceTest(keyop_bin, tera_key1, tera_key2, "binary long same all");

    keyop_bin->EncodeTeraKey(key2, column2, qualifier2, ts1, type1, &tera_key1);
    keyop_bin->EncodeTeraKey(key2, column2, qualifier2, ts1, type1, &tera_key2);
    ComparePerformanceTest(keyop_bin, tera_key1, tera_key2, "binary short");

    keyop_read->EncodeTeraKey(key1, column1, qualifier1, ts1, type1, &tera_key1);
    keyop_read->EncodeTeraKey(key2, column2, qualifier2, ts2, type1, &tera_key2);
    ComparePerformanceTest(keyop_read, tera_key1, tera_key2, "readable long same none");

    keyop_read->EncodeTeraKey(key1, column1, qualifier1, ts1, type1, &tera_key1);
    keyop_read->EncodeTeraKey(key1, column2, qualifier2, ts2, type1, &tera_key2);
    ComparePerformanceTest(keyop_read, tera_key1, tera_key2, "readable long same row");

    keyop_read->EncodeTeraKey(key1, column1, qualifier1, ts1, type1, &tera_key1);
    keyop_read->EncodeTeraKey(key1, column1, qualifier2, ts2, type1, &tera_key2);
    ComparePerformanceTest(keyop_read, tera_key1, tera_key2, "readable long same row/col");

    keyop_read->EncodeTeraKey(key1, column1, qualifier1, ts1, type1, &tera_key1);
    keyop_read->EncodeTeraKey(key1, column1, qualifier1, ts2, type1, &tera_key2);
    ComparePerformanceTest(keyop_read, tera_key1, tera_key2, "readable long same row/col/qu");

    keyop_read->EncodeTeraKey(key1, column1, qualifier1, ts1, type1, &tera_key1);
    keyop_read->EncodeTeraKey(key1, column1, qualifier1, ts1, type1, &tera_key2);
    ComparePerformanceTest(keyop_read, tera_key1, tera_key2, "readable long same all");

    keyop_read->EncodeTeraKey(key2, column2, qualifier2, ts1, type1, &tera_key1);
    keyop_read->EncodeTeraKey(key2, column2, qualifier2, ts1, type1, &tera_key2);
    ComparePerformanceTest(keyop_read, tera_key1, tera_key2, "readable short");
}
}  // namespace leveldb

int main(int argc, char* argv[]) {
    return leveldb::test::RunAllTests();
}
