// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/filename.h"

#include "db/dbformat.h"
#include "port/port.h"
#include "util/logging.h"
#include "util/testharness.h"

namespace leveldb {

class FileNameTest { };

TEST(FileNameTest, Parse) {
  Slice db;
  FileType type;
  uint64_t number;

  // Successful parses
  static struct {
    const char* fname;
    uint64_t number;
    FileType type;
  } cases[] = {
    { "100.log",            100,   kLogFile },
    { "0.log",              0,     kLogFile },
    { "0.sst",              0,     kTableFile },
    { "CURRENT",            0,     kCurrentFile },
    { "LOCK",               0,     kDBLockFile },
    { "MANIFEST-2",         2,     kDescriptorFile },
    { "MANIFEST-7",         7,     kDescriptorFile },
    { "LOG",                0,     kInfoLogFile },
    { "LOG.old",            0,     kInfoLogFile },
    { "18446744073709551615.log", 18446744073709551615ull, kLogFile },
  };
  for (uint32_t i = 0; i < sizeof(cases) / sizeof(cases[0]); i++) {
    std::string f = cases[i].fname;
    ASSERT_TRUE(ParseFileName(f, &number, &type)) << f;
    ASSERT_EQ(cases[i].type, type) << f;
    ASSERT_EQ(cases[i].number, number) << f;
  }

  // Errors
  static const char* errors[] = {
    "",
    "foo",
    "foo-dx-100.log",
    ".log",
    "",
    "manifest",
    "CURREN",
    "CURRENTX",
    "MANIFES",
    "MANIFEST",
    "MANIFEST-",
    "XMANIFEST-3",
    "MANIFEST-3x",
    "LOC",
    "LOCKx",
    "LO",
    "LOGx",
    "18446744073709551616.log",
    "184467440737095516150.log",
    "100",
    "100.",
    "100.lop"
  };
  for (uint32_t i = 0; i < sizeof(errors) / sizeof(errors[0]); i++) {
    std::string f = errors[i];
    ASSERT_TRUE(!ParseFileName(f, &number, &type)) << f;
  }
}

TEST(FileNameTest, Construction) {
  uint64_t number;
  FileType type;
  std::string fname;

  fname = CurrentFileName("foo");
  ASSERT_EQ("foo/", std::string(fname.data(), 4));
  ASSERT_TRUE(ParseFileName(fname.c_str() + 4, &number, &type));
  ASSERT_EQ(0UL, number);
  ASSERT_EQ(kCurrentFile, type);

  fname = LockFileName("foo");
  ASSERT_EQ("foo/", std::string(fname.data(), 4));
  ASSERT_TRUE(ParseFileName(fname.c_str() + 4, &number, &type));
  ASSERT_EQ(0UL, number);
  ASSERT_EQ(kDBLockFile, type);

  fname = LogFileName("foo", 192);
  ASSERT_EQ("foo/", std::string(fname.data(), 4));
  ASSERT_TRUE(ParseFileName(fname.c_str() + 4, &number, &type));
  ASSERT_EQ(192UL, number);
  ASSERT_EQ(kLogFile, type);

  fname = TableFileName("bar/table/0", 200);
  ASSERT_EQ("bar/table/0", std::string(fname.data(), 11));
  ASSERT_TRUE(ParseFileName(fname.c_str() + 12, &number, &type));
  ASSERT_EQ(200UL, number);
  ASSERT_EQ(kTableFile, type);

  fname = TableFileName("bar/tablet00000030/0", 0x8000000100000200);
  ASSERT_EQ("bar/tablet00000001/0", std::string(fname.data(), 20));
  ASSERT_TRUE(ParseFileName(fname.c_str() + 21, &number, &type));
  ASSERT_EQ(0x200UL, number);
  ASSERT_EQ(kTableFile, type);

  fname = DescriptorFileName("bar", 100);
  ASSERT_EQ("bar/", std::string(fname.data(), 4));
  ASSERT_TRUE(ParseFileName(fname.c_str() + 4, &number, &type));
  ASSERT_EQ(100UL, number);
  ASSERT_EQ(kDescriptorFile, type);

  fname = TempFileName("tmp", 999);
  ASSERT_EQ("tmp/", std::string(fname.data(), 4));
  ASSERT_TRUE(ParseFileName(fname.c_str() + 4, &number, &type));
  ASSERT_EQ(999UL, number);
  ASSERT_EQ(kTempFile, type);
}

TEST(FileNameTest, BuildFullFileNumber) {
  std::string dbname;
  uint64_t number;

  dbname = "/hello/meta/0";
  number = 0;
  ASSERT_EQ(BuildFullFileNumber(dbname, number), 0UL);
  number = 255;
  ASSERT_EQ(BuildFullFileNumber(dbname, number), 0xFFUL);

  dbname = "/hello/meta/tablet00000000/0";
  number = 0;
  ASSERT_EQ(BuildFullFileNumber(dbname, number), 0x8000000000000000);

  // 1234567=0x12d687, 89=0x59, 987654321=0x3ADE68B1
  dbname = "/hello/meta/tablet001234567/89";
  number = 987654321;
  ASSERT_EQ(BuildFullFileNumber(dbname, number), 0x8012d6873ADE68B1);
}

TEST(FileNameTest, ParseFullFileNumber) {
  uint64_t number, tablet, file;

  number = 0x8000000300000001;
  ASSERT_TRUE(ParseFullFileNumber(number, &tablet, &file));
  ASSERT_EQ(tablet, 3UL);
  ASSERT_EQ(file, 1UL);

  number = 0x3;
  ASSERT_TRUE(ParseFullFileNumber(number, &tablet, &file));
  ASSERT_EQ(tablet, 0UL);
  ASSERT_EQ(file, 3UL);

  number = 0x3;
  ASSERT_TRUE(ParseFullFileNumber(number, NULL, &file));
  ASSERT_EQ(file, 3UL);
}

TEST(FileNameTest, BuildTableFilePath) {
  std::string prefix;
  uint64_t lg, number;

  prefix = "/hello";
  lg = 1;
  number = 0x8000000300000001;
  ASSERT_EQ("/hello/tablet00000003/1/00000001.sst", BuildTableFilePath(prefix, lg, number));
}

TEST(FileNameTest, RealDbName) {
  std::string dbname;
  uint64_t tablet;

  dbname = "/hello/tablet00000001/0";
  tablet = 2;
  ASSERT_EQ("/hello/tablet00000002/0", RealDbName(dbname, tablet));

  dbname = "/hello/tablet00000001/0";
  tablet = 123456;
  ASSERT_EQ("/hello/tablet00123456/0", RealDbName(dbname, tablet));

  dbname = "/hello/meta/0";
  tablet = 0;
  ASSERT_EQ(dbname, RealDbName(dbname, tablet));
}

TEST(FileNameTest, ParseDbName) {
  std::string dbname, prefix;
  uint64_t tablet, lg;

  dbname = "/hello/meta/0";
  ASSERT_EQ(false, ParseDbName(dbname, &prefix, &tablet, &lg));
  ASSERT_EQ(prefix, "/hello/meta");
  ASSERT_EQ(lg, 0UL);
  ASSERT_EQ(false, ParseDbName(dbname, &prefix, NULL, NULL));
  ASSERT_EQ(prefix, "/hello/meta");

  dbname = "/hello/t1/tablet00000001/0";
  ASSERT_EQ(true, ParseDbName(dbname, &prefix, &tablet, &lg));
  ASSERT_EQ(prefix, "/hello/t1");
  ASSERT_EQ(tablet, 1UL);
  ASSERT_EQ(lg, 0UL);
  ASSERT_EQ(true, ParseDbName(dbname, NULL, &tablet, NULL));
  ASSERT_EQ(tablet, 1UL);

  // 1234567=0x12d687, 89=0x59, 987654321=0x3ADE68B1
  dbname = "/hello/t1/tablet01234567/89";
  ASSERT_EQ(true, ParseDbName(dbname, &prefix, &tablet, &lg));
  ASSERT_EQ(prefix, "/hello/t1");
  ASSERT_EQ(tablet, 1234567UL);
  ASSERT_EQ(lg, 89UL);
  ASSERT_EQ(true, ParseDbName(dbname, NULL, NULL, NULL));
}

TEST(FileNameTest, GetTabletPathFromNum) {
  std::string tablename;
  uint64_t tablet;

  tablename = "table1";
  tablet = 1;
  ASSERT_EQ("table1/tablet00000001", GetTabletPathFromNum(tablename, tablet));

  tablename = "table1";
  tablet = 123456;
  ASSERT_EQ("table1/tablet00123456", GetTabletPathFromNum(tablename, tablet));
}

TEST(FileNameTest, GetChildTabletPath) {
  std::string tablename;
  uint64_t tablet;

  tablename = "table1/tablet00000001";
  tablet = 2;
  ASSERT_EQ("table1/tablet00000002", GetChildTabletPath(tablename, tablet));

  tablename = "table1/tablet00654321";
  tablet = 123456;
  ASSERT_EQ("table1/tablet00123456", GetChildTabletPath(tablename, tablet));
}

TEST(FileNameTest, GetTabletNumFromPath) {
  std::string tabletpath;

  tabletpath = "table1/tablet00000001";
  ASSERT_EQ(1UL, GetTabletNumFromPath(tabletpath));

  tabletpath = "table1/tablet00123456";
  ASSERT_EQ(123456UL, GetTabletNumFromPath(tabletpath));

  tabletpath = "table1/meta";
  ASSERT_EQ(0UL, GetTabletNumFromPath(tabletpath));
}

TEST(FileNameTest, IsTableFileInherited) {
  uint64_t tablet, number;

  tablet = 3;
  number = 0x8000000100000000;
  ASSERT_EQ(true, IsTableFileInherited(tablet, number));
  tablet = 1;
  ASSERT_EQ(false, IsTableFileInherited(tablet, number));

  // 1234567=0x12d687, 89=0x59, 987654321=0x3ADE68B1
  tablet = 0x123456;
  number = 0x8012345601234567;
  ASSERT_EQ(false, IsTableFileInherited(tablet, number));
  number = 0x8065432101234567;
  ASSERT_EQ(true, IsTableFileInherited(tablet, number));
}

TEST(FileNameTest, FileNumberDebugString) {
  uint64_t number = 0x8000000100000000;
  ASSERT_EQ("[00000001 00000000.sst]", FileNumberDebugString(number));

  // 1234567=0x12d687, 89=0x59, 987654321=0x3ADE68B1
  number = 0x8012d6870012d687;
  ASSERT_EQ("[01234567 01234567.sst]", FileNumberDebugString(number));
}
}  // namespace leveldb

int main(int argc, char** argv) {
  return leveldb::test::RunAllTests();
}
