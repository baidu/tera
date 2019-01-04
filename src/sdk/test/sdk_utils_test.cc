// Copyright (c) 2015-2017, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Author: baorenyi@baidu.com

#include "sdk_utils.h"

#include "gtest/gtest.h"

namespace tera {

class SdkUtilsTest : public ::testing::Test {
 public:
  SdkUtilsTest() {}
  ~SdkUtilsTest() {}
};

TEST(SdkUtilsTest, ExtendNotifyLgToDescriptor0) {
  // all disable notify
  tera::TableDescriptor schema("t1");
  schema.AddLocalityGroup("lg0");
  tera::ColumnFamilyDescriptor* cfd1 = schema.AddColumnFamily("cf1");
  cfd1->DisableNotify();
  tera::ColumnFamilyDescriptor* cfd2 = schema.AddColumnFamily("cf2");
  cfd2->DisableNotify();
  auto before_num = schema.LocalityGroupNum();
  EXPECT_TRUE(ExtendNotifyLgToDescriptor(&schema));
  EXPECT_TRUE(schema.LocalityGroupNum() == before_num);
}

TEST(SdkUtilsTest, ExtendNotifyLgToDescriptor1) {
  // some disable notify
  tera::TableDescriptor schema("t1");
  schema.AddLocalityGroup("lg0");
  tera::ColumnFamilyDescriptor* cfd1 = schema.AddColumnFamily("cf1");
  cfd1->EnableNotify();
  tera::ColumnFamilyDescriptor* cfd2 = schema.AddColumnFamily("cf2");
  cfd2->DisableNotify();
  auto before_num = schema.LocalityGroupNum();
  EXPECT_TRUE(ExtendNotifyLgToDescriptor(&schema));
  EXPECT_TRUE(schema.LocalityGroupNum() == before_num + 1);
}

TEST(SdkUtilsTest, ExtendNotifyLgToDescriptor2) {
  // some disable notify
  tera::TableDescriptor schema("t1");
  schema.AddLocalityGroup("lg0");
  tera::ColumnFamilyDescriptor* cfd1 = schema.AddColumnFamily("cf1");
  cfd1->DisableNotify();
  tera::ColumnFamilyDescriptor* cfd2 = schema.AddColumnFamily("cf2");
  cfd2->EnableNotify();
  auto before_num = schema.LocalityGroupNum();
  EXPECT_TRUE(ExtendNotifyLgToDescriptor(&schema));
  EXPECT_TRUE(schema.LocalityGroupNum() == before_num + 1);
}

TEST(SdkUtilsTest, ExtendNotifyLgToDescriptor3) {
  // all enable notify
  tera::TableDescriptor schema("t1");
  schema.AddLocalityGroup("lg0");
  tera::ColumnFamilyDescriptor* cfd1 = schema.AddColumnFamily("cf1");
  cfd1->EnableNotify();
  tera::ColumnFamilyDescriptor* cfd2 = schema.AddColumnFamily("cf2");
  cfd2->EnableNotify();
  auto before_num = schema.LocalityGroupNum();
  EXPECT_TRUE(ExtendNotifyLgToDescriptor(&schema));
  EXPECT_TRUE(schema.LocalityGroupNum() == before_num + 1);
}

TEST(SdkUtilsTest, ExtendNotifyLgToDescriptor4) {
  // have lg named 'notify' but not set any cf 'notify=on'
  tera::TableDescriptor schema("t1");
  schema.AddLocalityGroup("notify");
  tera::ColumnFamilyDescriptor* cfd1 = schema.AddColumnFamily("cf1", "notify");
  tera::ColumnFamilyDescriptor* cfd2 = schema.AddColumnFamily("cf2", "notify");
  auto before_num = schema.LocalityGroupNum();
  EXPECT_TRUE(ExtendNotifyLgToDescriptor(&schema));
  EXPECT_TRUE(schema.LocalityGroupNum() == before_num);
}

TEST(SdkUtilsTest, ExtendNotifyLgToDescriptor5) {
  // have lg named 'notify' and set some cf 'notify=on'
  tera::TableDescriptor schema("t1");
  schema.AddLocalityGroup("notify");
  tera::ColumnFamilyDescriptor* cfd1 = schema.AddColumnFamily("cf1", "notify");
  cfd1->EnableNotify();
  tera::ColumnFamilyDescriptor* cfd2 = schema.AddColumnFamily("cf2", "notify");
  auto before_num = schema.LocalityGroupNum();
  EXPECT_FALSE(ExtendNotifyLgToDescriptor(&schema));
  EXPECT_TRUE(schema.LocalityGroupNum() == before_num);
}

TEST(SdkUtilsTest, ExtendNotifyLgToDescriptor6) {
  // have cf named '_N_' but not set any cf 'notify=on'
  tera::TableDescriptor schema("t1");
  schema.AddLocalityGroup("lg0");
  tera::ColumnFamilyDescriptor* cfd1 = schema.AddColumnFamily("_N_");
  tera::ColumnFamilyDescriptor* cfd2 = schema.AddColumnFamily("cf2");
  auto before_num = schema.LocalityGroupNum();
  EXPECT_TRUE(ExtendNotifyLgToDescriptor(&schema));
  EXPECT_TRUE(schema.LocalityGroupNum() == before_num);
}

TEST(SdkUtilsTest, ExtendNotifyLgToDescriptor7) {
  // have cf named '_N_' but some set cf 'notify=on'
  tera::TableDescriptor schema("t1");
  schema.AddLocalityGroup("lg0");
  tera::ColumnFamilyDescriptor* cfd1 = schema.AddColumnFamily("_N_");
  cfd1->EnableNotify();
  tera::ColumnFamilyDescriptor* cfd2 = schema.AddColumnFamily("cf2");
  auto before_num = schema.LocalityGroupNum();
  EXPECT_FALSE(ExtendNotifyLgToDescriptor(&schema));
  EXPECT_TRUE(schema.LocalityGroupNum() == before_num);
}

TEST(SdkUtilsTest, SetMutationErrorIfInvalid) {
  ErrorCode err;
  std::string test_str = std::string(1 + (64 << 10), 'h');
  SetMutationErrorIfInvalid(test_str, FieldType::kRowkey, &err);
  EXPECT_TRUE(err.GetType() == ErrorCode::kBadParam);
  err.SetFailed(ErrorCode::kOK, "");

  SetMutationErrorIfInvalid(test_str, FieldType::kKVColumnFamily, &err);
  EXPECT_TRUE(err.GetType() == ErrorCode::kBadParam);
  err.SetFailed(ErrorCode::kOK, "");

  SetMutationErrorIfInvalid(test_str, FieldType::kKVQualifier, &err);
  EXPECT_TRUE(err.GetType() == ErrorCode::kBadParam);
  err.SetFailed(ErrorCode::kOK, "");

  SetMutationErrorIfInvalid(test_str, FieldType::kQualifier, &err);
  EXPECT_TRUE(err.GetType() == ErrorCode::kBadParam);
  err.SetFailed(ErrorCode::kOK, "");

  test_str = std::string(1 + (32 << 20), 'h');
  SetMutationErrorIfInvalid(test_str, FieldType::kValue, &err);
  EXPECT_TRUE(err.GetType() == ErrorCode::kBadParam);
}

}  // namespace tera
