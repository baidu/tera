// Copyright (c) 2015-2018, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include <chrono>
#include <functional>
#include <gtest/gtest.h>
#include <gflags/gflags.h>
#include <thread>
#include "quota/master_quota_entry.h"
#include "master/master_env.h"
#include "quota/helpers/master_quota_helper.h"

namespace tera {
namespace quota {
namespace test {

static const std::string test_table("test");
static const int64_t quota_limit = 3000;
static const int64_t consume_amount = 2000;
static const int64_t quota_period = 2;
static const int64_t default_quota_period = 1;

class QuotaBuilder {
 public:
  QuotaBuilder() {}

  virtual ~QuotaBuilder() {}

  static TableQuota* BuildTableQuota2() {
    // build table_quota
    std::unique_ptr<TableQuota> table_quota(new TableQuota);
    table_quota->set_table_name(test_table);
    table_quota->set_type(TableQuota::kDelQuota);

    // add write req limit 1s
    QuotaInfo* quota_info = table_quota->add_quota_infos();
    quota_info->set_type(kQuotaWriteReqs);
    quota_info->set_limit(quota_limit);
    quota_info->set_period(default_quota_period);

    // add write bytes 2s
    quota_info = table_quota->add_quota_infos();
    quota_info->set_type(kQuotaWriteBytes);
    quota_info->set_limit(quota_limit);
    quota_info->set_period(quota_period);

    // add read req limit 2s
    quota_info = table_quota->add_quota_infos();
    quota_info->set_type(kQuotaReadReqs);
    quota_info->set_limit(quota_limit);
    quota_info->set_period(quota_period);

    // add read req limit 1s
    quota_info = table_quota->add_quota_infos();
    quota_info->set_type(kQuotaReadBytes);
    quota_info->set_limit(quota_limit);
    quota_info->set_period(default_quota_period);

    // add scan req limit 1s
    quota_info = table_quota->add_quota_infos();
    quota_info->set_type(kQuotaScanReqs);
    quota_info->set_limit(quota_limit);
    quota_info->set_period(default_quota_period);

    // add scan bytes limit 2s
    quota_info = table_quota->add_quota_infos();
    quota_info->set_type(kQuotaScanBytes);
    quota_info->set_limit(quota_limit);
    quota_info->set_period(quota_period);

    EXPECT_TRUE(table_quota->type() == TableQuota::kDelQuota);
    return table_quota.release();
  }
};

class MaterQuotaHelperTest : public ::testing::Test {
 public:
  MaterQuotaHelperTest() {}
  virtual ~MaterQuotaHelperTest() {}
};

TEST_F(MaterQuotaHelperTest, SetDefaultQuotaInfo) {
  std::unique_ptr<QuotaInfo> quota_info(new QuotaInfo);
  MasterQuotaHelper::SetDefaultQuotaInfo(quota_info.get(), kQuotaWriteReqs);
  EXPECT_TRUE(quota_info->type() == kQuotaWriteReqs);
  EXPECT_TRUE(quota_info->limit() == -1);
  EXPECT_TRUE(quota_info->period() == 1);
}

TEST_F(MaterQuotaHelperTest, MetaWriteRecord) {
  std::unique_ptr<TableQuota> table_quota(QuotaBuilder::BuildTableQuota2());
  EXPECT_TRUE(table_quota->type() == TableQuota::kDelQuota);
  std::unique_ptr<master::MetaWriteRecord> meta_write_record(
      MasterQuotaHelper::NewMetaRecordFromQuota(*table_quota));
  EXPECT_TRUE(!!meta_write_record);
  EXPECT_TRUE(MasterQuotaHelper::GetTableNameFromMetaKey(meta_write_record->key) == test_table);
  EXPECT_TRUE((meta_write_record->value).size() > 0);
  std::unique_ptr<TableQuota> table_quota2(
      MasterQuotaHelper::NewTableQuotaFromMetaValue(meta_write_record->value));
  EXPECT_TRUE(!!table_quota2);
  EXPECT_TRUE(table_quota2->type() == TableQuota::kDelQuota);
  EXPECT_TRUE(table_quota2->table_name() == table_quota->table_name());
  EXPECT_TRUE(table_quota2->quota_infos_size() == table_quota->quota_infos_size());
}

TEST_F(MaterQuotaHelperTest, MergeTableQuota) {
  std::unique_ptr<TableQuota> table_quota(QuotaBuilder::BuildTableQuota2());
  std::unique_ptr<TableQuota> target_table_quota(new TableQuota);

  target_table_quota->set_table_name(test_table);
  // peroid = 1, limit = -1
  quota::MasterQuotaHelper::SetDefaultQuotaInfo(target_table_quota->add_quota_infos(),
                                                kQuotaWriteReqs);
  quota::MasterQuotaHelper::SetDefaultQuotaInfo(target_table_quota->add_quota_infos(),
                                                kQuotaWriteBytes);
  quota::MasterQuotaHelper::SetDefaultQuotaInfo(target_table_quota->add_quota_infos(),
                                                kQuotaReadReqs);
  quota::MasterQuotaHelper::SetDefaultQuotaInfo(target_table_quota->add_quota_infos(),
                                                kQuotaReadBytes);
  quota::MasterQuotaHelper::SetDefaultQuotaInfo(target_table_quota->add_quota_infos(),
                                                kQuotaScanReqs);
  quota::MasterQuotaHelper::SetDefaultQuotaInfo(target_table_quota->add_quota_infos(),
                                                kQuotaScanBytes);

  EXPECT_TRUE(MasterQuotaHelper::MergeTableQuota(*table_quota, target_table_quota.get()));
  EXPECT_FALSE(MasterQuotaHelper::MergeTableQuota(*table_quota, target_table_quota.get()));
}
}
}
}
