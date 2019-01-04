// Copyright (c) 2015-2018, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include <chrono>
#include <functional>
#include <gtest/gtest.h>
#include <gflags/gflags.h>
#include <thread>
#include "quota/quota_entry.h"

DECLARE_int64(tera_quota_normal_estimate_value);
DECLARE_string(tera_quota_limiter_type);

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

  static TableQuota BuildTableQuota() {
    // build table_quota
    TableQuota table_quota;
    table_quota.set_table_name(test_table);

    // add write req limit 1s
    QuotaInfo* quota_info = table_quota.add_quota_infos();
    quota_info->set_type(kQuotaWriteReqs);
    quota_info->set_limit(quota_limit);
    quota_info->set_period(default_quota_period);

    // add write bytes 2s
    quota_info = table_quota.add_quota_infos();
    quota_info->set_type(kQuotaWriteBytes);
    quota_info->set_limit(quota_limit);
    quota_info->set_period(quota_period);

    // add read req limit 2s
    quota_info = table_quota.add_quota_infos();
    quota_info->set_type(kQuotaReadReqs);
    quota_info->set_limit(quota_limit);
    quota_info->set_period(quota_period);

    // add read req limit 1s
    quota_info = table_quota.add_quota_infos();
    quota_info->set_type(kQuotaReadBytes);
    quota_info->set_limit(quota_limit);
    quota_info->set_period(default_quota_period);

    // add scan req limit 1s
    quota_info = table_quota.add_quota_infos();
    quota_info->set_type(kQuotaScanReqs);
    quota_info->set_limit(quota_limit);
    quota_info->set_period(default_quota_period);

    // add scan bytes limit 2s
    quota_info = table_quota.add_quota_infos();
    quota_info->set_type(kQuotaScanBytes);
    quota_info->set_limit(quota_limit);
    quota_info->set_period(quota_period);

    return std::move(table_quota);
  }
};

class QuotaEntryTest : public ::testing::Test {
 public:
  QuotaEntryTest() {}

  virtual ~QuotaEntryTest() {}

  QuotaEntry* NewQuotaEntry() {
    FLAGS_tera_quota_limiter_type = "general_quota_limiter";
    quota_entry_.reset(new QuotaEntry);
    return quota_entry_.get();
  }

  QuotaEntry* NewNotGeneralQuotaEntry() {
    FLAGS_tera_quota_limiter_type = "not_general_quota_limiter";
    quota_entry_.reset(new QuotaEntry);
    return quota_entry_.get();
  }

  void ReadEstimateBytesPerReqMap(const TableQuota& table_quota) {
    int64_t bytes = consume_amount * FLAGS_tera_quota_normal_estimate_value;
    EXPECT_TRUE(bytes ==
                quota_entry_->Estimate(table_quota.table_name(), kQuotaReadBytes, consume_amount));
  }

  void WriteEstimateBytesPerReqMap(const TableQuota& table_quota) {
    EXPECT_TRUE(quota_entry_->Reset(table_quota));
  }

  void EstimateBytesPerReqMapTest(const TableQuota& table_quota, int job_num) {
    std::vector<std::thread> threads;
    for (int i = 0; i < job_num; ++i) {
      threads.emplace_back(
          std::bind(&QuotaEntryTest::ReadEstimateBytesPerReqMap, this, table_quota));
      threads.emplace_back(
          std::bind(&QuotaEntryTest::WriteEstimateBytesPerReqMap, this, table_quota));
    }
    for (auto& t : threads) {
      t.join();
    }
  }

  void MultiResetQuota(const TableQuota& table_quota) {
    EXPECT_TRUE(quota_entry_->Reset(table_quota));
  }

  void MultiCheckAndConsumeQuota(const TableQuota& table_quota) {
    quota_entry_->CheckAndConsume(table_quota.table_name(), OpTypeAmountList{std::make_pair(
                                                                kQuotaWriteBytes, consume_amount)});
  }

  void MultiResetCheckAndConsumeQuota(const TableQuota& table_quota, int job_num) {
    std::vector<std::thread> threads;
    for (int i = 0; i < job_num; ++i) {
      threads.emplace_back(std::bind(&QuotaEntryTest::MultiResetQuota, this, table_quota));
      threads.emplace_back(
          std::bind(&QuotaEntryTest::MultiCheckAndConsumeQuota, this, table_quota));
    }
    for (auto& t : threads) {
      t.join();
    }
  }

 private:
  std::unique_ptr<QuotaEntry> quota_entry_;
};

TEST_F(QuotaEntryTest, NotGeneralQuotaLimitType) {
  QuotaEntry* quota_entry = NewNotGeneralQuotaEntry();
  TableQuota table_quota = QuotaBuilder::BuildTableQuota();
  EXPECT_FALSE(quota_entry->Reset(table_quota));
}

TEST_F(QuotaEntryTest, ResetCheckAndConsumeTableWriteReqQuota) {
  QuotaEntry* quota_entry = NewQuotaEntry();
  TableQuota table_quota = QuotaBuilder::BuildTableQuota();
  EXPECT_TRUE(quota_entry->Reset(table_quota));
  EXPECT_TRUE(quota_entry->CheckAndConsume(
      test_table, OpTypeAmountList{std::make_pair(kQuotaWriteReqs, consume_amount)}));
  EXPECT_FALSE(quota_entry->CheckAndConsume(
      test_table, OpTypeAmountList{std::make_pair(kQuotaWriteReqs, consume_amount)}));
}

TEST_F(QuotaEntryTest, ResetCheckAndConsumeTableWriteBytePeriodQuota) {
  QuotaEntry* quota_entry = NewQuotaEntry();
  TableQuota table_quota = QuotaBuilder::BuildTableQuota();
  EXPECT_TRUE(quota_entry->Reset(table_quota));
  EXPECT_TRUE(quota_entry->CheckAndConsume(
      test_table, OpTypeAmountList{std::make_pair(kQuotaWriteBytes, consume_amount)}));
  EXPECT_FALSE(quota_entry->CheckAndConsume(
      test_table, OpTypeAmountList{std::make_pair(kQuotaWriteBytes, consume_amount)}));

  std::chrono::seconds wait_sec(quota_period);
  std::this_thread::sleep_for(wait_sec);
  EXPECT_TRUE(quota_entry->CheckAndConsume(
      test_table, OpTypeAmountList{std::make_pair(kQuotaWriteBytes, consume_amount)}));
  std::this_thread::sleep_for(wait_sec);
  EXPECT_TRUE(quota_entry->CheckAndConsume(
      test_table, OpTypeAmountList{std::make_pair(kQuotaWriteBytes, consume_amount)}));
}

TEST_F(QuotaEntryTest, ResetCheckAndConsumeTableReadReqPeriodQuota) {
  QuotaEntry* quota_entry = NewQuotaEntry();
  TableQuota table_quota = QuotaBuilder::BuildTableQuota();
  EXPECT_TRUE(quota_entry->Reset(table_quota));
  EXPECT_TRUE(quota_entry->CheckAndConsume(
      test_table, OpTypeAmountList{std::make_pair(kQuotaReadReqs, consume_amount)}));
  EXPECT_FALSE(quota_entry->CheckAndConsume(
      test_table, OpTypeAmountList{std::make_pair(kQuotaReadReqs, consume_amount)}));

  std::chrono::seconds wait_sec(quota_period);
  std::this_thread::sleep_for(wait_sec);
  EXPECT_TRUE(quota_entry->CheckAndConsume(
      test_table, OpTypeAmountList{std::make_pair(kQuotaReadReqs, consume_amount)}));
  std::this_thread::sleep_for(wait_sec);
  EXPECT_TRUE(quota_entry->CheckAndConsume(
      test_table, OpTypeAmountList{std::make_pair(kQuotaReadReqs, consume_amount)}));
}

TEST_F(QuotaEntryTest, ResetCheckAndConsumeTableReadByteQuota) {
  QuotaEntry* quota_entry = NewQuotaEntry();
  TableQuota table_quota = QuotaBuilder::BuildTableQuota();
  EXPECT_TRUE(quota_entry->Reset(table_quota));
  EXPECT_TRUE(quota_entry->CheckAndConsume(
      test_table, OpTypeAmountList{std::make_pair(kQuotaReadBytes, consume_amount)}));
}

TEST_F(QuotaEntryTest, ResetCheckAndConsumeTableScanReqQuota) {
  QuotaEntry* quota_entry = NewQuotaEntry();
  TableQuota table_quota = QuotaBuilder::BuildTableQuota();
  EXPECT_TRUE(quota_entry->Reset(table_quota));
  EXPECT_TRUE(quota_entry->CheckAndConsume(
      test_table, OpTypeAmountList{std::make_pair(kQuotaScanReqs, consume_amount)}));
  EXPECT_FALSE(quota_entry->CheckAndConsume(
      test_table, OpTypeAmountList{std::make_pair(kQuotaScanReqs, consume_amount)}));
}

TEST_F(QuotaEntryTest, ResetCheckAndConsumeTableScanBytePeriodQuota) {
  QuotaEntry* quota_entry = NewQuotaEntry();
  TableQuota table_quota = QuotaBuilder::BuildTableQuota();
  EXPECT_TRUE(quota_entry->Reset(table_quota));
  EXPECT_TRUE(quota_entry->CheckAndConsume(
      test_table, OpTypeAmountList{std::make_pair(kQuotaScanBytes, consume_amount)}));

  std::chrono::seconds wait_sec(quota_period);
  std::this_thread::sleep_for(wait_sec);
  EXPECT_TRUE(quota_entry->CheckAndConsume(
      test_table, OpTypeAmountList{std::make_pair(kQuotaScanBytes, consume_amount)}));
  std::this_thread::sleep_for(wait_sec);
  EXPECT_TRUE(quota_entry->CheckAndConsume(
      test_table, OpTypeAmountList{std::make_pair(kQuotaScanBytes, consume_amount)}));
}

TEST_F(QuotaEntryTest, EstimateBytesPerReqMap) {
  QuotaEntry* quota_entry = NewQuotaEntry();
  TableQuota table_quota = QuotaBuilder::BuildTableQuota();
  EstimateBytesPerReqMapTest(table_quota, 10);
}

TEST_F(QuotaEntryTest, MultiResetCheckAndConsumeQuota) {
  QuotaEntry* quota_entry = NewQuotaEntry();
  TableQuota table_quota = QuotaBuilder::BuildTableQuota();
  MultiResetCheckAndConsumeQuota(table_quota, 10);
}

TEST_F(QuotaEntryTest, Update) {
  QuotaEntry* quota_entry = NewQuotaEntry();
  std::unique_ptr<QueryRequest> request(new QueryRequest);
  std::unique_ptr<QueryResponse> response(new QueryResponse);
  request->set_quota_version(1);
  quota_entry->Update(request.get(), response.get());
  EXPECT_TRUE(1 == response->quota_version());
}
}
}
}
