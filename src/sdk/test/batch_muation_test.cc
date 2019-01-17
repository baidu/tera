// Copyright (c) 2015-2018, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Author: baorenyi@baidu.com

#include <atomic>
#include <iostream>
#include <string>
#include <thread>
#include <memory>

#include "gflags/gflags.h"
#include "glog/logging.h"
#include "gtest/gtest.h"

#include "sdk/batch_mutation_impl.h"
#include "tera.h"
#include "sdk/sdk_task.h"
#include "sdk/sdk_zk.h"
#include "sdk/table_impl.h"
#include "sdk/test/mock_table.h"

#include "tera.h"

DECLARE_string(tera_coord_type);

namespace tera {

class BatchMutationTest : public ::testing::Test {
 public:
  BatchMutationTest() : batch_mu_(NULL) {
    batch_mu_ = static_cast<BatchMutationImpl*>(OpenTable("batch_mu_test")->NewBatchMutation());
  }
  virtual ~BatchMutationTest() {}

  std::shared_ptr<Table> OpenTable(const std::string& tablename) {
    FLAGS_tera_coord_type = "fake_zk";
    std::shared_ptr<MockTable> table_(new MockTable(tablename, &thread_pool_));
    return table_;
  }

  BatchMutationImpl* batch_mu_;
  common::ThreadPool thread_pool_;
};

TEST_F(BatchMutationTest, Put0) {
  batch_mu_->Put("rowkey", "value", 12);
  batch_mu_->Put("rowkey", "value1", 22);
  EXPECT_EQ(batch_mu_->mu_map_.size(), 1);
  EXPECT_EQ(batch_mu_->GetRows().size(), 1);
}

TEST_F(BatchMutationTest, Put1) {
  batch_mu_->Put("rowkey", "value", 12);
  batch_mu_->Put("rowkey1", "value1", 22);
  EXPECT_EQ(batch_mu_->mu_map_.size(), 2);
  EXPECT_EQ(batch_mu_->GetRows().size(), 2);
}

TEST_F(BatchMutationTest, Put2) {
  batch_mu_->Put("rowkey", "cf", "qu", "value");
  batch_mu_->Put("rowkey", "", "qu", "value");
  batch_mu_->Put("rowkey2", "cf", "", "value");
  batch_mu_->Put("rowkey3", "", "", "value");
  batch_mu_->Put("rowkey4", "cf", "qu", "value", 0);
  batch_mu_->Put("rowkey5", "cf", "qu", "value", 1);
  batch_mu_->Put("rowkey6", "cf", "qu", "value", -1);
  EXPECT_EQ(batch_mu_->mu_map_.size(), 6);
  EXPECT_EQ(batch_mu_->GetRows().size(), 6);
  EXPECT_EQ(batch_mu_->mu_map_["rowkey"].size(), 2);
  EXPECT_EQ(batch_mu_->mu_map_["rowkey1"].size(), 0);
}

TEST_F(BatchMutationTest, OtherOps) {
  batch_mu_->Add("rowkey", "cf", "qu", 12);
  EXPECT_EQ(batch_mu_->mu_map_["rowkey"].back().type, RowMutation::kAdd);

  batch_mu_->PutIfAbsent("rowkey", "cf", "qu", "value");
  EXPECT_EQ(batch_mu_->mu_map_["rowkey"].back().type, RowMutation::kPutIfAbsent);

  batch_mu_->Append("rowkey", "cf", "qu", "value");
  EXPECT_EQ(batch_mu_->mu_map_["rowkey"].back().type, RowMutation::kAppend);

  batch_mu_->DeleteRow("rowkey");
  EXPECT_EQ(batch_mu_->mu_map_["rowkey"].back().type, RowMutation::kDeleteRow);
  EXPECT_EQ(batch_mu_->mu_map_["rowkey"].back().timestamp, kLatestTimestamp);

  batch_mu_->DeleteFamily("rowkey", "cf");
  EXPECT_EQ(batch_mu_->mu_map_["rowkey"].back().type, RowMutation::kDeleteFamily);
  EXPECT_EQ(batch_mu_->mu_map_["rowkey"].back().timestamp, kLatestTimestamp);

  batch_mu_->DeleteColumns("rowkey", "cf", "qu");
  EXPECT_EQ(batch_mu_->mu_map_["rowkey"].back().type, RowMutation::kDeleteColumns);
  EXPECT_EQ(batch_mu_->mu_map_["rowkey"].back().timestamp, kLatestTimestamp);

  batch_mu_->DeleteColumn("rowkey", "cf", "qu", -1);
  EXPECT_EQ(batch_mu_->mu_map_["rowkey"].back().type, RowMutation::kDeleteColumn);
  EXPECT_EQ(batch_mu_->mu_map_["rowkey"].back().timestamp, kLatestTimestamp);

  const std::string& huge_str = std::string(1 + (32 << 20), 'h');
  batch_mu_->Put(huge_str, "cf", "qu", "v");
  EXPECT_EQ(batch_mu_->GetError().GetType(), ErrorCode::kBadParam);
  batch_mu_->Put("r", "cf", huge_str, "v");
  EXPECT_EQ(batch_mu_->GetError().GetType(), ErrorCode::kBadParam);
  batch_mu_->Put("r", "cf", "qu", huge_str);
  EXPECT_EQ(batch_mu_->GetError().GetType(), ErrorCode::kBadParam);
}

void MockOpStatCallback(Table* table, SdkTask* task) {
  // Nothing to do
}

TEST_F(BatchMutationTest, RunCallback) {
  EXPECT_FALSE(batch_mu_->IsAsync());
  std::shared_ptr<Table> table = OpenTable("test");
  batch_mu_->Prepare(MockOpStatCallback);
  EXPECT_TRUE(batch_mu_->on_finish_callback_ != NULL);
  EXPECT_TRUE(batch_mu_->start_ts_ > 0);
  // set OpStatCallback
  batch_mu_->RunCallback();
  EXPECT_TRUE(batch_mu_->finish_);
  EXPECT_TRUE(batch_mu_->IsFinished());
}

TEST_F(BatchMutationTest, Size) {
  EXPECT_EQ(batch_mu_->Size(), 0);
  int64_t ts = -1;
  batch_mu_->Put("r", "cf", "qu", "v");
  EXPECT_EQ(batch_mu_->Size(), 6 + sizeof(ts));
  batch_mu_->Put("r", "cf", "qu", "v");
  // only calc one rowkey
  EXPECT_EQ(batch_mu_->Size(), (6 + sizeof(ts)) * 2 - 1);
  batch_mu_->Put("R", "cf", "qu", "v");
  EXPECT_EQ(batch_mu_->Size(), (6 + sizeof(ts)) * 3 - 1);
}

TEST_F(BatchMutationTest, GetMutation) {
  batch_mu_->Put("r", "cf", "qu", "v");
  batch_mu_->Put("r", "cf", "qu1", "v");
  batch_mu_->Put("r2", "cf", "qu", "v");
  batch_mu_->Put("r3", "cf", "qu", "v");
  EXPECT_EQ((batch_mu_->GetMutation("r", 1)).qualifier, "qu1");
}
}
