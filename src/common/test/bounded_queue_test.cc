// Copyright (c) 2018, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include <memory>
#include "gtest/gtest.h"
#include "common/base/bounded_queue.h"

namespace tera {

class BoundedQueueTest : public ::testing::Test {
 public:
  BoundedQueueTest() {}

  virtual void SetUp() {}

  virtual void TearDown() {}

  virtual void Reset(int64_t limit) { bq_.reset(new common::BoundedQueue<int64_t>{limit}); }

 private:
  std::unique_ptr<common::BoundedQueue<int64_t>> bq_;
};

TEST_F(BoundedQueueTest, BaseTest) {
  Reset(5);
  EXPECT_TRUE(bq_.get());
  EXPECT_EQ(bq_->Average(), 0);
  EXPECT_EQ(bq_->Sum(), 0);
  bq_->Push(1);
  bq_->Push(2);
  bq_->Push(3);
  bq_->Push(4);
  bq_->Push(5);
  EXPECT_EQ(bq_->Average(), 3);
  EXPECT_EQ(bq_->Sum(), 15);
  bq_->Push(2);
  bq_->Push(2);
  bq_->Push(2);
  bq_->Push(2);
  bq_->Push(2);
  EXPECT_EQ(bq_->Average(), 2);
  EXPECT_EQ(bq_->Sum(), 10);
  EXPECT_EQ(bq_->qu_.size(), bq_->Size());
  EXPECT_EQ(bq_->qu_.size(), 5);
}
}  // end namespace tera

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
