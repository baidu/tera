// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "gflags/gflags.h"
#include "glog/logging.h"
#include "gtest/gtest.h"

#include "load_balancer/random.h"

namespace tera {
namespace load_balancer {

class RandomTest : public ::testing::Test {};

TEST_F(RandomTest, CommonTest) {
  int start = 0;
  int end = 3;
  size_t times = 100;

  for (size_t i = 0; i < times; ++i) {
    int rand = Random::Rand(start, end);
    ASSERT_TRUE(rand >= start);
    ASSERT_TRUE(rand < end);
  }
}

TEST_F(RandomTest, NegativeTest) {
  int start = -10;
  int end = 10;
  size_t times = 100;

  for (size_t i = 0; i < times; ++i) {
    int rand = Random::RandStd(start, end);
    ASSERT_TRUE(rand >= start);
    ASSERT_TRUE(rand < end);
  }
}

}  // namespace load_balancer
}  // namespace tera

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
