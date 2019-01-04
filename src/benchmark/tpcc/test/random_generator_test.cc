// Copyright (c) 2015-2017, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Author: baorenyi@baidu.com

#include "benchmark/tpcc/random_generator.h"

#include "gtest/gtest.h"

namespace tera {
namespace tpcc {

class RandomGenerator;

class RandomGeneratorTest : public ::testing::Test, public RandomGenerator {
 public:
  RandomGeneratorTest() : RandomGenerator() { SetRandomConstant(); }

  ~RandomGeneratorTest() {}
};

TEST_F(RandomGeneratorTest, MakeFloat) {
  EXPECT_EQ(MakeFloat(1.0, 1.0, 1), 1.0);
  float f = MakeFloat(0, 1.0, 2);
  std::cout << std::to_string(f) << std::endl;
  EXPECT_TRUE(f >= 0 && f <= 1);
}

TEST_F(RandomGeneratorTest, MakeAString) {
  EXPECT_TRUE(MakeAString(0, 0) == "");
  EXPECT_TRUE((MakeAString(1, 1)).length() == 1);
  std::string a_str = MakeAString(1, 10);
  EXPECT_TRUE(a_str.length() <= 10 && a_str.length() >= 1);
  std::string a_str1 = MakeAString(26, 27);
  int cnt = 0;
  for (int i = 0; i < a_str1.length(); ++i) {
    for (int j = i + 1; j < a_str1.length(); ++j) {
      if (a_str1[i] == a_str1[j]) {
        ++cnt;
      }
    }
  }
  EXPECT_TRUE(cnt > 0);
}

TEST_F(RandomGeneratorTest, MakeNString) {
  EXPECT_TRUE(MakeNString(0, 0) == "");
  EXPECT_TRUE((MakeNString(1, 1)).length() == 1);
  std::string n_str = MakeNString(1, 10);
  EXPECT_TRUE(n_str.length() <= 10 && n_str.length() >= 1);
}

TEST_F(RandomGeneratorTest, MakeDisOrderList) {
  std::vector<int> dis_order_list = MakeDisOrderList(10, 20);
  sort(dis_order_list.begin(), dis_order_list.end());
  for (int i = 10; i <= 20; ++i) {
    EXPECT_EQ(dis_order_list[i - 10], i);
  }
}

TEST_F(RandomGeneratorTest, SetRandomConstant) {
  SetRandomConstant();
  NURandConstant c = GetRandomConstant();
  EXPECT_TRUE(c.c_last >= 0 && c.c_last <= 255);
  EXPECT_TRUE(c.c_last >= 0 && c.c_last <= 1023);
  EXPECT_TRUE(c.c_last >= 0 && c.c_last <= 8191);
}

TEST_F(RandomGeneratorTest, GetRandom) {
  EXPECT_EQ(GetRandom(1, 1), 1);
  int rand_num = GetRandom(0, 1);
  int rand_num1 = GetRandom(1, 0);
  EXPECT_TRUE(rand_num == 0 || rand_num == 1);
  EXPECT_TRUE(rand_num == 0 || rand_num == 1);
}

}  // namespace tpcc
}  // namespace tera
