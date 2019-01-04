// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include <signal.h>

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>
#include "timeoracle/timeoracle.h"
#include "utils/utils_cmd.h"

DECLARE_string(log_dir);
DECLARE_string(tera_coord_type);
DECLARE_string(tera_leveldb_env_type);
DECLARE_string(tera_fake_zk_path_prefix);

namespace tera {
namespace timeoracle {

class TimeoracleTest : public ::testing::Test {
 public:
};

TEST_F(TimeoracleTest, UniqueTimestampMsTest) {
  int64_t ts0 = Timeoracle::UniqueTimestampMs();
  for (int i = 0; i < 10000; ++i) {
    int64_t ts = Timeoracle::UniqueTimestampMs();
    EXPECT_LT(ts0, ts);
    ts0 = ts;
  }
}

TEST_F(TimeoracleTest, TimeoracleFunc) {
  Timeoracle to(1024LL);

  auto tmp = to.GetTimestamp(10LL);
  EXPECT_EQ(tmp, 0);

  tmp = to.UpdateLimitTimestamp(10LL);
  EXPECT_EQ(tmp, 10);

  tmp = to.GetTimestamp(10LL);
  EXPECT_EQ(tmp, 0);

  tmp = to.UpdateLimitTimestamp(2000LL);
  EXPECT_EQ(tmp, 2000);

  tmp = to.GetTimestamp(10LL);
  EXPECT_EQ(tmp, 1044);

  tmp = to.GetTimestamp(10LL);
  EXPECT_EQ(tmp, 1054);

  EXPECT_EQ(to.GetStartTimestamp(), 1064);

  tmp = to.UpdateStartTimestamp();

  EXPECT_GT(tmp, 1064);

  auto new_ts = to.GetTimestamp(10LL);
  EXPECT_EQ(new_ts, 0);
}

}  // namespace timeoracle
}  // namespace tera

int main(int argc, char** argv) {
  ::google::ParseCommandLineFlags(&argc, &argv, true);
  ::google::InitGoogleLogging(argv[0]);
  FLAGS_tera_coord_type = "fake_zk";
  FLAGS_tera_leveldb_env_type = "local";

  tera::utils::SetupLog("timeorcale_test");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
