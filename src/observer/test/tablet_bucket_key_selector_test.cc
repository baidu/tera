// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include <iostream>
#include <string>
#include <vector>

#include "glog/logging.h"
#include "gtest/gtest.h"

#include "observer/executor/tablet_bucket_key_selector.h"

namespace tera {

class TabletBucketKeySelectorTest : public ::testing::Test {
 public:
  TabletBucketKeySelectorTest() : selector_(new observer::TabletBucketKeySelector(0, 0)) {}
  ~TabletBucketKeySelectorTest() { delete selector_; }

  void ResetKeySelector(int32_t bucket_id, int32_t bucket_cnt) {
    selector_->bucket_id_ = bucket_id;
    selector_->bucket_cnt_ = bucket_cnt;
  }
  void AddTabletInfo(const std::string& table_name, const std::string& start_key,
                     const std::string& end_key) {
    TabletInfo tablet;
    tablet.start_key = start_key;
    tablet.end_key = end_key;
    std::map<std::string, std::vector<TabletInfo>>* tables = selector_->tables_.get();
    (*tables)[table_name].push_back(tablet);
  }

  void AddObseverTable(const std::string& table_name) {
    selector_->observe_tables_.push_back(table_name);
  }

  observer::TabletBucketKeySelector& GetSelector() { return *selector_; }

 private:
  observer::TabletBucketKeySelector* selector_;
};

TEST_F(TabletBucketKeySelectorTest, SelectRange0) {
  ResetKeySelector(0, 0);
  AddObseverTable("t1");
  AddTabletInfo("t2", "a", "z");
  AddTabletInfo("t3", "a", "z");
  std::string table_name, start_key, end_key;
  EXPECT_FALSE(GetSelector().SelectRange(&table_name, &start_key, &end_key));
}

TEST_F(TabletBucketKeySelectorTest, SelectRange1) {
  ResetKeySelector(1, 1);
  AddObseverTable("t1");
  AddTabletInfo("t1", "", "b");
  AddTabletInfo("t1", "b", "");
  EXPECT_TRUE(2 == (*(GetSelector().tables_.get()))["t1"].size());
  std::string table_name, start_key, end_key;
  EXPECT_FALSE(GetSelector().SelectRange(&table_name, &start_key, &end_key));
  EXPECT_TRUE(table_name == "t1");
  EXPECT_TRUE(start_key == "");
  EXPECT_TRUE(end_key == "");
}

TEST_F(TabletBucketKeySelectorTest, SelectRange2) {
  ResetKeySelector(0, 1);
  AddObseverTable("t1");
  AddTabletInfo("t1", "", "b");
  AddTabletInfo("t1", "b", "");
  EXPECT_TRUE(2 == (*(GetSelector().tables_.get()))["t1"].size());
  std::string table_name, start_key, end_key;
  EXPECT_TRUE(GetSelector().SelectRange(&table_name, &start_key, &end_key));
  EXPECT_TRUE(table_name == "t1");
  EXPECT_TRUE(start_key == "");
  EXPECT_TRUE(end_key == "");
}

TEST_F(TabletBucketKeySelectorTest, SelectRange3) {
  ResetKeySelector(0, 1);
  AddObseverTable("t1");
  AddTabletInfo("t1", "", "b");
  AddTabletInfo("t1", "b", "c");
  AddTabletInfo("t1", "c", "d");
  AddTabletInfo("t1", "d", "e");
  AddTabletInfo("t1", "e", "f");
  AddTabletInfo("t1", "f", "g");
  AddTabletInfo("t1", "g", "");
  EXPECT_TRUE(7 == (*(GetSelector().tables_.get()))["t1"].size());
  std::string table_name, start_key, end_key;
  EXPECT_TRUE(GetSelector().SelectRange(&table_name, &start_key, &end_key));
  EXPECT_TRUE(table_name == "t1");
  EXPECT_TRUE(start_key == "");
  EXPECT_TRUE(end_key == "");
}

TEST_F(TabletBucketKeySelectorTest, SelectRange4) {
  ResetKeySelector(3, 4);
  AddObseverTable("t1");
  AddTabletInfo("t1", "", "b");
  AddTabletInfo("t1", "b", "c");
  AddTabletInfo("t1", "c", "d");
  AddTabletInfo("t1", "d", "e");
  AddTabletInfo("t1", "e", "f");
  AddTabletInfo("t1", "f", "g");
  AddTabletInfo("t1", "g", "");
  EXPECT_TRUE(7 == (*(GetSelector().tables_.get()))["t1"].size());
  std::string table_name, start_key, end_key;
  EXPECT_TRUE(GetSelector().SelectRange(&table_name, &start_key, &end_key));
  EXPECT_TRUE(table_name == "t1");
  EXPECT_TRUE(start_key == "g");
  EXPECT_TRUE(end_key == "");
}

TEST_F(TabletBucketKeySelectorTest, SelectRange5) {
  ResetKeySelector(3, 100);
  AddObseverTable("t1");
  AddTabletInfo("t1", "", "b");
  AddTabletInfo("t1", "b", "c");
  AddTabletInfo("t1", "c", "d");
  AddTabletInfo("t1", "d", "e");
  AddTabletInfo("t1", "e", "f");
  AddTabletInfo("t1", "f", "g");
  AddTabletInfo("t1", "g", "");
  EXPECT_TRUE(7 == (*(GetSelector().tables_.get()))["t1"].size());
  std::string table_name, start_key, end_key;
  EXPECT_TRUE(GetSelector().SelectRange(&table_name, &start_key, &end_key));
  EXPECT_TRUE(table_name == "t1");
  EXPECT_TRUE(start_key == "d");
  EXPECT_TRUE(end_key == "e");
}

}  // tera

int main(int argc, char* argv[]) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
