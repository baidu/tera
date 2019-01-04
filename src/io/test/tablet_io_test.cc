// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "io/tablet_io.h"

#include <iostream>
#include <thread>
#include <vector>
#include <stdlib.h>

#include "gflags/gflags.h"
#include "glog/logging.h"
#include "gtest/gtest.h"

#include "common/base/scoped_ptr.h"
#include "common/base/string_format.h"
#include "common/base/string_number.h"
#include "db/filename.h"
#include "leveldb/raw_key_operator.h"
#include "leveldb/table_utils.h"
#include "proto/proto_helper.h"
#include "proto/status_code.pb.h"
#include "common/timer.h"
#include "utils/utils_cmd.h"
#include "utils/string_util.h"
#include "io/tablet_scanner.h"

DECLARE_string(tera_tabletnode_path_prefix);
DECLARE_int32(tera_io_retry_max_times);
DECLARE_int64(tera_tablet_living_period);
DECLARE_string(tera_leveldb_env_type);

DECLARE_int64(tera_tablet_max_write_buffer_size);
DECLARE_string(log_dir);

namespace tera {
namespace io {

const std::string working_dir = "testdata/";
const uint32_t N = 50000;

class TabletIOTest : public ::testing::Test {
 public:
  TabletIOTest() {
    std::string cmd = std::string("mkdir -p ") + working_dir;
    FLAGS_tera_tabletnode_path_prefix = "./";
    system(cmd.c_str());

    InitSchema();
  }

  ~TabletIOTest() {
    std::string cmd = std::string("rm -rf ") + working_dir;
    system(cmd.c_str());
  }

  const TableSchema& GetTableSchema() { return schema_; }

  void InitSchema() {
    schema_.set_name("tera");
    schema_.set_raw_key(Binary);

    LocalityGroupSchema* lg = schema_.add_locality_groups();
    lg->set_name("lg0");

    ColumnFamilySchema* cf = schema_.add_column_families();
    cf->set_name("column");
    cf->set_locality_group("lg0");
    cf->set_max_versions(3);
  }

  std::map<uint64_t, uint64_t> empty_snaphsots_;
  std::map<uint64_t, uint64_t> empty_rollback_;
  TableSchema schema_;
};

// prepare test data
bool PrepareTestData(TabletIO* tablet, uint64_t e, uint64_t s = 0) {
  leveldb::WriteBatch batch;
  for (uint64_t i = s; i < e; ++i) {
    std::string str = StringFormat("%011llu", i);  // NumberToString(i);
    batch.Put(str, str);
  }
  return tablet->WriteBatch(&batch);
}

TEST_F(TabletIOTest, General) {
  std::string tablet_path = working_dir + "general";
  std::string key_start = "";
  std::string key_end = "";
  StatusCode status;

  TabletIO tablet(key_start, key_end, tablet_path);
  EXPECT_TRUE(tablet.Load(TableSchema(), tablet_path, std::vector<uint64_t>(),
                          std::set<std::string>(), NULL, NULL, NULL, &status));

  std::string key = "555";
  std::string value = "value of 555";

  EXPECT_TRUE(tablet.WriteOne(key, value));

  std::string read_value;

  EXPECT_TRUE(tablet.Read(key, &read_value));

  EXPECT_EQ(value, read_value);

  EXPECT_TRUE(tablet.Unload());
}

TEST_F(TabletIOTest, Split) {
  std::string tablet_path = working_dir + "split_tablet";
  std::string key_start = "";
  std::string key_end = "";
  StatusCode status;
  uint64_t size = 0;

  TabletIO tablet(key_start, key_end, tablet_path);
  EXPECT_TRUE(tablet.Load(TableSchema(), tablet_path, std::vector<uint64_t>(),
                          std::set<std::string>(), NULL, NULL, NULL, &status));

  // prepare test data
  EXPECT_TRUE(PrepareTestData(&tablet, N));

  // for first tablet
  tablet.GetDataSize(&size, NULL, NULL, &status);
  LOG(INFO) << "table[" << key_start << ", " << key_end << "]: size = " << size;

  std::string split_key;
  EXPECT_TRUE(tablet.Split(&split_key, &status));
  LOG(INFO) << "split key = " << split_key;
  //     EXPECT_TRUE((split_key == "00000035473"));
  EXPECT_TRUE(tablet.Unload());

  // open tablet for other key scope
  key_start = "5000";
  key_end = "8000";
  TabletIO other_tablet(key_start, key_end, tablet_path);
  EXPECT_TRUE(other_tablet.Load(TableSchema(), tablet_path, std::vector<uint64_t>(),
                                std::set<std::string>(), NULL, NULL, NULL, &status));
  other_tablet.GetDataSize(&size, NULL, NULL, &status);
  LOG(INFO) << "table[" << key_start << ", " << key_end << "]: size = " << size;
  split_key.clear();
  EXPECT_TRUE(other_tablet.Split(&split_key, &status));
  LOG(INFO) << "split key = " << split_key << ", code " << StatusCodeToString(status);
  // EXPECT_LG(split_key, "6");
  EXPECT_LT(key_start, split_key);
  EXPECT_LT(split_key, key_end);
  EXPECT_TRUE(other_tablet.Unload());

  key_start = "";
  key_end = "5000";
  TabletIO l_tablet(key_start, key_end, tablet_path);
  EXPECT_TRUE(l_tablet.Load(TableSchema(), tablet_path, std::vector<uint64_t>(),
                            std::set<std::string>(), NULL, NULL, NULL, &status));
  l_tablet.GetDataSize(&size, NULL, NULL, &status);
  LOG(INFO) << "table[" << key_start << ", " << key_end << "]: size = " << size;
  EXPECT_TRUE(l_tablet.Unload());

  key_start = "8000";
  key_end = "";
  TabletIO r_tablet(key_start, key_end, tablet_path);
  EXPECT_TRUE(r_tablet.Load(TableSchema(), tablet_path, std::vector<uint64_t>(),
                            std::set<std::string>(), NULL, NULL, NULL, &status));
  r_tablet.GetDataSize(&size, NULL, NULL, &status);
  LOG(INFO) << "table[" << key_start << ", " << key_end << "]: size = " << size;
  EXPECT_TRUE(r_tablet.Unload());
}

TEST_F(TabletIOTest, SplitAndCheckSize) {
  LOG(INFO) << "SplitAndCheckSize() begin ...";
  std::string tablet_path = working_dir + "split_tablet_check";
  std::string key_start = "";
  std::string key_end = "";
  StatusCode status;
  uint64_t size = 0;

  TabletIO tablet(key_start, key_end, tablet_path);
  EXPECT_TRUE(tablet.Load(TableSchema(), tablet_path, std::vector<uint64_t>(),
                          std::set<std::string>(), NULL, NULL, NULL, &status));

  // prepare test data
  EXPECT_TRUE(PrepareTestData(&tablet, N));

  // for first tablet
  tablet.GetDataSize(&size, NULL, NULL, &status);
  LOG(INFO) << "table[" << key_start << ", " << key_end << "]: size = " << size;

  std::string split_key;
  EXPECT_TRUE(tablet.Split(&split_key));
  LOG(INFO) << "split key = " << split_key;
  LOG(INFO) << "table[" << key_start << ", " << split_key << "]";
  LOG(INFO) << "table[" << split_key << ", " << key_end << "]";
  EXPECT_TRUE(tablet.Unload());

  // open from split key to check scope size
  TabletIO l_tablet(key_start, split_key, tablet_path);
  EXPECT_TRUE(l_tablet.Load(TableSchema(), tablet_path, std::vector<uint64_t>(),
                            std::set<std::string>(), NULL, NULL, NULL, &status));
  l_tablet.GetDataSize(&size, NULL, NULL, &status);
  LOG(INFO) << "table[" << key_start << ", " << split_key << "]: size = " << size;
  EXPECT_TRUE(l_tablet.Unload());

  TabletIO r_tablet(split_key, key_end, tablet_path);
  EXPECT_TRUE(r_tablet.Load(TableSchema(), tablet_path, std::vector<uint64_t>(),
                            std::set<std::string>(), NULL, NULL, NULL, &status));
  r_tablet.GetDataSize(&size, NULL, NULL, &status);
  LOG(INFO) << "table[" << split_key << ", " << key_end << "]: size = " << size;
  EXPECT_TRUE(r_tablet.Unload());

  LOG(INFO) << "SplitAndCheckSize() end ...";
}

TEST_F(TabletIOTest, OverWrite) {
  std::string tablet_path = working_dir + "general_tablet";
  std::string key_start = "";
  std::string key_end = "";
  StatusCode status;

  TabletIO tablet(key_start, key_end, tablet_path);
  EXPECT_TRUE(tablet.Load(TableSchema(), tablet_path, std::vector<uint64_t>(),
                          std::set<std::string>(), NULL, NULL, NULL, &status));

  std::string key = "555";
  std::string value = "value of 555";
  EXPECT_TRUE(tablet.WriteOne(key, value));

  value = "value of 666";
  EXPECT_TRUE(tablet.WriteOne(key, value));

  std::string read_value;
  EXPECT_TRUE(tablet.Read(key, &read_value));

  EXPECT_EQ(value, read_value);

  EXPECT_TRUE(tablet.Unload());
}

// TEST_F(TabletIOTest, DISABLED_Compact) {
TEST_F(TabletIOTest, Compact) {
  std::string tablet_path = working_dir + "compact_tablet";
  std::string key_start = "";
  std::string key_end = "";
  StatusCode status;

  TabletIO tablet(key_start, key_end, tablet_path);
  EXPECT_TRUE(tablet.Load(TableSchema(), tablet_path, std::vector<uint64_t>(),
                          std::set<std::string>(), NULL, NULL, NULL, &status));

  // prepare test data
  EXPECT_TRUE(PrepareTestData(&tablet, 100));

  uint64_t table_size = 0;
  tablet.GetDataSize(&table_size, NULL, NULL, &status);
  LOG(INFO) << "table[" << key_start << ", " << key_end << "]: size = " << table_size;
  EXPECT_TRUE(tablet.Unload());

  // open another scope
  std::string new_key_start = StringFormat("%011llu", 5);  // NumberToString(500);
  std::string new_key_end = StringFormat("%011llu", 50);   // NumberToString(800);
  TabletIO new_tablet(new_key_start, new_key_end, tablet_path);
  EXPECT_TRUE(new_tablet.Load(TableSchema(), tablet_path, std::vector<uint64_t>(),
                              std::set<std::string>(), NULL, NULL, NULL, &status));
  EXPECT_TRUE(new_tablet.Compact(0, &status));

  uint64_t new_table_size = 0;
  new_tablet.GetDataSize(&new_table_size, NULL, NULL, &status);
  LOG(INFO) << "table[" << new_key_start << ", " << new_key_end << "]: size = " << new_table_size;

  for (int i = 0; i < 100; ++i) {
    std::string key = StringFormat("%011llu", i);  // NumberToString(i);
    std::string value;
    if (i >= 5 && i < 50) {
      EXPECT_TRUE(new_tablet.Read(key, &value));
      EXPECT_EQ(key, value);
    } else {
      EXPECT_FALSE(new_tablet.Read(key, &value));
    }
  }

  EXPECT_TRUE(new_tablet.Unload());
}

TEST_F(TabletIOTest, LowLevelSeek) {
  std::string tablet_path = working_dir + "llseek_tablet";
  std::string key_start = "";
  std::string key_end = "";
  StatusCode status;

  TabletIO tablet(key_start, key_end, tablet_path);
  EXPECT_TRUE(tablet.Load(GetTableSchema(), tablet_path, std::vector<uint64_t>(),
                          std::set<std::string>(), NULL, NULL, NULL, &status));

  // init scan
  ScanOptions scan_options;
  ColumnFamilyMap cf_map;
  std::set<std::string> qu_set;
  qu_set.insert("qualifer");
  qu_set.insert("2a");
  qu_set.insert("1a");
  cf_map["column"] = qu_set;
  scan_options.column_family_list = cf_map;
  scan_options.iter_cf_set.insert("column");

  std::string tkey1;
  // delete this key
  tablet.GetRawKeyOperator()->EncodeTeraKey("row", "", "", get_micros(), leveldb::TKT_DEL, &tkey1);
  tablet.WriteOne(tkey1, "", false, NULL);
  tablet.GetRawKeyOperator()->EncodeTeraKey("row1", "", "", get_micros(), leveldb::TKT_DEL, &tkey1);
  tablet.WriteOne(tkey1, "", false, NULL);

  // write cell
  tablet.GetRawKeyOperator()->EncodeTeraKey("row", "column", "qualifer", get_micros(),
                                            leveldb::TKT_VALUE, &tkey1);
  tablet.WriteOne(tkey1, "lala", false, NULL);
  RowResult value_list;

  EXPECT_TRUE(tablet.LowLevelSeek("row", scan_options, &value_list, &status));
  EXPECT_EQ(value_list.key_values_size(), 1);

  // delete cell
  tablet.GetRawKeyOperator()->EncodeTeraKey("row", "", "", get_micros(), leveldb::TKT_DEL, &tkey1);
  tablet.WriteOne(tkey1, "", false, NULL);
  EXPECT_TRUE(tablet.LowLevelSeek("row", scan_options, &value_list, &status));
  EXPECT_EQ(value_list.key_values_size(), 0);

  // write cell again
  tablet.GetRawKeyOperator()->EncodeTeraKey("row", "column", "2a", get_micros(), leveldb::TKT_VALUE,
                                            &tkey1);
  tablet.WriteOne(tkey1, "lala", false, NULL);
  EXPECT_TRUE(tablet.LowLevelSeek("row", scan_options, &value_list, &status));
  EXPECT_EQ(value_list.key_values_size(), 1);

  // clean
  tablet.GetRawKeyOperator()->EncodeTeraKey("row", "", "", get_micros(), leveldb::TKT_DEL, &tkey1);
  tablet.WriteOne(tkey1, "", false, NULL);
  tablet.GetRawKeyOperator()->EncodeTeraKey("row1", "", "", get_micros(), leveldb::TKT_DEL, &tkey1);
  tablet.WriteOne(tkey1, "", false, NULL);

  // write 5 versions
  tablet.GetRawKeyOperator()->EncodeTeraKey("row", "column", "1a", get_micros(), leveldb::TKT_VALUE,
                                            &tkey1);
  tablet.WriteOne(tkey1, "lala1", false, NULL);
  int64_t start_ts = get_micros();
  tablet.GetRawKeyOperator()->EncodeTeraKey("row", "column", "1a", start_ts, leveldb::TKT_VALUE,
                                            &tkey1);
  tablet.WriteOne(tkey1, "lala2", false, NULL);
  tablet.GetRawKeyOperator()->EncodeTeraKey("row", "column", "1a", get_micros(), leveldb::TKT_VALUE,
                                            &tkey1);
  tablet.WriteOne(tkey1, "lala3", false, NULL);
  int64_t end_ts = get_micros();
  tablet.GetRawKeyOperator()->EncodeTeraKey("row", "column", "1a", end_ts, leveldb::TKT_VALUE,
                                            &tkey1);
  tablet.WriteOne(tkey1, "lala4", false, NULL);
  tablet.GetRawKeyOperator()->EncodeTeraKey("row", "column", "1a", get_micros(), leveldb::TKT_VALUE,
                                            &tkey1);
  tablet.WriteOne(tkey1, "lala5", false, NULL);
  tablet.GetRawKeyOperator()->EncodeTeraKey("row1", "column", "1a", get_micros(),
                                            leveldb::TKT_VALUE, &tkey1);
  tablet.WriteOne(tkey1, "lala5", false, NULL);

  // read all versions ( write 5 versions, but schema set max_versions = 3 )
  EXPECT_TRUE(tablet.LowLevelSeek("row", scan_options, &value_list, &status));
  EXPECT_EQ(value_list.key_values_size(), 3);

  // for max_versions
  // read 2 versions
  scan_options.max_versions = 2;
  EXPECT_TRUE(tablet.LowLevelSeek("row", scan_options, &value_list, &status));
  EXPECT_EQ(value_list.key_values_size(), 2);

  // for timerange and max_versions
  // read 2 versions ( write 5 versions, but schema set max_versions = 3)
  scan_options.max_versions = 4;
  scan_options.ts_start = start_ts;
  scan_options.ts_end = end_ts;
  EXPECT_TRUE(tablet.LowLevelSeek("row", scan_options, &value_list, &status));
  EXPECT_EQ(value_list.key_values_size(), 2);

  // start_ts not in top 3 versions
  scan_options.ts_start = start_ts;
  scan_options.ts_end = start_ts;
  EXPECT_TRUE(tablet.LowLevelSeek("row", scan_options, &value_list, &status));
  EXPECT_EQ(value_list.key_values_size(), 0);

  // end_ts in top 3 versions
  scan_options.ts_start = end_ts;
  scan_options.ts_end = end_ts;
  EXPECT_TRUE(tablet.LowLevelSeek("row", scan_options, &value_list, &status));
  EXPECT_EQ(value_list.key_values_size(), 1);

  std::string tkey2;
  std::string rawkey2 = "row2";
  int64_t ts = 10;
  // write/del with the same timestamp
  tablet.GetRawKeyOperator()->EncodeTeraKey(rawkey2, "column", "Lqu0", ts,
                                            leveldb::TKT_DEL_QUALIFIERS, &tkey2);
  tablet.WriteOne(tkey2, "", false, NULL);
  tablet.GetRawKeyOperator()->EncodeTeraKey(rawkey2, "column", "Lqu0", ts, leveldb::TKT_VALUE,
                                            &tkey2);
  tablet.WriteOne(tkey2, "value0L", false, NULL);

  // Read the Del Qua
  // Get the same result with lowlevelscan
  ScanOptions scan_options2;
  ColumnFamilyMap cf_map2;
  std::set<std::string> qu_set2;
  qu_set2.insert("Lqu0");
  cf_map2["column"] = qu_set2;
  scan_options2.column_family_list = cf_map2;
  scan_options2.iter_cf_set.insert("column");
  scan_options2.ts_start = 10;
  scan_options2.ts_end = 10;
  EXPECT_TRUE(tablet.LowLevelSeek(rawkey2, scan_options2, &value_list, &status));
  EXPECT_EQ(value_list.key_values_size(), 0);

  EXPECT_TRUE(tablet.Unload());
}

TEST_F(TabletIOTest, LowLevelScan) {
  std::string tablet_path = working_dir + "llscan_tablet";
  std::string key_start = "";
  std::string key_end = "";
  StatusCode status;

  TabletIO tablet(key_start, key_end, tablet_path);
  EXPECT_TRUE(tablet.Load(GetTableSchema(), tablet_path, std::vector<uint64_t>(),
                          std::set<std::string>(), NULL, NULL, NULL, &status));

  std::string tkey1;

  // delete this key
  tablet.GetRawKeyOperator()->EncodeTeraKey("row", "", "", get_micros(), leveldb::TKT_DEL, &tkey1);
  tablet.WriteOne(tkey1, "", false, NULL);
  tablet.GetRawKeyOperator()->EncodeTeraKey("row1", "", "", get_micros(), leveldb::TKT_DEL, &tkey1);
  tablet.WriteOne(tkey1, "", false, NULL);

  // write cell
  tablet.GetRawKeyOperator()->EncodeTeraKey("row", "column", "qualifer", get_micros(),
                                            leveldb::TKT_VALUE, &tkey1);
  tablet.WriteOne(tkey1, "lala", false, NULL);

  std::string start_tera_key;
  std::string end_row_key;
  RowResult value_list;
  KeyValuePair next_start_point;
  uint32_t read_row_count = 0;
  uint32_t read_cell_count = 0;
  uint32_t read_bytes = 0;
  bool is_complete = false;
  EXPECT_TRUE(tablet.LowLevelScan(start_tera_key, "", ScanOptions(), &value_list, &next_start_point,
                                  &read_row_count, &read_cell_count, &read_bytes, &is_complete,
                                  &status));
  EXPECT_EQ(value_list.key_values_size(), 1);

  tablet.GetRawKeyOperator()->EncodeTeraKey("row", "", "", get_micros(), leveldb::TKT_DEL, &tkey1);
  tablet.WriteOne(tkey1, "lala", false, NULL);
  EXPECT_TRUE(tablet.LowLevelScan(start_tera_key, "", ScanOptions(), &value_list, &next_start_point,
                                  &read_row_count, &read_cell_count, &read_bytes, &is_complete,
                                  &status));
  EXPECT_EQ(value_list.key_values_size(), 0);

  tablet.GetRawKeyOperator()->EncodeTeraKey("row", "column", "2a", get_micros(), leveldb::TKT_VALUE,
                                            &tkey1);
  tablet.WriteOne(tkey1, "lala", false, NULL);
  EXPECT_TRUE(tablet.LowLevelScan(start_tera_key, "", ScanOptions(), &value_list, &next_start_point,
                                  &read_row_count, &read_cell_count, &read_bytes, &is_complete,
                                  &status));
  EXPECT_EQ(value_list.key_values_size(), 1);

  tablet.GetRawKeyOperator()->EncodeTeraKey("row", "", "", get_micros(), leveldb::TKT_DEL, &tkey1);
  tablet.WriteOne(tkey1, "lala", false, NULL);
  tablet.GetRawKeyOperator()->EncodeTeraKey("row1", "", "", get_micros(), leveldb::TKT_DEL, &tkey1);
  tablet.WriteOne(tkey1, "lala", false, NULL);

  tablet.GetRawKeyOperator()->EncodeTeraKey("row", "column", "1a", get_micros(), leveldb::TKT_VALUE,
                                            &tkey1);
  tablet.WriteOne(tkey1, "lala", false, NULL);
  tablet.GetRawKeyOperator()->EncodeTeraKey("row", "column", "1a", get_micros(), leveldb::TKT_VALUE,
                                            &tkey1);
  tablet.WriteOne(tkey1, "lala", false, NULL);
  tablet.GetRawKeyOperator()->EncodeTeraKey("row", "column", "1a", get_micros(), leveldb::TKT_VALUE,
                                            &tkey1);
  tablet.WriteOne(tkey1, "lala", false, NULL);

  tablet.GetRawKeyOperator()->EncodeTeraKey("row1", "column", "1a", get_micros(),
                                            leveldb::TKT_VALUE, &tkey1);
  tablet.WriteOne(tkey1, "lala", false, NULL);
  tablet.GetRawKeyOperator()->EncodeTeraKey("row1", "column", "2b", get_micros(),
                                            leveldb::TKT_VALUE, &tkey1);
  tablet.WriteOne(tkey1, "lala", false, NULL);
  tablet.GetRawKeyOperator()->EncodeTeraKey("row", "", "", 0, leveldb::TKT_FORSEEK,
                                            &start_tera_key);
  end_row_key = std::string("row1\0", 5);
  ScanOptions scan_options;
  EXPECT_TRUE(tablet.LowLevelScan(start_tera_key, end_row_key, scan_options, &value_list,
                                  &next_start_point, &read_row_count, &read_cell_count, &read_bytes,
                                  &is_complete, &status));
  EXPECT_EQ(value_list.key_values_size(), 5);
  tablet.GetRawKeyOperator()->EncodeTeraKey("row", "", "", 0, leveldb::TKT_FORSEEK,
                                            &start_tera_key);
  end_row_key = std::string("row\0", 5);
  scan_options.column_family_list["column"].insert("1a");
  EXPECT_TRUE(tablet.LowLevelScan(start_tera_key, end_row_key, scan_options, &value_list,
                                  &next_start_point, &read_row_count, &read_cell_count, &read_bytes,
                                  &is_complete, &status));
  EXPECT_EQ(value_list.key_values_size(), 3);
  scan_options.max_versions = 2;
  EXPECT_TRUE(tablet.LowLevelScan(start_tera_key, end_row_key, scan_options, &value_list,
                                  &next_start_point, &read_row_count, &read_cell_count, &read_bytes,
                                  &is_complete, &status));
  EXPECT_EQ(value_list.key_values_size(), 2);

  std::string rawkey2 = "row2";
  int64_t ts = 10;
  // write/del with the same timestamp
  tablet.GetRawKeyOperator()->EncodeTeraKey(rawkey2, "column", "Lqu0", ts,
                                            leveldb::TKT_DEL_QUALIFIERS, &start_tera_key);
  tablet.WriteOne(start_tera_key, "", false, NULL);
  tablet.GetRawKeyOperator()->EncodeTeraKey(rawkey2, "column", "Lqu0", ts, leveldb::TKT_VALUE,
                                            &start_tera_key);
  tablet.WriteOne(start_tera_key, "value0L", false, NULL);

  // Scan the row where put and del with the same timestamp.
  ScanOptions scan_options2;
  end_row_key = std::string("row2\0", 5);
  // Scan All Cf
  EXPECT_TRUE(tablet.LowLevelScan(start_tera_key, end_row_key, scan_options2, &value_list,
                                  &next_start_point, &read_row_count, &read_cell_count, &read_bytes,
                                  &is_complete, &status));
  EXPECT_EQ(value_list.key_values_size(), 0);
  // Scan column cf
  std::set<std::string> qu_set;
  ColumnFamilyMap cf_map;
  cf_map["column"] = qu_set;
  scan_options2.column_family_list = cf_map;
  EXPECT_TRUE(tablet.LowLevelScan(start_tera_key, end_row_key, scan_options2, &value_list,
                                  &next_start_point, &read_row_count, &read_cell_count, &read_bytes,
                                  &is_complete, &status));
  EXPECT_EQ(value_list.key_values_size(), 0);
  // Scan column && qua
  scan_options2.column_family_list["column"].insert("Lqu0");
  EXPECT_TRUE(tablet.LowLevelScan(start_tera_key, end_row_key, scan_options2, &value_list,
                                  &next_start_point, &read_row_count, &read_cell_count, &read_bytes,
                                  &is_complete, &status));
  EXPECT_EQ(value_list.key_values_size(), 0);

  EXPECT_TRUE(tablet.Unload());
}

TEST_F(TabletIOTest, SplitToSubTable) {
  LOG(INFO) << "SplitToSubTable() begin ...";
  std::string tablet_path = leveldb::GetTabletPathFromNum(working_dir, 1);
  std::string key_start = "";
  std::string key_end = "";
  StatusCode status;
  uint64_t size = 0;

  TabletIO tablet(key_start, key_end, tablet_path);
  EXPECT_TRUE(tablet.Load(TableSchema(), tablet_path, std::vector<uint64_t>(),
                          std::set<std::string>(), NULL, NULL, NULL, &status));

  // prepare test data
  EXPECT_TRUE(PrepareTestData(&tablet, N / 2, 0));
  EXPECT_TRUE(PrepareTestData(&tablet, N, N / 2));

  // make sure all data are dumped into sst
  EXPECT_TRUE(tablet.Unload());
  EXPECT_TRUE(tablet.Load(TableSchema(), tablet_path, std::vector<uint64_t>(),
                          std::set<std::string>(), NULL, NULL, NULL, &status));

  // for first tablet
  tablet.GetDataSize(&size, NULL, NULL, &status);
  LOG(INFO) << "table[" << key_start << ", " << key_end << "]: size = " << size;

  std::string split_key;
  EXPECT_TRUE(tablet.Split(&split_key));
  LOG(INFO) << "split key = " << split_key;
  LOG(INFO) << "table[" << key_start << ", " << split_key << "]";
  LOG(INFO) << "table[" << split_key << ", " << key_end << "]";
  EXPECT_TRUE(tablet.Unload());

  // open from split key to check scope size
  std::string split_path_1;
  std::string split_path_2;
  split_path_1 = leveldb::GetTabletPathFromNum(working_dir, 2);
  split_path_2 = leveldb::GetTabletPathFromNum(working_dir, 3);
  // ASSERT_TRUE(leveldb::GetSplitPath(tablet_path, &split_path_1,
  // &split_path_2));
  LOG(INFO) << tablet_path << ", lpath " << split_path_1 << ", rpath " << split_path_2 << "\n";
  std::vector<uint64_t> parent_tablet;
  parent_tablet.push_back(1);

  // 1. load sub-table 1
  TabletIO l_tablet(key_start, split_key, split_path_1);
  EXPECT_TRUE(l_tablet.Load(TableSchema(), split_path_1, parent_tablet, std::set<std::string>(),
                            NULL, NULL, NULL, &status));
  l_tablet.GetDataSize(&size, NULL, NULL, &status);
  LOG(INFO) << "table[" << key_start << ", " << split_key << "]: size = " << size;
  // varify result
  int split_key_num = atoi(split_key.c_str());
  LOG(INFO) << "split_key_num " << split_key_num;
  for (uint64_t i = 0; i < (uint64_t)split_key_num; ++i) {
    std::string key = StringFormat("%011llu", i);
    std::string value;
    EXPECT_TRUE(l_tablet.Read(key, &value));
    ASSERT_EQ(key, value);
  }
  EXPECT_TRUE(l_tablet.Unload());

  // 2. load sub-table 2
  TabletIO r_tablet(split_key, key_end, split_path_2);
  EXPECT_TRUE(r_tablet.Load(TableSchema(), split_path_2, parent_tablet, std::set<std::string>(),
                            NULL, NULL, NULL, &status));
  r_tablet.GetDataSize(&size, NULL, NULL, &status);
  LOG(INFO) << "table[" << split_key << ", " << key_end << "]: size = " << size;
  // varify result
  for (uint64_t i = (uint64_t)split_key_num; i < N; ++i) {
    std::string key = StringFormat("%011llu", i);
    std::string value;
    EXPECT_TRUE(r_tablet.Read(key, &value));
    ASSERT_EQ(key, value);
  }
  EXPECT_TRUE(r_tablet.Unload());

  LOG(INFO) << "SplitToSubTable() end ...";
}

TEST_F(TabletIOTest, FindAverageKey) {
  std::string start, end, ave;

  start = "abc";
  end = "abe";
  ASSERT_TRUE(TabletIO::FindAverageKey(start, end, &ave));
  // ASSERT_EQ(ave, "abd");
  ASSERT_LT(start, ave);
  ASSERT_LT(ave, end);

  start = "helloa";
  end = "hellob";
  ASSERT_TRUE(TabletIO::FindAverageKey(start, end, &ave));
  ASSERT_EQ(ave, "helloa\x80");

  start = "a";
  end = "b";
  ASSERT_TRUE(TabletIO::FindAverageKey(start, end, &ave));
  ASSERT_EQ(ave, "a\x80");

  start = "a";
  // b(0x62), 1(0x31)
  end = "ab";
  ASSERT_TRUE(TabletIO::FindAverageKey(start, end, &ave));
  ASSERT_LT(start, ave);
  ASSERT_LT(ave, end);

  // _(0x5F)
  start = "a\x10";
  end = "b";
  ASSERT_TRUE(TabletIO::FindAverageKey(start, end, &ave));
  // ASSERT_EQ(ave, "a\x88");
  ASSERT_LT(start, ave);
  ASSERT_LT(ave, end);

  start = "";
  end = "";
  ASSERT_TRUE(TabletIO::FindAverageKey(start, end, &ave));
  ASSERT_EQ(ave, "\x7F");

  start = "";
  end = "b";
  ASSERT_TRUE(TabletIO::FindAverageKey(start, end, &ave));
  ASSERT_EQ(ave[0], '1');
  ASSERT_EQ(ave[1], '\0');

  start = "b";
  end = "";
  ASSERT_TRUE(TabletIO::FindAverageKey(start, end, &ave));
  // ASSERT_EQ(ave, "\xb0");
  ASSERT_LT(start, ave);
  ASSERT_NE(ave, start);
  std::cout << DebugString(start) << ", " << DebugString(ave) << ", " << std::endl;

  start = "000000000000001480186993";
  end = "000000000000002147352684";
  ASSERT_TRUE(TabletIO::FindAverageKey(start, end, &ave));
  ASSERT_LT(start, ave);
  ASSERT_LT(ave, end);

  start = std::string("000017\xF0");
  end = "000018000000001397050688";
  ASSERT_TRUE(TabletIO::FindAverageKey(start, end, &ave));
  ASSERT_LT(start, ave);
  ASSERT_LT(ave, end);

  start = std::string("0000\177");
  end = std::string("0000\200");
  ASSERT_TRUE(TabletIO::FindAverageKey(start, end, &ave));
  ASSERT_LT(start, ave);
  ASSERT_LT(ave, end);

  start = "";
  end = "\x1";
  ASSERT_TRUE(TabletIO::FindAverageKey(start, end, &ave));
  ASSERT_EQ(ave, std::string("\x0", 1));

  start = "";
  end = std::string("\x0", 1);
  ASSERT_FALSE(TabletIO::FindAverageKey(start, end, &ave));

  start = "aaa";
  end = "aaa";
  end.append(1, '\0');
  ASSERT_FALSE(TabletIO::FindAverageKey(start, end, &ave));

  start = "a\xff\xff";
  end = "b";
  ASSERT_TRUE(TabletIO::FindAverageKey(start, end, &ave));
  ASSERT_EQ(ave, "a\xff\xff\x80");
}

static void TabletUnloadWapper(TabletIO* tablet) { tablet->Unload(); }

TEST_F(TabletIOTest, TryUnload) {
  std::string tablet_path = working_dir + "unload_try";
  std::string key_start = "";
  std::string key_end = "";
  StatusCode status;

  TabletIO tablet(key_start, key_end, tablet_path);
  EXPECT_TRUE(tablet.Load(GetTableSchema(), tablet_path, std::vector<uint64_t>(),
                          std::set<std::string>(), NULL, NULL, NULL, &status));
  tablet.db_ref_count_++;
  std::vector<std::thread> threads;
  threads.reserve(2);
  EXPECT_TRUE(tablet.try_unload_count_ == 0);
  for (int i = 0; i < 2; ++i) {
    threads.push_back(std::thread(&TabletUnloadWapper, &tablet));
  }
  sleep(2);
  EXPECT_TRUE(tablet.try_unload_count_ == 2);
  tablet.db_ref_count_--;
  for (int i = 0; i < 2; ++i) {
    threads[i].join();
  }
  threads.clear();
}

TEST_F(TabletIOTest, OnSlowUnloadOP) {
  std::string tablet_path = working_dir + "unload_slow_op";
  std::string key_start = "";
  std::string key_end = "";
  StatusCode status;

  TabletIO tablet(key_start, key_end, tablet_path);
  EXPECT_TRUE(tablet.Load(GetTableSchema(), tablet_path, std::vector<uint64_t>(),
                          std::set<std::string>(), NULL, NULL, NULL, &status));
  tablet.db_ref_count_++;
  std::vector<std::thread> threads;
  threads.reserve(3);
  EXPECT_TRUE(tablet.try_unload_count_ == 0);
  for (int i = 0; i < 3; ++i) {
    threads.push_back(std::thread(&TabletUnloadWapper, &tablet));
  }
  sleep(5);
  EXPECT_TRUE(tablet.try_unload_count_ == 3);
  uint64_t size = 0;
  std::vector<uint64_t> lgsize;
  // GetDataSize
  EXPECT_TRUE(tablet.GetDataSize(&size, &lgsize, NULL, &status) == false);
  EXPECT_TRUE(size == 0);
  EXPECT_TRUE(lgsize.size() == 0);

  // LowLevelScan

  std::string start_tera_key;
  tablet.GetRawKeyOperator()->EncodeTeraKey("123213", "", "", kLatestTs, leveldb::TKT_FORSEEK,
                                            &start_tera_key);
  std::string end_row_key = "123213" + '\0';

  RowResult value_list;
  KeyValuePair next_start_point;
  uint32_t read_row_count = 0;
  uint32_t read_cell_count = 0;
  uint32_t read_bytes = 0;
  bool is_complete = false;
  status = kTabletNodeOk;
  EXPECT_FALSE(tablet.LowLevelScan(start_tera_key, end_row_key, ScanOptions(), &value_list,
                                   &next_start_point, &read_row_count, &read_cell_count,
                                   &read_bytes, &is_complete, &status));
  EXPECT_TRUE(status == kKeyNotInRange);
  status = kTabletNodeOk;

  // LowLevelSeek
  EXPECT_FALSE(tablet.LowLevelSeek("row", ScanOptions(), &value_list, &status));
  EXPECT_TRUE(status == kKeyNotInRange);
  status = kTabletNodeOk;

  // ReadRows
  RowReaderInfo row_reader;
  EXPECT_FALSE(tablet.ReadCells(row_reader, &value_list, 0, &status, 1000));
  EXPECT_TRUE(status != kTabletNodeOk);
  status = kTabletNodeOk;

  tablet.db_ref_count_--;
  for (int i = 0; i < 3; ++i) {
    threads[i].join();
  }
  threads.clear();
}

TEST_F(TabletIOTest, RowBloomFilter) {
  const int32_t NR = 10000;
  const int32_t CR = 10;
  std::string tablet_path = working_dir + "row_bloomfilter";
  std::string key_start = "";
  std::string key_end = "";
  StatusCode status;

  TabletIO tablet(key_start, key_end, tablet_path);
  EXPECT_TRUE(tablet.Load(GetTableSchema(), tablet_path, std::vector<uint64_t>(),
                          std::set<std::string>(), NULL, NULL, NULL, &status));

  // prepare data
  leveldb::WriteBatch batch;
  for (int32_t i = 0; i < NR; i++) {
    char buf[16];
    snprintf(buf, sizeof(buf), "%06d", i);
    std::string row(buf);

    for (int32_t j = 0; j < CR; j++) {
      char buf[16];
      snprintf(buf, sizeof(buf), "%03d", j);
      std::string col(buf);

      std::string tera_key;
      tablet.GetRawKeyOperator()->EncodeTeraKey(row, "column", col, get_micros(),
                                                leveldb::TKT_VALUE, &tera_key);
      batch.Put(tera_key, "");
    }
  }
  ASSERT_TRUE(tablet.WriteBatch(&batch, false, true, NULL));

  // read and verify
  for (int32_t i = 0; i < NR; i++) {
    char buf[16];
    snprintf(buf, sizeof(buf), "%06d", i);
    std::string row(buf);

    std::string start_tera_key;
    tablet.GetRawKeyOperator()->EncodeTeraKey(row, "", "", kLatestTs, leveldb::TKT_FORSEEK,
                                              &start_tera_key);
    std::string end_row_key = row + '\0';

    RowResult value_list;
    KeyValuePair next_start_point;
    uint32_t read_row_count = 0;
    uint32_t read_cell_count = 0;
    uint32_t read_bytes = 0;
    bool is_complete = false;
    ASSERT_TRUE(tablet.LowLevelScan(start_tera_key, end_row_key, ScanOptions(), &value_list,
                                    &next_start_point, &read_row_count, &read_cell_count,
                                    &read_bytes, &is_complete, &status));
    ASSERT_EQ(value_list.key_values_size(), CR);
    for (int32_t j = 0; j < CR; j++) {
      char buf[16];
      snprintf(buf, sizeof(buf), "%03d", j);
      std::string col(buf);

      const KeyValuePair& kv = value_list.key_values(j);
      EXPECT_EQ(kv.key(), row);
      EXPECT_EQ(kv.qualifier(), col);
    }
  }
}

class TabletIOKVOnlyTest : public ::testing::Test {
 public:
  TabletIOKVOnlyTest() {
    std::string cmd = std::string("mkdir -p ") + working_dir;
    FLAGS_tera_tabletnode_path_prefix = "./";
    system(cmd.c_str());

    InitSchema();
  }

  ~TabletIOKVOnlyTest() {
    std::cout << "kvonly clean" << std::endl;
    ;
    std::string cmd = std::string("rm -rf ") + working_dir;
    system(cmd.c_str());
  }

  const TableSchema& GetTableSchema() { return schema_; }

  void InitSchema() {
    schema_.set_name("terakv");
    schema_.set_raw_key(GeneralKv);
  }

  std::map<uint64_t, uint64_t> empty_snaphsots_;
  std::map<uint64_t, uint64_t> empty_rollback_;
  TableSchema schema_;
};

TEST_F(TabletIOKVOnlyTest, KvTableScan) {
  std::string tablet_path = working_dir + "kvscan_tablet";
  std::string key_start = "";
  std::string key_end = "";
  StatusCode status;

  TabletIO tablet(key_start, key_end, tablet_path);
  EXPECT_TRUE(tablet.Load(GetTableSchema(), tablet_path, std::vector<uint64_t>(),
                          std::set<std::string>(), NULL, NULL, NULL, &status));

  uint32_t read_row_count = 0;
  uint32_t read_bytes = 0;

  // case 1, empty table
  RowResult value_list;
  ScanContext* scan_context = new ScanContext;
  scan_context->start_tera_key = "";
  scan_context->end_row_key = "";
  scan_context->scan_options = ScanOptions();
  scan_context->it = NULL;
  scan_context->result = &value_list;
  scan_context->ret_code = kTabletNodeOk;
  scan_context->data_idx = 0;
  scan_context->complete = false;
  scan_context->compact_strategy = tablet.ldb_options_.compact_strategy_factory->NewInstance();
  tablet.InitScanIterator(scan_context->start_tera_key, scan_context->end_row_key,
                          scan_context->scan_options, &(scan_context->it));
  std::cout << "kv scan test1" << std::endl;
  EXPECT_TRUE(tablet.KvTableScan(scan_context, &read_row_count, &read_bytes));
  EXPECT_EQ(value_list.key_values_size(), 0);
  delete scan_context->it;  // for db iterator
  delete scan_context;

  // case 2, 5 row, scan "" ""
  tablet.WriteOne("row1", "helloword1", false, NULL);
  tablet.WriteOne("row2", "helloword2", false, NULL);
  tablet.WriteOne("row3", "helloword3", false, NULL);
  tablet.WriteOne("row4", "helloword4", false, NULL);
  tablet.WriteOne("row5", "helloword5", false, NULL);

  value_list.clear_key_values();
  scan_context = new ScanContext;
  scan_context->start_tera_key = "";
  scan_context->end_row_key = "";
  scan_context->scan_options = ScanOptions();
  scan_context->it = NULL;
  scan_context->result = &value_list;
  scan_context->ret_code = kTabletNodeOk;
  scan_context->data_idx = 0;
  scan_context->complete = false;
  scan_context->compact_strategy = tablet.ldb_options_.compact_strategy_factory->NewInstance();
  tablet.InitScanIterator(scan_context->start_tera_key, scan_context->end_row_key,
                          scan_context->scan_options, &(scan_context->it));
  std::cout << "kv scan test2" << std::endl;
  EXPECT_TRUE(tablet.KvTableScan(scan_context, &read_row_count, &read_bytes));
  EXPECT_EQ(value_list.key_values_size(), 5);
  for (int32_t j = 0; j < 5; j++) {
    char buf[16];
    snprintf(buf, sizeof(buf), "%03d", j);
    std::string col(buf);

    const KeyValuePair& kv = value_list.key_values(j);
    std::string row = "row" + std::to_string(j + 1);
    std::cout << kv.key() << row << std::endl;
    EXPECT_EQ(kv.key(), row);
    std::string value = "helloword" + std::to_string(j + 1);
    std::cout << kv.value() << value << std::endl;
    EXPECT_EQ(kv.value(), value);
  }
  delete scan_context->it;  // for db iterator
  delete scan_context;

  // case 3, scan "row1" "row3"
  value_list.clear_key_values();
  scan_context = new ScanContext;
  scan_context->start_tera_key = "row1";
  scan_context->end_row_key = "row3";
  scan_context->scan_options = ScanOptions();
  scan_context->it = NULL;
  scan_context->result = &value_list;
  scan_context->ret_code = kTabletNodeOk;
  scan_context->data_idx = 0;
  scan_context->complete = false;
  scan_context->compact_strategy = tablet.ldb_options_.compact_strategy_factory->NewInstance();
  tablet.InitScanIterator(scan_context->start_tera_key, scan_context->end_row_key,
                          scan_context->scan_options, &(scan_context->it));
  std::cout << "kv scan test3" << std::endl;
  EXPECT_TRUE(tablet.KvTableScan(scan_context, &read_row_count, &read_bytes));
  EXPECT_EQ(value_list.key_values_size(), 2);

  delete scan_context->it;  // for db iterator
  delete scan_context;
  // EXPECT_TRUE(tablet.Unload());
}

}  // namespace io
}  // namespace tera

int main(int argc, char** argv) {
  FLAGS_tera_io_retry_max_times = 1;
  FLAGS_tera_tablet_living_period = 0;
  FLAGS_tera_tablet_max_write_buffer_size = 1;
  FLAGS_tera_leveldb_env_type = "local";
  ::google::InitGoogleLogging(argv[0]);
  FLAGS_log_dir = "./log";
  if (access(FLAGS_log_dir.c_str(), F_OK)) {
    mkdir(FLAGS_log_dir.c_str(), 0777);
  }
  std::string pragram_name("tera");
  tera::utils::SetupLog(pragram_name);
  ::google::ParseCommandLineFlags(&argc, &argv, true);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
