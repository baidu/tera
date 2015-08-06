// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "io/tablet_io.h"
#include "utils/timer.h"

#include "common/base/scoped_ptr.h"
#include "common/base/string_format.h"
#include "common/base/string_number.h"
#include "gflags/gflags.h"
#include "glog/logging.h"
#include "gtest/gtest.h"
#include "leveldb/table_utils.h"
#include "leveldb/raw_key_operator.h"

#include "proto/status_code.pb.h"
#include "proto/proto_helper.h"

DECLARE_string(tera_tabletnode_path_prefix);
DECLARE_int32(tera_io_retry_max_times);
DECLARE_int64(tera_tablet_living_period);
DECLARE_string(tera_leveldb_env_type);

DECLARE_int64(tera_tablet_write_buffer_size);

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

    const TableSchema& GetTableSchema() {
        return schema_;

    }

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
    TableSchema schema_;
};

TEST_F(TabletIOTest, General) {
    std::string tablet_path = working_dir + "general";
    std::string key_start = "";
    std::string key_end = "";

    TabletIO tablet;
    EXPECT_TRUE(tablet.Load(TableSchema(), key_start, key_end, tablet_path, std::vector<uint64_t>(), empty_snaphsots_));

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

    TabletIO tablet;
    EXPECT_TRUE(tablet.Load(TableSchema(), key_start, key_end, tablet_path, std::vector<uint64_t>(), empty_snaphsots_));

    // prepare test data
    for (uint32_t i = 0; i < N; ++i) {
        std::string str = StringFormat("%011llu", i); // NumberToString(i);
        EXPECT_TRUE(tablet.WriteOne(str, str));
//         if (i % 10 == 0) {
//             LOG(INFO) << "already write: " << i;
//         }
    }

    // for first tablet

    LOG(INFO) << "table[" << key_start << ", " << key_end
        << "]: size = " << tablet.GetDataSize();

    std::string split_key;
//     EXPECT_TRUE(tablet.Split(&split_key));
    LOG(INFO) << "split key = " << split_key;
//     EXPECT_TRUE((split_key == "00000035473"));
    EXPECT_TRUE(tablet.Unload());

    // open tablet for other key scope

    key_start = "5000";
    key_end = "8000";
    EXPECT_TRUE(tablet.Load(TableSchema(), key_start, key_end, tablet_path, std::vector<uint64_t>(), empty_snaphsots_));
    LOG(INFO) << "table[" << key_start << ", " << key_end
        << "]: size = " << tablet.GetDataSize();
    split_key.clear();
    EXPECT_FALSE(tablet.Split(&split_key));
    LOG(INFO) << "split key = " << split_key;
    EXPECT_TRUE((split_key == ""));
    EXPECT_TRUE(tablet.Unload());

    key_start = "";
    key_end = "5000";
    EXPECT_TRUE(tablet.Load(TableSchema(), key_start, key_end, tablet_path, std::vector<uint64_t>(), empty_snaphsots_));
    LOG(INFO) << "table[" << key_start << ", " << key_end
        << "]: size = " << tablet.GetDataSize();
    EXPECT_TRUE(tablet.Unload());

    key_start = "8000";
    key_end = "";
    EXPECT_TRUE(tablet.Load(TableSchema(), key_start, key_end, tablet_path, std::vector<uint64_t>(), empty_snaphsots_));
    LOG(INFO) << "table[" << key_start << ", " << key_end
        << "]: size = " << tablet.GetDataSize();
    EXPECT_TRUE(tablet.Unload());
}

TEST_F(TabletIOTest, SplitAndCheckSize) {
    LOG(INFO) << "SplitAndCheckSize() begin ...";
    std::string tablet_path = working_dir + "split_tablet_check";
    std::string key_start = "";
    std::string key_end = "";

    TabletIO tablet;
    EXPECT_TRUE(tablet.Load(TableSchema(), key_start, key_end, tablet_path, std::vector<uint64_t>(), empty_snaphsots_));

    // prepare test data
    for (uint32_t i = 0; i < N; ++i) {
        std::string str = StringFormat("%011llu", i); // NumberToString(i);
        EXPECT_TRUE(tablet.WriteOne(str, str));
//         if (i % 10 == 0) {
//             LOG(INFO) << "already write: " << i;
//         }
    }

    // for first tablet

    LOG(INFO) << "table[" << key_start << ", " << key_end
        << "]: size = " << tablet.GetDataSize();

    std::string split_key;
//     EXPECT_TRUE(tablet.Split(&split_key));
    LOG(INFO) << "split key = " << split_key;
    LOG(INFO) << "table[" << key_start << ", " << split_key
        << "]: size = " << tablet.GetDataSize(key_start, split_key);
    LOG(INFO) << "table[" << split_key << ", " << key_end
        << "]: size = " << tablet.GetDataSize(split_key, key_end);
    EXPECT_TRUE(tablet.Unload());

    // open from split key to check scope size
    EXPECT_TRUE(tablet.Load(TableSchema(), key_start, split_key, tablet_path, std::vector<uint64_t>(), empty_snaphsots_));
    LOG(INFO) << "table[" << key_start << ", " << split_key
        << "]: size = " << tablet.GetDataSize();
    EXPECT_TRUE(tablet.Unload());

    EXPECT_TRUE(tablet.Load(TableSchema(), split_key, key_end, tablet_path, std::vector<uint64_t>(), empty_snaphsots_));
    LOG(INFO) << "table[" << split_key << ", " << key_end
        << "]: size = " << tablet.GetDataSize();
    EXPECT_TRUE(tablet.Unload());

    LOG(INFO) << "SplitAndCheckSize() end ...";
}

TEST_F(TabletIOTest, OverWrite) {
    std::string tablet_path = working_dir + "general_tablet";
    std::string key_start = "";
    std::string key_end = "";

    TabletIO tablet;
    EXPECT_TRUE(tablet.Load(TableSchema(), key_start, key_end, tablet_path, std::vector<uint64_t>(), empty_snaphsots_));

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

TEST_F(TabletIOTest, DISABLED_Compact) {
    std::string tablet_path = working_dir + "compact_tablet";
    std::string key_start = "";
    std::string key_end = "";

    TabletIO tablet;
    EXPECT_TRUE(tablet.Load(TableSchema(), key_start, key_end, tablet_path, std::vector<uint64_t>(), empty_snaphsots_));

    // prepare test data
    for (int i = 0; i < 100; ++i) {
        std::string str = StringFormat("%011llu", i); // NumberToString(i);
        EXPECT_TRUE(tablet.WriteOne(str, str));
    }

    int64_t table_size = tablet.GetDataSize();
    LOG(INFO) << "table[" << key_start << ", " << key_end
        << "]: size = " << table_size;
    EXPECT_TRUE(tablet.Unload());

    // open another scope
    std::string new_key_start = StringFormat("%011llu", 5); // NumberToString(500);
    std::string new_key_end = StringFormat("%011llu", 50); // NumberToString(800);
    EXPECT_TRUE(tablet.Load(TableSchema(), key_start, key_end, tablet_path, std::vector<uint64_t>(), empty_snaphsots_));
    EXPECT_TRUE(tablet.Compact());

    int64_t new_table_size = tablet.GetDataSize();
    LOG(INFO) << "table[" << new_key_start << ", " << new_key_end
        << "]: size = " << new_table_size;

    for (int i = 0; i < 100; ++i) {
        std::string key = StringFormat("%011llu", i); // NumberToString(i);
        std::string value;
        if (i >= 5 && i < 50) {
            EXPECT_TRUE(tablet.Read(key, &value));
            EXPECT_EQ(key, value);
        } else {
            EXPECT_FALSE(tablet.Read(key, &value));
        }
    }

    EXPECT_TRUE(tablet.Unload());
}

TEST_F(TabletIOTest, LowLevelScan) {
    std::string tablet_path = working_dir + "llscan_tablet";
    std::string key_start = "";
    std::string key_end = "";

    TabletIO tablet;
    EXPECT_TRUE(tablet.Load(GetTableSchema(), key_start, key_end, tablet_path, std::vector<uint64_t>(), empty_snaphsots_));
    std::string tkey1;

    // delete this key
    tablet.GetRawKeyOperator()->EncodeTeraKey("row", "", "", get_micros(), leveldb::TKT_DEL, &tkey1);
    tablet.WriteOne(tkey1, "" , false, NULL);
    tablet.GetRawKeyOperator()->EncodeTeraKey("row1", "", "", get_micros(), leveldb::TKT_DEL, &tkey1);
    tablet.WriteOne(tkey1, "" , false, NULL);


    // write cell
    tablet.GetRawKeyOperator()->EncodeTeraKey("row", "column", "qualifer", get_micros(), leveldb::TKT_VALUE, &tkey1);
    tablet.WriteOne(tkey1, "lala" , false, NULL);

    std::string start_tera_key;
    std::string end_row_key;
    RowResult value_list;
    uint32_t read_row_count = 0;
    uint32_t read_bytes = 0;
    bool is_complete = false;
    EXPECT_TRUE(tablet.LowLevelScan(start_tera_key, "", TabletIO::ScanOptions(),
                                    &value_list, &read_row_count, &read_bytes, &is_complete, NULL));
    EXPECT_EQ(value_list.key_values_size(), 1);

    tablet.GetRawKeyOperator()->EncodeTeraKey("row", "", "", get_micros(), leveldb::TKT_DEL, &tkey1);
    tablet.WriteOne(tkey1, "lala" , false, NULL);
    EXPECT_TRUE(tablet.LowLevelScan(start_tera_key, "", TabletIO::ScanOptions(),
                                    &value_list, &read_row_count, &read_bytes, &is_complete, NULL));
    EXPECT_EQ(value_list.key_values_size(), 0);

    tablet.GetRawKeyOperator()->EncodeTeraKey("row", "column", "2a", get_micros(), leveldb::TKT_VALUE, &tkey1);
    tablet.WriteOne(tkey1, "lala" , false, NULL);
    EXPECT_TRUE(tablet.LowLevelScan(start_tera_key, "", TabletIO::ScanOptions(),
                                    &value_list, &read_row_count, &read_bytes, &is_complete, NULL));
    EXPECT_EQ(value_list.key_values_size(), 1);

    tablet.GetRawKeyOperator()->EncodeTeraKey("row", "", "", get_micros(), leveldb::TKT_DEL, &tkey1);
    tablet.WriteOne(tkey1, "lala", false, NULL);
    tablet.GetRawKeyOperator()->EncodeTeraKey("row1", "", "", get_micros(), leveldb::TKT_DEL, &tkey1);
    tablet.WriteOne(tkey1, "lala", false, NULL);

    tablet.GetRawKeyOperator()->EncodeTeraKey("row", "column", "1a", get_micros(), leveldb::TKT_VALUE, &tkey1);
    tablet.WriteOne(tkey1, "lala", false, NULL);
    tablet.GetRawKeyOperator()->EncodeTeraKey("row", "column", "1a", get_micros(), leveldb::TKT_VALUE, &tkey1);
    tablet.WriteOne(tkey1, "lala", false, NULL);
    tablet.GetRawKeyOperator()->EncodeTeraKey("row", "column", "1a", get_micros(), leveldb::TKT_VALUE, &tkey1);
    tablet.WriteOne(tkey1, "lala", false, NULL);

    tablet.GetRawKeyOperator()->EncodeTeraKey("row1", "column", "1a", get_micros(), leveldb::TKT_VALUE, &tkey1);
    tablet.WriteOne(tkey1, "lala", false, NULL);
    tablet.GetRawKeyOperator()->EncodeTeraKey("row1", "column", "2b", get_micros(), leveldb::TKT_VALUE, &tkey1);
    tablet.WriteOne(tkey1, "lala", false, NULL);
    tablet.GetRawKeyOperator()->EncodeTeraKey("row", "", "", 0, leveldb::TKT_FORSEEK, &start_tera_key);
    end_row_key = std::string("row1\0", 5);
    TabletIO::ScanOptions scan_options;
    EXPECT_TRUE(tablet.LowLevelScan(start_tera_key, end_row_key, scan_options,
                                    &value_list, &read_row_count, &read_bytes, &is_complete, NULL));
    EXPECT_EQ(value_list.key_values_size(), 5);
    tablet.GetRawKeyOperator()->EncodeTeraKey("row", "", "", 0, leveldb::TKT_FORSEEK, &start_tera_key);
    end_row_key = std::string("row\0", 5);
    scan_options.column_family_list["column"].insert("1a");
    EXPECT_TRUE(tablet.LowLevelScan(start_tera_key, end_row_key, scan_options,
                                    &value_list, &read_row_count, &read_bytes, &is_complete, NULL));
    EXPECT_EQ(value_list.key_values_size(), 3);
    scan_options.max_versions = 2;
    EXPECT_TRUE(tablet.LowLevelScan(start_tera_key, end_row_key, scan_options,
                                    &value_list, &read_row_count, &read_bytes, &is_complete, NULL));
    EXPECT_EQ(value_list.key_values_size(), 2);
    EXPECT_TRUE(tablet.Unload());
}

TEST_F(TabletIOTest, DISABLED_SplitToSubTable) {
    LOG(INFO) << "SplitToSubTable() begin ...";
    std::string tablet_path = working_dir + "split_to_subtable";
    std::string key_start = "";
    std::string key_end = "";

    TabletIO tablet;
    EXPECT_TRUE(tablet.Load(GetTableSchema(), key_start, key_end, tablet_path, std::vector<uint64_t>(), empty_snaphsots_));

    // prepare test data
    for (int i = 0; i < N; ++i) {
        std::string str = StringFormat("%011llu", i); // NumberToString(i);
        EXPECT_TRUE(tablet.WriteOne(str, str));
    }

    // for first tablet

    LOG(INFO) << "table[" << key_start << ", " << key_end
        << "]: size = " << tablet.GetDataSize();

    std::string split_key;
    EXPECT_TRUE(tablet.Split(&split_key));
    LOG(INFO) << "split key = " << split_key;
    LOG(INFO) << "table[" << key_start << ", " << split_key
        << "]: size = " << tablet.GetDataSize(key_start, split_key);
    LOG(INFO) << "table[" << split_key << ", " << key_end
        << "]: size = " << tablet.GetDataSize(split_key, key_end);
    EXPECT_TRUE(tablet.Unload());

    // open from split key to check scope size
    std::string split_path_1;
    std::string split_path_2;
    ASSERT_TRUE(leveldb::GetSplitPath(tablet_path, &split_path_1, &split_path_2));

    // 1. load sub-table 1
    EXPECT_TRUE(tablet.Load(GetTableSchema(), key_start, split_key, split_path_1, std::vector<uint64_t>(), empty_snaphsots_));
    LOG(INFO) << "table[" << key_start << ", " << split_key
        << "]: size = " << tablet.GetDataSize();
    // varify result
    for (int i = 0; i < N / 3; ++i) {
        std::string key = StringFormat("%011llu", i);
        std::string value;
        EXPECT_TRUE(tablet.Read(key, &value));
        ASSERT_EQ(key, value);
    }
    EXPECT_TRUE(tablet.Unload());

    // 2. load sub-table 2
    EXPECT_TRUE(tablet.Load(GetTableSchema(), split_key, key_end, split_path_2, std::vector<uint64_t>(), empty_snaphsots_));
    LOG(INFO) << "table[" << split_key << ", " << key_end
        << "]: size = " << tablet.GetDataSize();
    // varify result
    for (int i = N / 3; i < N; ++i) {
        std::string key = StringFormat("%011llu", i);
        std::string value;
        EXPECT_TRUE(tablet.Read(key, &value));
        ASSERT_EQ(key, value);
    }
    EXPECT_TRUE(tablet.Unload());

    LOG(INFO) << "SplitToSubTable() end ...";
}

} // namespace io
} // namespace tera

int main(int argc, char** argv) {
    FLAGS_tera_io_retry_max_times = 1;
    FLAGS_tera_tablet_living_period = 0;
    FLAGS_tera_tablet_write_buffer_size = 1;
    FLAGS_tera_leveldb_env_type = "local";
    ::google::InitGoogleLogging(argv[0]);
    ::google::ParseCommandLineFlags(&argc, &argv, true);
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

