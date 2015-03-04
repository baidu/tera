// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "tera/utils/utils_cmd.h"

#include <map>

#include "common/base/string_ext.h"
#include "common/base/string_format.h"
#include "common/base/string_number.h"
#include "common/file/file_path.h"
#include "thirdparty/gflags/gflags.h"
#include "thirdparty/glog/logging.h"
#include "thirdparty/gtest/gtest.h"

#include "tera/io/tablet_io.h"

DECLARE_string(tera_tabletnode_path_prefix);

namespace tera {
namespace utils {

const std::string working_dir = "testdata_utils_merge_data/";
const std::string table_1 = "table1";
const std::string table_2 = "table2";
const std::string merge_table = "merge_table";

const std::string lg_id_str = "0";

std::map<uint64_t, uint64_t> empty_snaphsots;


class UtilsLeveldbTest : public ::testing::Test {
public:
    UtilsLeveldbTest() {
        FLAGS_tera_tabletnode_path_prefix = working_dir;
        std::string cmd = std::string("mkdir -p ") + working_dir;
        system(cmd.c_str());
    }
    ~UtilsLeveldbTest() {
        std::string cmd = std::string("rm -rf ") + working_dir;
        system(cmd.c_str());
    }

    std::string CreateTestTable(const std::string& table_name,
                                int32_t key_start, int32_t key_end) {
        std::string tablet_path = table_name;
        io::TabletIO tablet;
        EXPECT_TRUE(tablet.Load(TableSchema(), "", "", tablet_path, empty_snaphsots));
        for (int32_t i = key_start; i < key_end; ++i) {
            std::string str = StringFormat("%08llu", i);
            EXPECT_TRUE(tablet.WriteOne(str, str));
        }
        tablet.Compact();
        EXPECT_TRUE(tablet.Unload());

        std::string manifest_file;
        std::string cmd = std::string("ls " + working_dir + tablet_path +
                                      "/" + lg_id_str + "/MANIFEST-*");
        EXPECT_TRUE(ExecuteShellCmd(cmd, &manifest_file));
        return manifest_file;
    }

    void CopyDirToDir(const std::string& from, const std::string& to,
                      std::map<uint64_t, uint64_t>* sst_rename) {
        std::string table_from = working_dir + from;
        std::string table_to = working_dir + to;

        std::vector<std::string> file_list;
        ListCurrentDir(table_from, &file_list);
        for (uint32_t i = 0; i < file_list.size(); ++i) {
            std::string file = file_list[i];
            if (StringStartWith(file, "MANIFEST-")
                || StringEndsWith(file, ".log")) {
                continue;
            }
            if (StringEndsWith(file, ".sst") && sst_rename) {
                std::vector<std::string> items;
                SplitString(file, ".", &items);
                int32_t file_num;
                EXPECT_TRUE(StringToNumber(items[0], &file_num, 10));
                std::map<uint64_t, uint64_t>::iterator it =
                    sst_rename->find(file_num);
                if (it != sst_rename->end()) {
                    uint64_t new_num = it->second;
                    file = StringFormat("%06llu.sst", new_num);
                } else {
                    LOG(ERROR) << "not find the map for: " << file;
                }
            }
            std::string cmd = std::string("cp ")
                + table_from + "/" + file_list[i]
                + " "
                + table_to + "/" + file;
            LOG(INFO) << "exec: " << cmd;
            EXPECT_TRUE(ExecuteShellCmd(cmd, NULL));
        }
    }

    void RemoveNotSstFile(const std::string& path) {
        std::string cmd = std::string("cd ") + path + lg_id_str
            + std::string(" && rm -rf *.log CURRENT LOG LOCK");
        EXPECT_TRUE(ExecuteShellCmd(cmd, NULL));
    }

    void VerifyMerge(const std::string& table_name,
                     int32_t key_start, int32_t key_end) {
        std::string tablet_path = table_name;
        io::TabletIO tablet;
        EXPECT_TRUE(tablet.Load(TableSchema(), "", "", tablet_path, empty_snaphsots));
        for (int32_t i = key_start; i < key_end; ++i) {
            std::string key = StringFormat("%08llu", i);
            std::string value;
            ASSERT_TRUE(tablet.Read(key, &value));
            ASSERT_EQ(key, value);
        }
        EXPECT_TRUE(tablet.Unload());
    }

    void PrintMap(const std::map<uint64_t, uint64_t>& file_map) {
        std::map<uint64_t, uint64_t>::const_iterator it;
        for (it = file_map.begin(); it != file_map.end(); ++it) {
            std::cout << it->first << "-" << it->second << ", ";
        }
        std::cout << std::endl;
    }
};

TEST_F(UtilsLeveldbTest, MergeTable_1and2equal3) {
    std::string mf1 = CreateTestTable(table_1, 0, 10000);
    std::string mf2 = CreateTestTable(table_2, 10000, 20000);

    LOG(INFO) << "prepare ...";

    std::string table_1_lg = table_1 + "/" + lg_id_str;
    std::string table_2_lg = table_2 + "/" + lg_id_str;
    std::string merge_table_lg = merge_table + "/" + lg_id_str;

    CreateDirWithRetry(working_dir + merge_table_lg);
    CopyDirToDir(table_1_lg, merge_table_lg, NULL);

    LOG(INFO) << "merging: " << mf1 << " & " << mf2;

    mf1 = working_dir + table_1_lg + "/MANIFEST-000002";
    mf2 = working_dir + table_2_lg + "/MANIFEST-000002";
    std::string merge_mf = working_dir + merge_table_lg + "/MANIFEST-000002";
    std::map<uint64_t, uint64_t> file_map;
    ASSERT_TRUE(MergeTables(merge_mf, mf1, mf2, &file_map));
    PrintMap(file_map);

    LOG(INFO) << "merge sst files ...";
    CopyDirToDir(table_2_lg, merge_table_lg, &file_map);

    LOG(INFO) << "verify ...";
    VerifyMerge(merge_table, 0, 20000);
}

TEST_F(UtilsLeveldbTest, MergeTable_1and2equal1) {
    std::string other = "other_";
    CreateTestTable(other + table_1, 0, 10000);
    CreateTestTable(other + table_2, 10000, 20000);

    // prepare
    std::string table_1_lg = table_1 + "/" + lg_id_str;
    std::string table_2_lg = table_2 + "/" + lg_id_str;
    std::string merge_path = working_dir + other + table_1_lg;

    std::string merge_mf = working_dir + other + table_1_lg + "/MANIFEST-000002";
    std::string mf1 = merge_mf + ".org";
    std::string mf2 = working_dir + other + table_2_lg + "/MANIFEST-000002";
    EXPECT_TRUE(MoveLocalFile(merge_mf, mf1));
    RemoveNotSstFile(merge_path);

    std::map<uint64_t, uint64_t> file_map;
    ASSERT_TRUE(MergeTables(merge_mf, mf1, mf2, &file_map));
    PrintMap(file_map);
    CopyDirToDir(other + table_2_lg, other + table_1_lg, &file_map);

    VerifyMerge(other + table_1, 0, 20000);
}

TEST_F(UtilsLeveldbTest, MergeTable_1and2in1) {
    std::string tb1 = "tb1";
    std::string tb2 = "tb2";
    CreateTestTable(tb1, 0, 10000);
    CreateTestTable(tb2, 10000, 20000);

    ASSERT_TRUE(MergeTablesWithLG(tb1, tb2));
    VerifyMerge(tb1, 0, 20000);
}

TEST_F(UtilsLeveldbTest, DISABLED_Specific_test) {
    std::string data_prepare_cmd =
        "cp -rf /home/users/qinan/tera-svn/temp/* " + working_dir;
    system(data_prepare_cmd.c_str());

    std::string tb1 = "tablet11001";
    std::string tb2 = "tablet1101";
    std::string tb3 = "1_tablet";

    ASSERT_TRUE(MergeTables(tb1, tb2, tb3));
}

} // namespace utils
} // namespace tera

int main(int argc, char** argv) {
    ::google::InitGoogleLogging(argv[0]);
    ::google::ParseCommandLineFlags(&argc, &argv, true);
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
