// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "io/utils_leveldb.h"

#include <algorithm>
#include <string>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "common/base/string_ext.h"
#include "common/base/string_number.h"
#include "common/file/file_path.h"
#include "io/timekey_comparator.h"
#include "leveldb/comparator.h"
#include "leveldb/env_dfs.h"
#include "leveldb/table_utils.h"
#include "utils/timer.h"

DECLARE_string(tera_leveldb_env_type);
DECLARE_string(tera_leveldb_env_dfs_type);
DECLARE_string(tera_leveldb_env_nfs_mountpoint);
DECLARE_string(tera_leveldb_env_nfs_conf_path);
DECLARE_string(tera_leveldb_env_hdfs2_nameservice_list);
DECLARE_string(tera_tabletnode_path_prefix);
DECLARE_string(tera_dfs_so_path);
DECLARE_string(tera_dfs_conf);

namespace tera {
namespace io {

void InitDfsEnv() {
    if (FLAGS_tera_leveldb_env_dfs_type == "nfs") {
        if (access(FLAGS_tera_leveldb_env_nfs_conf_path.c_str(), R_OK) == 0) {
            LOG(INFO) << "init nfs system: use configure file "
                << FLAGS_tera_leveldb_env_nfs_conf_path;
            leveldb::InitNfsEnv(FLAGS_tera_leveldb_env_nfs_mountpoint,
                                FLAGS_tera_leveldb_env_nfs_conf_path);
        } else {
            LOG(FATAL) << "init nfs system: no configure file found";
        }
    } else if (FLAGS_tera_leveldb_env_dfs_type == "hdfs2") {
        LOG(INFO) << "init hdfs2 system";
        leveldb::InitHdfs2Env(FLAGS_tera_leveldb_env_hdfs2_nameservice_list);
    } else if (FLAGS_tera_leveldb_env_dfs_type == "hdfs") {
        LOG(INFO) << "init hdfs system";
        leveldb::InitHdfsEnv();
    } else {
        LOG(INFO) << "Init dfs system: " << FLAGS_tera_dfs_so_path << "("
                  << FLAGS_tera_dfs_conf << ")";
        leveldb::InitDfsEnv(FLAGS_tera_dfs_so_path, FLAGS_tera_dfs_conf);
    }
}

leveldb::Env* LeveldbEnv() {
    if (FLAGS_tera_leveldb_env_type == "local") {
        return leveldb::NewPosixEnv();
    } else {
        return leveldb::EnvDfs();
    }
}

bool MoveEnvDirToTrash(const std::string& tablename) {
    leveldb::Env* env = LeveldbEnv();
    std::string src_dir = FLAGS_tera_tabletnode_path_prefix + "/" + tablename;

    const std::string trash("#trash");
    std::string trash_dir = FLAGS_tera_tabletnode_path_prefix + "/" + trash;
    if (!env->FileExists(trash_dir)) {
        if (!env->CreateDir(trash_dir).ok()) {
            LOG(ERROR) << "fail to create trash dir: " << trash_dir;
            return false;
        } else {
            LOG(INFO) << "succeed in creating trash dir: " << trash_dir;
        }
    }

    std::string time = get_curtime_str();
    std::replace(time.begin(), time.end(), ':', '-');
    std::string dest_dir = trash_dir + "/" + tablename + "." + time;
    if (!env->RenameFile(src_dir, dest_dir).ok()) {
        LOG(ERROR) << "fail to move dir to trash, dir: " << src_dir;
        return false;
    }
    LOG(INFO) << "Move dir to trash, dir: " << src_dir;
    return true;
}

bool DeleteEnvDir(const std::string& subdir) {
    leveldb::Env* env = LeveldbEnv();
    std::string dir_name = FLAGS_tera_tabletnode_path_prefix + "/" + subdir;
    if (!env->DeleteDir(dir_name).ok()) {
        LOG(ERROR) << "fail to delete dir in file system, dir: " << dir_name;
        return false;
    }
    LOG(INFO) << "delete dir in file system, dir: " << dir_name;
    return true;
}

bool MergeTables(const std::string& mf, const std::string& mf1,
                 const std::string& mf2,
                 std::map<uint64_t, uint64_t>* mf2_file_maps,
                 leveldb::Env* db_env) {
    leveldb::Env* env = db_env;
    if (!env) {
        env = LeveldbEnv();
    }
    leveldb::Comparator* cmp = new io::TimekeyComparator(leveldb::BytewiseComparator());
    if (!leveldb::MergeBoth(env, cmp, mf, mf1, mf2, mf2_file_maps)) {
        return false;
    }

    return true;
}

bool MergeTables(const std::string& table_1,
                 const std::string& table_2,
                 const std::string& merged_table,
                 leveldb::Env* db_env) {
    leveldb::Env* env = db_env;
    if (!env) {
        env = LeveldbEnv();
    }
    std::string path_prefix = FLAGS_tera_tabletnode_path_prefix;
    std::string mf1;
    std::string mf2;

    if (!leveldb::BackupNotSSTFile(env, path_prefix + table_1, &mf1)) {
        LOG(ERROR) << "not found the manifest file in: " << table_1;
        leveldb::RollbackBackupNotSst(env, path_prefix + table_1);
        return false;
    }
    if (!leveldb::BackupNotSSTFile(env, path_prefix + table_2, &mf2)) {
        LOG(ERROR) << "not found the manifest file in: " << table_2;
        leveldb::RollbackBackupNotSst(env, path_prefix + table_2);
        return false;
    }

    std::map<uint64_t, uint64_t> file_map;
    std::string mf1_path = path_prefix + table_1 + "/" + mf1;
    std::string mf2_path = path_prefix + table_2 + "/" + mf2;
    std::string merge_path;
    SplitStringEnd(mf1_path, &merge_path, NULL);
    if (!MergeTables(merge_path, mf1_path, mf2_path,
                     &file_map, env)) {
        LOG(ERROR) << "fail to merge manifest: " << mf1_path << " & " << mf2_path;
//         leveldb::RollbackBackupNotSst(env, path_prefix + table_1);
//         leveldb::RollbackBackupNotSst(env, path_prefix + table_2);
        return false;
    }
//     std::map<uint64_t, uint64_t>::iterator it;
//     for (it = file_map.begin(); it != file_map.end(); ++it) {
//         std::cout << it->first << "->" << it->second << ", ";
//     }

    if (!leveldb::MoveMergedSst(env, path_prefix + table_2,
                                path_prefix + table_1, file_map)) {
        LOG(ERROR) << "fail to move sst files in: " << table_1;
        return false;
    } else if (!CleanFlagFile(env, path_prefix + table_1)) {
        LOG(ERROR) << "fail to clean all flag files in: " << table_1;
    }

    if (!merged_table.empty() && !leveldb::RenameTable(env,
            path_prefix + table_1, path_prefix + merged_table)) {
        LOG(ERROR) << "fail to rename from " << table_1
            << " to " << merged_table;
        return false;
    }
//     leveldb::DeleteTable(env, path_prefix + table_2);

    return true;
}

bool MergeTablesWithLG(const std::string& table_1,
                       const std::string& table_2,
                       const std::string& merged_table,
                       uint32_t lg_num) {
    // not need to multiple thread because manifest merge
    // would be quickly ready
    leveldb::Env* env = LeveldbEnv();
    bool ret = true;
    for (uint32_t i = 0; i < lg_num; ++i) {
        std::string lg_id_str = NumberToString(i);
        std::string table_1_lg = table_1 + "/" + lg_id_str;
        std::string table_2_lg = table_2 + "/" + lg_id_str;
        if (!MergeTables(table_1_lg, table_2_lg, "", env)) {
            LOG(ERROR) << "fail to merge lg: "
                << table_1_lg << " & " << table_2_lg
                << " => " << table_1_lg;
            ret = false;
        }
    }
    if (!ret) {
        return ret;
    }
    std::string path_prefix = FLAGS_tera_tabletnode_path_prefix;
    if (!merged_table.empty() && !leveldb::RenameTable(env,
            path_prefix + table_1, path_prefix + merged_table)) {
        LOG(ERROR) << "fail to rename from " << table_1
            << " to " << merged_table;
        return false;
    }
    return ret;
}

} // namespace io
} // namespace leveldb
