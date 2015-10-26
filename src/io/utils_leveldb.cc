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
#include "common/mutex.h"
#include "io/timekey_comparator.h"
#include "leveldb/comparator.h"
#include "leveldb/env_dfs.h"
#include "leveldb/env_flash.h"
#include "leveldb/env_inmem.h"
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

leveldb::Env* LeveldbBaseEnv() {
    if (FLAGS_tera_leveldb_env_type == "local") {
        return leveldb::Env::Default();
    } else {
        return leveldb::EnvDfs();
    }
}

leveldb::Env* LeveldbMemEnv() {
    static Mutex mutex;
    static leveldb::Env* mem_env = NULL;
    MutexLock locker(&mutex);
    if (mem_env) {
        return mem_env;
    }
    leveldb::Env* base_env = LeveldbBaseEnv();
    mem_env = leveldb::NewInMemoryEnv(base_env);
    return mem_env;
}

leveldb::Env* LeveldbFlashEnv() {
    static Mutex mutex;
    static leveldb::Env* flash_env = NULL;
    MutexLock locker(&mutex);
    if (flash_env) {
        return flash_env;
    }
    leveldb::Env* base_env = LeveldbBaseEnv();
    flash_env = leveldb::NewFlashEnv(base_env);
    return flash_env;
}

bool MoveEnvDirToTrash(const std::string& tablename) {
    leveldb::Env* env = LeveldbBaseEnv();
    std::string src_dir = FLAGS_tera_tabletnode_path_prefix + "/" + tablename;
    if (!env->FileExists(src_dir)) {
        return true;
    }

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
    leveldb::Env* env = LeveldbBaseEnv();
    std::string dir_name = FLAGS_tera_tabletnode_path_prefix + "/" + subdir;
    if (!env->DeleteDir(dir_name).ok()) {
        LOG(ERROR) << "fail to delete dir in file system, dir: " << dir_name;
        return false;
    }
    LOG(INFO) << "delete dir in file system, dir: " << dir_name;
    return true;
}
} // namespace io
} // namespace leveldb
