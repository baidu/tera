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
#include "leveldb/block_cache.h"
#include "leveldb/comparator.h"
#include "leveldb/env_dfs.h"
#include "leveldb/env_flash.h"
#include "leveldb/env_inmem.h"
#include "leveldb/env_mock.h"
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
DECLARE_int32(tera_leveldb_block_cache_env_thread_num);

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

// Tcache: default env
static pthread_once_t block_cache_once = PTHREAD_ONCE_INIT;
static leveldb::Env* default_block_cache_env;
static void InitDefaultBlockCacheEnv() {
    default_block_cache_env = new leveldb::BlockCacheEnv(LeveldbBaseEnv());
    default_block_cache_env->SetBackgroundThreads(FLAGS_tera_leveldb_block_cache_env_thread_num);
    LOG(INFO) << "init block cache, thread num " << FLAGS_tera_leveldb_block_cache_env_thread_num;
}

leveldb::Env* DefaultBlockCacheEnv() {
    pthread_once(&block_cache_once, InitDefaultBlockCacheEnv);
    return default_block_cache_env;
}

// mem env
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

// flash env
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

leveldb::Env* LeveldbMockEnv() {
    return leveldb::NewMockEnv();
}

std::string GetTrashDir() {
    const std::string trash("#trash");
    return FLAGS_tera_tabletnode_path_prefix + "/" + trash;
}

bool MoveEnvDirToTrash(const std::string& tablename) {
    leveldb::Env* env = LeveldbBaseEnv();
    std::string src_dir = FLAGS_tera_tabletnode_path_prefix + "/" + tablename;
    leveldb::Status s = env->FileExists(src_dir);
    if (s.ok()) {
        // exists, do nothing in here
    } else if(s.IsNotFound()) {
        // not found, so no need to delete
        return true;
    } else {
        // unknown status
        return false;
    }

    std::string trash_dir = GetTrashDir();
    s = env->FileExists(trash_dir);
    if (s.IsNotFound()) {
        if (!env->CreateDir(trash_dir).ok()) {
            LOG(ERROR) << "fail to create trash dir: " << trash_dir;
            return false;
        } else {
            LOG(INFO) << "succeed in creating trash dir: " << trash_dir;
        }
    } else if (s.ok()) {
        // trash dir exists, do nothing in here
    } else {
        // unknown status
        return false;
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

void CleanTrashDir() {
    leveldb::Env* env = LeveldbBaseEnv();
    std::string trash_dir = GetTrashDir();
    std::vector<std::string> children;
    leveldb::Status s;
    s = env->GetChildren(trash_dir, &children);
    if (!s.ok()) {
        return;
    }
    for (size_t i = 0; i < children.size(); ++i) {
        std::string c_dir = trash_dir + '/' + children[i];
        DeleteEnvDir(c_dir);
    }
    return;
}

leveldb::Status DeleteEnvDir(const std::string& dir) {
    leveldb::Status s;
    static bool is_support_rmdir = true;

    leveldb::Env* env = LeveldbBaseEnv();
    s = env->DeleteFile(dir);
    if (s.ok()) {
        LOG(INFO) << "[gc] delete: " << dir;
        return s;
    }
    if (is_support_rmdir) {
        s = env->DeleteDir(dir);
        if (s.ok()) {
            LOG(INFO) << "[gc] delete: " << dir;
            return s;
        } else {
            is_support_rmdir = false;
            LOG(INFO) << "[gc] file system not support rmdir"
                << ", status: " << s.ToString();
        }
    }

    // file system do not support delete dir, try delete recursively
    std::vector<std::string> children;
    s = env->GetChildren(dir, &children);
    if (!s.ok()) {
        LOG(ERROR) << "[gc] fail to get children, dir: " << dir
            << ", status: " << s.ToString();
        return s;
    }
    for (size_t i = 0; i < children.size(); ++i) {
        std::string c_dir = dir + '/' + children[i];
        DeleteEnvDir(c_dir);
    }
    s = env->DeleteDir(dir);
    if (s.ok()) {
        LOG(INFO) << "[gc] delete: " << dir;
        return s;
    }
    return s;
}
} // namespace io
} // namespace leveldb
