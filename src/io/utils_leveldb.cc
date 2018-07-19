// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "io/utils_leveldb.h"

#include <errno.h>
#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
#include <string>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "common/base/string_ext.h"
#include "common/base/string_number.h"
#include "common/file/file_path.h"
#include "common/mutex.h"
#include "common/timer.h"
#include "db/filename.h"
#include "io/timekey_comparator.h"
#include "leveldb/env_flash_block_cache.h"
#include "leveldb/comparator.h"
#include "leveldb/env_dfs.h"
#include "leveldb/env_flash.h"
#include "leveldb/env_inmem.h"
#include "leveldb/env_mock.h"
#include "leveldb/table_utils.h"
#include "common/timer.h"

DECLARE_string(tera_leveldb_env_type);
DECLARE_string(tera_leveldb_env_dfs_type);
DECLARE_string(tera_leveldb_env_nfs_mountpoint);
DECLARE_string(tera_leveldb_env_nfs_conf_path);
DECLARE_string(tera_leveldb_env_hdfs2_nameservice_list);
DECLARE_string(tera_tabletnode_path_prefix);
DECLARE_string(tera_dfs_so_path);
DECLARE_string(tera_dfs_conf);
DECLARE_int64(tera_master_gc_trash_expire_time_s);
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
static pthread_once_t flash_block_cache_once = PTHREAD_ONCE_INIT;
static leveldb::Env* default_flash_block_cache_env;
static void InitDefaultFlashBlockCacheEnv() {
    default_flash_block_cache_env = new leveldb::FlashBlockCacheEnv(LeveldbBaseEnv());
    default_flash_block_cache_env->SetBackgroundThreads(FLAGS_tera_leveldb_block_cache_env_thread_num);
    LOG(INFO) << "init block cache, thread num " << FLAGS_tera_leveldb_block_cache_env_thread_num;
}

leveldb::Env* DefaultFlashBlockCacheEnv() {
    pthread_once(&flash_block_cache_once, InitDefaultFlashBlockCacheEnv);
    return default_flash_block_cache_env;
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

std::string GetTrackableGcTrashDir() {
    const std::string trash("#trackable_gc_trash");
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

leveldb::Status MoveSstToTrackableGcTrash(const std::string& table_name,
                                          uint64_t tablet_id,
                                          uint32_t lg_id,
                                          uint64_t file_id) {
    leveldb::Status s;
    leveldb::Env* env = LeveldbBaseEnv();
    std::string table_path = FLAGS_tera_tabletnode_path_prefix + "/" + table_name;
    std::string src_path = leveldb::BuildTableFilePath(table_path, tablet_id, lg_id, file_id);

    s = env->FileExists(src_path);
    if(s.IsNotFound()) {
        // not found, so no need to move
        return leveldb::Status::OK();
    } else if (!s.ok()) {
        // unknown status
        return s;
    }

    std::string trash_dir = GetTrackableGcTrashDir();
    s = env->FileExists(trash_dir);
    if (s.IsNotFound()) {
        if (!env->CreateDir(trash_dir).ok()) {
            LOG(ERROR) << "[gc] fail to create trackable gc trash dir: " << trash_dir;
            return leveldb::Status::IOError("fail to create trackable gc trash dir");
        } else {
            LOG(INFO) << "[gc] succeed in creating trackable gc trash dir: " << trash_dir;
        }
    } else if (!s.ok()) {
        // unknown status
        return s;
    }

    std::string time = get_curtime_str();
    std::replace(time.begin(), time.end(), ':', '-');
    std::string dest_path = leveldb::BuildTrashTableFilePath(
            trash_dir + "/" + table_name, tablet_id, lg_id, file_id, time);

    size_t dir_pos = dest_path.rfind("/");
    if (dir_pos == std::string::npos) {
        LOG(ERROR) << "[gc] invalid dest path: " << dest_path;
        return leveldb::Status::IOError("invalid dest path");
    }
    std::string lg_path = dest_path.substr(0, dir_pos);
    s = env->FileExists(lg_path);
    if(s.IsNotFound()) {
        // not found, so no need to mkdir
        s = env->CreateDir(lg_path);
        if (!s.ok()) {
            LOG(ERROR) << "[gc] create lg dir in trash: " << lg_path
                       << " failed: " << s.ToString();
            return s;
        }
    } else if (!s.ok()) {
        // unknown status
        return s;
    }

    s = env->RenameFile(src_path, dest_path);
    if (!s.ok()) {
        LOG(ERROR) << "[gc] fail to move file to trackable gc trash, src_path: " << src_path
            << ", dest_path: " << dest_path << ", status: " << s.ToString();
        return s;
    }
    VLOG(29) << "[gc] move file to trackable gc trash, src_path: " << src_path
            << ", dest_path: " << dest_path;

    return leveldb::Status::OK();
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

bool TryDeleteEmptyDir(const std::string& dir_path,
                       size_t total_children_size,
                       size_t deleted_children_size) {
    bool deleted = false;

    if (deleted_children_size == total_children_size) {
        leveldb::Status s;
        leveldb::Env* env = LeveldbBaseEnv();
        s = env->DeleteDir(dir_path);
        if (s.ok()) {
            LOG(INFO) << "[gc] delete empty dir: " << dir_path;
            deleted = true;
        } else {
            LOG(WARNING) << "[gc] fail to delete empty dir: "
                << dir_path <<" status: " << s.ToString();
            deleted = false;
        }
    }

    return deleted;
}

leveldb::Status DeleteTrashFileIfExpired(const std::string& file_path) {
    leveldb::Status s;
    leveldb::Env* env = LeveldbBaseEnv();

    std::string file_time_str = leveldb::GetTimeStrFromTrashFile(file_path);
    if (file_time_str.empty()) {
        LOG(ERROR) << "[gc] skip invalid trash file path: " << file_path;
        return leveldb::Status::Corruption("invalid trash file path");
    }

    // change time format
    // eg.: change "20170801-15-54-23" to "20170801-15:54:23"
    file_time_str = file_time_str.replace(file_time_str.rfind("-"), 1, ":");
    file_time_str = file_time_str.replace(file_time_str.rfind("-"), 1, ":");

    int64_t file_time = get_timestamp_from_str(file_time_str);
    int64_t current_time = time(nullptr);
    if (current_time - file_time > FLAGS_tera_master_gc_trash_expire_time_s) {
        s = env->DeleteFile(file_path);
        if (s.ok()) {
            LOG(INFO) << "[gc] delete expired trash file: " << file_path
                << ", file added to trash time: " << get_time_str(file_time)
                << ", current time: " << get_time_str(current_time);
        } else {
            LOG(ERROR) << "[gc] fail to delete expired trash file: " << file_path
                <<" status: " << s.ToString();
            return s;
        }
    } else {
        return leveldb::Status::Corruption("file not expired");
    }

    return s;
}

void CleanTrackableGcTrash() {
    leveldb::Status s;
    leveldb::Env* env = LeveldbBaseEnv();
    std::string trash_dir = GetTrackableGcTrashDir();

    s = env->FileExists(trash_dir);
    if (s.IsNotFound()) {
        LOG(INFO) << "[gc] skip empty trash dir: " << trash_dir
            <<" status: " << s.ToString();
        return;
    }

    std::vector<std::string> tables;
    s = env->GetChildren(trash_dir, &tables);
    if (!s.ok()) {
        LOG(ERROR) << "[gc] fail to list trash dir: " << trash_dir
            <<" status: " << s.ToString();
        return;
    }

    for (const auto& table : tables) {
        std::string table_path = trash_dir + "/" + table;
        std::vector<std::string> tablets;
        s = env->GetChildren(table_path, &tablets);
        if (!s.ok()) {
            LOG(ERROR) << "[gc] skip due to fail to list table dir: " << table_path
                <<" status: " << s.ToString();
            continue;
        }

        size_t deleted_empty_tablet_num = 0;
        for (const auto& tablet : tablets) {
            std::string tablet_path = table_path + "/" + tablet;
            std::vector<std::string> lgs;
            s = env->GetChildren(tablet_path, &lgs);
            if (!s.ok()) {
                LOG(ERROR) << "[gc] skip due to fail to list tablet dir: " << tablet_path
                    <<" status: " << s.ToString();
                continue;
            }

            size_t deleted_empty_lg_num = 0;
            for (const auto& lg : lgs) {
                std::string lg_path = tablet_path + "/" + lg;
                std::vector<std::string> files;
                s = env->GetChildren(lg_path, &files);
                if (!s.ok()) {
                    LOG(ERROR) << "[gc] skip due to fail to list lg dir: " << lg_path
                        <<" status: " << s.ToString();
                    continue;
                }

                size_t deleted_file_num = 0;
                for (const auto& file : files) {
                    std::string file_path = lg_path + "/" + file;
                    if (DeleteTrashFileIfExpired(file_path).ok()) {
                        ++deleted_file_num;
                    }
                }
                if (TryDeleteEmptyDir(lg_path, files.size(), deleted_file_num)) {
                    ++ deleted_empty_lg_num;
                }
            }
            if (TryDeleteEmptyDir(tablet_path, lgs.size(), deleted_empty_lg_num)) {
                ++ deleted_empty_tablet_num;
            }
        }
        TryDeleteEmptyDir(table_path, tablets.size(), deleted_empty_tablet_num);
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
    leveldb::FileLock* file_lock = nullptr;
    env->LockFile(dir + "/", &file_lock);
    delete file_lock;
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

leveldb::Status DeleteOldFlashCache(const std::vector<std::string>& path_list) {
    LOG(INFO) << "delete old falsh cache begin";
    int64_t begin_ts_us = get_micros();
    leveldb::Env* posix_env = leveldb::Env::Default();
    leveldb::Status s;
    for (uint32_t i = 0; i < path_list.size(); ++i) {
        LOG(INFO) << "deal with: " << path_list[i];
        std::vector<std::string> childs;
        s = posix_env->GetChildren(path_list[i], &childs);
        if (!s.ok()) {
            LOG(WARNING) << "get children of dir fail when clean old flash cache, dir: "
                << path_list[i] << ", status: " << s.ToString();
            continue;
        }
        for (size_t j = 0; j < childs.size(); ++j) {
            if (childs[j] == "." || childs[j] == ".." || childs[j] == "flash_block_cache") {
                LOG(INFO) << "skip child: " << childs[j];
                continue;
            }
            std::string child_path = path_list[i] + "/" + childs[j];
            LOG(INFO) << "deal with child path: " << child_path;
            struct stat info;
            if (0 != stat(child_path.c_str(), &info)) {
                s = leveldb::Status::IOError(child_path, strerror(errno));
                LOG(WARNING) << "stat dir fail when clean old flash cache, dir: "
                    << child_path << ", status: " << s.ToString();
                continue;
            }
            if (S_ISDIR(info.st_mode)) {
                s = posix_env->DeleteDirRecursive(child_path);
                if (!s.ok()) {
                    LOG(WARNING) << "delete dir recursive fail when clean old flash cache, dir: "
                        << child_path << ", status: " << s.ToString();
                    continue;
                }
            } else {
                s = posix_env->DeleteFile(child_path);
                if (!s.ok()) {
                    LOG(WARNING) << "delete file fail when clean old flash cache, file: "
                        << child_path << ", status: " << s.ToString();
                    continue;
                }
            }
        }
    }
    LOG(INFO) << "delete old flash cache end, time used(ms): " << (get_micros() - begin_ts_us) / 1000;
    return s;
}

} // namespace io
} // namespace leveldb
