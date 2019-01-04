// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "io/utils_leveldb.h"

#include <errno.h>
#include <sys/stat.h>
#include <sys/statfs.h>
#include <unistd.h>

#include <algorithm>
#include <mutex>
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
#include "leveldb/comparator.h"
#include "leveldb/env_dfs.h"
#include "leveldb/env_flash.h"
#include "leveldb/env_inmem.h"
#include "leveldb/env_mock.h"
#include "leveldb/table_utils.h"
#include "common/timer.h"
#include "leveldb/persistent_cache.h"
#include "leveldb/env.h"
#include "utils/utils_cmd.h"

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
DECLARE_bool(tera_leveldb_use_direct_io_read);
DECLARE_bool(tera_leveldb_use_direct_io_write);
DECLARE_uint64(tera_leveldb_posix_write_buffer_size);

DECLARE_string(tera_tabletnode_cache_paths);
DECLARE_bool(tera_enable_persistent_cache);
DECLARE_bool(tera_enable_persistent_cache_transfer_flash_env_files);
DECLARE_uint64(persistent_cache_write_retry_times);
DECLARE_string(persistent_cache_sizes_in_MB);

namespace tera {
namespace io {

void InitDfsEnv() {
  if (FLAGS_tera_leveldb_env_dfs_type == "nfs") {
    if (access(FLAGS_tera_leveldb_env_nfs_conf_path.c_str(), R_OK) == 0) {
      LOG(INFO) << "init nfs system: use configure file " << FLAGS_tera_leveldb_env_nfs_conf_path;
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
    LOG(INFO) << "Init dfs system: " << FLAGS_tera_dfs_so_path << "(" << FLAGS_tera_dfs_conf << ")";
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
  if (GetCachePaths().empty() || FLAGS_tera_enable_persistent_cache) {
    return nullptr;
  }
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

leveldb::Env* LeveldbMockEnv() { return leveldb::NewMockEnv(); }

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
  } else if (s.IsNotFound()) {
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

leveldb::Status MoveSstToTrackableGcTrash(const std::string& table_name, uint64_t tablet_id,
                                          uint32_t lg_id, uint64_t file_id) {
  leveldb::Status s;
  leveldb::Env* env = LeveldbBaseEnv();
  std::string table_path = FLAGS_tera_tabletnode_path_prefix + "/" + table_name;
  std::string src_path = leveldb::BuildTableFilePath(table_path, tablet_id, lg_id, file_id);

  s = env->FileExists(src_path);
  if (s.IsNotFound()) {
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
  std::string dest_path = leveldb::BuildTrashTableFilePath(trash_dir + "/" + table_name, tablet_id,
                                                           lg_id, file_id, time);

  size_t dir_pos = dest_path.rfind("/");
  if (dir_pos == std::string::npos) {
    LOG(ERROR) << "[gc] invalid dest path: " << dest_path;
    return leveldb::Status::IOError("invalid dest path");
  }
  std::string lg_path = dest_path.substr(0, dir_pos);
  s = env->FileExists(lg_path);
  if (s.IsNotFound()) {
    // not found, so no need to mkdir
    s = env->CreateDir(lg_path);
    if (!s.ok()) {
      LOG(ERROR) << "[gc] create lg dir in trash: " << lg_path << " failed: " << s.ToString();
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

bool TryDeleteEmptyDir(const std::string& dir_path, size_t total_children_size,
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
      LOG(WARNING) << "[gc] fail to delete empty dir: " << dir_path << " status: " << s.ToString();
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
                 << " status: " << s.ToString();
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
    LOG(INFO) << "[gc] skip empty trash dir: " << trash_dir << " status: " << s.ToString();
    return;
  }

  std::vector<std::string> tables;
  s = env->GetChildren(trash_dir, &tables);
  if (!s.ok()) {
    LOG(ERROR) << "[gc] fail to list trash dir: " << trash_dir << " status: " << s.ToString();
    return;
  }

  for (const auto& table : tables) {
    std::string table_path = trash_dir + "/" + table;
    std::vector<std::string> tablets;
    s = env->GetChildren(table_path, &tablets);
    if (!s.ok()) {
      LOG(ERROR) << "[gc] skip due to fail to list table dir: " << table_path
                 << " status: " << s.ToString();
      continue;
    }

    size_t deleted_empty_tablet_num = 0;
    for (const auto& tablet : tablets) {
      std::string tablet_path = table_path + "/" + tablet;
      std::vector<std::string> lgs;
      s = env->GetChildren(tablet_path, &lgs);
      if (!s.ok()) {
        LOG(ERROR) << "[gc] skip due to fail to list tablet dir: " << tablet_path
                   << " status: " << s.ToString();
        continue;
      }

      size_t deleted_empty_lg_num = 0;
      for (const auto& lg : lgs) {
        std::string lg_path = tablet_path + "/" + lg;
        std::vector<std::string> files;
        s = env->GetChildren(lg_path, &files);
        if (!s.ok()) {
          LOG(ERROR) << "[gc] skip due to fail to list lg dir: " << lg_path
                     << " status: " << s.ToString();
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
          ++deleted_empty_lg_num;
        }
      }
      if (TryDeleteEmptyDir(tablet_path, lgs.size(), deleted_empty_lg_num)) {
        ++deleted_empty_tablet_num;
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
    LOG(ERROR) << "[gc] fail to get children, dir: " << dir << ", status: " << s.ToString();
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

static std::vector<std::string> ParseCachePath(const std::string& path) {
  std::vector<std::string> paths;

  size_t beg = 0;
  const char* str = path.c_str();
  for (size_t i = 0; i <= path.size(); ++i) {
    if ((str[i] == '\0' || str[i] == ';') && i - beg > 0) {
      paths.emplace_back(std::string(str + beg, i - beg));
      beg = i + 1;
      if (!leveldb::Env::Default()->FileExists(paths.back()).ok() &&
          !leveldb::Env::Default()->CreateDir(paths.back()).ok()) {
        LOG(ERROR) << "[env_flash] cannot access cache dir: \n" << paths.back();
        paths.pop_back();
      }
    }
  }
  return paths;
};

static leveldb::Status GetPathDiskSize(const std::string& path, uint64_t* size) {
  struct statfs disk_info;
  if (statfs(path.c_str(), &disk_info) != 0) {
    return leveldb::Status::IOError(("Get disk size failed for " + path), strerror(errno));
  }
  uint64_t block_size = disk_info.f_bsize;
  uint64_t total_size = block_size * disk_info.f_blocks;
  *size = total_size;
  return leveldb::Status::OK();
}

const std::vector<std::string>& GetCachePaths() {
  static std::vector<std::string> cache_paths;
  static std::once_flag once_flag;
  std::call_once(once_flag, []() {
    cache_paths = ParseCachePath(FLAGS_tera_tabletnode_cache_paths);
    for (auto& path : cache_paths) {
      if (path.back() != '/') {
        path.append("/");
      }
    }
  });
  return cache_paths;
}

static std::string FormatPathString(const std::string& path) {
  if (path.empty()) {
    return "";
  }
  std::vector<std::string> terms;
  SplitString(path, "/", &terms);
  std::string ret;
  if (path[0] == '/') {
    ret += "/";
  }
  for (const auto& str : terms) {
    if (!str.empty()) {
      ret += (str + "/");
    }
  }
  if (path.back() != '/' && ret.size() > 0 && ret.back() == '/') {
    ret.pop_back();
  }
  return ret;
}

const std::vector<std::string>& GetPersistentCachePaths() {
  static std::vector<std::string> persistent_cache_paths;
  static std::once_flag once_flag;
  std::call_once(once_flag, []() {
    persistent_cache_paths = GetCachePaths();
    for (auto& path : persistent_cache_paths) {
      path += FLAGS_tera_tabletnode_path_prefix;
      if (path.back() != '/') {
        path.append("/");
      }
      path.assign(FormatPathString(path));
    }
  });
  return persistent_cache_paths;
}

static std::vector<int64_t> ParseCacheSize(const std::string& size) {
  if (size.empty()) {
    return {};
  }
  std::vector<std::string> sizes_str;
  SplitString(size, ";", &sizes_str);
  std::vector<int64_t> sizes;
  for (const auto& sz : sizes_str) {
    sizes.emplace_back(std::stoll(sz));
  }
  return sizes;
};

const std::vector<int64_t>& GetPersistentCacheSizes() {
  static std::vector<int64_t> persistent_cache_sizes;
  static std::once_flag once_flag;
  std::call_once(once_flag, []() {
    persistent_cache_sizes = ParseCacheSize(FLAGS_persistent_cache_sizes_in_MB);
  });
  return persistent_cache_sizes;
}

leveldb::Status GetPersistentCache(std::shared_ptr<leveldb::PersistentCache>* cache) {
  static std::shared_ptr<leveldb::PersistentCache> persistent_cache = nullptr;
  static std::once_flag once_flag;
  static leveldb::Status status;
  std::call_once(once_flag, []() {
    if (!FLAGS_tera_enable_persistent_cache) {
      return;
    }
    auto& cache_paths = GetPersistentCachePaths();
    auto& cache_sizes = GetPersistentCacheSizes();
    if (!cache_sizes.empty() && (cache_sizes.size() != cache_paths.size())) {
      LOG(FATAL) << "Unmatch cache size and cache path, please check tera.flag. Cache path num: "
                 << cache_paths.size() << ". Cache size num: " << cache_sizes.size();
    }
    if (cache_paths.empty()) {
      status = leveldb::Status::InvalidArgument("Empty persistent cache path.");
      return;
    }
    for (auto& cache_path : cache_paths) {
      leveldb::Env::Default()->CreateDir(cache_path);
    }
    leveldb::EnvOptions opt;
    opt.use_direct_io_read = FLAGS_tera_leveldb_use_direct_io_read;
    opt.use_direct_io_write = FLAGS_tera_leveldb_use_direct_io_write;
    opt.posix_write_buffer_size = FLAGS_tera_leveldb_posix_write_buffer_size;
    std::vector<leveldb::PersistentCacheConfig> configs;
    for (size_t i = 0; i != cache_paths.size(); ++i) {
      uint64_t size{0};
      status = GetPathDiskSize(cache_paths[i], &size);
      if (!status.ok()) {
        return;
      }
      assert(size > 2L << 30);
      // Use 2G of cache size for persistent cache meta data and other overheads.
      int64_t cache_size = cache_sizes.empty() ? size - (2L << 30) : cache_sizes[i] << 20;
      configs.emplace_back(leveldb::Env::Default(), cache_paths[i], cache_size);

      LOG(INFO) << "Initing persistent pache, path: " << cache_paths[i]
                << ", size: " << utils::ConvertByteToString(size)
                << ", cache size: " << utils::ConvertByteToString(cache_size);
      configs.back().SetEnvOptions(opt);
      configs.back().write_retry_times = FLAGS_persistent_cache_write_retry_times;
      if (FLAGS_tera_enable_persistent_cache_transfer_flash_env_files) {
        configs.back().transfer_flash_env_files = true;
      }
    }
    status = leveldb::NewShardedPersistentCache(configs, &persistent_cache);
  });

  if (status.ok()) {
    *cache = persistent_cache;
  }

  return status;
}

}  // namespace io
}  // namespace leveldb
