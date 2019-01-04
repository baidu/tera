// Copyright (c) 2018, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// This file is methods used for transfering from env_flash to persistent cache.
// Maybe deprecated in future version.

#include <algorithm>
#include "persistent_cache_impl.h"
#include "util/stop_watch.h"

namespace leveldb {

static void CollectLgFiles(const std::string &lg_path, Env *env, std::vector<std::string> *result) {
  assert(result);
  std::vector<std::string> sst_files;
  auto status = env->GetChildren(lg_path, &sst_files);
  if (!status.ok()) {
    LEVELDB_LOG(
        "Get children failed when move env_flash file to persistent cache, path: %s, "
        "reason: %s\n",
        lg_path.c_str(), status.ToString().c_str());
    return;
  }
  std::for_each(sst_files.begin(), sst_files.end(),
                [result, &lg_path, env](const std::string &sst_file_name) {
                  auto file_path = lg_path + "/" + sst_file_name;
                  Slice file_path_slice{file_path};
                  if (!file_path_slice.ends_with(".sst")) {
                    return;
                  }

                  SystemFileType type;
                  auto s = env->GetFileType(file_path, &type);
                  if (!s.ok()) {
                    LEVELDB_LOG("Get file type failed, path %s, reason %s.", file_path.c_str(),
                                s.ToString().c_str());
                    return;
                  }

                  if (type == SystemFileType::kRegularFile) {
                    result->emplace_back(std::move(file_path));
                  }
                });
}

static void CollectTabletFiles(const std::string &tablet_path, Env *env,
                               std::vector<std::string> *result) {
  std::vector<std::string> lg_paths;
  auto status = env->GetChildren(tablet_path, &lg_paths);
  if (!status.ok()) {
    LEVELDB_LOG(
        "Get children failed when move env_flash file to persistent cache, path: %s, "
        "reason: %s\n",
        tablet_path.c_str(), status.ToString().c_str());
    return;
  }
  for (const auto &lg_path : lg_paths) {
    CollectLgFiles(tablet_path + "/" + lg_path, env, result);
  }
}

static void CollectTableFiles(const std::string &table_path, Env *env,
                              std::vector<std::string> *result) {
  std::vector<std::string> tablet_paths;
  auto status = env->GetChildren(table_path, &tablet_paths);
  if (!status.ok()) {
    LEVELDB_LOG(
        "Get children failed when move env_flash file to persistent cache, path: %s, "
        "reason: %s\n",
        table_path.c_str(), status.ToString().c_str());
  }
  for (const auto &tablet_path : tablet_paths) {
    CollectTabletFiles(table_path + "/" + tablet_path, env, result);
  }
}

void PersistentCacheImpl::AddExistingFile(const Slice &key, const std::string &file_name) {
  auto env = opt_.env;
  Status s;

  auto cache_file_name = file_name + "." + std::to_string(writer_cache_id_) + ".rc";

  if (!(s = env->RenameFile(file_name, cache_file_name)).ok()) {
    LEVELDB_LOG("Rename cache file from %s to %s failed, reason: %s.\n", file_name.c_str(),
                cache_file_name.c_str(), s.ToString().c_str());
    return;
  }

  std::unique_ptr<RandomAccessCacheFile> file{
      new RandomAccessCacheFile{writer_cache_id_, opt_.env, opt_.env_opt, cache_file_name}};

  uint64_t file_size;
  if (!(s = env->GetFileSize(file->Path(), &file_size)).ok()) {
    LEVELDB_LOG("Get file size failed, file: %s, reason: %s.\n", file->Path().c_str(),
                s.ToString().c_str());
    return;
  }

  if (!(s = file->Open()).ok()) {
    LEVELDB_LOG("Open cache file failed, reason: %s.\n", s.ToString().c_str());
    return;
  }

  if (!(s = MakeRoomForWrite(file_size)).ok()) {
    LEVELDB_LOG("Make room for cache file failed, reason: %s.\n", s.ToString().c_str());
    return;
  }

  ++writer_cache_id_;
  auto raw_file_ptr = file.release();
  auto success = metadata_.AddCacheFile(raw_file_ptr);
  assert(success);

  if (!Insert(key, raw_file_ptr)) {
    ForceEvict(raw_file_ptr);
    return;
  }

  LEVELDB_LOG("Add existing cache file success, key:%s, file:%s.\n", key.ToString().c_str(),
              file_name.c_str());
}

void PersistentCacheImpl::PullEnvFlashFiles() {
  StopWatchMicro timer(opt_.env, true);
  LEVELDB_LOG("Start pulling env flash files to persistent cache in path %s.\n",
              GetCachePath().c_str());
  std::vector<std::string> sst_files;
  std::vector<std::string> table_paths;

  auto status = opt_.env->GetChildren(GetCachePath(), &table_paths);
  if (!status.ok()) {
    LEVELDB_LOG(
        "Get children failed when move env_flash file to persistent cache, path: %s, "
        "reason: %s\n",
        GetCachePath().c_str(), status.ToString().c_str());
  } else {
    for (const auto &table_path : table_paths) {
      auto full_table_path = GetCachePath() + "/" + table_path;
      bool is_same_path = false;
      status = opt_.env->IsSamePath(full_table_path, GetMetaPath(), &is_same_path);
      if (!status.ok()) {
        LEVELDB_LOG("Error checking same path, path1 %s, path2 %s, reason %s, skip it.\n",
                    full_table_path.c_str(), GetMetaPath().c_str(), status.ToString().c_str());
        continue;
      }
      if (!is_same_path) {
        CollectTableFiles(full_table_path, opt_.env, &sst_files);
      }
    }

    for (auto &file_name : sst_files) {
      Slice key{file_name};
      key.remove_specified_prefix(GetCachePath());
      while (key.starts_with("/")) {
        key.remove_prefix(1);
      }
      assert(key.size());
      AddExistingFile(key, file_name);
    }
  }
  LEVELDB_LOG("Pull env flash files in path %s done, cost: %lu ms.", GetCachePath().c_str(),
              timer.ElapsedMicros() / 1000);
}

}  // namespace leveldb
