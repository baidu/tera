// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef TERA_IO_UTILS_LEVELDB_H_
#define TERA_IO_UTILS_LEVELDB_H_

#include <map>
#include <string>

#include "leveldb/env.h"
#include "leveldb/persistent_cache.h"

namespace tera {
namespace io {

void InitDfsEnv();

// return the base env leveldb used (dfs/local), singleton
leveldb::Env* LeveldbBaseEnv();

// return the ssd block cache env, singleton
leveldb::Env* DefaultFlashBlockCacheEnv();

// return the mem env leveldb used, singleton
leveldb::Env* LeveldbMemEnv();

// return the flash env leveldb used, singleton
leveldb::Env* LeveldbFlashEnv();

// return the mock env leveldb used, singleton
// for testing
leveldb::Env* LeveldbMockEnv();

std::string GetTrashDir();

std::string GetTrackableGcTrashDir();

bool MoveEnvDirToTrash(const std::string& subdir);

leveldb::Status MoveSstToTrackableGcTrash(const std::string& table_name, uint64_t tablet_id,
                                          uint32_t lg_id, uint64_t file_id);

void CleanTrashDir();

bool TryDeleteEmptyDir(const std::string& dir_path, size_t total_children_size,
                       size_t deleted_children_size);

leveldb::Status DeleteTrashFileIfExpired(const std::string& file_path);

void CleanTrackableGcTrash();

leveldb::Status DeleteEnvDir(const std::string& subdir);

const std::vector<std::string>& GetCachePaths();
const std::vector<std::string>& GetPersistentCachePaths();

leveldb::Status GetPersistentCache(std::shared_ptr<leveldb::PersistentCache>* cache);
}  // namespace io
}  // namespace tera

#endif  // TERA_IO_UTILS_LEVELDB_H
