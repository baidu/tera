// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef TERA_IO_UTILS_LEVELDB_H_
#define TERA_IO_UTILS_LEVELDB_H_

#include <map>
#include <string>

#include "leveldb/env.h"

namespace tera {
namespace io {

void InitBaseEnv();

void InitCacheEnv();

// return the base env leveldb used (dfs/local), singleton
leveldb::Env* LeveldbBaseEnv();

// return the cache env leveldb used (dfs/local), singleton
leveldb::Env* LeveldbCacheEnv();

// return the mem env leveldb used, singleton
leveldb::Env* LeveldbMemEnv();

// return the flash env leveldb used, singleton
leveldb::Env* LeveldbFlashEnv();

// return the mock env leveldb used, singleton
// for testing
leveldb::Env* LeveldbMockEnv();

std::string GetTrashDir();

bool MoveEnvDirToTrash(const std::string& subdir);

void CleanTrashDir();

bool DeleteEnvDir(const std::string& subdir, leveldb::Env* env);

} // namespace io
} // namespace tera

#endif // TERA_IO_UTILS_LEVELDB_H
