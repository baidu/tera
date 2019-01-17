// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef STORAGE_LEVELDB_DB_TABLE_UTILS_H
#define STORAGE_LEVELDB_DB_TABLE_UTILS_H

#include <map>
#include <string>

#include "leveldb/env.h"
#include "leveldb/comparator.h"

namespace leveldb {

void ArchiveFile(Env* env, const std::string& fname);

bool HandleDumpCommand(Env* env, char** files, int num);

bool DumpFile(Env* env, const std::string& fname);
}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_TABLE_UTILS_H
