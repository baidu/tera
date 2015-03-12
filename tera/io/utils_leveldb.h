// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef TERA_UTILS_UTILS_LEVELDB_H
#define TERA_UTILS_UTILS_LEVELDB_H

#include <map>
#include <string>

#include "leveldb/env.h"

namespace tera {
namespace io {

bool MergeTables(const std::string& mf, const std::string& mf1,
                 const std::string& mf2,
                 std::map<uint64_t, uint64_t>* mf2_file_maps,
                 leveldb::Env* db_env = NULL);

bool MergeTables(const std::string& table_path_1,
                 const std::string& table_path_2,
                 const std::string& merged_table = "",
                 leveldb::Env* db_env = NULL);

bool MergeTablesWithLG(const std::string& table_1,
                       const std::string& table_2,
                       const std::string& merged_table = "",
                       uint32_t lg_num = 1);

void InitDfsEnv();

leveldb::Env* LeveldbEnv();

bool MoveEnvDirToTrash(const std::string& subdir);

bool DeleteEnvDir(const std::string& subdir);

} // namespace io
} // namespace tera

#endif // TERA_UTILS_UTILS_CMD_H
