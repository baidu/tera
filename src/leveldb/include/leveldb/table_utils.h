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

bool HandleMergeCommnad(Env* env, char** files, int num);

bool DumpFile(Env* env, const std::string& fname);

// for merge

bool MergeManifestFile(Env* env, const std::string& final_name,
                       char** files, int num,
                       std::map<std::string, std::map<uint64_t, uint64_t> >* file_maps);

bool IsTeraFlagFile(const std::string& file_path);

bool BackupNotSSTFile(Env* env, const std::string& dir_path,
                      std::string* mf);

bool RollbackBackupNotSst(Env* env, const std::string& dir_path);

bool CleanFlagFile(Env* env, const std::string& path);

bool DumpFileMapToDir(Env* env, const std::string& to_path,
                      const std::string& from_path,
                      const std::map<uint64_t, uint64_t>& file_map);

bool MoveMergedSst(Env* env, const std::string& path);

bool MoveMergedSst(Env* env, const std::string& from_path,
                   const std::string& to_path,
                   const std::map<uint64_t, uint64_t>& file_map);

bool RenameTable(Env* env, const std::string& from,
                 const std::string& to);
bool DeleteTable(Env* env, const std::string& name);

bool MigrateTableFile(Env* env, const std::string& dbpath,
                      const std::string& sst_file);

bool MergeBoth(Env* env, Comparator* cmp,
               const std::string& merged_fn,
               const std::string& fn1, const std::string& fn2,
               std::map<uint64_t, uint64_t>* file_num_map);

// for split

bool GetSplitPath(const std::string& path, std::string* split_path1,
                  std::string* split_path2);
bool GetParentPath(const std::string& path, std::string* parent_path);
bool GetDBpathAndLGid(const std::string& path, std::string* db_path,
                      std::string* lg_id_str = NULL, uint64_t* lg_id = NULL);
std::string GetSplitGapNumString(const std::string& cur_path,
                                 const std::string& real_path);

} // namespace leveldb

#endif // STORAGE_LEVELDB_DB_TABLE_UTILS_H
