// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "leveldb/db.h"

#include <iostream>

#include "db/builder.h"
#include "db/db_iter.h"
#include "db/dbformat.h"
#include "db/filename.h"
#include "db/log_reader.h"
#include "db/log_writer.h"
#include "db/memtable.h"
#include "db/table_cache.h"
#include "db/version_set.h"
#include "db/write_batch_internal.h"
#include "leveldb/db.h"
#include "leveldb/env.h"
#include "leveldb/status.h"
#include "leveldb/table.h"
#include "leveldb/table_builder.h"
#include "leveldb/table_utils.h"
#include "db/db_table.h"
#include "util/string_ext.h"

namespace leveldb {


// Default implementations of convenience methods that subclasses of DB
// can call if they wish
Status DB::Put(const WriteOptions& opt, const Slice& key, const Slice& value) {
  WriteBatch batch;
  batch.Put(key, value);
  return Write(opt, &batch);
}

Status DB::Delete(const WriteOptions& opt, const Slice& key) {
  WriteBatch batch;
  batch.Delete(key);
  return Write(opt, &batch);
}

DB::~DB() { }

Status DB::Open(const Options& options, const std::string& dbname,
                DB** dbptr) {
    *dbptr = NULL;

    DBTable* db_table = new DBTable(options, dbname);
    Status s = db_table->Init();
    if (s.ok()) {
        *dbptr = db_table;
    } else {
        delete db_table;
    }
    return s;
}

Status DestroyLG(const std::string& lgname, const Options& options) {
    Env* env = options.env;
    std::vector<std::string> filenames;
    env->GetChildren(lgname, &filenames);
    if (filenames.empty()) {
        return Status::OK();
    }

    FileLock* lock;
    const std::string lockname = LockFileName(lgname);
    Status result = env->LockFile(lockname, &lock);
    if (!result.ok()) {
        return result;
    }

    uint64_t number;
    FileType type;
    for (size_t i = 0; i < filenames.size(); i++) {
        if (ParseFileName(filenames[i], &number, &type) &&
            type != kDBLockFile) {  // Lock file will be deleted at end
                Status del = env->DeleteFile(lgname + "/" + filenames[i]);
                if (result.ok() && !del.ok()) {
                result = del;
            }
        }
    }

    filenames.clear();
    env->GetChildren(lgname + "/lost", &filenames);
    for (size_t i = 0; i < filenames.size(); i++) {
        Status del = env->DeleteFile(lgname + "/lost/" + filenames[i]);
    }
    env->DeleteDir(lgname + "/lost");
    env->UnlockFile(lock);  // Ignore error since state is already gone
    env->DeleteDir(lgname);  // Ignore error in case dir contains other files

    return result;
}

Status DestroyDB(const std::string& dbname, const Options& opt) {
    Options options = opt;
    Env* env = options.env;

    std::vector<std::string> filenames;
    env->GetChildren(dbname, &filenames);
    if (filenames.empty()) {
        return Status::OK();
    }

    FileLock* lock;
    const std::string lockname = LockFileName(dbname);
    Status result = env->LockFile(lockname, &lock);
    if (!result.ok()) {
        return result;
    }

    // clean db/lg dir
    if (options.exist_lg_list == NULL) {
        options.exist_lg_list = new std::set<uint32_t>;
        options.exist_lg_list->insert(0);
    }
    std::set<uint32_t>::iterator it = options.exist_lg_list->begin();
    for (; it != options.exist_lg_list->end(); ++it) {
        std::string lgname = dbname + "/" + Uint64ToString(*it);
        Options lg_opt = options;
        if (options.lg_info_list && options.lg_info_list->size() > 0) {
            std::map<uint32_t, LG_info*>::iterator info_it =
                options.lg_info_list->find(*it);
            if (info_it != options.lg_info_list->end() && info_it->second != NULL) {
                LG_info* lg_info = info_it->second;
                if (lg_info->env && lg_info->env != lg_opt.env) {
                    lg_opt.env = lg_info->env;
                }
                lg_opt.compression = lg_info->compression;
                delete lg_info;
                info_it->second = NULL;
            }
        } else if (options.lg_info_list) {
            delete options.lg_info_list;
            options.lg_info_list = NULL;
        }
        Status lg_ret = DestroyLG(lgname, lg_opt);
        if (!lg_ret.ok()) {
            result = lg_ret;
        }
    }
    delete options.exist_lg_list;

    // clean db/ dir
    uint64_t number;
    FileType type;
    for (size_t i = 0; i < filenames.size(); i++) {
        if (ParseFileName(filenames[i], &number, &type) &&
            type != kDBLockFile) {  // Lock file will be deleted at end
                Status del = env->DeleteFile(dbname + "/" + filenames[i]);
                if (result.ok() && !del.ok()) {
                result = del;
            }
        }
    }

    // clean db/lost dir
    filenames.clear();
    env->GetChildren(dbname + "/lost", &filenames);
    for (size_t i = 0; i < filenames.size(); i++) {
        Status del = env->DeleteFile(dbname + "/lost/" + filenames[i]);
    }
    env->DeleteDir(dbname + "/lost");
    env->UnlockFile(lock);  // Ignore error since state is already gone
    env->DeleteDir(dbname);  // Ignore error in case dir contains other files

    return result;
}

} // namespace leveldb
