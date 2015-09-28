// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include <iostream>

#include "db/builder.h"

#include "db/dbformat.h"
#include "db/filename.h"
#include "db/table_cache.h"
#include "db/version_edit.h"
#include "leveldb/db.h"
#include "leveldb/env.h"
#include "util/coding.h"

namespace leveldb {

bool SaveKVToTable(void* arg, const char* buffer, ssize_t size) {
    TableBuilder* builder = reinterpret_cast<TableBuilder*>(arg);
    const char* delim = strchr(buffer, '\t');
    if (NULL == delim || buffer == delim || buffer + size - 1 == delim) {
        return false;
    }
    Slice key(buffer, delim - buffer);
    Slice value(delim + 1, buffer + size - delim - 1);
    builder->Add(key, value);
    return true;
}

bool SaveKVToDB(void* arg, const char* buffer, ssize_t size) {
    DB* db = reinterpret_cast<DB*>(arg);
    const char* delim = strchr(buffer, '\t');
    if (NULL == delim || buffer == delim || buffer + size - 1 == delim) {
        return false;
    }
    Slice key(buffer, delim - buffer);
    Slice value(delim + 1, buffer + size - delim - 1);
    db->Put(WriteOptions(), key, value);
    return true;
}

// buffer should be formatted:
//    [key_size:uint32][key][value_size:uint32][value]
//
bool SaveSeqKVToDB(void* arg, const char* buffer, ssize_t size) {
    DB* db = reinterpret_cast<DB*>(arg);
    if (size <= static_cast<ssize_t>(sizeof(int64_t)) * 2) {
        return false;
    }
    Slice input(buffer, size);
    Slice key;
    Slice value;
    if (!GetLengthPrefixedSlice(&input, &key)) {
        std::cerr << "fail to parse key" << std::endl;
        return false;
    }
    if (!GetLengthPrefixedSlice(&input, &value)) {
        std::cerr << "fail to parse value" << std::endl;
        return false;
    }
    db->Put(WriteOptions(), key, value);
    return true;
}

bool GetKeyValueFromStdin(void* arg, bool (*saver)(void*, const char*, ssize_t)) {
    static size_t n = 10240;
    static char* buffer = new char[n];

    ssize_t line_size = 0;
    while ((line_size = getline(&buffer, &n, stdin)) != -1) {
        if (line_size > 0 && buffer[line_size - 1] == '\n') {
            line_size--;
        }
        if (line_size < 3) {
            std::cerr << "ignore empty line" << std::endl;
            continue;
        }
        if (!(*saver)(arg, buffer, line_size)) {
            std::cerr << "ignore invalid line: " << buffer << std::endl;
            continue;
        }
        return true;
    }
    return false;
}

// directly build sst table file
Status DirectBuildTable(const std::string& dbname,
                        int32_t file_no,
                        Env* env,
                        const Options& options) {
  Status s;

  std::string fname = TableFileName(dbname, file_no);
  uint64_t file_size = 0;
    WritableFile* file;
    s = env->NewWritableFile(fname, &file);
    if (!s.ok()) {
      return s;
    }

    TableBuilder* builder = new TableBuilder(options, file);
    while (GetKeyValueFromStdin(builder, SaveKVToTable));

    // Finish and check for builder errors
    if (s.ok()) {
      s = builder->Finish();
      if (s.ok()) {
          file_size = builder->FileSize();
      }
    } else {
      builder->Abandon();
    }
    delete builder;

    // Finish and check for file errors
    if (s.ok()) {
      s = file->Sync();
    }
    if (s.ok()) {
      s = file->Close();
    }
    delete file;
    file = NULL;

  if (s.ok() && file_size > 0) {
    // Keep it
  } else {
    env->DeleteFile(fname);
  }
  return s;
}

// directly build db
Status DirectBuildDB(const std::string& dbname,
                     const Options& options) {
    DB* db = NULL;
    Status s;

    s = DB::Open(options, dbname, &db);
    if (!s.ok()) {
        std::cerr << "fail to open db: " << dbname
            << ", error: " << s.ToString() << std::endl;
        return s;
    }
    while (GetKeyValueFromStdin(db, SaveSeqKVToDB));
    delete db;

    return s;
}

}  // namespace leveldb

void Usage() {
    std::cout << "Usage:" << std::endl;
    std::cout << "    ./db_import <db name> < kv_file" << std::endl;
    std::cout << std::endl << "<kv_file> format: "
        << "[key_size:uint32][key][value_size:uint32][value]" << std::endl;
}

int main(int argc, char* argv[]) {
    if (argc < 2) {
        Usage();
        return -1;
    }
    leveldb::Options opts;
    std::string dbname = argv[1];
    leveldb::DirectBuildDB(dbname, opts);
    return 0;
}

