// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// Thread-safe (provides internal synchronization)

#ifndef STORAGE_LEVELDB_DB_TABLE_CACHE_H_
#define STORAGE_LEVELDB_DB_TABLE_CACHE_H_

#include <map>
#include <string>
#include <stdint.h>
#include "db/dbformat.h"
#include "leveldb/cache.h"
#include "leveldb/table.h"
#include "port/port.h"

namespace leveldb {

class Env;

class TableCache {
 public:
  TableCache(size_t byte_size);
  ~TableCache();

  // Return an iterator for the specified file number (the corresponding
  // file length must be exactly "file_size" bytes).  If "tableptr" is
  // non-NULL, also sets "*tableptr" to point to the Table object
  // underlying the returned iterator, or NULL if no Table object underlies
  // the returned iterator.  The returned "*tableptr" object is owned by
  // the cache and should not be deleted, and is valid for as long as the
  // returned iterator is live.
  Iterator* NewIterator(const ReadOptions& options,
                        const std::string& dbname,
                        uint64_t file_number,
                        uint64_t file_size,
                        Table** tableptr = NULL);

  // Specify key range of iterator [smallest, largest]. There are some
  // out-of-range keys in table file after tablet merging and splitting.
  Iterator* NewIterator(const ReadOptions& options,
                        const std::string& dbname,
                        uint64_t file_number,
                        uint64_t file_size,
                        const Slice& smallest,
                        const Slice& largest,
                        Table** tableptr = NULL);

  // If a seek to internal key "k" in specified file finds an entry,
  // call (*handle_result)(arg, found_key, found_value).
  Status Get(const ReadOptions& options,
             const std::string& dbname,
             uint64_t file_number,
             uint64_t file_size,
             const Slice& k,
             void* arg,
             void (*handle_result)(void*, const Slice&, const Slice&));

  // Evict any entry for the specified file number
  void Evict(const std::string& dbname, uint64_t file_number);

  // Returns hit rate
  double HitRate(bool force_clear) { return cache_->HitRate(force_clear); }

  // Returns table entries
  size_t TableEntries() { return cache_->Entries(); }

  // Returns memory usage of opened table's index block
  size_t ByteSize() { return cache_->TotalCharge(); }

 private:
  Cache* cache_;

  port::Mutex mu_;
  struct Waiter {
    port::CondVar cv;
    int wait_num;
    Status status;
    bool done;
    Waiter(port::Mutex* mu):cv(mu), wait_num(0), done(false) {}
  };
  typedef std::map<std::string, Waiter*> WaitFileList;
  WaitFileList wait_files_;
  Status FindTable(const std::string& dbname, const Options* options,
                   uint64_t file_number, uint64_t file_size, Cache::Handle**);
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_TABLE_CACHE_H_
