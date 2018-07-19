// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/table_cache.h"

#include "db/filename.h"
#include "leveldb/env.h"
#include "leveldb/table.h"
#include "util/coding.h"
#include "util/mutexlock.h"

namespace leveldb {

struct TableAndFile {
  RandomAccessFile* file;
  Table* table;
};

static void DeleteEntry(const Slice& key, void* value) {
  TableAndFile* tf = reinterpret_cast<TableAndFile*>(value);
  delete tf->table;
  delete tf->file;
  delete tf;
}

static void UnrefEntry(void* arg1, void* arg2) {
  Cache* cache = reinterpret_cast<Cache*>(arg1);
  Cache::Handle* h = reinterpret_cast<Cache::Handle*>(arg2);
  cache->Release(h);
}

static std::string GetTableFileSign(const std::string& dbname,
                                    const uint64_t* file_number) {
    return dbname + std::string(reinterpret_cast<const char*>(file_number),
                                sizeof(*file_number));
}

TableCache::TableCache(size_t byte_size)
    : cache_(NewLRUCache(byte_size)) {
}

TableCache::~TableCache() {
  delete cache_;
}

Status TableCache::FindTable(const std::string& dbname, const Options* options,
                             uint64_t file_number, uint64_t file_size,
                             Cache::Handle** handle) {
  Status s;
  std::string sign = GetTableFileSign(dbname, &file_number);
  Slice key(sign);
  MutexLock lock(&mu_);
  *handle = cache_->Lookup(key);
  if (*handle == NULL) {
    //printf("file not in cache %s, try open it\n", fname.c_str());
    Waiter* w = NULL;
    WaitFileList::iterator it = wait_files_.find(sign);
    if (it != wait_files_.end()){
      //printf("file in open_list %s, wait\n", fname.c_str());
      w = it->second;
      w->wait_num ++;
      while (!w->done) {
        w->cv.Wait();
      }
      *handle = cache_->Lookup(key);
      if (*handle == NULL && !w->status.ok()) {
        s = w->status;
      } else if (*handle == NULL) {
        s = Status::IOError("Open sst error in FindTablet");
      }

      if (--w->wait_num == 0) {
        // last thread wait for open
        wait_files_.erase(sign);
        //printf("wait done %s, delete cv\n", fname.c_str());
        delete w;
      } else {
        //printf("wait done %s, not last\n", fname.c_str());
      }
    } else {
      //printf("file not in open_list %s, Do open\n", fname.c_str());
      w = new Waiter(&mu_);
      w->wait_num = 1;
      wait_files_[sign] = w;

      // Unlock when open file
      mu_.Unlock();
      RandomAccessFile* file = NULL;
      Table* table = NULL;
      std::string fname = TableFileName(dbname, file_number);
      s = options->env->NewRandomAccessFile(fname, file_size, &file, EnvOptions(*options));
      if (s.ok()) {
        s = Table::Open(*options, file, file_size, &table);
      }

      if (!s.ok()) {
        assert(table == NULL);
        fprintf(stderr, "open sstable file failed: [%s] %s\n", fname.c_str(), s.ToString().c_str());
        delete file;
        // We do not cache error results so that if the error is transient,
        // or somebody repairs the file, we recover automatically.
      } else {
        TableAndFile* tf = new TableAndFile;
        tf->file = file;
        tf->table = table;
        *handle = cache_->Insert(key, tf, table->IndexBlockSize(), &DeleteEntry);
      }
      mu_.Lock();
      if (--w->wait_num == 0) {
        wait_files_.erase(sign);
        //printf("open done %s, no wait thread\n", fname.c_str());
        delete w;
      } else {
        //printf("open done %s, signal all wait thread\n", fname.c_str());
        w->status = s;
        w->done = true;
        w->cv.SignalAll();
      }
    }
  }
  return s;
}

Iterator* TableCache::NewIterator(const ReadOptions& options,
                                  const std::string& dbname,
                                  uint64_t file_number,
                                  uint64_t file_size,
                                  Table** tableptr) {
  return NewIterator(options, dbname, file_number, file_size, "", "", tableptr);
}

Iterator* TableCache::NewIterator(const ReadOptions& options,
                                  const std::string& dbname,
                                  uint64_t file_number,
                                  uint64_t file_size,
                                  const Slice& smallest,
                                  const Slice& largest,
                                  Table** tableptr) {
  assert(options.db_opt);
  if (tableptr != NULL) {
    *tableptr = NULL;
  }

  Cache::Handle* handle = NULL;
  Status s = FindTable(dbname, options.db_opt, file_number, file_size, &handle);
  if (!s.ok()) {
    return NewErrorIterator(s);
  }

  Table* table = reinterpret_cast<TableAndFile*>(cache_->Value(handle))->table;
  Iterator* result = table->NewIterator(options, smallest, largest);
  result->RegisterCleanup(&UnrefEntry, cache_, handle);
  if (tableptr != NULL) {
    *tableptr = table;
  }
  return result;
}

Status TableCache::Get(const ReadOptions& options,
                       const std::string& dbname,
                       uint64_t file_number,
                       uint64_t file_size,
                       const Slice& k,
                       void* arg,
                       void (*saver)(void*, const Slice&, const Slice&)) {
  assert(options.db_opt);
  Cache::Handle* handle = NULL;
  Status s = FindTable(dbname, options.db_opt, file_number, file_size, &handle);
  if (s.ok()) {
    Table* t = reinterpret_cast<TableAndFile*>(cache_->Value(handle))->table;
    s = t->InternalGet(options, k, arg, saver);
    cache_->Release(handle);
  }
  return s;
}

void TableCache::Evict(const std::string& dbname, uint64_t file_number) {
  cache_->Erase(Slice(GetTableFileSign(dbname, &file_number)));
}

}  // namespace leveldb
