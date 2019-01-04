// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Author: Junyi Sun (sunjunyi01@baidu.com)
// Description: memtable built on leveldb

#ifndef STORAGE_LEVELDB_DB_MEMTABLE_ON_LEVELDB_H_
#define STORAGE_LEVELDB_DB_MEMTABLE_ON_LEVELDB_H_

#include <atomic>
#include "db/memtable.h"
#include "helpers/memenv/memenv.h"
#include "db/db_impl.h"
#include "leveldb/env.h"

namespace leveldb {

class MemTableOnLevelDB : public MemTable {
 public:
  MemTableOnLevelDB(const std::string& dbname, const InternalKeyComparator& comparator,
                    CompactStrategyFactory* compact_strategy_factory, size_t write_buffer_size,
                    size_t block_size, Logger* info_log);

  ~MemTableOnLevelDB();

  size_t ApproximateMemoryUsage();

  Iterator* NewIterator();

  void Add(SequenceNumber seq, ValueType type, const Slice& key, const Slice& value);

  uint64_t GetSnapshot(uint64_t last_sequence);

  void ReleaseSnapshot(uint64_t sequence_number);

  SequenceNumber GetLastSequence() const { return last_seq_; }

  void Ref() { ++refs_; }

  void Unref() {
    --refs_;
    assert(refs_ >= 0);
    if (refs_ <= 0) {
      delete this;
    }
  }

  bool Empty() { return empty_; }

  void SetNonEmpty() { empty_ = false; }

  bool BeingFlushed() { return being_flushed_; }

  void SetBeingFlushed(bool flag) {
    assert(flag != being_flushed_);
    being_flushed_ = flag;
  }

  // No body use this method for the followed reasons:
  // 1. memtable_on_level_db is only used in lg's schema.
  // 2. Get method in memtable/leveldb is only used in kv-table.
  // 3. A table with lg schema is not a kv-table.
  bool Get(const LookupKey& key, std::string* value, const std::map<uint64_t, uint64_t>&,
           Status* s);

 private:
  SequenceNumber last_seq_{0};
  int refs_{0};
  bool being_flushed_{false};
  bool empty_{true};
  Env* GetBaseEnv();
  leveldb::DBImpl* memdb_;
  leveldb::Env* memenv_;
  static std::atomic<int64_t> unique_id_;
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB__MEMTABLE_ON_LEVELDB_H_
