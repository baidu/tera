// Copyright (c) 2018, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include <algorithm>
#include <numeric>

#include "sharded_memtable.h"
#include "leveldb/table/merger.h"

namespace leveldb {

// For Base Mem Table
ShardedMemTable::ShardedMemTable(const InternalKeyComparator& cmp,
                                 CompactStrategyFactory* compact_strategy_factory,
                                 int32_t shard_num)
    : comparator_(cmp) {
  sharded_memtable_.resize(shard_num, nullptr);
  std::for_each(sharded_memtable_.begin(), sharded_memtable_.end(), [&](MemTable*& mem) {
    mem = new BaseMemTable(cmp, compact_strategy_factory);
    mem->Ref();
  });
  current_memtable_ = sharded_memtable_.begin();
}

// For MemTable on LevelDB
ShardedMemTable::ShardedMemTable(const std::string& dbname, const InternalKeyComparator& cmp,
                                 CompactStrategyFactory* compact_strategy_factory,
                                 size_t write_buffer_size, size_t block_size, Logger* info_log,
                                 int32_t shard_num)
    : comparator_(cmp) {
  sharded_memtable_.resize(shard_num, nullptr);
  std::for_each(sharded_memtable_.begin(), sharded_memtable_.end(), [&](MemTable*& mem) {
    mem = new MemTableOnLevelDB(dbname, cmp, compact_strategy_factory, write_buffer_size,
                                block_size, info_log);
    mem->Ref();
  });
  current_memtable_ = sharded_memtable_.begin();
}

ShardedMemTable::~ShardedMemTable() {
  assert(refs_ == 0);
  std::for_each(sharded_memtable_.begin(), sharded_memtable_.end(),
                [](MemTable* mem) { mem->Unref(); });
}

size_t ShardedMemTable::ApproximateMemoryUsage() {
  return std::accumulate(
      sharded_memtable_.begin(), sharded_memtable_.end(), (size_t)0,
      [](size_t sum, MemTable* mem) { return sum + mem->ApproximateMemoryUsage(); });
}

Iterator* ShardedMemTable::NewIterator() {
  std::vector<Iterator*> mem_iterators;
  mem_iterators.reserve(sharded_memtable_.size());
  std::for_each(sharded_memtable_.begin(), sharded_memtable_.end(),
                [&mem_iterators](MemTable* mem) { mem_iterators.push_back(mem->NewIterator()); });

  return NewMergingIterator(&comparator_, &mem_iterators[0], mem_iterators.size());
}

void ShardedMemTable::Add(SequenceNumber seq, ValueType type, const Slice& key,
                          const Slice& value) {
  if (current_memtable_ == sharded_memtable_.end()) {
    current_memtable_ = sharded_memtable_.begin();
  }
  (*current_memtable_)->Add(seq, type, key, value);
  ++current_memtable_;
  assert(last_seq_ < seq || seq == 0);
  last_seq_ = seq;
}

bool ShardedMemTable::Get(const LookupKey& key, std::string* value,
                          const std::map<uint64_t, uint64_t>& rollbacks, Status* s) {
  // This method is only used for kv-table,
  // but ShardedMemTable is not used for kv-table
  abort();
  return false;
}

uint64_t ShardedMemTable::GetSnapshot(uint64_t last_sequence) {
  std::for_each(sharded_memtable_.begin(), sharded_memtable_.end(),
                [last_sequence](MemTable* mem) { mem->GetSnapshot(last_sequence); });
  return last_sequence;
}

void ShardedMemTable::ReleaseSnapshot(uint64_t sequence_number) {
  std::for_each(sharded_memtable_.begin(), sharded_memtable_.end(),
                [sequence_number](MemTable* mem) { mem->ReleaseSnapshot(sequence_number); });
}
}
