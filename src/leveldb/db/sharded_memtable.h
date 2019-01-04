// Copyright (c) 2018, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#pragma once
#include "memtable.h"
#include "memtable_on_leveldb.h"

namespace leveldb {

class ShardedMemTable : public MemTable {
 public:
  // For Base MemTble
  ShardedMemTable(const InternalKeyComparator &cmp,
                  CompactStrategyFactory *compact_strategy_factory, int32_t shard_num);

  // For MemTable on LevelDB
  ShardedMemTable(const std::string &dbname, const InternalKeyComparator &cmp,
                  CompactStrategyFactory *compact_strategy_factory, size_t write_buffer_size,
                  size_t block_size, Logger *info_log, int32_t shard_num);

  virtual void Ref() override { ++refs_; }

  virtual void Unref() override {
    --refs_;
    assert(refs_ >= 0);
    if (refs_ <= 0) {
      delete this;
    }
  }

  virtual ~ShardedMemTable() override;

  virtual size_t ApproximateMemoryUsage() override;

  virtual Iterator *NewIterator() override;

  virtual void Add(SequenceNumber number, ValueType type, const Slice &slice,
                   const Slice &slice1) override;

  virtual bool Get(const LookupKey &key, std::string *value,
                   const std::map<uint64_t, uint64_t> &rollbacks, Status *s) override;

  virtual SequenceNumber GetLastSequence() const override { return last_seq_; }

  virtual bool Empty() override { return empty_; }

  virtual void SetNonEmpty() override { empty_ = false; }

  virtual bool BeingFlushed() override { return being_flushed_; }

  virtual void SetBeingFlushed(bool flag) override {
    assert(flag != being_flushed_);
    being_flushed_ = flag;
  }

  virtual uint64_t GetSnapshot(uint64_t last_sequence) override;

  virtual void ReleaseSnapshot(uint64_t sequence_number) override;

 private:
  InternalKeyComparator comparator_;
  std::vector<MemTable *>::iterator current_memtable_;
  std::vector<MemTable *> sharded_memtable_;

  SequenceNumber last_seq_{0};
  int refs_{0};
  bool being_flushed_{false};
  bool empty_{true};
};
}
