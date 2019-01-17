// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_DB_MEMTABLE_H_
#define STORAGE_LEVELDB_DB_MEMTABLE_H_

#include <string>
#include "leveldb/db.h"
#include "leveldb/compact_strategy.h"
#include "db/dbformat.h"
#include "db/skiplist.h"
#include "util/arena.h"

namespace leveldb {

class InternalKeyComparator;
class Mutex;
class BaseMemTableIterator;

class MemTable {
  // MemTables are reference counted.  The initial reference count
  // is zero and the caller must call Ref() at least once.
 public:
  MemTable() = default;
  virtual ~MemTable() {}
  // Increase reference count.
  virtual void Ref() = 0;

  // Drop reference count.  Delete if no more references exist.
  virtual void Unref() = 0;

  // Returns an estimate of the number of bytes of data in use by this
  // data structure.
  // REQUIRES: external synchronization to prevent simultaneous
  // operations on the same BaseMemTable.
  virtual size_t ApproximateMemoryUsage() = 0;

  // Return an iterator that yields the contents of the memtable.
  //
  // The caller must ensure that the underlying BaseMemTable remains live
  // while the returned iterator is live.  The keys returned by this
  // iterator are internal keys encoded by AppendInternalKey in the
  // db/format.{h,cc} module.
  virtual Iterator* NewIterator() = 0;

  // Add an entry into memtable that maps key to value at the
  // specified sequence number and with the specified type.
  // Typically value will be empty if type==kTypeDeletion.
  virtual void Add(SequenceNumber seq, ValueType type, const Slice& key, const Slice& value) = 0;

  // If memtable contains a value for key, store it in *value and return true.
  // If memtable contains a deletion for key, store a NotFound() error
  // in *status and return true.
  // Else, return false.
  virtual bool Get(const LookupKey& key, std::string* value,
                   const std::map<uint64_t, uint64_t>& rollbacks, Status* s) = 0;

  // These two methods are only used for memtable on leveldb
  virtual uint64_t GetSnapshot(uint64_t) = 0;
  virtual void ReleaseSnapshot(uint64_t) = 0;

  virtual SequenceNumber GetLastSequence() const = 0;
  virtual bool Empty() = 0;
  virtual void SetNonEmpty() = 0;
  virtual bool BeingFlushed() = 0;
  virtual void SetBeingFlushed(bool flag) = 0;
  // No copying allowed
  MemTable(const MemTable&) = delete;
  void operator=(const MemTable&) = delete;
};

class BaseMemTable : public MemTable {
 public:
  BaseMemTable(const InternalKeyComparator& comparator,
               CompactStrategyFactory* compact_strategy_factory);

  void Ref() { ++refs_; }

  void Unref() {
    --refs_;
    assert(refs_ >= 0);
    if (refs_ <= 0) {
      delete this;
    }
  }

  virtual size_t ApproximateMemoryUsage();
  virtual Iterator* NewIterator();

  virtual void Add(SequenceNumber seq, ValueType type, const Slice& key, const Slice& value);

  virtual bool Get(const LookupKey& key, std::string* value,
                   const std::map<uint64_t, uint64_t>& rollbacks, Status* s);

  SequenceNumber GetLastSequence() const { return last_seq_; }

  bool Empty() { return empty_; }
  void SetNonEmpty() { empty_ = false; }

  bool BeingFlushed() { return being_flushed_; }
  void SetBeingFlushed(bool flag) {
    assert(flag != being_flushed_);
    being_flushed_ = flag;
  }

  // GetSnapshot and ReleaseSnapshot are not used for base memtable;
  virtual uint64_t GetSnapshot(uint64_t) {
    abort();
    return -1;
  }
  virtual void ReleaseSnapshot(uint64_t) { abort(); }

  virtual ~BaseMemTable();

 private:
  SequenceNumber last_seq_;
  struct KeyComparator {
    const InternalKeyComparator comparator;
    explicit KeyComparator(const InternalKeyComparator& c) : comparator(c) {}
    int operator()(const char* a, const char* b) const;
  };
  friend class BaseMemTableIterator;
  friend class BaseMemTableBackwardIterator;

  typedef SkipList<const char*, KeyComparator> Table;

  KeyComparator comparator_;
  int refs_;
  bool being_flushed_;

  Arena arena_;
  Table table_;
  bool empty_;
  CompactStrategyFactory* compact_strategy_factory_;
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_MEMTABLE_H_
