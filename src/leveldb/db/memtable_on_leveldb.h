// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Author: Junyi Sun (sunjunyi01@baidu.com)
// Description: memtable built on leveldb

#ifndef  STORAGE_LEVELDB_DB_MEMTABLE_ON_LEVELDB_H_
#define  STORAGE_LEVELDB_DB_MEMTABLE_ON_LEVELDB_H_

#include "db/memtable.h"
#include "helpers/memenv/memenv.h"
#include "db/db_impl.h"
#include "leveldb/env.h"

namespace leveldb {

class MemTableOnLevelDB : public MemTable{

public:

    MemTableOnLevelDB (const InternalKeyComparator& comparator,
                       CompactStrategyFactory* compact_strategy_factory,
                       size_t write_buffer_size,
                       size_t block_size,
                       Logger* info_log);

    ~MemTableOnLevelDB();

    size_t ApproximateMemoryUsage();

    Iterator* NewIterator();

    void Add(SequenceNumber seq, ValueType type,
             const Slice& key,
             const Slice& value);

    bool Get(const LookupKey& key, std::string* value, Status* s);

    const uint64_t GetSnapshot(uint64_t last_sequence);

    void TryReleaseSnapshot(uint64_t sequence_number);

private:
    Env* GetBaseEnv();
    leveldb::DBImpl* memdb_;
    leveldb::Env* memenv_;
};

} //namespace leveldb

#endif  //STORAGE_LEVELDB_DB__MEMTABLE_ON_LEVELDB_H_

