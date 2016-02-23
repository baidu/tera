// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef TERA_IO_TTLKV_COMPACT_STRATEGY_H_
#define TERA_IO_TTLKV_COMPACT_STRATEGY_H_

#include "common/mutex.h"
#include "leveldb/compact_strategy.h"
#include "leveldb/raw_key_operator.h"
#include "proto/table_schema.pb.h"

namespace tera {
namespace io {

class KvCompactStrategy : public leveldb::CompactStrategy {
public:
    KvCompactStrategy(const TableSchema& schema);
    virtual ~KvCompactStrategy();

    virtual bool Drop(const leveldb::Slice& k, uint64_t n,
                      const std::string& lower_bound);

    // tera-specific, based on all-level iterators.
    // used in LowLevelScan
    virtual bool ScanDrop(const leveldb::Slice& k, uint64_t n);

    virtual bool ScanMergedValue(leveldb::Iterator* it, std::string* merged_value,
                                 int64_t* merged_num);

    virtual bool MergeAtomicOPs(leveldb::Iterator* it, std::string* merged_value,
                                std::string* merged_key);

    virtual const char* Name() const;

    virtual void SetSnapshot(uint64_t snapshot);

private:
    TableSchema schema_;
    const leveldb::RawKeyOperator* raw_key_operator_;
    uint64_t snapshot_;
};

class KvCompactStrategyFactory : public leveldb::CompactStrategyFactory {
public:
    KvCompactStrategyFactory(const TableSchema& schema);
    virtual KvCompactStrategy* NewInstance();
    virtual const char* Name() const {
        return "tera.TTLKvCompactStrategyFactory";
    }
    virtual void SetArg(const void* arg) {
        MutexLock lock(&mutex_);
        schema_.CopyFrom(*(TableSchema*)arg);
    }

private:
    TableSchema schema_;
    mutable Mutex mutex_;
};

} // namespace io
} // namespace tera

#endif // TERA_IO_TTLKV_COMPACT_STRATEGY_H_
