// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "io/ttlkv_compact_strategy.h"
#include "io/io_utils.h"
#include "leveldb/slice.h"

namespace tera {
namespace io {

using namespace leveldb;

KvCompactStrategy::KvCompactStrategy(const TableSchema& schema)
    : schema_(schema),
      raw_key_operator_(GetRawKeyOperatorFromSchema(schema_)) {
    VLOG(11) << "KvCompactStrategy construct";
}

KvCompactStrategy::~KvCompactStrategy() {
}

const char* KvCompactStrategy::Name() const {
    return "tera.TTLKvCompactStrategy";
}

void KvCompactStrategy::SetSnapshot(uint64_t snapshot) {
    snapshot_ = snapshot;
}

bool KvCompactStrategy::Drop(const leveldb::Slice& tera_key, uint64_t n,
                             const std::string& lower_bound) {
    leveldb::Slice row_key;
    int64_t expire_timestamp;
    raw_key_operator_->ExtractTeraKey(tera_key, &row_key, NULL, NULL,
                                      &expire_timestamp, NULL);

    int64_t now = get_micros() / 1000000;
    if (expire_timestamp <= 0 /*上溢,永不过期*/
        || expire_timestamp > now) {
        VLOG(11) << "[KvCompactStrategy-Not-Drop] row_key:[" << row_key.ToString()
            << "] expire_timestamp:[" << expire_timestamp
            << "] now:[" << now << "]";
        return false;
    }
    VLOG(11) << "[KvCompactStrategy-Drop] row_key:[" << row_key.ToString()
        << "] expire_timestamp:[" << expire_timestamp
        << "] now:[" << now << "]";
    return true;
}

bool KvCompactStrategy::ScanDrop(const leveldb::Slice& tera_key, uint64_t n) {
    return Drop(tera_key, n, ""); // used in scan.
}

bool KvCompactStrategy::ScanMergedValue(Iterator* it, std::string* merged_value,
                                        int64_t* merged_num) {
    return false;
}

bool KvCompactStrategy::MergeAtomicOPs(Iterator* it, std::string* merged_value,
                                std::string* merged_key) {
    return false;
}

KvCompactStrategyFactory::KvCompactStrategyFactory(const TableSchema& schema) :
        schema_(schema) {
}

KvCompactStrategy* KvCompactStrategyFactory::NewInstance() {
    MutexLock lock(&mutex_);
    return new KvCompactStrategy(schema_);
}

} // namespace io
} // namespace tera
