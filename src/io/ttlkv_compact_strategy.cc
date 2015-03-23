// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "io/ttlkv_compact_strategy.h"
#include "io/io_utils.h"
#include "leveldb/slice.h"

namespace tera {
namespace io {

using namespace leveldb;

KvCompactStrategy::KvCompactStrategy(const TableSchema& schema) :
        schema_(schema), raw_key_operator_(GetRawKeyOperatorFromSchema(schema_)) {
    VLOG(11) << "KvCompactStrategy construct";
}

KvCompactStrategy::~KvCompactStrategy() {
}

const char* KvCompactStrategy::Name() const {
    return "tera.TTLKvCompactStrategy";
}

bool KvCompactStrategy::Drop(const leveldb::Slice& tera_key, uint64_t n) {
    // If expire timestamp + schema's TTL <= time(NULL), Then Drop.
    // Desc: 当前TTL的语义理解为：假设用户指定了key在03:10分过期，
    // 同时Schema的TTL为+300(延后5分钟), 那么这个key将在03:15分过期.
    //
    // 这种语义下, 如果希望一个key提前过期, 只需要修改schema让TTL为负值
    // 例如-300(提前5分钟), 那么这个key将在03:05分过期.
    //
    // 不过, 对于用户曾经插入的永不过期的key, 无论怎么调整schema的TTL都不会产生任何作用。

    leveldb::Slice row_key;
    int64_t expire_timestamp;
    raw_key_operator_->ExtractTeraKey(tera_key, &row_key, NULL, NULL,
            &expire_timestamp, NULL);

    int64_t now = get_micros() / 1000000;

    int64_t final_expire_timestamp = expire_timestamp
            + schema_.column_families(0).time_to_live();
    if (final_expire_timestamp <= 0 /*上溢,永不过期*/
    || final_expire_timestamp > now) {
        VLOG(11) << "[KvCompactStrategy-Not-Drop] row_key:[" << row_key.ToString()
            << "] expire_timestamp:[" << expire_timestamp
            << "] now:[" << now << "] time_to_live=["
            << schema_.column_families(0).time_to_live() << "]";
        return false;
    }
    VLOG(11) << "[KvCompactStrategy-Drop] row_key:[" << row_key.ToString()
        << "] expire_timestamp:[" << expire_timestamp
        << "] now:[" << now << "] time_to_live=["
        << schema_.column_families(0).time_to_live() << "]";
    return true;
}

bool KvCompactStrategy::ScanDrop(const leveldb::Slice& tera_key, uint64_t n) {
    return Drop(tera_key, n); // used in scan.
}

bool KvCompactStrategy::ScanMergedValue(Iterator* it, std::string* merged_value) {
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
    return new KvCompactStrategy(schema_);
}

} // namespace io
} // namespace tera
