// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef TERA_IO_DEFAULT_COMPACT_STRATEGY_H_
#define TERA_IO_DEFAULT_COMPACT_STRATEGY_H_

#include "leveldb/compact_strategy.h"
#include "leveldb/slice.h"

#include "common/mutex.h"
#include "io/io_utils.h"
#include "proto/table_schema.pb.h"

namespace tera {
namespace io {

using leveldb::Slice;

class DefaultCompactStrategy : public leveldb::CompactStrategy {
public:
    DefaultCompactStrategy(const TableSchema& schema);
    virtual ~DefaultCompactStrategy();

    virtual bool Drop(const Slice& k, uint64_t n,
                      const std::string& lower_bound);

    // tera-specific, based on all-level iterators.
    // used in LowLevelScan
    virtual bool ScanDrop(const Slice& k, uint64_t n);

    virtual const char* Name() const;

    virtual void SetSnapshot(uint64_t snapshot);
    virtual bool CheckTag(const leveldb::Slice& tera_key, bool* del_tag, int64_t* ttl_tag);

    virtual bool ScanMergedValue(leveldb::Iterator* it,
                                 std::string* merged_value,
                                 int64_t* merged_num = NULL);

    virtual bool MergeAtomicOPs(leveldb::Iterator* it, std::string* merged_value,
                                std::string* merged_key);

private:
    bool DropIllegalColumnFamily(const std::string& column_family,
                            int32_t* cf_idx = NULL) const;
    bool DropByLifeTime(int32_t cf_idx, int64_t timestamp) const;

    bool InternalMergeProcess(leveldb::Iterator* it, std::string* merged_value,
                              std::string* merged_key,
                              bool merge_put_flag, bool is_internal_key,
                              int64_t* merged_num);

    bool CheckCompactLowerBound(const Slice& cur_key,
                                const std::string& lower_bound);

private:
    std::map<std::string, int32_t> cf_indexs_;
    TableSchema schema_;
    const leveldb::RawKeyOperator* raw_key_operator_;

    std::string last_key_;
    std::string last_col_;
    std::string last_qual_;
    int64_t last_ts_;
    leveldb::TeraKeyType last_type_;
    leveldb::TeraKeyType cur_type_;
    int64_t del_row_ts_;
    int64_t del_col_ts_;
    int64_t del_qual_ts_;
    int64_t cur_ts_;
    uint64_t del_row_seq_;
    uint64_t del_col_seq_;
    uint64_t del_qual_seq_;
    uint32_t version_num_;
    uint64_t snapshot_;
    bool has_put_;
};

class DefaultCompactStrategyFactory : public leveldb::CompactStrategyFactory {
public:
    DefaultCompactStrategyFactory(const TableSchema& schema);
    virtual DefaultCompactStrategy* NewInstance();
    virtual void SetArg(const void* arg);
    virtual const char* Name() const {
        return "tera.DefaultCompactStrategyFactory";
    }

private:
    TableSchema schema_;
    mutable Mutex mutex_;
};

} // namespace io
} // namespace tera

#endif // TERA_IO_DEFAULT_COMPACT_STRATEGY_H_
