// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef TERA_IO_DEFAULT_COMPACT_STRATEGY_H_
#define TERA_IO_DEFAULT_COMPACT_STRATEGY_H_

#include "leveldb/compact_strategy.h"

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
    std::map<std::string, int32_t> m_cf_indexs;
    TableSchema m_schema;
    const leveldb::RawKeyOperator* m_raw_key_operator;

    std::string m_last_key;
    std::string m_last_col;
    std::string m_last_qual;
    int64_t m_last_ts;
    leveldb::TeraKeyType m_last_type;
    leveldb::TeraKeyType m_cur_type;
    int64_t m_del_row_ts;
    int64_t m_del_col_ts;
    int64_t m_del_qual_ts;
    int64_t m_cur_ts;
    uint64_t m_del_row_seq;
    uint64_t m_del_col_seq;
    uint64_t m_del_qual_seq;
    uint32_t m_version_num;
    uint64_t m_snapshot;
    bool m_has_put;
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
    TableSchema m_schema;
    mutable Mutex m_mutex;
};

} // namespace io
} // namespace tera

#endif // TERA_IO_DEFAULT_COMPACT_STRATEGY_H_
