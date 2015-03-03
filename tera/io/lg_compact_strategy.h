// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "leveldb/compact_strategy.h"

#include "tera/proto/table_schema.pb.h"

namespace tera {
namespace io {

class LGCompactStrategy : public leveldb::CompactStrategy {
public:
    LGCompactStrategy(const TableSchema& schema);
    virtual ~LGCompactStrategy();

    virtual bool Drop(const leveldb::Slice& k, uint64_t n);

    // tera-specific, based on all-level iterators.
    // used in LowLevelScan
    virtual bool ScanDrop(const leveldb::Slice& k, uint64_t n) {
        return false;
    }

    virtual bool ScanMergedValue(leveldb::Iterator* it, std::string* merged_value) {
        return false;
    }

    virtual bool MergeAtomicOPs(leveldb::Iterator* it, std::string* merged_value,
                                std::string* merged_key) {
        return false;
    }

    virtual const char* Name() const;

private:
    bool DropByColumnFamily(const std::string& column_family,
                            uint32_t* cf_idx = NULL) const;
    bool DropByVersion(uint32_t cf_idx, int64_t version) const;
    bool DropByLifeTime(uint32_t cf_idx, int64_t timestamp) const;

private:
    std::map<std::string, uint32_t> m_cf_indexs;
    TableSchema m_schema;
};

class LGCompactStrategyFactory : public leveldb::CompactStrategyFactory {
public:
    LGCompactStrategyFactory(const TableSchema& schema);
    virtual LGCompactStrategy* NewInstance();

    virtual const char* Name() const {
        return "leveldb.LGCompactStrategyFactory";
    }

private:
    TableSchema m_schema;
};

} // namespace io
} // namespace tera

