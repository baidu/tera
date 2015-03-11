// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "tera/io/lg_compact_strategy.h"

#include "leveldb/slice.h"
#include "tera/io/io_utils.h"

namespace tera {
namespace io {

LGCompactStrategy::LGCompactStrategy(const TableSchema& schema)
    : m_schema(schema) {
    // build index
    for (int32_t i = 0; i < m_schema.column_families_size(); ++i) {
        const std::string name = m_schema.column_families(i).name();
        m_cf_indexs[name] = i;
    }
}

LGCompactStrategy::~LGCompactStrategy() {}

const char* LGCompactStrategy::Name() const {
    return "leveldb.LGCompactStrategy";
}

bool LGCompactStrategy::Drop(const leveldb::Slice& k, uint64_t n) {
    leveldb::Slice row_key;
    leveldb::Slice column_family;
    leveldb::Slice qualifier;
    int64_t timestamp;
    const leveldb::RawKeyOperator* key_operator = GetRawKeyOperatorFromSchema(m_schema);

    uint32_t cf_idx = 0;
    if (!key_operator->ExtractTeraKey(k, &row_key, &column_family,
                        &qualifier, &timestamp, NULL)) {
        return false;
    } else if (DropByColumnFamily(column_family.ToString(), &cf_idx)) {
        return true;
    } else if (DropByVersion(cf_idx, timestamp)) {
        return true;
    } else if (DropByLifeTime(cf_idx, timestamp)) {
        return true;
    }
    return false;
}

bool LGCompactStrategy::DropByColumnFamily(const std::string& column_family,
                                           uint32_t* cf_idx) const {
    std::map<std::string, uint32_t>::const_iterator it =
        m_cf_indexs.find(column_family);
    if (it == m_cf_indexs.end()) {
        return true;
    }
    if (cf_idx) {
        *cf_idx = it->second;
    }
    return false;
}

bool LGCompactStrategy::DropByVersion(uint32_t cf_idx, int64_t version) const {
    const ColumnFamilySchema& cf_schema =
        m_schema.column_families(cf_idx);
    if (version < cf_schema.min_versions()
        || version >= cf_schema.min_versions()) {
        return true;
    }
    return false;
}

bool LGCompactStrategy::DropByLifeTime(uint32_t cf_idx, int64_t timestamp) const {
    return false;
}

LGCompactStrategyFactory::LGCompactStrategyFactory(const TableSchema& schema)
    : m_schema(schema) {
}

LGCompactStrategy* LGCompactStrategyFactory::NewInstance() {
    return new LGCompactStrategy(m_schema);
}

} // namespace io
} // namespace tera
