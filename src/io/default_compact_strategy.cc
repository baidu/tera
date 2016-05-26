// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "db/dbformat.h"
#include "io/atomic_merge_strategy.h"
#include "io/default_compact_strategy.h"
#include "leveldb/slice.h"

namespace tera {
namespace io {

DefaultCompactStrategy::DefaultCompactStrategy(const TableSchema& schema)
    : m_schema(schema),
      m_raw_key_operator(GetRawKeyOperatorFromSchema(m_schema)),
      m_last_ts(-1), m_del_row_ts(-1), m_del_col_ts(-1), m_del_qual_ts(-1), m_cur_ts(-1),
      m_del_row_seq(0), m_del_col_seq(0), m_del_qual_seq(0), m_version_num(0),
      m_snapshot(leveldb::kMaxSequenceNumber) {
    // build index
    for (int32_t i = 0; i < m_schema.column_families_size(); ++i) {
        const std::string name = m_schema.column_families(i).name();
        m_cf_indexs[name] = i;
    }
    m_has_put = false;
    VLOG(11) << "DefaultCompactStrategy construct";
}

DefaultCompactStrategy::~DefaultCompactStrategy() {}

const char* DefaultCompactStrategy::Name() const {
    return "tera.DefaultCompactStrategy";
}

void DefaultCompactStrategy::SetSnapshot(uint64_t snapshot) {
    VLOG(11) << "tera.DefaultCompactStrategy: set snapshot to " << snapshot;
    m_snapshot = snapshot;
}

bool DefaultCompactStrategy::Drop(const Slice& tera_key, uint64_t n,
                                  const std::string& lower_bound) {
    Slice key, col, qual;
    int64_t ts = -1;
    leveldb::TeraKeyType type;

    if (!m_raw_key_operator->ExtractTeraKey(tera_key, &key, &col, &qual, &ts, &type)) {
        LOG(WARNING) << "invalid tera key: " << tera_key.ToString();
        return true;
    }

    m_cur_type = type;
    m_cur_ts = ts;
    int32_t cf_id = -1;
    if (type != leveldb::TKT_DEL && DropIllegalColumnFamily(col.ToString(), &cf_id)) {
        // drop illegal column family
        return true;
    }

    if (type >= leveldb::TKT_VALUE && DropByLifeTime(cf_id, ts)) {
        // drop illegal column family
        return true;
    }

    if (key.compare(m_last_key) != 0) {
        // reach a new row
        m_last_key.assign(key.data(), key.size());
        m_last_col.assign(col.data(), col.size());
        m_last_qual.assign(qual.data(), qual.size());
        m_del_row_ts = m_del_col_ts = m_del_qual_ts = -1;
        m_version_num = 0;
        m_has_put = false;
        // no break in switch: need to set multiple variables
        switch (type) {
            case leveldb::TKT_DEL:
                m_del_row_ts = ts;
                m_del_row_seq = n;
            case leveldb::TKT_DEL_COLUMN:
                m_del_col_ts = ts;
                m_del_col_seq = n;
            case leveldb::TKT_DEL_QUALIFIERS: {
                m_del_qual_ts = ts;
                m_del_qual_seq = n;
                if (CheckCompactLowerBound(key, lower_bound) && m_snapshot == leveldb::kMaxSequenceNumber) {
                    VLOG(15) << "tera.DefaultCompactStrategy: can drop delete row tag";
                    return true;
                }
            }
            default:;
        }
    } else if (m_del_row_ts >= ts && m_del_row_seq <= m_snapshot) {
        // skip deleted row and the same row_del mark
        return true;
    } else if (col.compare(m_last_col) != 0) {
        // reach a new column family
        m_last_col.assign(col.data(), col.size());
        m_last_qual.assign(qual.data(), qual.size());
        m_del_col_ts = m_del_qual_ts = -1;
        m_version_num = 0;
        m_has_put = false;
        // no break in switch: need to set multiple variables
        switch (type) {
            case leveldb::TKT_DEL_COLUMN:
                m_del_col_ts = ts;
                m_del_col_seq = n;
            case leveldb::TKT_DEL_QUALIFIERS: {
                m_del_qual_ts = ts;
                m_del_qual_seq = n;
                if (CheckCompactLowerBound(key, lower_bound) && m_snapshot == leveldb::kMaxSequenceNumber) {
                  VLOG(15) << "tera.DefaultCompactStrategy: can drop delete col tag";
                  return true;
                }
            }
            default:;
        }
    } else if (m_del_col_ts > ts && m_del_col_seq <= m_snapshot) {
        // skip deleted column family
        return true;
    } else if (qual.compare(m_last_qual) != 0) {
        // reach a new qualifier
        m_last_qual.assign(qual.data(), qual.size());
        m_del_qual_ts = -1;
        m_version_num = 0;
        m_has_put = false;
        if (type == leveldb::TKT_DEL_QUALIFIERS) {
            m_del_qual_ts = ts;
            m_del_qual_seq = n;
            if (CheckCompactLowerBound(key, lower_bound) && m_snapshot == leveldb::kMaxSequenceNumber) {
              VLOG(15) << "tera.DefaultCompactStrategy: can drop delete qualifier tag";
              return true;
            }
        }
    } else if (m_del_qual_ts > ts && m_del_qual_seq <= m_snapshot) {
        // skip deleted qualifier
        return true;
    }

    if (type == leveldb::TKT_VALUE) {
        m_has_put = true;
        if (n <= m_snapshot) {
            if (++m_version_num > static_cast<uint32_t>(m_schema.column_families(cf_id).max_versions())) {
                // drop out-of-range version
                VLOG(20) << "compact drop true: " << key.ToString()
                    << ", version " << m_version_num
                    << ", timestamp " << ts;
                return true;
            }
        }
    }

    if (IsAtomicOP(type) && m_has_put) {
        // drop ADDs which is later than Put
        return true;
    }
    VLOG(20) << "compact drop false: " << key.ToString()
        << ", version " << m_version_num
        << ", timestamp " << ts;
    return false;
}

bool DefaultCompactStrategy::ScanMergedValue(leveldb::Iterator* it,
                                             std::string* merged_value,
                                             int64_t* merged_num) {
    std::string merged_key;
    bool has_merge =  InternalMergeProcess(it, merged_value, &merged_key,
                                           true, false, merged_num);
    return has_merge;
}

bool DefaultCompactStrategy::MergeAtomicOPs(leveldb::Iterator* it,
                                            std::string* merged_value,
                                            std::string* merged_key) {
    bool merge_put_flag = false; // don't merge the last PUT if we have
    return InternalMergeProcess(it, merged_value, merged_key, merge_put_flag,
                                true, NULL);
}

bool DefaultCompactStrategy::InternalMergeProcess(leveldb::Iterator* it,
                                                  std::string* merged_value,
                                                  std::string* merged_key,
                                                  bool merge_put_flag,
                                                  bool is_internal_key,
                                                  int64_t* merged_num) {
    if (!tera::io::IsAtomicOP(m_cur_type)) {
        return false;
    }
    assert(merged_key);
    assert(merged_value);

    AtomicMergeStrategy atom_merge;
    atom_merge.Init(merged_key, merged_value, it->key(), it->value(), m_cur_type);

    it->Next();
    int64_t merged_num_t = 1;
    int64_t last_ts_atomic = m_cur_ts;
    int64_t version_num = 0;

    while (it->Valid()) {
        merged_num_t++;
        if (version_num >= 1) {
            break; //avoid accumulate to many versions
        }
        Slice itkey = it->key();
        Slice key;
        Slice col;
        Slice qual;
        int64_t ts = -1;
        leveldb::TeraKeyType type;

        if (is_internal_key) {
            leveldb::ParsedInternalKey ikey;
            leveldb::ParseInternalKey(itkey, &ikey);
            if (!m_raw_key_operator->ExtractTeraKey(ikey.user_key, &key, &col, &qual, &ts, &type)) {
                LOG(WARNING) << "invalid internal key for tera: " << itkey.ToString();
                break;
            }
        } else {
            if (!m_raw_key_operator->ExtractTeraKey(itkey, &key, &col, &qual, &ts, &type)) {
                LOG(WARNING) << "invalid tera key: " << itkey.ToString();
                break;
            }
        }

        if (m_last_qual != qual || m_last_col != col || m_last_key != key) {
            break; // out of the current cell
        }

        if (!IsAtomicOP(type) && type != leveldb::TKT_VALUE) {
            break;
        } else if (type == leveldb::TKT_VALUE) {
            if (!merge_put_flag || ++version_num > 1) {
                break;
            }
        }

        if (ts != last_ts_atomic || type ==  leveldb::TKT_VALUE) {
            atom_merge.MergeStep(it->key(), it->value(), type);
        }
        last_ts_atomic = ts;
        it->Next();
    }
    atom_merge.Finish();
    if (merged_num) {
        *merged_num = merged_num_t;
    }
    return true;
}

bool DefaultCompactStrategy::ScanDrop(const Slice& tera_key, uint64_t n) {
    Slice key, col, qual;
    int64_t ts = -1;
    leveldb::TeraKeyType type;

    if (!m_raw_key_operator->ExtractTeraKey(tera_key, &key, &col, &qual, &ts, &type)) {
        LOG(WARNING) << "invalid tera key: " << tera_key.ToString();
        return true;
    }

    m_cur_type = type;
    m_last_ts = m_cur_ts;
    m_cur_ts = ts;
    int32_t cf_id = -1;
    if (type != leveldb::TKT_DEL && DropIllegalColumnFamily(col.ToString(), &cf_id)) {
        // drop illegal column family
        return true;
    }

    if (type >= leveldb::TKT_VALUE && DropByLifeTime(cf_id, ts)) {
        // drop out-of-life-time record
        return true;
    }

    if (key.compare(m_last_key) != 0) {
        // reach a new row
        m_last_key.assign(key.data(), key.size());
        m_last_col.assign(col.data(), col.size());
        m_last_qual.assign(qual.data(), qual.size());
        m_last_type = type;
        m_version_num = 0;
        m_del_row_ts = m_del_col_ts = m_del_qual_ts = -1;
        m_has_put = false;

        // no break in switch: need to set multiple variables
        switch (type) {
            case leveldb::TKT_DEL:
                m_del_row_ts = ts;
            case leveldb::TKT_DEL_COLUMN:
                m_del_col_ts = ts;
            case leveldb::TKT_DEL_QUALIFIERS:
                m_del_qual_ts = ts;
            default:;
        }
    } else if (m_del_row_ts >= ts) {
        // skip deleted row and the same row_del mark
        return true;
    } else if (col.compare(m_last_col) != 0) {
        // reach a new column family
        m_last_col.assign(col.data(), col.size());
        m_last_qual.assign(qual.data(), qual.size());
        m_last_type = type;
        m_version_num = 0;
        m_del_col_ts = m_del_qual_ts = -1;
        m_has_put = false;
        // set both variables when type is leveldb::TKT_DEL_COLUMN
        switch (type) {
            case leveldb::TKT_DEL_COLUMN:
                m_del_col_ts = ts;
            case leveldb::TKT_DEL_QUALIFIERS:
                m_del_qual_ts = ts;
            default:;
        }
    } else if (m_del_col_ts > ts) {
        // skip deleted column family
        return true;
    } else if (qual.compare(m_last_qual) != 0) {
        // reach a new qualifier
        m_last_qual.assign(qual.data(), qual.size());
        m_last_type = type;
        m_version_num = 0;
        m_del_qual_ts = -1;
        m_has_put = false;
        if (type == leveldb::TKT_DEL_QUALIFIERS) {
            m_del_qual_ts = ts;
        }
    } else if (m_del_qual_ts > ts) {
        // skip deleted qualifier
        return true;
    } else if (type == leveldb::TKT_DEL_QUALIFIERS) {
        // reach a delete-all-qualifier mark
        m_del_qual_ts = ts;
    } else if (m_last_type == leveldb::TKT_DEL_QUALIFIER) {
        // skip latest deleted version
        m_last_type = type;
        if (type == leveldb::TKT_VALUE) {
            m_version_num++;
        }
        return true;
    } else {
        m_last_type = type;
    }

    if (type != leveldb::TKT_VALUE && !IsAtomicOP(type)) {
        return true;
    }

    if (type == leveldb::TKT_VALUE) {
        m_has_put = true;
    }

    if (IsAtomicOP(type) && m_has_put) {
        return true;
    }

    CHECK(cf_id >= 0) << "illegel column family";
    if (type == leveldb::TKT_VALUE) {
        if (m_cur_ts == m_last_ts && m_last_qual == qual.ToString() &&
            m_last_col == col.ToString() && m_last_key == key.ToString()) {
            // this is the same key, do not chang version num
        } else {
            m_version_num++;
        }
        if (m_version_num >
            static_cast<uint32_t>(m_schema.column_families(cf_id).max_versions())) {
            // drop out-of-range version
            VLOG(20) << "scan drop true: " << key.ToString()
                << ", version " << m_version_num
                << ", timestamp " << ts;
            return true;
        }
    }
    VLOG(20) << "scan drop false: " << key.ToString()
        << ", version " << m_version_num
        << ", timestamp " << ts;
    return false;
}

bool DefaultCompactStrategy::DropIllegalColumnFamily(const std::string& column_family,
                                                int32_t* cf_idx) const {
    std::map<std::string, int32_t>::const_iterator it =
        m_cf_indexs.find(column_family);
    if (it == m_cf_indexs.end()) {
        return true;
    }
    if (cf_idx) {
        *cf_idx = it->second;
    }
    return false;
}

bool DefaultCompactStrategy::DropByLifeTime(int32_t cf_idx, int64_t timestamp) const {
    int64_t ttl = m_schema.column_families(cf_idx).time_to_live() * 1000000LL;
    if (ttl <= 0) {
        // do not drop
        return false;
    }
    int64_t cur_time = get_micros();
    if (timestamp + ttl > cur_time) {
        return false;
    } else {
        return true;
    }
}

bool DefaultCompactStrategy::CheckCompactLowerBound(const Slice& cur_key,
                                                    const std::string& lower_bound) {
    if (lower_bound.empty()) {
        return false;
    }

    Slice rkey;
    CHECK (m_raw_key_operator->ExtractTeraKey(lower_bound, &rkey, NULL, NULL, NULL, NULL));
    int res = rkey.compare(cur_key);
    if (res > 0) {
        return true;
    } else {
        return false;
    }
}

DefaultCompactStrategyFactory::DefaultCompactStrategyFactory(const TableSchema& schema)
    : m_schema(schema) {}

void DefaultCompactStrategyFactory::SetArg(const void* arg) {
    MutexLock lock(&m_mutex);
    m_schema.CopyFrom(*(TableSchema*)arg);
}

DefaultCompactStrategy* DefaultCompactStrategyFactory::NewInstance() {
    MutexLock lock(&m_mutex);
    return new DefaultCompactStrategy(m_schema);
}

} // namespace io
} // namespace tera
