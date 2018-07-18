// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "db/dbformat.h"
#include "io/atomic_merge_strategy.h"
#include "io/default_compact_strategy.h"
#include "leveldb/slice.h"

namespace tera {
namespace io {

DefaultCompactStrategy::DefaultCompactStrategy(const TableSchema& schema,
                                               const std::map<std::string, int32_t>& cf_indexs,
                                               const leveldb::RawKeyOperator& raw_key_operator,
                                               leveldb::Comparator* cmp)
    : schema_(schema),
      cf_indexs_(cf_indexs),
      raw_key_operator_(raw_key_operator),
      cmp_(cmp),
      last_ts_(-1), last_type_(leveldb::TKT_FORSEEK), cur_type_(leveldb::TKT_FORSEEK),
      del_row_ts_(-1), del_col_ts_(-1), del_qual_ts_(-1), cur_ts_(-1),
      del_row_seq_(0), del_col_seq_(0), del_qual_seq_(0), version_num_(0),
      snapshot_(leveldb::kMaxSequenceNumber) {
    has_put_ = false;
    VLOG(11) << "DefaultCompactStrategy construct";
}

const leveldb::Comparator* DefaultCompactStrategy::RowKeyComparator() {
    return cmp_;
}

const char* DefaultCompactStrategy::Name() const {
    return "tera.DefaultCompactStrategy";
}

void DefaultCompactStrategy::SetSnapshot(uint64_t snapshot) {
    VLOG(11) << "tera.DefaultCompactStrategy: set snapshot to " << snapshot;
    snapshot_ = snapshot;
}

bool DefaultCompactStrategy::Drop(const Slice& tera_key, uint64_t n,
                                  const std::string& lower_bound) {
    Slice key, col, qual;
    int64_t ts = -1;
    leveldb::TeraKeyType type;

    if (!raw_key_operator_.ExtractTeraKey(tera_key, &key, &col, &qual, &ts, &type)) {
        LOG(WARNING) << "invalid tera key: " << tera_key.ToString();
        return true;
    }

    cur_type_ = type;
    cur_ts_ = ts;
    int32_t cf_id = -1;
    if (type != leveldb::TKT_DEL && DropIllegalColumnFamily(col.ToString(), &cf_id)) {
        // drop illegal column family
        return true;
    }

    if (type >= leveldb::TKT_VALUE && DropByLifeTime(cf_id, ts)) {
        // drop illegal column family
        return true;
    }

    if (key.compare(last_key_) != 0) {
        // reach a new row
        last_key_.assign(key.data(), key.size());
        last_col_.assign(col.data(), col.size());
        last_qual_.assign(qual.data(), qual.size());
        del_row_ts_ = del_col_ts_ = del_qual_ts_ = -1;
        version_num_ = 0;
        has_put_ = false;
        // no break in switch: need to set multiple variables
        switch (type) {
            case leveldb::TKT_DEL:
                del_row_ts_ = ts;
                del_row_seq_ = n;
            case leveldb::TKT_DEL_COLUMN:
                del_col_ts_ = ts;
                del_col_seq_ = n;
            case leveldb::TKT_DEL_QUALIFIERS: {
                del_qual_ts_ = ts;
                del_qual_seq_ = n;
                if (CheckCompactLowerBound(key, lower_bound) && snapshot_ == leveldb::kMaxSequenceNumber) {
                    VLOG(15) << "tera.DefaultCompactStrategy: can drop delete row tag";
                    return true;
                }
            }
            default:;
        }
    } else if (del_row_ts_ >= ts && del_row_seq_ <= snapshot_) {
        // skip deleted row and the same row_del mark
        return true;
    } else if (col.compare(last_col_) != 0) {
        // reach a new column family
        last_col_.assign(col.data(), col.size());
        last_qual_.assign(qual.data(), qual.size());
        del_col_ts_ = del_qual_ts_ = -1;
        version_num_ = 0;
        has_put_ = false;
        // no break in switch: need to set multiple variables
        switch (type) {
            case leveldb::TKT_DEL_COLUMN:
                del_col_ts_ = ts;
                del_col_seq_ = n;
            case leveldb::TKT_DEL_QUALIFIERS: {
                del_qual_ts_ = ts;
                del_qual_seq_ = n;
                if (CheckCompactLowerBound(key, lower_bound) && snapshot_ == leveldb::kMaxSequenceNumber) {
                  VLOG(15) << "tera.DefaultCompactStrategy: can drop delete col tag";
                  return true;
                }
            }
            default:;
        }
    } else if (del_col_ts_ > ts && del_col_seq_ <= snapshot_) {
        // skip deleted column family
        return true;
    } else if (qual.compare(last_qual_) != 0) {
        // reach a new qualifier
        last_qual_.assign(qual.data(), qual.size());
        del_qual_ts_ = -1;
        version_num_ = 0;
        has_put_ = false;
        if (type == leveldb::TKT_DEL_QUALIFIERS) {
            del_qual_ts_ = ts;
            del_qual_seq_ = n;
            if (CheckCompactLowerBound(key, lower_bound) && snapshot_ == leveldb::kMaxSequenceNumber) {
              VLOG(15) << "tera.DefaultCompactStrategy: can drop delete qualifiers tag";
              return true;
            }
        }
    } else if (del_qual_ts_ > ts && del_qual_seq_ <= snapshot_) {
        // skip deleted qualifier
        return true;
    }

    if (type == leveldb::TKT_VALUE) {
        has_put_ = true;
        if (n <= snapshot_) {
            if (++version_num_ > static_cast<uint32_t>(schema_.column_families(cf_id).max_versions())) {
                // drop out-of-range version
                VLOG(20) << "compact drop true: " << key.ToString()
                    << ", version " << version_num_
                    << ", timestamp " << ts;
                return true;
            }
        }
    } 
    
    if (type == leveldb::TKT_DEL_QUALIFIER) {
        if (n <= snapshot_) {
            uint32_t max_versions = static_cast<uint32_t>(schema_.column_families(cf_id).max_versions());
            if (version_num_ >= max_versions) {
                // drop out-of-range delete qualifier mark
                VLOG(20) << "compact drop true: " << key.ToString()
                         << ", version " << version_num_ 
                         << ", timestamp " << ts;
                return true;
            }
        }
    }

    if (IsAtomicOP(type) && has_put_) {
        // drop ADDs which is later than Put
        return true;
    }
    VLOG(20) << "compact drop false: " << key.ToString()
        << ", version " << version_num_
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
    if (!tera::io::IsAtomicOP(cur_type_)) {
        return false;
    }
    assert(merged_key);
    assert(merged_value);

    AtomicMergeStrategy atom_merge;
    atom_merge.Init(merged_key, merged_value, it->key(), it->value(), cur_type_);

    it->Next();
    int64_t merged_num_t = 1;
    int64_t last_ts_atomic = cur_ts_;
    int64_t version_num = 0;

    while (it->Valid()) {
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
            if (ikey.sequence > snapshot_) {
                break;
            }
            if (!raw_key_operator_.ExtractTeraKey(ikey.user_key, &key, &col, &qual, &ts, &type)) {
                LOG(WARNING) << "invalid internal key for tera: " << itkey.ToString();
                break;
            }
        } else {
            if (!raw_key_operator_.ExtractTeraKey(itkey, &key, &col, &qual, &ts, &type)) {
                LOG(WARNING) << "invalid tera key: " << itkey.ToString();
                break;
            }
        }

        if (last_qual_ != qual || last_col_ != col || last_key_ != key) {
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
        merged_num_t++;
    }
    atom_merge.Finish();
    if (merged_num) {
        *merged_num = merged_num_t;
    }
    return true;
}

bool DefaultCompactStrategy::ScanDrop(const Slice& tera_key, uint64_t n) {
    bool key_col_qual_same = false;
    Slice key, col, qual;
    int64_t ts = -1;
    leveldb::TeraKeyType type;

    if (!raw_key_operator_.ExtractTeraKey(tera_key, &key, &col, &qual, &ts, &type)) {
        LOG(WARNING) << "invalid tera key: " << tera_key.ToString();
        return true;
    }

    cur_type_ = type;
    last_ts_ = cur_ts_;
    cur_ts_ = ts;
    int32_t cf_id = -1;
    if (type != leveldb::TKT_DEL && DropIllegalColumnFamily(col.ToString(), &cf_id)) {
        // drop illegal column family
        return true;
    }

    if (type >= leveldb::TKT_VALUE && DropByLifeTime(cf_id, ts)) {
        // drop out-of-life-time record
        return true;
    }

    if (key.compare(last_key_) != 0) {
        // reach a new row
        last_key_.assign(key.data(), key.size());
        last_col_.assign(col.data(), col.size());
        last_qual_.assign(qual.data(), qual.size());
        last_type_ = type;
        version_num_ = 0;
        del_row_ts_ = del_col_ts_ = del_qual_ts_ = -1;
        has_put_ = false;

        // no break in switch: need to set multiple variables
        switch (type) {
            case leveldb::TKT_DEL:
                del_row_ts_ = ts;
            case leveldb::TKT_DEL_COLUMN:
                del_col_ts_ = ts;
            case leveldb::TKT_DEL_QUALIFIERS:
                del_qual_ts_ = ts;
            default:;
        }
    } else if (del_row_ts_ >= ts) {
        // skip deleted row and the same row_del mark
        return true;
    } else if (col.compare(last_col_) != 0) {
        // reach a new column family
        last_col_.assign(col.data(), col.size());
        last_qual_.assign(qual.data(), qual.size());
        last_type_ = type;
        version_num_ = 0;
        del_col_ts_ = del_qual_ts_ = -1;
        has_put_ = false;
        // set both variables when type is leveldb::TKT_DEL_COLUMN
        switch (type) {
            case leveldb::TKT_DEL_COLUMN:
                del_col_ts_ = ts;
            case leveldb::TKT_DEL_QUALIFIERS:
                del_qual_ts_ = ts;
            default:;
        }
    } else if (del_col_ts_ > ts) {
        // skip deleted column family
        return true;
    } else if (qual.compare(last_qual_) != 0) {
        // reach a new qualifier
        last_qual_.assign(qual.data(), qual.size());
        last_type_ = type;
        version_num_ = 0;
        del_qual_ts_ = -1;
        has_put_ = false;
        if (type == leveldb::TKT_DEL_QUALIFIERS) {
            del_qual_ts_ = ts;
        }
    } else if (del_qual_ts_ > ts) {
        // skip deleted qualifier
        return true;
    } else if (type == leveldb::TKT_DEL_QUALIFIERS) {
        // reach a delete-all-qualifier mark
        del_qual_ts_ = ts;
    } else if (last_type_ == leveldb::TKT_DEL_QUALIFIER) {
        // skip latest deleted version
        last_type_ = type;
        if (type == leveldb::TKT_VALUE) {
            version_num_++;
        }
        return true;
    } else {
        key_col_qual_same = true;
        last_type_ = type;
    }

    if (type != leveldb::TKT_VALUE && !IsAtomicOP(type)) {
        return true;
    }

    if (type == leveldb::TKT_VALUE) {
        has_put_ = true;
    }

    if (IsAtomicOP(type) && has_put_) {
        return true;
    }

    CHECK(cf_id >= 0) << "illegel column family";
    if (type == leveldb::TKT_VALUE) {
        if (cur_ts_ == last_ts_ && key_col_qual_same) {
            // this is the same key, do not chang version num
        } else {
            version_num_++;
        }
        if (version_num_ >
            static_cast<uint32_t>(schema_.column_families(cf_id).max_versions())) {
            // drop out-of-range version
            VLOG(20) << "scan drop true: " << key.ToString()
                << ", version " << version_num_
                << ", timestamp " << ts;
            return true;
        }
    }
    VLOG(20) << "scan drop false: " << key.ToString()
        << ", version " << version_num_
        << ", timestamp " << ts;
    return false;
}

bool DefaultCompactStrategy::DropIllegalColumnFamily(const std::string& column_family,
                                                int32_t* cf_idx) const {
    std::map<std::string, int32_t>::const_iterator it =
        cf_indexs_.find(column_family);
    if (it == cf_indexs_.end()) {
        return true;
    }
    if (cf_idx) {
        *cf_idx = it->second;
    }
    return false;
}

bool DefaultCompactStrategy::DropByLifeTime(int32_t cf_idx, int64_t timestamp) const {
    int64_t ttl = schema_.column_families(cf_idx).time_to_live() * 1000000LL;
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

bool DefaultCompactStrategy::CheckTag(const Slice& tera_key, bool* del_tag, int64_t* ttl_tag) {
    *del_tag = false;
    *ttl_tag = -1;
    Slice key, col, qual;
    int64_t ts = -1;
    leveldb::TeraKeyType type;

    if (!raw_key_operator_.ExtractTeraKey(tera_key, &key, &col, &qual, &ts, &type)) {
        LOG(WARNING) << "invalid tera key: " << tera_key.ToString();
        return false;
    }

    if (type == leveldb::TKT_DEL ||
        type == leveldb::TKT_DEL_COLUMN ||
        type == leveldb::TKT_DEL_QUALIFIERS ||
        type == leveldb::TKT_DEL_QUALIFIER) {
        *del_tag = true;
    }
    int32_t cf = -1;
    int64_t ttl = -1;
    if (!DropIllegalColumnFamily(col.ToString(), &cf) &&
        schema_.column_families(cf).time_to_live() > 0) {
        ttl = schema_.column_families(cf).time_to_live();
        *ttl_tag = ts + ttl * 1000000LL;
    }
    VLOG(11) << "default strategy, del " << *del_tag << ", key_ts " << ts
             << ", ttl " << ttl
             << ", ttl_tag " << *ttl_tag;
    return true;
}

bool DefaultCompactStrategy::CheckCompactLowerBound(const Slice& cur_key,
                                                    const std::string& lower_bound) {
    if (lower_bound.empty()) {
        return false;
    }

    Slice rkey;
    CHECK (raw_key_operator_.ExtractTeraKey(lower_bound, &rkey, NULL, NULL, NULL, NULL));
    int res = rkey.compare(cur_key);
    if (res > 0) {
        return true;
    } else {
        return false;
    }
}

DefaultCompactStrategyFactory::DefaultCompactStrategyFactory(const TableSchema& schema)
    : schema_(schema),
      raw_key_operator_(GetRawKeyOperatorFromSchema(schema_)),
      cmp_(NewRowKeyComparator(raw_key_operator_)) {
    // build index at tablet io loading
    for (int32_t i = 0; i < schema_.column_families_size(); ++i) {
        const std::string& name = schema_.column_families(i).name();
        cf_indexs_[name] = i;
    }
}

DefaultCompactStrategyFactory::~DefaultCompactStrategyFactory() {
    delete cmp_;
}

void DefaultCompactStrategyFactory::SetArg(const void* arg) {
    MutexLock lock(&mutex_);
    schema_.CopyFrom(*(TableSchema*)arg);
}

DefaultCompactStrategy* DefaultCompactStrategyFactory::NewInstance() {
    MutexLock lock(&mutex_);
    return new DefaultCompactStrategy(schema_, cf_indexs_, *raw_key_operator_, cmp_);
}

} // namespace io
} // namespace tera
