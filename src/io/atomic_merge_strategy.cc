// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "atomic_merge_strategy.h"
#include "io/coding.h"

namespace tera {
namespace io {

bool IsAtomicOP(leveldb::TeraKeyType keyType){
    if (keyType == leveldb::TKT_ADD ||
        keyType == leveldb::TKT_PUT_IFABSENT ||
        keyType == leveldb::TKT_APPEND) {
        return true;
    }
    return false;
}

AtomicMergeStrategy::AtomicMergeStrategy() : m_merged_key(NULL),
                                           m_merged_value(NULL),
                                           m_counter(0) {

}

void AtomicMergeStrategy::Init(std::string* merged_key,
                               std::string* merged_value,
                               const leveldb::Slice& latest_key,
                               const leveldb::Slice& latest_value,
                               leveldb::TeraKeyType latest_key_type) {
    m_merged_key = merged_key;
    m_merged_value = merged_value;
    assert(m_merged_key);
    assert(m_merged_value);
    m_latest_key_type = latest_key_type;

    switch (latest_key_type) {
        case leveldb::TKT_ADD:
            m_merged_key->assign(latest_key.data(), latest_key.size());
            m_counter = io::DecodeBigEndain(latest_value.data());
            break;
        case leveldb::TKT_PUT_IFABSENT:
            m_merged_key->assign(latest_key.data(), latest_key.size());
            m_merged_value->assign(latest_value.data(), latest_value.size());
            break;
        case leveldb::TKT_APPEND:
            m_merged_key->assign(latest_key.data(), latest_key.size());
            m_append_buffer.assign(latest_value.data(), latest_value.size());
            break;
        default:
            assert(0); //invalid status
            break;
    }
}

void AtomicMergeStrategy::MergeStep(const leveldb::Slice& key,
                                    const leveldb::Slice& value,
                                    leveldb::TeraKeyType key_type) {
    switch (m_latest_key_type) {
        case leveldb::TKT_ADD:
            if (key_type == leveldb::TKT_ADD || key_type == leveldb::TKT_VALUE) {
                m_counter += io::DecodeBigEndain(value.data());
            }
            break;
        case leveldb::TKT_PUT_IFABSENT:
            if (key_type == leveldb::TKT_PUT_IFABSENT || key_type == leveldb::TKT_VALUE) {
                m_merged_value->assign(value.data(), value.size());
            }
            break;
        case leveldb::TKT_APPEND:
            if (key_type == leveldb::TKT_APPEND || key_type == leveldb::TKT_VALUE) {
                m_append_buffer.insert(0,std::string(value.data(),value.size()));
            }
            break;
        default:
            assert(0); //invalid status
            break;
    }
}

bool AtomicMergeStrategy::Finish() {
    switch (m_latest_key_type) {
        case leveldb::TKT_ADD:
            char buf[sizeof(int64_t)];
            io::EncodeBigEndian(buf, m_counter);
            m_merged_value->assign(buf, sizeof(buf));
            break;
        case leveldb::TKT_PUT_IFABSENT:
            //do nothing
            break;
        case leveldb::TKT_APPEND:
            *m_merged_value = m_append_buffer;
            break;
        default:
            assert(0); //invalid status
            break;
    }
    return true;
}

} //namespace io
} //namespace tera

