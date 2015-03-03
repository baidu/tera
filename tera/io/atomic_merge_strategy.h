// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef  ATOMIC_MERGE_STRATEGY_H
#define  ATOMIC_MERGE_STRATEGY_H

#include "leveldb/raw_key_operator.h"
#include "leveldb/slice.h"

namespace tera {
namespace io {

bool IsAtomicOP(leveldb::TeraKeyType keyType);

class AtomicMergeStrategy{
public:
    AtomicMergeStrategy();

    void Init(std::string* merged_key,
              std::string* merged_value,
              const leveldb::Slice& latest_key,
              const leveldb::Slice& latest_value,
              leveldb::TeraKeyType latest_key_type);

    void MergeStep(const leveldb::Slice& key,
                   const leveldb::Slice& value,
                   leveldb::TeraKeyType key_type);

    bool Finish();

private:
    std::string* m_merged_key;
    std::string* m_merged_value;
    leveldb::TeraKeyType m_latest_key_type;
    int64_t m_counter; //for ADD
    std::string m_append_buffer; //for Append
};

} //namespace io
} //namespace tera

#endif  //ATOMIC_MERGE_STRATEGY_H

