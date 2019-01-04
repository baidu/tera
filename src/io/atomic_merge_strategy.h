// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef TERA_IO_ATOMIC_MERGE_STRATEGY_H_
#define TERA_IO_ATOMIC_MERGE_STRATEGY_H_

#include "leveldb/raw_key_operator.h"
#include "leveldb/slice.h"

namespace tera {
namespace io {

bool IsAtomicOP(leveldb::TeraKeyType keyType);

class AtomicMergeStrategy {
 public:
  AtomicMergeStrategy();

  void Init(std::string* merged_key, std::string* merged_value, const leveldb::Slice& latest_key,
            const leveldb::Slice& latest_value, leveldb::TeraKeyType latest_key_type);

  void MergeStep(const leveldb::Slice& key, const leveldb::Slice& value,
                 leveldb::TeraKeyType key_type);

  bool Finish();

 private:
  std::string* merged_key_;
  std::string* merged_value_;
  leveldb::TeraKeyType latest_key_type_;
  int64_t counter_;            // for ADD
  int64_t int64_;              // for int64(add)
  std::string append_buffer_;  // for Append
};

}  // namespace io
}  // namespace tera

#endif  // TERA_IO_ATOMIC_MERGE_STRATEGY_H_
