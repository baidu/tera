// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "atomic_merge_strategy.h"

#include "io/coding.h"

namespace tera {
namespace io {

bool IsAtomicOP(leveldb::TeraKeyType keyType) {
  if (keyType == leveldb::TKT_ADD || keyType == leveldb::TKT_ADDINT64 ||
      keyType == leveldb::TKT_PUT_IFABSENT || keyType == leveldb::TKT_APPEND) {
    return true;
  }
  return false;
}

AtomicMergeStrategy::AtomicMergeStrategy()
    : merged_key_(NULL),
      merged_value_(NULL),
      latest_key_type_(leveldb::TKT_FORSEEK),
      counter_(0),
      int64_(0) {}

void AtomicMergeStrategy::Init(std::string* merged_key, std::string* merged_value,
                               const leveldb::Slice& latest_key, const leveldb::Slice& latest_value,
                               leveldb::TeraKeyType latest_key_type) {
  merged_key_ = merged_key;
  merged_value_ = merged_value;
  assert(merged_key_);
  assert(merged_value_);
  latest_key_type_ = latest_key_type;

  switch (latest_key_type) {
    case leveldb::TKT_ADD:
      merged_key_->assign(latest_key.data(), latest_key.size());
      counter_ = io::DecodeBigEndain(latest_value.data());
      break;
    case leveldb::TKT_ADDINT64:
      merged_key_->assign(latest_key.data(), latest_key.size());
      int64_ = *(int64_t*)latest_value.data();
      break;
    case leveldb::TKT_PUT_IFABSENT:
      merged_key_->assign(latest_key.data(), latest_key.size());
      merged_value_->assign(latest_value.data(), latest_value.size());
      break;
    case leveldb::TKT_APPEND:
      merged_key_->assign(latest_key.data(), latest_key.size());
      append_buffer_.assign(latest_value.data(), latest_value.size());
      break;
    default:
      assert(0);  // invalid status
      break;
  }
}

void AtomicMergeStrategy::MergeStep(const leveldb::Slice& key, const leveldb::Slice& value,
                                    leveldb::TeraKeyType key_type) {
  switch (latest_key_type_) {
    case leveldb::TKT_ADD:
      if (key_type == leveldb::TKT_ADD || key_type == leveldb::TKT_VALUE) {
        counter_ += io::DecodeBigEndain(value.data());
      }
      break;
    case leveldb::TKT_ADDINT64:
      if (key_type == leveldb::TKT_ADDINT64 || key_type == leveldb::TKT_VALUE) {
        int64_ += *(int64_t*)value.data();
      }
      break;
    case leveldb::TKT_PUT_IFABSENT:
      if (key_type == leveldb::TKT_PUT_IFABSENT || key_type == leveldb::TKT_VALUE) {
        merged_value_->assign(value.data(), value.size());
      }
      break;
    case leveldb::TKT_APPEND:
      if (key_type == leveldb::TKT_APPEND || key_type == leveldb::TKT_VALUE) {
        append_buffer_.insert(0, std::string(value.data(), value.size()));
      }
      break;
    default:
      assert(0);  // invalid status
      break;
  }
}

bool AtomicMergeStrategy::Finish() {
  switch (latest_key_type_) {
    case leveldb::TKT_ADD:
      char buf[sizeof(int64_t)];
      io::EncodeBigEndian(buf, counter_);
      merged_value_->assign(buf, sizeof(buf));
      break;
    case leveldb::TKT_ADDINT64:
      merged_value_->assign(std::string((char*)&int64_, sizeof(int64_t)));
      break;
    case leveldb::TKT_PUT_IFABSENT:
      // do nothing
      break;
    case leveldb::TKT_APPEND:
      *merged_value_ = append_buffer_;
      break;
    default:
      assert(0);  // invalid status
      break;
  }
  return true;
}

}  // namespace io
}  // namespace tera
