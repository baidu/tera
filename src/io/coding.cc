// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "io/coding.h"

#include "glog/logging.h"

namespace tera {
namespace io {

bool ParseKeySlice(const leveldb::Slice& key, int64_t* timestamp, UserKeyType* type,
                   leveldb::Slice* short_key, leveldb::Slice* time_key) {
  if (key.size() < sizeof(UserKeyType)) {
    return false;
  } else if (time_key) {
    *time_key = leveldb::Slice(key.data(), key.size() - sizeof(UserKeyType));
  }

  if (key.size() < sizeof(uint64_t)) {
    return false;
  }

  if (short_key) {
    *short_key = leveldb::Slice(key.data(), key.size() - sizeof(uint64_t));
  }
  uint64_t num = DecodeFixed64(key.data() + key.size() - sizeof(uint64_t));
  if (type) {
    *type = static_cast<UserKeyType>(num & 0xff);
  }
  if (timestamp) {
    *timestamp = static_cast<int64_t>(num >> sizeof(UserKeyType));
  }
  return true;
}

void PackUserKey(const std::string& key, int64_t timestamp, UserKeyType type,
                 std::string* packed_key) {
  packed_key->assign(key);
  PutFixed64(packed_key, PackTimestampAndType(timestamp, type));
}

bool UnpackUserKey(const leveldb::Slice& packed_key, leveldb::Slice* short_key, int64_t* timestamp,
                   UserKeyType* type) {
  return ParseKeySlice(packed_key, timestamp, type, short_key, NULL);
}

leveldb::Slice ExtractTimeKey(const leveldb::Slice& key_slice) {
  leveldb::Slice sub_key;
  CHECK(ParseKeySlice(key_slice, NULL, NULL, NULL, &sub_key));
  return sub_key;
}

leveldb::Slice ExtractShortKey(const leveldb::Slice& key_slice) {
  leveldb::Slice sub_key;
  CHECK(ParseKeySlice(key_slice, NULL, NULL, &sub_key, NULL));
  return sub_key;
}

UserKeyType ExtractKeyType(const leveldb::Slice& key_slice) {
  UserKeyType type;
  CHECK(ParseKeySlice(key_slice, NULL, &type, NULL, NULL));
  return type;
}

}  // namespace io
}  // namespace tera
