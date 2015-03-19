// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef TERA_IO_CODING_H_
#define TERA_IO_CODING_H_

#include <stdint.h>
#include "leveldb/slice.h"

namespace tera {
namespace io {

inline uint32_t DecodeFixed32(const char* ptr) {
    uint32_t result;
    memcpy(&result, ptr, sizeof(result));
    return result;
}

inline uint64_t DecodeFixed64(const char* ptr) {
    uint64_t result;
    memcpy(&result, ptr, sizeof(result));
    return result;
}

inline void EncodeFixed64(char* buf, uint64_t value) {
    memcpy(buf, &value, sizeof(value));
}


inline void PutFixed64(std::string* dst, uint64_t value) {
    char buf[sizeof(value)];
    EncodeFixed64(buf, value);
    dst->append(buf, sizeof(buf));
}

inline void EncodeBigEndian32(char* buf, uint32_t value) {
    buf[0] = (value >> 24) & 0xff;
    buf[1] = (value >> 16) & 0xff;
    buf[2] = (value >> 8) & 0xff;
    buf[3] = value & 0xff;
}

inline uint32_t DecodeBigEndain32(const char* ptr) {
    return ((static_cast<uint32_t>(static_cast<unsigned char>(ptr[3])))
        | (static_cast<uint32_t>(static_cast<unsigned char>(ptr[2])) << 8)
        | (static_cast<uint32_t>(static_cast<unsigned char>(ptr[1])) << 16)
        | (static_cast<uint32_t>(static_cast<unsigned char>(ptr[0])) << 24));
}

inline void EncodeBigEndian(char* buf, uint64_t value) {
    buf[0] = (value >> 56) & 0xff;
    buf[1] = (value >> 48) & 0xff;
    buf[2] = (value >> 40) & 0xff;
    buf[3] = (value >> 32) & 0xff;
    buf[4] = (value >> 24) & 0xff;
    buf[5] = (value >> 16) & 0xff;
    buf[6] = (value >> 8) & 0xff;
    buf[7] = value & 0xff;
}

inline uint64_t DecodeBigEndain(const char* ptr) {
    uint64_t lo = DecodeBigEndain32(ptr + 4);
    uint64_t hi = DecodeBigEndain32(ptr);
    return (hi << 32) | lo;
}

inline int32_t DecodeBigEndain32Sign(const char* ptr) {
    return ((static_cast<uint32_t>(static_cast<unsigned char>(ptr[3])))
        | (static_cast<uint32_t>(static_cast<unsigned char>(ptr[2])) << 8)
        | (static_cast<uint32_t>(static_cast<unsigned char>(ptr[1])) << 16)
        | (static_cast<int32_t>(static_cast<unsigned char>(ptr[0])) << 24));
}

inline int64_t DecodeBigEndainSign(const char* ptr) {
    uint64_t lo = DecodeBigEndain32(ptr + 4);
    int64_t hi = DecodeBigEndain32Sign(ptr);
    return (hi << 32) | lo;
}

enum UserKeyType {
    UKT_VALUE = 0,
    UKT_FORSEEK
};

inline uint64_t PackTimestampAndType(int64_t timestamp, UserKeyType key_type) {
    uint64_t stamp;
    if (timestamp >= 0) {
        stamp = static_cast<uint64_t>(timestamp);
    }
    return (stamp << sizeof(UserKeyType)) | key_type;
}

bool ParseKeySlice(const leveldb::Slice& key, int64_t* timestamp,
                   UserKeyType* type, leveldb::Slice* short_key,
                   leveldb::Slice* time_key);

void PackUserKey(const std::string& key, int64_t timestamp,
                 UserKeyType type, std::string* packed_key);

bool UnpackUserKey(const leveldb::Slice& packed_key,
                   leveldb::Slice* short_key, int64_t* timestamp, UserKeyType* type);

leveldb::Slice ExtractTimeKey(const leveldb::Slice& key_slice);

leveldb::Slice ExtractShortKey(const leveldb::Slice& key_slice);

UserKeyType ExtractKeyType(const leveldb::Slice& key_slice);

} // namespace io
} // namespace tera

#endif // TERA_IO_CODING_H_
