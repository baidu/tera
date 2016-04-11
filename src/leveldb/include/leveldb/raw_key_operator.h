// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef TERA_LEVELDB_UTILS_RAW_KEY_OPERATOR_H
#define TERA_LEVELDB_UTILS_RAW_KEY_OPERATOR_H

#include <stdint.h>

#include "leveldb/slice.h"
#include "leveldb/tera_key.h"

namespace leveldb {

class RawKeyOperator {
public:
    virtual void EncodeTeraKey(const std::string& row_key,
                               const std::string& family,
                               const std::string& qualifier,
                               int64_t timestamp,
                               TeraKeyType type,
                               std::string* tera_key) const = 0;

    virtual bool ExtractTeraKey(const Slice& tera_key,
                                Slice* row_key,
                                Slice* family,
                                Slice* qualifier,
                                int64_t* timestamp,
                                TeraKeyType* type) const = 0;
    virtual int Compare(const Slice& key1,
                        const Slice& key2) const = 0;
};

const RawKeyOperator* ReadableRawKeyOperator();
const RawKeyOperator* BinaryRawKeyOperator();
const RawKeyOperator* KvRawKeyOperator();

} // namespace leveldb
#endif //TERA_LEVELDB_UTILS_RAW_KEY_OPERATOR_H
