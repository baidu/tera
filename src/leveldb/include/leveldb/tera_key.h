// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef TERA_LEVELDB_UTILS_TERA_KEY_H
#define TERA_LEVELDB_UTILS_TERA_KEY_H

#include <stdint.h>

#include "leveldb/slice.h"

namespace leveldb {

enum TeraKeyType {
    TKT_FORSEEK        = 0,
    TKT_DEL            = 1,
    TKT_DEL_COLUMN     = 2,
    TKT_DEL_QUALIFIERS = 3,
    TKT_DEL_QUALIFIER  = 4,
    TKT_VALUE          = 5,
    // 6 is reserved, do not use
    TKT_ADD            = 7,
    TKT_PUT_IFABSENT   = 8,
    TKT_APPEND         = 9,
    TKT_ADDINT64       = 10,
    TKT_TYPE_NUM       = 11
};

class RawKeyOperator;

class TeraKey {
public:
    explicit TeraKey(const RawKeyOperator* op);
    explicit TeraKey(const TeraKey& tk);
    ~TeraKey();

    bool Encode(const std::string& key, const std::string& column,
                const std::string& qualifier, int64_t timestamp,
                TeraKeyType type);
    bool Decode(const Slice& raw_key);

    bool SameRow(const TeraKey& tk);
    bool SameColumn(const TeraKey& tk);
    bool SameQualifier(const TeraKey& tk);

    bool IsDel();
    int Compare(const TeraKey& tk);
    std::string DebugString();

    bool empty() const { return is_empty_; }
    Slice raw_key() const { return raw_key_; }
    Slice key() const { return key_; }
    Slice column() const { return column_; }
    Slice qualifier() const { return qualifier_; }
    int64_t timestamp() const { return timestamp_; }
    TeraKeyType type() const { return type_; }

private:
    TeraKey();
    const RawKeyOperator* operator_;
    std::string raw_key_;
    Slice key_;
    Slice column_;
    Slice qualifier_;
    int64_t timestamp_;
    TeraKeyType type_;
    bool is_empty_;
};

} // namespace leveldb
#endif //TERA_LEVELDB_UTILS_TERA_KEY_H
