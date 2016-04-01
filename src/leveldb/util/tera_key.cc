// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "leveldb/tera_key.h"

#include <pthread.h>
#include "coding.h"
#include "logging.h"
#include "leveldb/raw_key_operator.h"

namespace leveldb {

TeraKey::TeraKey(const RawKeyOperator* op)
    : operator_(op), is_empty_(true) {
}

TeraKey::TeraKey(const TeraKey& tk) {
    *this = tk;
    operator_->ExtractTeraKey(raw_key_, &key_, &column_,
                              &qualifier_, &timestamp_, &type_);
}

TeraKey::~TeraKey() {}

bool TeraKey::Encode(const std::string& key, const std::string& column,
                     const std::string& qualifier, int64_t timestamp,
                     TeraKeyType type) {
    is_empty_ = false;
    operator_->EncodeTeraKey(key, column, qualifier, timestamp, type, &raw_key_);
    return operator_->ExtractTeraKey(raw_key_, &key_, &column_,
                                     &qualifier_, &timestamp_, &type_);
}

bool TeraKey::Decode(const Slice& raw_key) {
    raw_key_ = raw_key.ToString();
    bool res =
        operator_->ExtractTeraKey(raw_key_, &key_, &column_, &qualifier_, &timestamp_, &type_);
    if (res) {
        is_empty_ = false;
        return true;
    } else {
        return false;
    }
}

bool TeraKey::SameRow(const TeraKey& tk) {
    return (key_.compare(tk.key()) == 0);
}

bool TeraKey::SameColumn(const TeraKey& tk) {
    return (key_.compare(tk.key()) == 0
            && column_.compare(tk.column()) == 0);
}

bool TeraKey::SameQualifier(const TeraKey& tk) {
    return (key_.compare(tk.key()) == 0
            && column_.compare(tk.column()) == 0
            && qualifier_.compare(tk.qualifier()) == 0);
}

bool TeraKey::IsDel() {
    switch (type_) {
    case TKT_DEL:
    case TKT_DEL_COLUMN:
    case TKT_DEL_QUALIFIERS:
    case TKT_DEL_QUALIFIER:
        return true;
    default:
        return false;
    }
}

int TeraKey::Compare(const TeraKey& tk) {
    int res = key_.compare(tk.key());
    if (res != 0) {
        return res;
    }
    res = column_.compare(tk.column());
    if (res != 0) {
        return res;
    }
    res = qualifier_.compare(tk.qualifier());
    if (res != 0) {
        return res;
    }
    if (timestamp_ != tk.timestamp()) {
        return timestamp_ > tk.timestamp() ? 1 : -1;
    }
    if (type_ != tk.type()) {
        return type_ > tk.type() ? 1 : -1;
    } else {
        return 0;
    }
}

std::string TeraKey::DebugString() {
    std::string r;
    r.append(EscapeString(key_) + " : ");
    r.append(EscapeString(column_) + " : ");
    r.append(EscapeString(qualifier_) + " : ");
    AppendNumberTo(&r, timestamp_);
    r.append(" : ");
    AppendNumberTo(&r, type_);
    return r;
}
} // namespace leveldb
