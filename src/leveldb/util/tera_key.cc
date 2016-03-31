// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "leveldb/tera_key.h"

#include <pthread.h>
#include "coding.h"
#include "leveldb/raw_key_operator.h"

namespace leveldb {

TeraKey::TeraKey(const RawKeyOperator* op)
    : operator_(op) {
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
    operator_->EncodeTeraKey(key, column, qualifier, timestamp, type, &raw_key_);
    return operator_->ExtractTeraKey(raw_key_, &key_, &column_,
                                     &qualifier_, &timestamp_, &type_);
}

bool TeraKey::Decode(const Slice& raw_key) {
    raw_key_ = raw_key.ToString();
    return operator_->ExtractTeraKey(raw_key_, &key_, &column_,
                                     &qualifier_, &timestamp_, &type_);
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

} // namespace leveldb
