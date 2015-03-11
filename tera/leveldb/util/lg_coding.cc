// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "leveldb/lg_coding.h"

#include "util/coding.h"

namespace leveldb {

const std::string KG_PREFIX = "//LG_ID//";

void PutFixed32LGId(std::string *dst, uint32_t lg_id) {
    std::string lg_str;
    PutLengthPrefixedSlice(&lg_str, KG_PREFIX);
    PutVarint32(&lg_str, lg_id);
    PutLengthPrefixedSlice(&lg_str, *dst);
    *dst = lg_str;
}

bool GetFixed32LGId(Slice* input, uint32_t* lg_id) {
    Slice lg_str(*input);
    Slice str;
    if (!GetLengthPrefixedSlice(&lg_str, &str)) {
        return false;
    } else if (str != KG_PREFIX) {
        return false;
    } else if (!GetVarint32(&lg_str, lg_id)) {
        return false;
    }
    GetLengthPrefixedSlice(&lg_str, input);
    return true;
}

} // namespace leveldb
