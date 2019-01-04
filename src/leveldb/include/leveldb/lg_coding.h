// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef STORAGE_LEVELDB_UTIL_LG_CODING_H_
#define STORAGE_LEVELDB_UTIL_LG_CODING_H_

#include <string>

#include "leveldb/slice.h"

namespace leveldb {

extern void PutFixed32LGId(std::string* dst, uint32_t lg_id);

extern bool GetFixed32LGId(Slice* input, uint32_t* lg_id);

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_UTIL_LG_CODING_H_
