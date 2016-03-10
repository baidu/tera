// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "utils/schema_utils.h"

#include <sstream>

#include <glog/logging.h>

namespace tera {

bool IsSchemaCfDiff(const TableSchema& a, const TableSchema& b) {
    std::stringstream s0;
    std::stringstream s1;
    for (int i = 0; i < a.column_families_size(); ++i) {
        s0 << a.column_families(i).ShortDebugString();
    }
    LOG(INFO) << "[utils] " << s0.str();
    for (int i = 0; i < b.column_families_size(); ++i) {
        s1 << b.column_families(i).ShortDebugString();
    }
    LOG(INFO) << "[utils] " << s1.str();
    return (s0.str().compare(s1.str()) != 0);
}

bool IsSchemaLgDiff(const TableSchema& a, const TableSchema& b) {
    std::stringstream s0;
    std::stringstream s1;
    for (int i = 0; i < a.locality_groups_size(); ++i) {
        s0 << a.locality_groups(i).ShortDebugString();
    }
    LOG(INFO) << "[utils] " << s0.str();
    for (int i = 0; i < b.locality_groups_size(); ++i) {
        s1 << b.locality_groups(i).ShortDebugString();
    }
    LOG(INFO) << "[utils] " << s1.str();
    return (s0.str().compare(s1.str()) != 0);
}

} // namespace tera
