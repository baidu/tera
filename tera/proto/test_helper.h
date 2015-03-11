// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef TERA_PROTO_TEST_HELPER_H
#define TERA_PROTO_TEST_HELPER_H

#include "tera/proto/table_schema.pb.h"

namespace tera {

TableSchema DefaultTableSchema();

ColumnFamilySchema DefaultCFSchema(const std::string& lg_name,
                                   uint32_t id);

LocalityGroupSchema DefaultLGSchema(uint32_t id);

} // namespace tera

#endif // TERA_PROTO_TEST_HELPER_H
