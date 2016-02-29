// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef TERA_SCHEMA_UTILS_H_
#define TERA_SCHEMA_UTILS_H_

#include "proto/table_schema.pb.h"

namespace tera {

bool IsSchemaCfDiff(const TableSchema& a, const TableSchema& b);

bool IsSchemaLgDiff(const TableSchema& a, const TableSchema& b);

} // namespace tera

#endif // TERA_SCHEMA_UTILS_H_
