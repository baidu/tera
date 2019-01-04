// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef TERA_IO_IO_UTILS_H_
#define TERA_IO_IO_UTILS_H_

#include <memory>

#include "common/semaphore.h"
#include "common/rwmutex.h"
#include "leveldb/raw_key_operator.h"
#include "leveldb/status.h"
#include "io/tablet_io.h"
#include "proto/status_code.pb.h"
#include "proto/table_schema.pb.h"

namespace tera {

StatusCode LeveldbCodeToTeraCode(const leveldb::Status& status);

void SetStatusCode(const leveldb::Status& db_status, StatusCode* tera_status);

void SetStatusCode(const io::TabletIO::TabletStatus& tablet_status, StatusCode* tera_status);

const leveldb::RawKeyOperator* GetRawKeyOperatorFromSchema(TableSchema& schema);
}  // namespace tera

#endif  // TERA_IO_IO_UTILS_H_
