// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "io/io_utils.h"

namespace tera {

StatusCode LeveldbCodeToTeraCode(const leveldb::Status& status) {
    if (status.ok()) {
        return kTabletNodeOk;
    } else if (status.IsNotFound()) {
        return kKeyNotExist;
    } else if (status.IsCorruption()) {
        return kTableCorrupt;
    }
    return kIOError;
}

void SetStatusCode(const leveldb::Status& db_status, StatusCode* tera_status) {
    if (tera_status) {
        *tera_status = LeveldbCodeToTeraCode(db_status);
    }
}

void SetStatusCode(const io::TabletIO::TabletStatus& tablet_status, StatusCode* tera_status) {
    if (tera_status) {
        *tera_status = static_cast<StatusCode>(tablet_status);
    }
}

void SetStatusCode(const StatusCode& code, StatusCode* tera_status) {
    if (tera_status) {
        *tera_status = code;
    }
}

void SetStatusCode(const CompactStatus& compact_status, StatusCode* tera_status) {
    if (tera_status) {
        *tera_status = static_cast<StatusCode>(compact_status);
    }
}

void SetStatusCode(const TabletStatus& tablet_status, StatusCode* tera_status) {
    if (tera_status) {
        *tera_status = static_cast<StatusCode>(tablet_status);
    }
}

void SetStatusCode(const TableStatus& table_status, StatusCode* tera_status) {
    if (tera_status) {
        *tera_status = static_cast<StatusCode>(table_status);
    }
}

const leveldb::RawKeyOperator* GetRawKeyOperatorFromSchema(TableSchema& schema) {
    // key_translator should be lg property, but here only support table
    // property. In future work, key_translator should be done in leveldb.
    RawKey raw_key = schema.raw_key();
    switch (raw_key) {
        case Binary:
            return leveldb::BinaryRawKeyOperator();
        case Readable:
            return leveldb::ReadableRawKeyOperator();
        default:
            return leveldb::KvRawKeyOperator();
    }
}
} // namespace tera
