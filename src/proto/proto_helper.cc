// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include <stdio.h>

#include "proto/proto_helper.h"

namespace tera {

std::string StatusCodeToString(StatusCode status) {
    return StatusCode_Name(status);
}

std::string StatusCodeToString(TabletStatus status) {
    return TabletStatus_Name(status);
}

std::string StatusCodeToString(TableStatus status) {
    return TableStatus_Name(status);
}

std::string StatusCodeToString(CompactStatus status) {
    return CompactStatus_Name(status);
}

void SetStatusCode(const StatusCode& code, StatusCode* tera_status) {
    if (tera_status) {
        *tera_status = code;
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

void SetStatusCode(const CompactStatus& compact_status, StatusCode* tera_status) {
    if (tera_status) {
        *tera_status = static_cast<StatusCode>(compact_status);
    }
}

} // namespace tera
