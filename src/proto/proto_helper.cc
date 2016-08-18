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

std::string StatusCodeToString(TabletNodeStatus status) {
    return TabletNodeStatus_Name(status);
}

std::string StatusCodeToString(int32_t status) {
    switch (status) {
    // master
    case kMasterNotInited:
        return "kMasterNotInited";
    case kMasterIsBusy:
        return "kMasterIsBusy";
    case kMasterIsSecondary:
        return "kMasterIsSecondary";
    case kMasterIsReadonly:
        return "kMasterIsReadonly";
    case kMasterOnRestore:
        return "kMasterOnRestore";
    case kMasterIsRunning:
        return "kMasterIsRunning";
    case kMasterOnWait:
        return "kMasterOnWait";
    default:
        ;
    }
    char num[16];
    snprintf(num, 16, "%d", status);
    num[15] = '\0';
    return num;
}

} // namespace tera
