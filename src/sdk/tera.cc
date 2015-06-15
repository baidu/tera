// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "sdk/tera.h"

#include <limits>

namespace tera {

TableOptions::TableOptions()
    : sequential_write(false) {
}

ErrorCode::ErrorCode() : _err(kOK) {
}

void ErrorCode::SetFailed(ErrorCodeType err, const std::string& reason) {
    _err= err;
    _reason = reason;
}

std::string ErrorCode::GetReason() const {
    return _reason;
}

ErrorCode::ErrorCodeType ErrorCode::GetType() const {
    return _err;
}

const char* strerr(ErrorCode error_code) {
    const char* ret = "Unknown error";
    switch (error_code.GetType()) {
    case ErrorCode::kOK:
        ret = "OK";
        break;
    case ErrorCode::kNotFound:
        ret = "Not Found";
        break;
    case ErrorCode::kBadParam:
        ret = "Bad Parameter";
        break;
    case ErrorCode::kSystem:
        ret = "SystemError";
        break;
    case ErrorCode::kTimeout:
        ret = "Timeout";
        break;
    case ErrorCode::kBusy:
        ret = "SystemBusy";
        break;
    case ErrorCode::kNoQuota:
        ret = "UserNoQuota";
        break;
    case ErrorCode::kNoAuth:
        ret = "UserUnauthorized";
        break;
    case ErrorCode::kNotImpl:
        ret = "Not Implement";
    default:
        ret = "UnkownError";
    }
    return ret;
}

const int64_t kLatestTimestamp = std::numeric_limits<long int>::max();
const int64_t kOldestTimestamp = std::numeric_limits<long int>::min();

}
