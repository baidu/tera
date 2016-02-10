// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "sdk/tera.h"

#include <limits>

namespace tera {

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
    const char* ret = NULL;
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
    case ErrorCode::kUnavailable:
        ret = "Temporarily Unavailable";
        break;
    case ErrorCode::kTimeout:
        ret = "Timeout";
        break;
    case ErrorCode::kTooBusy:
        ret = "Server Too Busy";
        break;
    case ErrorCode::kIOError:
        ret = "IO Error";
        break;
    case ErrorCode::kNetworkError:
        ret = "Network Error";
        break;
    case ErrorCode::kNoQuota:
        ret = "User No Quota";
        break;
    case ErrorCode::kNoAuth:
        ret = "User Unauthorized";
        break;
    case ErrorCode::kNotImpl:
        ret = "Not Implemented";
        break;
    default:
        ret = "Unkown Error";
        break;
    }
    return ret;
}

const int64_t kLatestTimestamp = std::numeric_limits<long int>::max();
const int64_t kOldestTimestamp = std::numeric_limits<long int>::min();

}
