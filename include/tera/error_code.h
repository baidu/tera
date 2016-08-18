// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef  TERA_ERROR_CODE_
#define  TERA_ERROR_CODE_

#include <stdint.h>
#include <list>
#include <map>
#include <set>
#include <string>
#include <vector>

#pragma GCC visibility push(default)
namespace tera {

class ErrorCode {
public:
    enum ErrorCodeType {
        kOK = 0,
        kNotFound,
        kBadParam,
        kSystem,
        kTimeout,
        kBusy,
        kNoQuota,
        kNoAuth,
        kUnknown,
        kNotImpl,
        kTxnFail
    };
    ErrorCode();
    std::string ToString() const;

    std::string GetReason() const;
    ErrorCodeType GetType() const;
    void SetFailed(ErrorCodeType err, const std::string& reason = "");

private:
    ErrorCodeType _err;
    std::string _reason;
};

const char* strerr(ErrorCode error_code);

} // namespace tera
#pragma GCC visibility pop

#endif  // TERA_ERROR_CODE_
