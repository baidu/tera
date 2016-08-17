// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef  TERA_ERROR_CODE_H_
#define  TERA_ERROR_CODE_H_

#include <string>

#pragma GCC visibility push(default)

namespace tera {

class ErrorCode {
public:
    enum ErrorCodeType {
        kOK        = 0,
        kNotFound  = 1,
        kBadParam  = 2,
        kSystem    = 3,
        kTimeout   = 4,
        kBusy      = 5,
        kNoQuota   = 6,
        kNoAuth    = 7,
        kUnknown   = 8,
        kNotImpl   = 9,
        kTxnFail   = 10
    };

public:
    // Return a string include type&reason
    // Format: "type [kOK], reason [success]"
    std::string ToString() const;

    ErrorCodeType GetType() const;
    std::string GetReason() const;

    // Internal funcion, do not use
    ErrorCode();
    void SetFailed(ErrorCodeType err, const std::string& reason = "");

private:
    ErrorCodeType _err;
    std::string _reason;
};

// deprecated, try error_code.ToString()
const char* strerr(ErrorCode error_code);

} // namespace tera
#pragma GCC visibility pop
#endif  // TERA_ERROR_CODE_H_
