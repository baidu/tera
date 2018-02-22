// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// A ErrorCode encapsulates the result of an operation.

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
        kTxnFail   = 10,

        // only for global transaction error
        kGTxnDataTooLarge      = 101,
        kGTxnNotSupport        = 102,
        kGTxnSchemaError       = 103,
        kGTxnOpAfterCommit     = 104,
        kGTxnPrimaryLost       = 105,
        kGTxnWriteConflict     = 106,
        kGTxnLockConflict      = 107,
        kGTxnOKButAckFailed    = 108,
        kGTxnOKButNotifyFailed = 109,
        kGTxnPrewriteTimeout   = 110,
        kGTxnPrimaryCommitTimeout  = 111,
        kGTxnTimestampLost     = 112
        // end of global transaction error
    };

public:
    // Returns a string includes type&reason
    // Format: "type [kOK], reason [success]"
    std::string ToString() const;

    ErrorCodeType GetType() const;
    std::string GetReason() const;

    // Internal funcion, do not use
    ErrorCode();
    void SetFailed(ErrorCodeType err, const std::string& reason = "");

private:
    ErrorCodeType err_;
    std::string reason_;
};

// DEPRECATED. Use error_code.ToString() instead.
const char* strerr(ErrorCode error_code);

} // namespace tera
#pragma GCC visibility pop
#endif  // TERA_ERROR_CODE_H_
