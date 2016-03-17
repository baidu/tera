// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef CALLBACK_CHECK_TERA_H_
#define CALLBACK_CHECK_TERA_H_

#include "sdk/tera.h"

namespace tera {

class CallChecker {
public:
    CallChecker() {}
    virtual ~CallChecker() {}
    virtual bool NeedCall(ErrorCode::ErrorCodeType code) = 0;
};

}

#endif

