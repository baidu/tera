// Copyright (c) 2015-2017, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef TERA_SCANNER_H_
#define TERA_SCANNER_H_

#include <memory>

#include "observer/executor/observer.h"
#include "tera/error_code.h"

#pragma GCC visibility push(default)
namespace tera {
namespace observer {

class Scanner {
public:
    static Scanner* GetScanner();

    virtual ~Scanner() {}

    // register user define observers
    // user should not destruct observers, which will be handled by scanner
    virtual ErrorCode Observe(const std::string& table_name,
                              const std::string& column_family,
                              const std::string& qualifier,
                              Observer* observer) = 0;

    virtual bool Init() = 0;

    virtual bool Start() = 0;

    virtual void Exit() = 0;
};

} // namespace observer
} // namespace tera
#pragma GCC visibility pop

#endif  // TERA_SCANNER_H_
