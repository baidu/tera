// Copyright (c) 2015-2017, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#pragma once

#include "observer/observer.h"
#include "observer/scanner.h"
#include "tera/tera_entry.h"

namespace tera {
namespace observer {

class Scanner;

class ScannerEntry : public TeraEntry {
public:
    ScannerEntry();
    virtual ~ScannerEntry();

    virtual bool StartServer();
    virtual bool Run();
    virtual void ShutdownServer();

    virtual ErrorCode Observe();
    virtual void SetOptions(const ScannerOptions& options);
    Scanner* GetScanner() const;

private:
    ScannerOptions options_;
};

} // namespace observer
} // namespace tera
