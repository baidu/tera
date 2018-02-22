// Copyright (c) 2015-2017, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef TERA_OBSERVER_EXECUTOR_SCANNER_ENTRY_H_
#define TERA_OBSERVER_EXECUTOR_SCANNER_ENTRY_H_

#include <memory>
#include <string>

#include "common/this_thread.h"
#include "observer/executor/observer.h"
#include "tera.h"
#include "tera_entry.h"

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
    Scanner* GetScanner() const;
private:
	std::unique_ptr<Scanner> scanner_;
};


} // namespace observer
} // namespace tera

#endif  // TERA_OBSERVER_EXECUTOR_SCANNER_ENTRY_H_