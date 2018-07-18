// Copyright (c) 2015-2017, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef TERA_OBSERVER_OBSERVER_DEMO_DEMO_ENTRY_H_
#define TERA_OBSERVER_OBSERVER_DEMO_DEMO_ENTRY_H_

#include <memory>
#include <string>

#include "observer/executor/scanner_entry.h"
#include "tera.h"

namespace tera {
namespace observer {

class DemoEntry : public ScannerEntry {
public:
	DemoEntry();
	virtual ~DemoEntry() {}

    virtual ErrorCode Observe();
};


} // namespace observer
} // namespace tera

#endif  // TERA_OBSERVER_OBSERVER_DEMO_DEMO_ENTRY_H_

