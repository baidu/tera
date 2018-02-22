// Copyright (c) 2015-2017, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef TERA_OBSERVER_EXECUTOR_KEY_SELECTOR_H_
#define TERA_OBSERVER_EXECUTOR_KEY_SELECTOR_H_

#include <string>
#include <vector>

#include "tera.h"

namespace tera {
namespace observer {

class KeySelector {
public:
	virtual ~KeySelector() {}

	// output: selected table name, selected start key
	virtual bool SelectStart(std::string* table_name,
							 std::string* start_key) = 0;
	virtual ErrorCode Observe(const std::string& table_name) = 0;
};

} // namespace observer
} // namespace tera

#endif  // TERA_OBSERVER_EXECUTOR_KEY_SELECTOR_H_
