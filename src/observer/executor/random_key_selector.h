// Copyright (c) 2015-2017, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef TERA_OBSERVER_EXECUTOR_RANDOM_KEY_SELECTOR_H_
#define TERA_OBSERVER_EXECUTOR_RANDOM_KEY_SELECTOR_H_

#include <map>
#include <memory>
#include <string>
#include <vector>

#include "common/mutex.h"
#include "common/thread.h"
#include "observer/executor/key_selector.h"
#include "tera.h"

namespace tera {
namespace observer {

class RandomKeySelector : public KeySelector {
public:
	RandomKeySelector();
	virtual ~RandomKeySelector();

	virtual bool SelectStart(std::string* table_name,
							 std::string* start_key);
	virtual ErrorCode Observe(const std::string& table_name);
private:
	void Update();

private:
	tera::Client* client_;
	mutable Mutex table_mutex_;
	std::vector<std::string> observe_tables_;
	std::shared_ptr<std::map<std::string, std::vector<tera::TabletInfo>>> tables_;
	common::Thread update_thread_;

	mutable Mutex quit_mutex_;
	bool quit_;
	common::CondVar cond_;
};

} // namespace observer
} // namespace tera

#endif  // TERA_OBSERVER_EXECUTOR_RANDOM_KEY_SELECTOR_H_