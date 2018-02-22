// Copyright (c) 2015-2017, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "observer/observer_demo/demo_entry.h"

#include "observer/executor/observer.h"
#include "observer/executor/scanner.h"
#include "observer/observer_demo/demo_observer.h"
#include "tera.h"

std::string GetTeraEntryName() {
    return "DemoEntry";
}

tera::TeraEntry* GetTeraEntry() {
    return new tera::observer::DemoEntry();
}

namespace tera {
namespace observer {

DemoEntry::DemoEntry() {}

ErrorCode DemoEntry::Observe() {
	ErrorCode err;
	// new an observer ptr and do not delete it
	Observer* demo = new DemoObserver();
	Observer* parser = new ParseObserver();
	Observer* single_row_observer = new SingleRowObserver();
	Observer* none_txn_observer = new NoneTransactionObserver();

	Scanner* scanner = GetScanner();
	err = scanner->Observe("observer_test_table", "Data", "Page", demo);
	if (tera::ErrorCode::kOK != err.GetType()) {
		return err;
	}
	err = scanner->Observe("observer_test_table", "Data", "Link", demo);
	if (tera::ErrorCode::kOK != err.GetType()) {
		return err;
	}

	err = scanner->Observe("observer_test_table", "Data", "Link", parser);
	if (tera::ErrorCode::kOK != err.GetType()) {
		return err;
	}

	err = scanner->Observe("single_row_test_table", "Data", "Link", single_row_observer);
	if (tera::ErrorCode::kOK != err.GetType()) {
		return err;
	}

	err = scanner->Observe("none_txn_test_table", "Data", "Link", none_txn_observer);
	return err;

}

} // namespace observer
} // namespace tera