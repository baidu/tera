// Copyright (c) 2015-2017, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include <gflags/gflags.h>

#include "observer/observer_demo/demo_entry.h"
#include "observer/observer_demo/demo_observer.h"
#include "tera.h"

DEFINE_bool(observer_demo_for_integration, false, "use this demo for test integration");

std::string GetTeraEntryName() { return "DemoEntry"; }

tera::TeraEntry* GetTeraEntry() { return new tera::observer::DemoEntry(); }

namespace tera {
namespace observer {

DemoEntry::DemoEntry() { hook_.reset(new DemoScanHook()); }

DemoEntry::~DemoEntry() { hook_.reset(); }

ErrorCode DemoEntry::Observe() {
  ErrorCode err;
  Scanner* scanner = GetScanner();
  scanner->SetScanHook(hook_);
  // new an observer ptr and do not delete it
  if (!FLAGS_observer_demo_for_integration) {
    Observer* demo = new DemoObserver();
    Observer* parser = new ParseObserver();
    err = scanner->Observe("observer_test_table", "Data", "Page", demo);
    if (tera::ErrorCode::kOK != err.GetType()) {
      return err;
    }

    err = scanner->Observe("observer_test_table", "Data", "Link", demo);
    if (tera::ErrorCode::kOK != err.GetType()) {
      return err;
    }

    err = scanner->Observe("observer_test_table", "Data", "Url", parser);
    if (tera::ErrorCode::kOK != err.GetType()) {
      return err;
    }
  } else {
    Observer* integration = new IntegrationObserver();
    err = scanner->Observe("observer_test_table", "Data", "qu2", integration);
    if (tera::ErrorCode::kOK != err.GetType()) {
      return err;
    }
  }
  return err;
}

}  // namespace observer
}  // namespace tera
