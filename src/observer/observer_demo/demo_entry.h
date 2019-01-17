// Copyright (c) 2015-2017, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef TERA_OBSERVER_OBSERVER_DEMO_DEMO_ENTRY_H_
#define TERA_OBSERVER_OBSERVER_DEMO_DEMO_ENTRY_H_

#include <iostream>
#include <memory>
#include <string>

#include "tera.h"

namespace tera {
namespace observer {

class DemoScanHook : public ScanHook {
  virtual void Before(const std::string& table_name, const ScanHook::Columns& columns) {
    std::cout << "demo scan filter before scan : " << table_name << std::endl;
    for (const auto& col : columns) {
      std::cout << col.first << "\t" << col.second << std::endl;
    }
  }

  virtual void After(const std::string& table_name, const ScanHook::Columns& columns,
                     bool scan_ret) {
    std::cout << "demo scan filter before scan : " << table_name << " scan_ret :" << scan_ret
              << std::endl;
    for (const auto& col : columns) {
      std::cout << col.first << "\t" << col.second << std::endl;
    }
  }
};

class DemoEntry : public ScannerEntry {
 public:
  DemoEntry();
  virtual ~DemoEntry();

  virtual ErrorCode Observe();

 private:
  std::shared_ptr<DemoScanHook> hook_;
};

}  // namespace observer
}  // namespace tera

#endif  // TERA_OBSERVER_OBSERVER_DEMO_DEMO_ENTRY_H_
