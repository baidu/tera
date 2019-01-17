// Copyright (c) 2015-2017, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#pragma once

#include <memory>
#include <set>
#include <string>

#include "observer/observer.h"
#include "tera/error_code.h"

#pragma GCC visibility push(default)
namespace tera {
namespace observer {

enum class ScanStrategy { kRandom = 0, kTabletBucket };

struct ScannerOptions {
  ScanStrategy strategy;
  int32_t bucket_cnt;  // When strategy=kTabletShared, this available
  int32_t bucket_id;   // When strategy=kTabletShared, this available

  ScannerOptions() : strategy(ScanStrategy::kRandom), bucket_cnt(1), bucket_id(0) {}
};

class ScanHook {
 public:
  typedef std::pair<std::string, std::string> Column;
  typedef std::set<ScanHook::Column> Columns;
  virtual ~ScanHook() {}

  // user can define self scan strategy before per round scan task
  virtual void Before(const std::string& table_name,
                      const ScanHook::Columns& columns) { /* default noting to do */
  }

  // user can define self scan strategy after per round scan task
  virtual void After(const std::string& table_name, const ScanHook::Columns& columns,
                     bool scan_ret) { /* default noting to do */
  }
};

class Scanner {
 public:
  static Scanner* GetScanner();

  virtual ~Scanner() {}

  // register user define observers
  // user should not destruct observers, which will be handled by scanner
  virtual ErrorCode Observe(const std::string& table_name, const std::string& column_family,
                            const std::string& qualifier, Observer* observer) = 0;

  virtual bool Init() = 0;

  virtual bool Start() = 0;

  virtual void Exit() = 0;

  virtual void SetOptions(const ScannerOptions& options) = 0;

  virtual void SetScanHook(const std::shared_ptr<ScanHook>& hook) = 0;
};
}  // namespace observer
}  // namespace tera
#pragma GCC visibility pop
