// Copyright (c) 2015-2018, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#pragma once

#include <functional>

namespace common {

class FuncScopeGuard {
 public:
  explicit FuncScopeGuard(std::function<void()> on_exit_scope)
      : on_exit_scope_(on_exit_scope), dismissed_(false) {}

  FuncScopeGuard(FuncScopeGuard const&) = delete;
  FuncScopeGuard& operator=(const FuncScopeGuard&) = delete;

  virtual ~FuncScopeGuard() {
    if (!dismissed_) {
      on_exit_scope_();
    }
  }

  void Dismiss() { dismissed_ = true; }

 private:
  std::function<void()> on_exit_scope_;
  bool dismissed_;
};
}

using common::FuncScopeGuard;
