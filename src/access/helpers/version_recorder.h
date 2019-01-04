// Copyright (c) 2015-2018, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#pragma once

#include <atomic>

namespace tera {
namespace auth {

class VersionRecorder {
 public:
  explicit VersionRecorder() : version_(0), need_update_(false) {}
  ~VersionRecorder() {}

  VersionRecorder(VersionRecorder&) = delete;
  VersionRecorder& operator==(const VersionRecorder&) = delete;

  bool NeedUpdate() { return need_update_; }

  void SetNeedUpdate(bool need_update) { need_update_.store(need_update); }

  void IncVersion() { version_.fetch_add(1); }

  void SetVersion(uint64_t version) { version_.store(version); }

  uint64_t GetVersion() { return version_.load(); }

  bool IsSameVersion(uint64_t version) { return version_.load() == version; }

 private:
  std::atomic<uint64_t> version_;
  std::atomic<bool> need_update_;
};
}
}
