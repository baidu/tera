// Copyright (c) 2018, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Author: tianye15@baidu.com

#pragma once
#include "common/semaphore.h"
#include "common/rwmutex.h"

namespace leveldb {

class DfsReadThreadLimiter {
 public:
  class Token {
   public:
    explicit Token(const std::shared_ptr<common::Semaphore> &limiter) : limiter_(limiter) {}
    Token(const Token &) = delete;
    Token(const Token &&) = delete;
    void operator=(const Token &) = delete;

    ~Token() { limiter_->Release(); }

   private:
    std::shared_ptr<common::Semaphore> limiter_;
  };

  DfsReadThreadLimiter(const DfsReadThreadLimiter &) = delete;
  DfsReadThreadLimiter(const DfsReadThreadLimiter &&) = delete;
  void operator=(const DfsReadThreadLimiter &) = delete;

  void SetLimit(int64_t val) {
    decltype(limiter_) new_limiter{new common::Semaphore{val}};
    common::WriteLock _(&lock_);
    swap(limiter_, new_limiter);
  }

  std::unique_ptr<Token> GetToken() {
    common::ReadLock _(&lock_);
    if (limiter_->TryAcquire()) {
      return std::unique_ptr<Token>{new Token{limiter_}};
    }
    return nullptr;
  }

  inline static DfsReadThreadLimiter &Instance();

 private:
  DfsReadThreadLimiter() = default;

  std::shared_ptr<common::Semaphore> limiter_;
  common::RWMutex lock_;
};

DfsReadThreadLimiter &DfsReadThreadLimiter::Instance() {
  static DfsReadThreadLimiter instance;
  return instance;
}
}
