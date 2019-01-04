// Copyright (c) 2015-2017, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#pragma once

#include <string>

#include "tera/table.h"

#pragma GCC visibility push(default)

namespace tera {
namespace observer {

class Notification {
 public:
  virtual ~Notification() {}

  typedef std::function<void(Notification* notification, const ErrorCode& error)> Callback;

  // when TransactionType is 'kNoneTransaction'
  // user can set ack callback/context as one please
  virtual void SetAckCallBack(Callback callback) = 0;
  virtual void SetAckContext(void* context) = 0;
  virtual void* GetAckContext() = 0;

  virtual void Ack(Table* t, const std::string& row_key, const std::string& column_family,
                   const std::string& qualifier) = 0;

  // when TransactionType is 'kNoneTransaction'
  // user can set notify callback/context as one please
  virtual void SetNotifyCallBack(Callback callback) = 0;
  virtual void SetNotifyContext(void* context) = 0;
  virtual void* GetNotifyContext() = 0;

  virtual void Notify(Table* t, const std::string& row_key, const std::string& column_family,
                      const std::string& qualifier) = 0;

  // relases resource after OnNotify finished
  // and delete this
  virtual void Done() = 0;
};

}  // namespace observer
}  // namespace tera

#pragma GCC visibility pop
