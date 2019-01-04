// Copyright (c) 2015-2018, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include <atomic>
#include <iostream>
#include <string>
#include <vector>

#include "gflags/gflags.h"
#include <glog/logging.h>
#include <gtest/gtest.h>
#include <memory>

#include "observer/executor/notification_impl.h"
#include "tera.h"

namespace tera {
namespace observer {

class NotificationImplTest : public ::testing::Test {
 public:
  NotificationImplTest() : semaphore_(10), notify_cell_(nullptr) {
    semaphore_.Acquire();
    notify_cell_.reset(new NotifyCell(semaphore_));
    notify_cell_->notify_transaction = nullptr;
  }

  ~NotificationImplTest() {}

  NotificationImpl* GetNotification() { return new NotificationImpl(notify_cell_); }

  common::Semaphore semaphore_;
  std::shared_ptr<NotifyCell> notify_cell_;
};

TEST_F(NotificationImplTest, SetAckCallBack) {
  NotificationImpl* n = GetNotification();
  n->SetAckContext(n);
  n->SetAckCallBack([](Notification* n1, const ErrorCode& err) {
    NotificationImpl* n2 = (NotificationImpl*)(n1->GetAckContext());
    EXPECT_EQ(n1, n2);
    EXPECT_EQ(err.GetType(), ErrorCode::kOK);
    delete n1;
  });
  ErrorCode ec;
  n->ack_callback_(n, ec);
}

TEST_F(NotificationImplTest, SetNotifyCallBack) {
  NotificationImpl* n = GetNotification();
  n->SetNotifyContext(n);
  n->SetNotifyCallBack([](Notification* n1, const ErrorCode& err) {
    NotificationImpl* n2 = (NotificationImpl*)(n1->GetNotifyContext());
    EXPECT_EQ(n1, n2);
    EXPECT_EQ(err.GetType(), ErrorCode::kOK);
    delete n1;
  });
  ErrorCode ec;
  n->notify_callback_(n, ec);
}

}  // namespace observer
}  // namespace tera
