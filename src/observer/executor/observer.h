// Copyright (c) 2015-2017, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef TERA_OBSERVER_H_
#define TERA_OBSERVER_H_

#include <string>

#include "tera/client.h"
#include "tera/error_code.h"
#include "tera/transaction.h"
#include "observer/executor/notification.h"

#pragma GCC visibility push(default)
namespace tera {
namespace observer {

enum TransactionType {
    kGlobalTransaction = 0,
    kSingleRowTransaction = 1,
    kNoneTransaction = 2,
};

class Observer {
public:
    virtual ~Observer() {}

    // (1) if notify and ack are needed during OnNotify,
    // call notifiaction->Ack and notification->Notify
    // before transaction commit
    // (2) have to call notification->Done() after all operators finished
    virtual void OnNotify(tera::Transaction* t,
                          tera::Client* client,
                          const std::string& table_name,
                          const std::string& family,
                          const std::string& qualifier,
                          const std::string& row,
                          const std::string& value,
                          int64_t timestamp,
                          Notification* notification) = 0;
    // return observer name
    virtual std::string GetObserverName() const = 0;
    
    // return TransactionType
    virtual TransactionType GetTransactionType() const = 0; 
};

} // namespace observer
}
#pragma GCC visibility pop

#endif  // TERA_OBSERVER_H_
