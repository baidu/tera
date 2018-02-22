// Copyright (c) 2015-2017, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef TERA_OBSERVER_OBSERVER_DEMO_DEMO_OBSERVER_H_
#define TERA_OBSERVER_OBSERVER_DEMO_DEMO_OBSERVER_H_

#include "observer/executor/observer.h"
#include "tera.h"

namespace tera {
namespace observer {

class DemoObserver : public tera::observer::Observer {
public:
    DemoObserver() {}
    virtual ~DemoObserver() {}
    virtual ErrorCode OnNotify(tera::Transaction* t,
                              tera::Client* client,
                              const std::string& table_name,
                              const std::string& family,
                              const std::string& qualifier,
                              const std::string& row,
                              const std::string& value,
                              int64_t timestamp,
                              Notification* notification);
    virtual std::string GetObserverName() const;
    virtual TransactionType GetTransactionType() const; 
};

class ParseObserver : public tera::observer::Observer {
public:
    ParseObserver() {}
    virtual ~ParseObserver() {}
    virtual ErrorCode OnNotify(tera::Transaction* t,
                              tera::Client* client,
                              const std::string& table_name,
                              const std::string& family,
                              const std::string& qualifier,
                              const std::string& row,
                              const std::string& value,
                              int64_t timestamp,
                              Notification* notification);
    virtual std::string GetObserverName() const;
    virtual TransactionType GetTransactionType() const; 
};

class SingleRowObserver : public tera::observer::Observer {
public:
    SingleRowObserver() {}
    virtual ~SingleRowObserver() {}
    virtual ErrorCode OnNotify(tera::Transaction* t,
                              tera::Client* client,
                              const std::string& table_name,
                              const std::string& family,
                              const std::string& qualifier,
                              const std::string& row,
                              const std::string& value,
                              int64_t timestamp,
                              Notification* notification);
    virtual std::string GetObserverName() const;
    virtual TransactionType GetTransactionType() const; 
};

class NoneTransactionObserver : public tera::observer::Observer {
public:
    NoneTransactionObserver() {}
    virtual ~NoneTransactionObserver() {}
    virtual ErrorCode OnNotify(tera::Transaction* t,
                              tera::Client* client,
                              const std::string& table_name,
                              const std::string& family,
                              const std::string& qualifier,
                              const std::string& row,
                              const std::string& value,
                              int64_t timestamp,
                              Notification* notification);
    virtual std::string GetObserverName() const;
    virtual TransactionType GetTransactionType() const; 
};

} // namespace observer
} // namespace tera

#endif  // TERA_OBSERVER_OBSERVER_DEMO_DEMO_OBSERVER_H_

