// Copyright (c) 2015-2017, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef TERA_OBSERVER_OBSERVER_DEMO_DEMO_OBSERVER_H_
#define TERA_OBSERVER_OBSERVER_DEMO_DEMO_OBSERVER_H_

#include "observer/executor/observer.h"
#include "tera.h"

#include <atomic>

namespace tera {
namespace observer {

class DemoObserver : public tera::observer::Observer {
public:
    DemoObserver() {}
    virtual ~DemoObserver() {}
    virtual void OnNotify(tera::Transaction* t,
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
    virtual void OnNotify(tera::Transaction* t,
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
    
public:
   struct TransactionContext {
      std::string table_name;
      std::string row;
   };
};

class IntegrationObserver : public tera::observer::Observer {
public:
    IntegrationObserver() : notify_cnt_(0), done_cnt_(0), fail_cnt_(0) {}
    virtual ~IntegrationObserver() {}
    virtual void OnNotify(tera::Transaction* t,
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
    
public:
    struct TxnContext {
        TxnContext() {}
        ~TxnContext() {
            delete input_table;
            delete output_table;
        }
        tera::observer::Observer* observer;
        tera::Transaction* txn;
        Notification* notification;
        tera::Table* input_table;
        tera::Table* output_table;
        std::string row;
        std::string family;
        std::string qualifier;
        int64_t begin_time;
    };
    std::atomic<int64_t> notify_cnt_;
    std::atomic<int64_t> done_cnt_;
    std::atomic<int64_t> fail_cnt_;
};

class SingleRowObserver : public tera::observer::Observer {
public:
    SingleRowObserver() {}
    virtual ~SingleRowObserver() {}
    virtual void OnNotify(tera::Transaction* t,
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
    virtual void OnNotify(tera::Transaction* t,
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

