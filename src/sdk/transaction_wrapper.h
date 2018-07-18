// Copyright (c) 2018, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef TERA_SDK_TRANSACTION_WRAPPER_H_
#define TERA_SDK_TRANSACTION_WRAPPER_H_

#include "tera.h"
#include <memory>

namespace tera {

template <class T>
class TransactionWrapper : public Transaction {
public:
    TransactionWrapper(const std::shared_ptr<T>& transaction_impl)
        : transaction_impl_(transaction_impl) {}
    ~TransactionWrapper() {}
    void ApplyMutation(RowMutation* row_mu) {
        transaction_impl_->ApplyMutation(row_mu);
    }
    ErrorCode Get(RowReader* row_reader) {
        return transaction_impl_->Get(row_reader);
    }
    void SetCommitCallback(Callback callback) {
        transaction_impl_->SetCommitCallback(callback);
    }
    Callback GetCommitCallback() {
        return transaction_impl_->GetCommitCallback();
    }
    void SetContext(void* context) {
        transaction_impl_->SetContext(context);
    }
    void* GetContext() {
        return transaction_impl_->GetContext();
    }
    const ErrorCode& GetError() {
        return transaction_impl_->GetError();
    }
    ErrorCode Commit() {
        return transaction_impl_->Commit();
    }
    int64_t GetStartTimestamp() {
        return transaction_impl_->GetStartTimestamp();
    }
    int64_t GetCommitTimestamp() {
        return transaction_impl_->GetCommitTimestamp();
    }
    void Ack(Table* t, 
             const std::string& row_key, 
             const std::string& column_family, 
             const std::string& qualifier) {
        transaction_impl_->Ack(t, row_key, column_family, qualifier);
    }
    void Notify(Table* t,
                const std::string& row_key, 
                const std::string& column_family, 
                const std::string& qualifier) {
        transaction_impl_->Notify(t, row_key, column_family, qualifier);
    }
    void SetIsolation(const IsolationLevel& isolation_level) {
        transaction_impl_->SetIsolation(isolation_level);
    }
    IsolationLevel Isolation() {
        return transaction_impl_->Isolation();
    }
    void SetTimeout(int64_t timeout_ms) {
        transaction_impl_->SetTimeout(timeout_ms);
    }
    std::shared_ptr<T> GetTransactionPtr() {
        return transaction_impl_;
    }
private:
    std::shared_ptr<T> transaction_impl_;

};
} // namespace tera

#endif // TERA_SDK_TRANSACTION_WRAPPER_H_