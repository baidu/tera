// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "common/thread_pool.h"

#include "sdk/read_impl.h"
#include "sdk/single_row_txn.h"
#include "sdk/table_impl.h"
#include "sdk/txn.h"

namespace tera {

Transaction* NewTransaction() {
    return TxnSyncImpl::NewTxnSyncImpl();
}

Transaction* TxnSyncImpl::NewTxnSyncImpl() {
    // int64_t start_ts = TimeOracle::GetTimestamp();
    int64_t start_ts = 42;
    if (start_ts > 0) {
        return new TxnSyncImpl(start_ts);
    } else {
        return NULL;
    }
}

TxnSyncImpl::TxnSyncImpl(int64_t start_ts)
   : start_ts_(start_ts) {}

TxnSyncImpl::~TxnSyncImpl() {}

std::string LockColumnName(const std::string& c) {
    return c + "__l__"; // lock
}

std::string WriteColumnName(const std::string& c) {
    return c + "__w__"; // write
}

bool TxnSyncImpl::IsWritingByOthers(RowMutation* row_mu, RowReader* reader) {
    return false;
}

bool TxnSyncImpl::IsLockedByOthers(RowMutation* row_mu, RowReader* reader) {
    return false;
}

ErrorCode TxnSyncImpl::Prewrite(RowMutation* w, RowMutation* primary) {
    ErrorCode status;
    return status;
}

bool TxnSyncImpl::LockExists(tera::Transaction* single_row_txn, RowMutation* row_mu) {
    return false;
}

ErrorCode TxnSyncImpl::Commit() {
    assert(writes_.size() > 0);

    ErrorCode status;
    return status;
}

void TxnSyncImpl::ApplyMutation(RowMutation* row_mu) {
    assert(row_mu != NULL);
    writes_.push_back(row_mu);
}

void TxnSyncImpl::Get(RowReader* row_reader) {
    assert(row_reader != NULL);
}

} // namespace tera

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
