// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "common/thread_pool.h"

#include "sdk/read_impl.h"
#include "sdk/single_row_txn.h"
#include "sdk/table_impl.h"
#include "sdk/multi_row_txn.h"

namespace tera {

Transaction* NewTransaction() {
    return MultiRowTxnSync::NewMultiRowTxnSync();
}

Transaction* MultiRowTxnSync::NewMultiRowTxnSync() {
    // int64_t start_ts = TimeOracle::GetTimestamp();
    int64_t start_ts = 42;
    if (start_ts > 0) {
        return new MultiRowTxnSync(start_ts);
    } else {
        return NULL;
    }
}

MultiRowTxnSync::MultiRowTxnSync(int64_t start_ts)
   : start_ts_(start_ts) {}

MultiRowTxnSync::~MultiRowTxnSync() {}

std::string LockColumnName(const std::string& c) {
    return c + "__l__"; // lock
}

std::string WriteColumnName(const std::string& c) {
    return c + "__w__"; // write
}

bool MultiRowTxnSync::IsWritingByOthers(RowMutation* row_mu, RowReader* reader) {
    return false;
}

bool MultiRowTxnSync::IsLockedByOthers(RowMutation* row_mu, RowReader* reader) {
    return false;
}

ErrorCode MultiRowTxnSync::Prewrite(RowMutation* w, RowMutation* primary) {
    ErrorCode status;
    return status;
}

bool MultiRowTxnSync::LockExists(tera::Transaction* single_row_txn, RowMutation* row_mu) {
    return false;
}

ErrorCode MultiRowTxnSync::Commit() {
    assert(writes_.size() > 0);

    ErrorCode status;
    return status;
}

void MultiRowTxnSync::ApplyMutation(RowMutation* row_mu) {
    assert(row_mu != NULL);
    writes_.push_back(row_mu);
}

ErrorCode MultiRowTxnSync::Get(RowReader* row_reader) {
    assert(row_reader != NULL);

    ErrorCode status;
    return status;
}

} // namespace tera

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
