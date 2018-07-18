// Copyright (c) 2015-2017, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "observer/executor/notification_impl.h"

#include <glog/logging.h>

#include "common/timer.h"
#include "common/base/string_number.h"
#include "sdk/global_txn_internal.h"
#include "types.h"

namespace tera {
namespace observer {

Notification* GetNotification(const std::shared_ptr<NotifyCell>& notify_cell) {
    return new NotificationImpl(notify_cell);
}

NotificationImpl::NotificationImpl(const std::shared_ptr<NotifyCell>& notify_cell)
    : notify_cell_(notify_cell),
    start_timestamp_(get_micros()),
    notify_timestamp_(0) {}

void NotificationImpl::Ack(Table* t,
                           const std::string& row_key,
                           const std::string& column_family,
                           const std::string& qualifier) {
    if (notify_cell_->notify_transaction != NULL) {
        notify_cell_->notify_transaction->Ack(t, row_key, column_family, qualifier);
        return;
    }

    // kNoneTransaction
    tera::RowMutation* mutation = t->NewRowMutation(row_key);
    std::string notify_qulifier = PackNotifyName(column_family, qualifier);
    mutation->DeleteColumns(kNotifyColumnFamily, notify_qulifier, start_timestamp_);
    t->ApplyMutation(mutation);
    delete mutation;
}

void NotificationImpl::Notify(Table* t,
                              const std::string& row_key,
                              const std::string& column_family,
                              const std::string& qualifier) {
    if (notify_cell_->notify_transaction != NULL) {
        notify_cell_->notify_transaction->Notify(t, row_key, column_family, qualifier);
        return;
    }

    // kNoneTransaction
    if (notify_timestamp_ == 0) {
        notify_timestamp_ = get_micros();
    }

    tera::ErrorCode err;
    std::string notify_qulifier = PackNotifyName(column_family, qualifier);
    t->Put(row_key, kNotifyColumnFamily, notify_qulifier, NumberToString(notify_timestamp_), notify_timestamp_, &err);
    if (err.GetType() != tera::ErrorCode::kOK) {
        LOG(ERROR) << "Notify error. table: " << t->GetName() << " row "
            << row_key << " pos: " << column_family << ":" << qualifier;
    }
}

void NotificationImpl::Done() {
    delete this;
}

} // namespace observer
} // namespace tera
