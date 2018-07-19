// Copyright (c) 2015-2017, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef TERA_OBSERVER_EXECUTOR_NOTIFICATION_IMPL_H_
#define TERA_OBSERVER_EXECUTOR_NOTIFICATION_IMPL_H_

#include <memory>
#include <string>
#include <vector>
#include "observer/executor/notify_cell.h"

#include "observer/executor/notification.h"
#include "tera.h"

namespace tera {
namespace observer {

Notification* GetNotification(const std::shared_ptr<NotifyCell>& notify_cell);

class NotificationImpl : public Notification {
public:
    explicit NotificationImpl(const std::shared_ptr<NotifyCell>& notify_cell);
    virtual ~NotificationImpl() {}

    virtual void Ack(Table* t,
                     const std::string& row_key,
                     const std::string& column_family,
                     const std::string& qualifier);

    virtual void Notify(Table* t,
                        const std::string& row_key,
                        const std::string& column_family,
                        const std::string& qualifier);

    virtual void Done();

private:
    std::shared_ptr<NotifyCell> notify_cell_;
    int64_t start_timestamp_;
    int64_t notify_timestamp_;
};

} // namespace observer
} // namespace tera

#endif  // TERA_OBSERVER_EXECUTOR_NOTIFICATION_IMPL_H_
