// Copyright (c) 2015-2017, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef TERA_OBSERVER_EXECUTOR_NOTIFICATION_H_
#define TERA_OBSERVER_EXECUTOR_NOTIFICATION_H_

#include <string>

#include "tera.h"

#pragma GCC visibility push(default)

namespace tera {
namespace observer {

class Notification {
public:
    virtual ~Notification() {}

    virtual void Ack(Table* t,
                     const std::string& row_key,
                     const std::string& column_family,
                     const std::string& qualifier) = 0;

    virtual void Notify(Table* t,
                        const std::string& row_key,
                        const std::string& column_family,
                        const std::string& qualifier) = 0;

    // relases resource after OnNotify finished
    // and delete this
    virtual void Done() = 0;
};

} // namespace observer
} // namespace tera

#pragma GCC visibility pop

#endif  // TERA_OBSERVER_EXECUTOR_NOTIFICATION_H_
