// Copyright (c) 2015-2017, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef TERA_OBSERVER_EXECUTOR_NOTIFY_CELL_H_
#define TERA_OBSERVER_EXECUTOR_TNOTIFY_CELL_H_

#include <set>
#include <map>
#include <vector>
#include <memory>

#include "gflags/gflags.h"
#include "glog/logging.h"

#include "observer/executor/observer.h"
#include "observer/rowlocknode/fake_rowlock_client.h"
#include "sdk/rowlock_client.h"
#include "tera.h"

DECLARE_string(rowlock_server_port);
DECLARE_string(rowlock_server_ip);
DECLARE_bool(mock_rowlock_enable);


namespace tera {
namespace observer {

struct Column {
    std::string table_name;
    std::string family;
    std::string qualifier;

    bool operator<(const Column& other) const {
        int32_t result = 0;
        result = table_name.compare(other.table_name);
        if (result != 0) {
            return result < 0;
        }
        result = family.compare(other.family);
        if (result != 0) {
            return result < 0;
        }
        result = qualifier.compare(other.qualifier);

        return result < 0;
    }

    bool operator==(const Column& other) const {
        return table_name == other.table_name && family == other.family
               && qualifier == other.qualifier;
    }
};

struct AutoRowUnlocker {
    AutoRowUnlocker(const std::string& table, 
                    const std::string& unlock_row)
        : table_name(table),
          row(unlock_row) {}
    AutoRowUnlocker() {}

    ~AutoRowUnlocker() {
        // UnLockRow

        if (FLAGS_mock_rowlock_enable == true) {
            client.reset(new FakeRowlockClient());
        } else {
            client.reset(new RowlockClient());
        }

        RowlockRequest request;
        RowlockResponse response;

        request.set_row(row);
        request.set_table_name(table_name);

        client->UnLock(&request, &response);    
        VLOG(12) <<"[time] Transaction finish. [row] " << row;
    }

    std::unique_ptr<RowlockClient> client;
    std::string table_name;  
    std::string row;  
};

// info inside scanner
struct NotifyCell {
    NotifyCell(tera::Transaction* t) : transaction(t), 
                                       table(NULL) {}
    ~NotifyCell() {
        if (transaction) {
            delete transaction;
        }
    }

    std::string row;
    std::string value;
    int64_t timestamp;
    
    Column observed_column;
    tera::Transaction* transaction;
    tera::Table* table;

    std::shared_ptr<AutoRowUnlocker> unlocker;
};

} // namespace observer
} // namespace tera

#endif  // TERA_OBSERVER_EXECUTOR_NOTIFY_CELL_H_
