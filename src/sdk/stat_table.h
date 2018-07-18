// Copyright (c) 2015-2018, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
#pragma once

#include <atomic>
#include <string>
#include <vector>
#include <queue>
#include <memory>
#include "common/thread_pool.h"
#include "proto/master_client.h"
#include "proto/stat_table.pb.h"
#include "proto/table_meta.pb.h"
#include "sdk/table_impl.h"
#include "tera.h"
#include "types.h"

namespace tera {
namespace sdk {

enum class StatTableCustomer {
    kMaster = 0,
    kTabletNode = 1,
    kClient = 2,
};

enum class CorruptPhase {
    kLoading = 0,
    kCompacting = 1,
};

class StatTable {
public:
    enum class CorruptType {
        kUnknown = 0,
        kSst = 1,
        kCurrent = 2,
        kManifest = 3,
        kLoadlock = 4,
    };
    // master and ts need set custmer explicit
    StatTable(ThreadPool* thread_pool,
              const StatTableCustomer& c = StatTableCustomer::kClient,
              const std::string& local_addr = "");
    // default select all fail msg 
    // set args to limit ts/tablet/timerange
    void SelectTabletsFailMessages(const std::string& ts_addr = "", 
                                   const std::string& tablet = "",
                                   int64_t start_ts = kOldestTs, 
                                   int64_t end_ts = kLatestTs);
    // record by tabletserver
    void RecordTabletCorrupt(const std::string& tablet,
                             const std::string& fail_msg);
    
    void ErasureTabletCorrupt(const std::string& tablet);
     
    static std::string SerializeLoadContext(const LoadTabletRequest& request,
                                            const std::string& tabletnode_session_id);

    static std::string SerializeCorrupt(CorruptPhase phase,
                                        const std::string& tabletnode,
                                        const std::string& tablet,
                                        const std::string& context_str, 
                                        const std::string& msg);

    void DeserializeCorrupt(const string& corrupt_str,
                            tera::TabletCorruptMessage* corrupt_msg);
   
    bool OpenStatTable();
    
private:
    bool CreateStatTable();
    static void RecordStatTableCallBack(RowMutation* mutation);
private:
    std::shared_ptr<TableImpl> stat_table_;
    std::atomic<bool> created_;
    std::atomic<bool> opened_;
    std::string local_addr_; 
    StatTableCustomer customer_type_;
    mutable Mutex mutex_;
    ThreadPool* thread_pool_;
};
}  // namespace sdk
}  // namespace tera
