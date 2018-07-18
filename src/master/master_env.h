// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#pragma once

#include <memory>
#include <deque>
#include <vector>
#include "master/availability.h"
#include "common/counter.h"
#include "common/thread_pool.h"
#include "master/master_impl.h"
#include "master/tablet_manager.h"
#include "master/tabletnode_manager.h"
#include "master/workload_scheduler.h"
#include "master/procedure_executor.h"
#include "types.h"

namespace tera {
namespace master {

class TabletNodeManager;
class TabletManager;
class TabletAvailability;
class Scheduler;
class SizeScheduler;
class LoadScheduler;
class MasterImpl; 

class MetaTask;

typedef std::function<void (bool)> UpdateMetaClosure;

struct MetaWriteRecord {
    MetaWriteRecord() {};
    MetaWriteRecord(std::string key_, std::string value_, bool is_delete_) : 
        key(key_), value(value_), is_delete(is_delete_) {}
    std::string key;
    std::string value;
    bool is_delete;
};

inline MetaWriteRecord PackMetaWriteRecord(TablePtr table, bool is_delete) {
    std::string key, value;
    table->ToMetaTableKeyValue(&key, &value);
    return MetaWriteRecord(key, value, is_delete);
}
inline MetaWriteRecord PackMetaWriteRecord(TabletPtr tablet, bool is_delete) {
    std::string key, value;
    tablet->ToMetaTableKeyValue(&key, &value);
    return MetaWriteRecord(key, value, is_delete);
}
inline void PackMetaWriteRecords(TablePtr table, bool is_delete, std::vector<MetaWriteRecord>& records) {
    records.emplace_back(PackMetaWriteRecord(table, is_delete));
}
inline void PackMetaWriteRecords(TabletPtr tablet, bool is_delete, std::vector<MetaWriteRecord>& records) {
    records.emplace_back(PackMetaWriteRecord(tablet, is_delete));
}

class TeraMasterEnv{
public:
    TeraMasterEnv() : master_(nullptr) {}
    void Init(MasterImpl* master,
            std::shared_ptr<TabletNodeManager> tabletnode_manager, 
            std::shared_ptr<TabletManager> tablet_manager, 
            std::shared_ptr<Scheduler> size_scheduler, 
            std::shared_ptr<Scheduler> load_scheduler,
            std::shared_ptr<ThreadPool> thread_pool, 
            std::shared_ptr<ProcedureExecutor> executor, 
            std::shared_ptr<TabletAvailability> tablet_availability,
            std::shared_ptr<tera::sdk::StatTable> stat_table) {
        master_ = master;
        tabletnode_manager_ = tabletnode_manager;
        tablet_manager_ = tablet_manager;
        size_scheduler_ = size_scheduler;
        load_scheduler_ = load_scheduler;
        thread_pool_ = thread_pool;
        executor_ = executor;
        tablet_availability_ = tablet_availability;
        stat_table_ = stat_table;
    }
    
    MasterImpl* GetMaster() {
        return master_;
    }

    std::shared_ptr<TabletNodeManager>& GetTabletNodeManager() {
        return tabletnode_manager_;
    }
    std::shared_ptr<TabletManager>& GetTabletManager() {
        return tablet_manager_;
    }
    std::shared_ptr<Scheduler>& GetSizeScheduler() {
        return size_scheduler_;
    }
    std::shared_ptr<Scheduler>& GetLoadScheduler() {
        return load_scheduler_;
    }

    std::shared_ptr<ThreadPool>& GetThreadPool() {
        return thread_pool_;
    }
    
    std::shared_ptr<ProcedureExecutor> GetExecutor() {
        return executor_;
    }

    std::shared_ptr<TabletAvailability> GetTabletAvailability() {
        return tablet_availability_;
    }

    std::shared_ptr<tera::sdk::StatTable> GetStatTable() {
        return stat_table_;
    }

    static Counter& SequenceId() {
        return sequence_id_;
    }

    typedef std::function<void (QueryRequest*, QueryResponse*, bool, int)> QueryClosure;
    typedef std::function<void (UpdateRequest*, UpdateResponse*, bool, int)> UpdateClosure;
    typedef std::function<void (LoadTabletRequest*, LoadTabletResponse*, bool, int)> LoadClosure;
    typedef std::function<void (UnloadTabletRequest*, UnloadTabletResponse*, bool, int)> UnloadClosure;
    typedef std::function<void (SplitTabletRequest*, SplitTabletResponse*, bool, int)> SplitClosure;
    typedef std::function<void (WriteTabletRequest*, WriteTabletResponse*, bool, int)> WriteClosure;
    typedef std::function<void (ScanTabletRequest*, ScanTabletResponse*, bool, int)> ScanClosure;

    static void BatchWriteMetaTableAsync(MetaWriteRecord record, UpdateMetaClosure done, int32_t left_try_times = -1);
    static void BatchWriteMetaTableAsync(std::vector<MetaWriteRecord> meta_entries, UpdateMetaClosure done, int32_t left_try_times = -1);
    
    static void UpdateMetaCallback(std::vector<MetaWriteRecord> records, 
            UpdateMetaClosure done, 
            int32_t left_try_times,
            WriteTabletRequest* request, 
            WriteTabletResponse* response, bool failed, int error_code);

    static void ScanMetaTableAsync(const std::string& table_name,
                            const std::string& tablet_key_start,
                            const std::string& tablet_key_end,
                            ScanClosure done);

    static void SuspendMetaOperation(MetaWriteRecord record, UpdateMetaClosure done, int32_t left_try_times);
    static void SuspendMetaOperation(std::vector<MetaWriteRecord> meta_entries, UpdateMetaClosure done, int32_t left_try_times);
    
    static void SuspendScanMetaOperation(const std::string& table_name, 
            const std::string& tablet_start_key, 
            const std::string& tablet_end_key, 
            ScanClosure done);

    static void PushToMetaPendingQueue(MetaTask* task);

    static void ResumeMetaOperation();

private:
    MasterImpl* master_;
    std::shared_ptr<TabletNodeManager> tabletnode_manager_;
    std::shared_ptr<TabletManager> tablet_manager_;
    std::shared_ptr<Scheduler> size_scheduler_;
    std::shared_ptr<Scheduler> load_scheduler_;
    std::shared_ptr<ThreadPool> thread_pool_;
    std::shared_ptr<ProcedureExecutor> executor_;
    std::shared_ptr<TabletAvailability> tablet_availability_;
    std::shared_ptr<tera::sdk::StatTable> stat_table_;
    static std::mutex meta_task_mutex_;
    static std::queue<MetaTask*> meta_task_queue_;
    static Counter sequence_id_;
};

inline TeraMasterEnv& MasterEnv() {
    static TeraMasterEnv master_env;
    return master_env;
}

}
}

