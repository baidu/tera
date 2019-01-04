// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef TERA_MASTER_TABLET_MANAGER_H_
#define TERA_MASTER_TABLET_MANAGER_H_

#include <atomic>
#include <iostream>
#include <limits>
#include <list>
#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <stdint.h>
#include <string>
#include <vector>
#include <glog/logging.h>
#include "common/mutex.h"
#include "common/thread_pool.h"
#include "master/tabletnode_manager.h"
#include "master/tablet_state_machine.h"
#include "master/table_state_machine.h"
#include "common/metric/metric_counter.h"
#include "proto/master_rpc.pb.h"
#include "proto/table_meta.pb.h"
#include "proto/tabletnode_rpc.pb.h"
#include "common/counter.h"
#include "utils/fragment.h"

using namespace std::placeholders;

namespace tera {
class UpdateTableResponse;
namespace master {

class TabletNode;
class MasterZkAdapterBase;
typedef std::shared_ptr<TabletNode> TabletNodePtr;

struct TabletFile {
  uint64_t tablet_id;
  uint32_t lg_id;
  uint64_t file_id;

  bool operator<(const TabletFile& f) const {
    return tablet_id < f.tablet_id ||
           (tablet_id == f.tablet_id &&
            (lg_id < f.lg_id || (lg_id == f.lg_id && file_id < f.file_id)));
  }

  bool operator==(const TabletFile& f) const {
    return tablet_id == f.tablet_id && lg_id == f.lg_id && file_id == f.file_id;
  }
};

struct InheritedFileInfo {
  uint32_t ref;
  InheritedFileInfo() : ref(0) {}
};

class MasterImpl;
class Table;
typedef std::shared_ptr<Table> TablePtr;

class Tablet {
  friend class TabletManager;
  friend class Table;
  friend std::ostream& operator<<(std::ostream& o, const Tablet& tablet);

 public:
  Tablet() = delete;
  Tablet(const Tablet&) = delete;
  Tablet& operator=(const Tablet&) = delete;
  Tablet(const TabletMeta& meta, TablePtr table);
  virtual ~Tablet();

  void ToMeta(TabletMeta* meta);
  const std::string& GetTableName();
  const std::string& GetServerAddr();
  std::string GetServerId();
  const std::string& GetPath();
  int64_t GetDataSize();
  void GetDataSize(int64_t* size, std::vector<int64_t>* lg_size);
  int64_t GetDataSizeOnFlash();
  void SetDataSizeOnFlash(int64_t size);
  int64_t GetQps();
  int64_t GetReadQps();
  int64_t GetWriteQps();
  int64_t GetScanQps();
  int64_t GetLRead();
  int64_t CreateTime() const { return create_time_; }
  uint64_t IncVersion();
  uint64_t Version();

  const std::string& GetKeyStart();
  const std::string& GetKeyEnd();
  const KeyRange& GetKeyRange();
  const TableSchema& GetSchema();
  const TabletCounter& GetCounter();
  const TabletCounter& GetAverageCounter();
  TabletMeta::TabletStatus GetStatus();
  CompactStatus GetCompactStatus();
  TablePtr GetTable();
  bool IsBusy();
  std::string DebugString();

  void UpdateSize(const TabletMeta& meta);
  bool HasFlashLg();

  void SetCounter(const TabletCounter& counter);
  void SetCompactStatus(CompactStatus compact_status);
  void SetStatus(const TabletMeta::TabletStatus status);

  TableStatus GetTableStatus();

  bool Verify(const std::string& table_name, const std::string& key_start,
              const std::string& key_end, const std::string& path, const std::string& server_addr,
              StatusCode* ret_status = NULL);

  void ToMetaTableKeyValue(std::string* packed_key = NULL, std::string* packed_value = NULL);
  bool GetSchemaIsSyncing();

  int64_t UpdateTime();
  int64_t SetUpdateTime(int64_t timestamp);
  int64_t ReadyTime();
  int64_t LastMoveTime() const;
  void SetLastMoveTime(int64_t time);

  bool TestAndSetSplitTimeStamp(int64_t ts);

  void AssignTabletNode(TabletNodePtr node);
  TabletNodePtr GetTabletNode() { return node_; }

  virtual bool DoStateTransition(const TabletEvent event);
  bool DoStateTransitionUnSafe(const TabletEvent event);

  // Will set a flag to ignore lost file error when tabletserver load tablet.
  // We should set specific locality_groups that avoid missing some of the
  // exceptions in others locality_groups.
  bool HasErrorIgnoredLGs() const;
  void GetErrorIgnoredLGs(std::vector<std::string>* lgs);
  bool SetErrorIgnoredLGs(const std::string& lg_list_str = "");

  bool LockTransition() {
    MutexLock lock(&mutex_);
    if (in_transition_ == true) {
      return false;
    }
    in_transition_ = true;
    return true;
  }

  void UnlockTransition() {
    MutexLock lock(&mutex_);
    in_transition_ = false;
  }
  bool InTransition() {
    MutexLock lock(&mutex_);
    return in_transition_;
  }

  int LoadFailedCount() { return load_fail_cnt_; }
  int IncLoadFailedCount() { return ++load_fail_cnt_; }
  void ClearLoadFailedCount() { load_fail_cnt_ = 0; }

 protected:
  explicit Tablet(const TabletMeta& meta);  // only used by subclass
  mutable Mutex mutex_;
  TabletMeta meta_;
  TabletStateMachine state_machine_;

 private:
  TabletNodePtr node_;
  TablePtr table_;
  int64_t update_time_;
  int64_t last_move_time_us_;
  int64_t data_size_on_flash_;
  std::string server_id_;

  std::vector<std::string> ignore_err_lgs_;  // lg array for ignore_err_
  std::list<TabletCounter> counter_list_;
  TabletCounter average_counter_;
  struct TabletAccumulateCounter {
    uint64_t low_read_cell;
    uint64_t scan_rows;
    uint64_t scan_kvs;
    uint64_t scan_size;
    uint64_t read_rows;
    uint64_t read_kvs;
    uint64_t read_size;
    uint64_t write_rows;
    uint64_t write_kvs;
    uint64_t write_size;

    TabletAccumulateCounter() { memset(this, 0, sizeof(TabletAccumulateCounter)); }
  } accumu_counter_;
  void* merge_param_;

  // Tablet Split History Tracing
  struct TabletSplitHistory {
    int64_t last_split_ts;

    TabletSplitHistory() : last_split_ts(0) {}
  } split_history_;

  bool in_transition_ = false;

  // protected by Table::mutex_
  bool gc_reported_;
  std::multiset<TabletFile> inh_files_;

  // sucessive load failed count, will be cleared on tablet load succeed
  std::atomic<int> load_fail_cnt_;
  const int64_t create_time_;
};

class UnknownTablet : public Tablet {
 public:
  explicit UnknownTablet(const TabletMeta& meta) : Tablet(meta) {}
  bool DoStateTransition(const TabletEvent);
};

class StaleTablet : public UnknownTablet {
 public:
  explicit StaleTablet(const TabletMeta& meta) : UnknownTablet(meta) {
    MutexLock lock(&mutex_);
    meta_.set_status(TabletMeta::kTabletReady);
    state_machine_.SetStatus(TabletMeta::kTabletReady);
  }
};

class MetaTablet : public Tablet {
 public:
  MetaTablet(const TabletMeta& meta, TablePtr table,
             std::shared_ptr<MasterZkAdapterBase> zk_adapter);
  // MetaTablet overwrite virtual function DoStateTransition() as it should
  // update
  // MetaTablet's address to zk/nexus when it's status switched to kTableReady
  bool DoStateTransition(const TabletEvent event);
  bool UpdateRootTabletAddr();

 private:
  std::shared_ptr<MasterZkAdapterBase> zk_adapter_;
};

typedef class std::shared_ptr<Tablet> TabletPtr;
typedef class std::shared_ptr<MetaTablet> MetaTabletPtr;
std::ostream& operator<<(std::ostream& o, const TabletPtr& tablet);
std::ostream& operator<<(std::ostream& o, const TablePtr& table);

class Table : public std::enable_shared_from_this<Table> {
  class TableMetric {
   public:
    TableMetric(const std::string& name)
        : table_name_(name),
          tablet_num_("tera_master_tablet_num", GetTableNameLabel(), {SubscriberType::LATEST},
                      false),
          not_ready_("tera_master_tablet_not_ready_num", GetTableNameLabel(),
                     {SubscriberType::LATEST}, false),
          table_size_("tera_master_table_size", GetTableNameLabel(), {SubscriberType::LATEST},
                      false),
          corrupt_num_("tera_master_table_corrupt", GetTableNameLabel(), {SubscriberType::LATEST},
                       false) {}

    void SetTabletNum(int64_t tablet_num) { tablet_num_.Set(tablet_num); }

    void SetNotReady(int64_t not_ready) { not_ready_.Set(not_ready); }

    void SetTableSize(int64_t table_size) { table_size_.Set(table_size); }

    void SetCorruptNum(int64_t corrupt_num) { corrupt_num_.Set(corrupt_num); }

   private:
    std::string GetTableNameLabel() { return "table:" + table_name_; }

    const std::string table_name_;
    tera::MetricCounter tablet_num_;
    tera::MetricCounter not_ready_;
    tera::MetricCounter table_size_;
    tera::MetricCounter corrupt_num_;
  };

  friend class Tablet;
  friend class TabletManager;
  friend std::ostream& operator<<(std::ostream& o, const Table& tablet);

 public:
  Table(const std::string& table_name, const TableSchema& schema, const TableStatus status);
  Table(const std::string& table_name, const TableMeta& meta);

  TabletPtr AddTablet(const TabletMeta& meta, StatusCode* ret_code);

  bool FindTablet(const std::string& key_start, TabletPtr* tablet);
  void FindTablet(const std::string& server_addr, std::vector<TabletPtr>* tablet_meta_list);

  bool FindOverlappedTablets(const std::string& key_start, const std::string& key_end,
                             std::vector<TabletPtr>* tablets);

  void GetTablet(std::vector<TabletPtr>* tablet_meta_list);
  const std::string& GetTableName();
  TableStatus GetStatus();

  const TableSchema& GetSchema();
  void SetSchema(const TableSchema& schema);
  const TableCounter& GetCounter();
  void AddDeleteTabletCount();
  bool NeedDelete();
  void ToMetaTableKeyValue(std::string* packed_key = NULL, std::string* packed_value = NULL);
  void ToMeta(TableMeta* meta);
  uint64_t GetNextTabletNo();
  void RefreshCounter();
  int64_t GetTabletsCount();
  void GetTsAddrTabletsCount(std::map<std::string, int64_t>* ts_addr_tablets_count);

  bool GetSchemaIsSyncing();
  void SetSchemaIsSyncing(bool flag);
  bool GetSchemaSyncLock();
  void ClearSchemaSyncLock();
  void SetOldSchema(TableSchema* schema);
  bool GetOldSchema(TableSchema* schema);
  void ClearOldSchema();
  bool PrepareUpdate(const TableSchema& schema);
  void AbortUpdate();
  void CommitUpdate();

  bool TryCollectInheritedFile();
  bool GetTabletsForGc(std::set<uint64_t>* live_tablets, std::set<uint64_t>* dead_tablets,
                       bool ignore_not_ready);
  bool CollectInheritedFileFromFilesystem(const std::string& tablename, uint64_t tablet_num,
                                          std::vector<TabletFile>* tablet_files);
  void MergeTablets(TabletPtr first_tablet, TabletPtr second_tablet, const TabletMeta& merged_meta,
                    TabletPtr* merged_tablet);
  void SplitTablet(TabletPtr splited_tablet, const TabletMeta& first_half,
                   const TabletMeta& second_half, TabletPtr* first_tablet,
                   TabletPtr* second_tablet);
  void GarbageCollect(const TabletInheritedFileInfo& tablet_inh_info);
  void EnableDeadTabletGarbageCollect(uint64_t tablet_id);
  void ReleaseInheritedFile(const TabletFile& file);
  void AddInheritedFile(const TabletFile& file, bool need_ref);
  void AddEmptyDeadTablet(uint64_t tablet_id);
  uint64_t CleanObsoleteFile();

  bool LockTransition() {
    MutexLock lock(&mutex_);
    if (in_transition_ == true) {
      return false;
    }
    in_transition_ = true;
    return true;
  }

  void UnlockTransition() {
    MutexLock lock(&mutex_);

    in_transition_ = false;
  }
  bool InTransition() {
    MutexLock lock(&mutex_);
    return in_transition_;
  }

  bool DoStateTransition(const TableEvent event);

  int64_t CreateTime() const { return create_time_; }

 private:
  Table(const Table&) = delete;
  Table& operator=(const Table&) = delete;
  typedef std::map<std::string, TabletPtr> TabletList;
  TabletList tablets_list_;
  mutable Mutex mutex_;
  std::string name_;
  TableSchema schema_;
  std::vector<uint64_t> snapshot_list_;
  std::vector<std::string> rollback_names_;
  uint32_t deleted_tablet_num_;
  uint64_t max_tablet_no_;
  const int64_t create_time_;
  TableCounter counter_;
  TableMetric metric_;
  bool schema_is_syncing_;  // is schema syncing to all ts(all tablets)
  TableSchema* old_schema_;

  // map from dead tablet's ID to its inherited files set
  typedef std::map<uint64_t, std::map<TabletFile, InheritedFileInfo> > InheritedFiles;
  InheritedFiles useful_inh_files_;
  std::queue<TabletFile> obsolete_inh_files_;
  // If there is any live tablet hasn't reported since a tablet died,
  // this dead tablet cannot GC.
  std::set<uint64_t> gc_disabled_dead_tablets_;
  uint32_t reported_live_tablets_num_;  // realtime live tablets num, which
                                        // already reported

  TableStateMachine state_machine_;
  bool in_transition_ = false;
};

class TabletManager {
 public:
  static TablePtr CreateTable(const TableMeta& meta);

  static TablePtr CreateTable(const std::string& table_name, const TableSchema& schema,
                              const TableStatus& status);

  static void PackTabletMeta(
      TabletMeta* meta, const std::string& table_name, const std::string& key_start = "",
      const std::string& key_end = "", const std::string& path = "",
      const std::string& server_addr = "",
      const TabletMeta::TabletStatus& table_status = TabletMeta::kTabletOffline,
      int64_t data_size = 0, uint64_t version = 1);

  typedef std::function<bool(const std::string&, StatusCode*)> FindCondCallback;

  TabletManager(Counter* sequence_id, MasterImpl* master_impl, ThreadPool* thread_pool);
  ~TabletManager();

  void Init();
  void Stop();

  bool DumpMetaTable(const std::string& addr, StatusCode* ret_status = NULL);
  bool ClearMetaTable(const std::string& addr, StatusCode* ret_status = NULL);

  bool DumpMetaTableToFile(const std::string& filename, StatusCode* ret_status = NULL);

  bool AddTable(TablePtr& table, StatusCode* ret_status);

  MetaTabletPtr AddMetaTablet(TabletNodePtr node, std::shared_ptr<MasterZkAdapterBase> zk_adapter);

  bool DeleteTable(const std::string& table_name, StatusCode* ret_status = NULL);

  bool DeleteTablet(const std::string& table_name, const std::string& key_start,
                    StatusCode* ret_status = NULL);

  bool FindTablet(const std::string& table_name, const std::string& key_start, TabletPtr* tablet,
                  StatusCode* ret_status = NULL);

  void FindTablet(const std::string& server_addr, std::vector<TabletPtr>* tablet_meta_list,
                  bool need_disabled_tables);

  bool FindTable(const std::string& table_name, std::vector<TabletPtr>* tablet_meta_list,
                 StatusCode* ret_status = NULL);

  bool SearchTablet(const std::string& table_name, const std::string& key, TabletPtr* tablet,
                    StatusCode* ret_status);

  bool FindTable(const std::string& table_name, TablePtr* table);

  int64_t SearchTable(std::vector<TabletPtr>* tablet_meta_list,
                      const std::string& prefix_table_name,
                      const std::string& start_table_name = "",
                      const std::string& start_tablet_key = "",
                      uint32_t max_found = std::numeric_limits<unsigned int>::max(),
                      StatusCode* ret_status = NULL);

  bool ShowTable(std::vector<TablePtr>* table_meta_list, std::vector<TabletPtr>* tablet_meta_list,
                 const std::string& start_table_name = "", const std::string& start_tablet_key = "",
                 uint32_t max_table_found = std::numeric_limits<unsigned int>::max(),
                 uint32_t max_tablet_found = std::numeric_limits<unsigned int>::max(),
                 bool* is_more = NULL, StatusCode* ret_status = NULL);

  bool GetMetaTabletAddr(std::string* addr);

  void ClearTableList();

  double OfflineTabletRatio();

  bool PickMergeTablet(TabletPtr& tablet, TabletPtr* tablet2);

  void LoadTableMeta(const std::string& key, const std::string& value);
  void LoadTabletMeta(const std::string& key, const std::string& value);

  int64_t GetAllTabletsCount();

 private:
  void WriteToStream(std::ofstream& ofs, const std::string& key, const std::string& value);

 private:
  typedef std::map<std::string, TablePtr> TableList;
  TableList all_tables_;
  mutable Mutex mutex_;
  Counter* this_sequence_id_;
  MasterImpl* master_impl_;
  MetaTabletPtr meta_tablet_;
  ThreadPool* thread_pool_;
};

int64_t CounterWeightedSum(int64_t a1, int64_t a2);

}  // namespace master
}  // namespace tera

#endif  // TERA_MASTER_TABLET_MANAGER_H_
