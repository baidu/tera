// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include <glog/logging.h>
#include "master/master_impl.h"
#include "master/master_env.h"
#include "master/tabletnode_manager.h"
#include "master/tablet_manager.h"
#include "proto/master_rpc.pb.h"
#include "proto/table_meta.pb.h"
#include "proto/tabletnode_client.h"
#include "proto/kv_helper.h"
#include "sdk/client_impl.h"
#include "sdk/table_impl.h"
#include "utils/string_util.h"

DECLARE_string(tera_master_meta_table_name);

using namespace std::placeholders;
namespace tera {
namespace master {

enum MetaTaskType { kWrite = 0, kScan, kRepair };
struct MetaTask {
  MetaTaskType type_;
};
struct WriteTask {
  MetaTaskType type_;
  UpdateMetaClosure done_;
  std::vector<MetaWriteRecord> meta_entries_;
  int32_t left_try_times_;
};
struct ScanTask {
  MetaTaskType type_;
  TeraMasterEnv::ScanClosure done_;
  std::string table_name_;
  std::string tablet_key_start_;
  std::string tablet_key_end_;
};

std::mutex TeraMasterEnv::meta_task_mutex_;
std::queue<MetaTask*> TeraMasterEnv::meta_task_queue_;
Counter TeraMasterEnv::sequence_id_;

void TeraMasterEnv::BatchWriteMetaTableAsync(MetaWriteRecord record, UpdateMetaClosure done,
                                             int32_t left_try_times) {
  std::vector<MetaWriteRecord> meta_entries;
  meta_entries.push_back(record);
  BatchWriteMetaTableAsync(meta_entries, done, left_try_times);
}

void TeraMasterEnv::BatchWriteMetaTableAsync(std::vector<MetaWriteRecord> meta_entries,
                                             UpdateMetaClosure done, int32_t left_try_times) {
  std::string meta_addr;
  if (!MasterEnv().GetTabletManager()->GetMetaTabletAddr(&meta_addr)) {
    SuspendMetaOperation(meta_entries, done, left_try_times);
    return;
  }

  WriteTabletRequest* request = new WriteTabletRequest;
  WriteTabletResponse* response = new WriteTabletResponse;
  request->set_sequence_id(SequenceId().Inc());
  request->set_tablet_name(FLAGS_tera_master_meta_table_name);
  request->set_is_sync(true);
  request->set_is_instant(true);
  MasterEnv().GetAccessBuilder()->BuildInternalGroupRequest(request);
  for (size_t i = 0; i < meta_entries.size(); ++i) {
    std::string packed_key = meta_entries[i].key;
    std::string packed_value = meta_entries[i].value;
    bool is_delete = meta_entries[i].is_delete;
    // bool is_delete = meta_entries[i](&packed_key, &packed_value);
    RowMutationSequence* mu_seq = request->add_row_list();
    mu_seq->set_row_key(packed_key);
    Mutation* mutation = mu_seq->add_mutation_sequence();
    if (!is_delete) {
      mutation->set_type(kPut);
      mutation->set_value(packed_value);
    } else {
      mutation->set_type(kDeleteRow);
    }
  }
  if (request->row_list_size() == 0) {
    delete request;
    delete response;
    return;
  } else {
    LOG(INFO) << "WriteMetaTableAsync id: " << request->sequence_id();
  }

  WriteClosure meta_done = std::bind(TeraMasterEnv::UpdateMetaCallback, meta_entries, done,
                                     left_try_times, _1, _2, _3, _4);

  tabletnode::TabletNodeClient meta_node_client(MasterEnv().GetThreadPool().get(), meta_addr);
  meta_node_client.WriteTablet(request, response, meta_done);
}

void TeraMasterEnv::UpdateMetaCallback(std::vector<MetaWriteRecord> records, UpdateMetaClosure done,
                                       int32_t left_try_times, WriteTabletRequest* request,
                                       WriteTabletResponse* response, bool failed, int error_code) {
  StatusCode status = response->status();
  if (!failed && status == kTabletNodeOk) {
    // all the row status should be the same
    CHECK_GT(response->row_status_list_size(), 0);
    status = response->row_status_list(0);
  }
  std::unique_ptr<WriteTabletRequest> request_holder(request);
  std::unique_ptr<WriteTabletResponse> response_holder(response);
  if (failed || status != kTabletNodeOk) {
    std::string errmsg =
        failed ? sofa::pbrpc::RpcErrorCodeToString(error_code) : StatusCodeToString(status);
    LOG(ERROR) << "fail to update meta tablet: error_msg: " << errmsg << ", will retry later";
    for (auto it = records.begin(); it != records.end(); ++it) {
      std::string op = (it->is_delete ? "DEL" : "PUT");
      LOG(WARNING) << "update meta records suspended and retry later, "
                   << "OP: " << op << ", key: " << DebugString(it->key)
                   << ", value: " << DebugString(it->value);
    }
    if (left_try_times == 0) {
      done(false);
      return;
    }
    left_try_times = left_try_times > 0 ? left_try_times - 1 : left_try_times;
    MasterEnv().SuspendMetaOperation(records, done, left_try_times);
    return;
  }
  for (auto it = records.begin(); it != records.end(); ++it) {
    std::string op = (it->is_delete ? "DEL" : "PUT");
    LOG(INFO) << "update meta tablet succ, "
              << "OP: " << op << ", key: " << DebugString(it->key)
              << ", value: " << DebugString(it->value);
  }
  done(true);
}

void TeraMasterEnv::ScanMetaTableAsync(const std::string& table_name,
                                       const std::string& tablet_key_start,
                                       const std::string& tablet_key_end, ScanClosure done) {
  std::string meta_addr;
  if (MasterEnv().GetTabletManager()->GetMetaTabletAddr(&meta_addr)) {
    SuspendScanMetaOperation(table_name, tablet_key_start, tablet_key_end, done);
    return;
  }

  ScanTabletRequest* request = new ScanTabletRequest;
  ScanTabletResponse* response = new ScanTabletResponse;
  request->set_sequence_id(SequenceId().Inc());
  request->set_table_name(FLAGS_tera_master_meta_table_name);
  std::string scan_key_start, scan_key_end;
  MetaTableScanRange(table_name, tablet_key_start, tablet_key_end, &scan_key_start, &scan_key_end);
  request->set_start(scan_key_start);
  request->set_end(scan_key_end);

  MasterEnv().GetAccessBuilder()->BuildInternalGroupRequest(request);

  LOG(INFO) << "ScanMetaTableAsync id: " << request->sequence_id() << ", "
            << "table: " << table_name << ", range: [" << DebugString(tablet_key_start) << ", "
            << DebugString(tablet_key_end);
  tabletnode::TabletNodeClient meta_node_client(MasterEnv().GetThreadPool().get(), meta_addr);
  meta_node_client.ScanTablet(request, response, done);
}

void TeraMasterEnv::SuspendScanMetaOperation(const std::string& table_name,
                                             const std::string& tablet_key_start,
                                             const std::string& tablet_key_end, ScanClosure done) {
  ScanTask* task = new ScanTask;
  task->type_ = kScan;
  task->done_ = done;
  task->table_name_ = table_name;
  task->tablet_key_start_ = tablet_key_start;
  task->tablet_key_end_ = tablet_key_end;
  PushToMetaPendingQueue((MetaTask*)task);
}

void TeraMasterEnv::SuspendMetaOperation(MetaWriteRecord record, UpdateMetaClosure done,
                                         int32_t left_try_times) {
  std::vector<MetaWriteRecord> meta_entries;
  meta_entries.push_back(record);
  SuspendMetaOperation(meta_entries, done, left_try_times);
}

void TeraMasterEnv::SuspendMetaOperation(std::vector<MetaWriteRecord> meta_entries,
                                         UpdateMetaClosure done, int32_t left_try_times) {
  WriteTask* task = new WriteTask;
  task->type_ = kWrite;
  task->done_ = done;
  task->meta_entries_ = meta_entries;
  task->left_try_times_ = left_try_times;
  PushToMetaPendingQueue((MetaTask*)task);
}

void TeraMasterEnv::PushToMetaPendingQueue(MetaTask* task) {
  std::lock_guard<std::mutex> lock(meta_task_mutex_);
  meta_task_queue_.push(task);
  if (meta_task_queue_.size() == 1) {
    TabletPtr meta_tablet;
    MasterEnv().GetTabletManager()->FindTablet(FLAGS_tera_master_meta_table_name, "", &meta_tablet);
    MasterEnv().GetMaster()->TryMoveTablet(meta_tablet);
  }
}

void TeraMasterEnv::ResumeMetaOperation() {
  meta_task_mutex_.lock();
  while (!meta_task_queue_.empty()) {
    MetaTask* task = meta_task_queue_.front();
    if (task->type_ == kWrite) {
      WriteTask* write_task = (WriteTask*)task;
      BatchWriteMetaTableAsync(write_task->meta_entries_, write_task->done_,
                               write_task->left_try_times_);
      delete write_task;
    } else if (task->type_ == kScan) {
      ScanTask* scan_task = (ScanTask*)task;
      ScanMetaTableAsync(scan_task->table_name_, scan_task->tablet_key_start_,
                         scan_task->tablet_key_end_, scan_task->done_);
      delete scan_task;
    }
    meta_task_queue_.pop();
  }
  meta_task_mutex_.unlock();
}
}
}
