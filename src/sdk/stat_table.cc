// Copyright (c) 2018, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
#include <iostream>
#include <gflags/gflags.h>

#include "sdk/stat_table.h"
#include "utils/tprinter.h"

DECLARE_bool(tera_stat_table_enabled);
DECLARE_int64(tera_stat_table_ttl);
DECLARE_int64(tera_stat_table_splitsize);
DECLARE_string(tera_auth_policy);

namespace tera {
namespace sdk {

StatTable::StatTable(ThreadPool* thread_pool, std::shared_ptr<auth::AccessBuilder> access_builder,
                     const StatTableCustomer& c, const std::string& local_addr)
    : created_(false),
      opened_(false),
      local_addr_(local_addr),
      customer_type_(c),
      thread_pool_(thread_pool),
      access_builder_(access_builder) {
  if (!access_builder_) {
    access_builder_.reset(new auth::AccessBuilder(FLAGS_tera_auth_policy));
    access_builder_->Login(auth::kInternalGroup, "", nullptr);
  }
}

void StatTable::SelectTabletsFailMessages(const std::vector<std::string>& filters, bool is_detail) {
  CorruptPhase phase = CorruptPhase::kUnknown;
  if (filters[0] == "Load") {
    phase = CorruptPhase::kLoading;
  } else if (filters[0] == "Comp") {
    phase = CorruptPhase::kCompacting;
  }
  const std::string& time_range = filters[4];
  int64_t start_ts = kOldestTs, end_ts = kLatestTs;
  std::size_t found = time_range.find(",");
  if (found != std::string::npos) {
    start_ts = get_timestamp_from_str(time_range.substr(0, found));
    end_ts = get_timestamp_from_str(time_range.substr(found + 1, time_range.size() - 1));
    if (start_ts != 0 && end_ts != 0) {
      start_ts *= 1000000;
      end_ts *= 1000000;
    }
  }
  SelectTabletsFailMessages(phase, filters[1], filters[2], filters[3], start_ts, end_ts, is_detail);
}

void StatTable::SelectTabletsFailMessages(const CorruptPhase& phase, const std::string& ts_addr,
                                          const std::string& tablename, const std::string& tablet,
                                          int64_t start_ts, int64_t end_ts, bool is_detail) {
  ErrorCode error_code;

  tera::ScanDescriptor scan_desc("!");
  scan_desc.SetEnd("\"");
  scan_desc.AddColumn("tsinfo", "corrupt");
  scan_desc.SetMaxVersions(1);
  scan_desc.SetTimeRange(end_ts, start_ts);

  tera::TPrinter printer;
  tera::TPrinter::PrintOpt printer_opt;
  printer_opt.print_head = true;
  int cols = 6;
  int row_cnt = 0;
  if (is_detail) {
    printer.Reset(cols, " ", "tablet", "server_addr", "time", "phase", "detail_msg");
  }
  std::vector<std::string> row;
  tera::ResultStream* scanner = stat_table_->Scan(scan_desc, &error_code);
  for (scanner->LookUp("!"); !scanner->Done(); scanner->Next()) {
    tera::TabletCorruptMessage corrupt_msg;
    DeserializeCorrupt(scanner->Value(), &corrupt_msg);
    int64_t record_time = scanner->Timestamp() / 1000 / 1000;
    if ((ts_addr != "" && corrupt_msg.tabletnode() != ts_addr) ||
        (tablet != "" && corrupt_msg.tablet() != tablet) ||
        (tablename != "" && corrupt_msg.tablet().find(tablename + "/") == std::string::npos)) {
      continue;
    }
    if (phase != CorruptPhase::kUnknown &&
        static_cast<CorruptPhase>(corrupt_msg.corrupt_phase()) != phase) {
      continue;
    }
    std::string corrupt_phase =
        static_cast<CorruptPhase>(corrupt_msg.corrupt_phase()) == CorruptPhase::kLoading ? "Load"
                                                                                         : "Comp";
    row.clear();
    row.push_back(std::to_string(row_cnt++));
    row.push_back(corrupt_msg.tablet());
    row.push_back(corrupt_msg.tabletnode());
    row.push_back(get_time_str(record_time));
    row.push_back(corrupt_phase);
    row.push_back(corrupt_msg.detail_message());
    if (is_detail) {
      printer.AddRow(row);
    }
  }
  if (is_detail) {
    printer.Print(printer_opt);
  } else {
    std::cout << "corruption tablet count :" << row_cnt << std::endl;
  }
  delete scanner;
}

void StatTable::RecordTabletCorrupt(const std::string& tablet, const std::string& corrupt_msg) {
  if (!opened_) {
    LOG(WARNING) << "stat_table not opened";
    return;
  }
  std::string key = "!" + tablet;
  RowMutation* mutation = stat_table_->NewRowMutation(key);
  mutation->Put("tsinfo", "corrupt", corrupt_msg);
  mutation->SetCallBack(&RecordStatTableCallBack);
  stat_table_->ApplyMutation(mutation);
}

void StatTable::ErasureTabletCorrupt(const std::string& tablet) {
  if (!opened_) {
    LOG(WARNING) << "stat_table not opened";
    return;
  }
  std::string key = "!" + tablet;
  RowMutation* mutation = stat_table_->NewRowMutation(key);
  mutation->DeleteRow(-1);
  mutation->SetCallBack(&RecordStatTableCallBack);
  stat_table_->ApplyMutation(mutation);
}

std::string StatTable::SerializeLoadContext(const LoadTabletRequest& request,
                                            const std::string& tabletnode_session_id) {
  tera::TabletLoadContext load_ctx;
  std::string load_context_str;
  LoadTabletRequest* req = load_ctx.mutable_load_request();
  req->CopyFrom(request);
  load_ctx.set_tabletnode_session_id(tabletnode_session_id);
  load_ctx.SerializeToString(&load_context_str);
  return load_context_str;
}

std::string StatTable::SerializeCorrupt(CorruptPhase phase, const std::string& tabletnode,
                                        const std::string& tablet,
                                        const std::string& load_context_str,
                                        const std::string& msg) {
  tera::TabletCorruptMessage corrupt_msg;
  std::string corrupt_msg_str;
  corrupt_msg.set_tablet(tablet);
  corrupt_msg.set_tabletnode(tabletnode);
  corrupt_msg.set_corrupt_phase(static_cast<int>(phase));
  corrupt_msg.set_corrupt_type(0);
  corrupt_msg.set_locality_group("");
  corrupt_msg.set_detail_message(msg);
  corrupt_msg.set_load_context(load_context_str);
  corrupt_msg.SerializeToString(&corrupt_msg_str);
  return corrupt_msg_str;
}

void StatTable::DeserializeCorrupt(const std::string& corrupt_msg_str,
                                   tera::TabletCorruptMessage* corrupt_msg) {
  corrupt_msg->ParseFromString(corrupt_msg_str);
}

bool StatTable::CreateStatTable() {
  master::MasterClient master_client(local_addr_);
  CreateTableRequest request;
  CreateTableResponse response;
  request.set_sequence_id(0);
  request.set_table_name(kStatTableName);
  access_builder_->BuildInternalGroupRequest(&request);
  TableSchema* schema = request.mutable_schema();
  schema->set_name(kStatTableName);
  schema->set_raw_key(Binary);
  schema->set_split_size(FLAGS_tera_stat_table_splitsize);
  LocalityGroupSchema* lg = schema->add_locality_groups();
  lg->set_name("lg0");
  lg->set_store_type(FlashStore);
  lg->set_id(0);
  ColumnFamilySchema* cf = schema->add_column_families();
  cf->set_name("tsinfo");
  cf->set_time_to_live(FLAGS_tera_stat_table_ttl);
  cf->set_locality_group("lg0");
  master_client.CreateTable(&request, &response);
  switch (response.status()) {
    case kMasterOk:
      return true;
    case kTableExist:
      return true;
    default:
      return false;
  }
}

bool StatTable::OpenStatTable() {
  MutexLock locker(&mutex_);
  // ts will not access stat_table
  if (customer_type_ == StatTableCustomer::kTabletNode || opened_) {
    return true;
  }
  if (customer_type_ == StatTableCustomer::kMaster && !created_) {
    created_ = CreateStatTable();
    if (!created_) {
      return false;
    }
  }
  ErrorCode err;
  stat_table_.reset(new TableImpl(kStatTableName, thread_pool_, std::shared_ptr<ClientImpl>()));
  if (stat_table_->OpenInternal({}, &err)) {
    opened_ = true;
    return true;
  } else {
    opened_ = false;
    stat_table_.reset();
    LOG(ERROR) << "fail to open stat_table.";
  }
  return false;
}

void StatTable::RecordStatTableCallBack(RowMutation* mutation) {
  const ErrorCode& error_code = mutation->GetError();
  if (error_code.GetType() != ErrorCode::kOK) {
    LOG(WARNING) << "dump stat exception occured, reason:" << error_code.GetReason();
  } else {
    LOG(INFO) << "dump stat success:" << mutation->RowKey();
  }
  delete mutation;
}

}  // namespace sdk
}  // namespace tera
