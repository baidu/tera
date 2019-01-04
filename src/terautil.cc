// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#include <fstream>
#include <iomanip>
#include <iostream>
#include <limits>
#include <map>
#include <memory>
#include <sstream>
#include <vector>

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/io/zero_copy_stream_impl_lite.h>

#include "ins_sdk.h"

#include "common/base/string_ext.h"
#include "common/base/string_number.h"
#include "common/console/progress_bar.h"
#include "common/file/file_path.h"
#include "common/log/log_cleaner.h"
#include "common/semaphore.h"
#include "common/func_scope_guard.h"
#include "common/thread_pool.h"
#include "leveldb/dfs.h"
#include "util/nfs.h"
#include "util/hdfs.h"
#include "io/coding.h"
#include "proto/kv_helper.h"
#include "proto/proto_helper.h"
#include "proto/table_meta.pb.h"
#include "proto/tabletnode.pb.h"
#include "proto/tabletnode_client.h"
#include "sdk/client_impl.h"
#include "sdk/cookie.h"
#include "sdk/mutate_impl.h"
#include "sdk/sdk_utils.h"
#include "sdk/sdk_zk.h"
#include "sdk/table_impl.h"
#include "tera.h"
#include "types.h"
#include "utils/config_utils.h"
#include "utils/crypt.h"
#include "utils/schema_utils.h"
#include "utils/string_util.h"
#include "utils/tprinter.h"
#include "utils/utils_cmd.h"
#include "version.h"

DECLARE_string(flagfile);
DECLARE_string(log_dir);
DECLARE_string(tera_master_meta_table_name);
DECLARE_bool(tera_info_log_clean_enable);

// dfs
DECLARE_string(tera_leveldb_env_type);
DECLARE_string(tera_leveldb_env_dfs_type);
DECLARE_string(tera_leveldb_env_nfs_mountpoint);
DECLARE_string(tera_leveldb_env_nfs_conf_path);
DECLARE_string(tera_leveldb_env_hdfs2_nameservice_list);
DECLARE_string(tera_dfs_so_path);
DECLARE_string(tera_dfs_conf);

DEFINE_string(dump_tera_src_conf, "../conf/src_tera.flag", "src cluster for tera");
DEFINE_string(dump_tera_dest_conf, "../conf/dest_tera.flag", "dest cluster for tera");
DEFINE_string(dump_tera_src_root_path, "/xxx_", "src tera root path");
DEFINE_string(dump_tera_dest_root_path, "/xxx_", "dest tera root path");
DEFINE_string(ins_cluster_addr, "terautil_ins", "terautil dump ins cluster conf");
DEFINE_string(ins_cluster_dump_root_path, "/terautil/dump/xxxx", "dump meta ins");
DEFINE_string(ins_cluster_diff_root_path, "/terautil/diff", "diff meta ins");
DEFINE_string(dump_tera_src_meta_addr, "", "src addr for meta_table");
DEFINE_string(dump_tera_dest_meta_addr, "", "dest addr for meta_table");
DEFINE_int64(dump_manual_split_interval, 1000, "manual split interval in ms");
DEFINE_bool(dump_enable_manual_split, false, "manual split may take a long time, so disable it");
DEFINE_int64(dump_concurrent_limit, 5000, "the qps limit of unit job to dump");
DEFINE_int64(dump_startime, 0, "the start time of dump");
DEFINE_int64(dump_endtime, 0, "the end time of dump");
DEFINE_string(tables_map_file, "",
              "tables_map_file to store the src table_name to dest table_name");
DEFINE_string(diff_tables_map_file, "",
              "tables_map_file for diff to store the src table_name to dest table_name");

DEFINE_string(dump_failed_kv_afs_path, "/user/tera/terautil", "afs path for dumping failed kv");
DEFINE_bool(enable_dump_failed_kv, false, "enable dump failed kv to afs");
DEFINE_string(dump_ut_kv_afs_path, "/user/tera/terautil_dump_ut", "dfs dir for dump ut");
DEFINE_string(diff_data_afs_path, "/user/tera/diff",
              "path for storing diff data for checking diff");
DEFINE_string(diff_bin_data_afs_path, "/user/tera/diffbin",
              "path for storing diff bin data for rewriting to dest claster");
DEFINE_string(dump_tables_map_path, "tables_map", "tables_map path");
DEFINE_string(dump_tables_lg_map_path, "tables_lg_map", "tables_lg_map path");
DEFINE_string(dump_tables_cf_map_path, "tables_cf_map", "tables_cf_map path");
DEFINE_string(dump_tables_cf_version_map_path, "tables_cf_version_map", "tables_cf_map path");
DEFINE_string(lg_and_cf_delimiter, "|", "lg & cf delimiter");
DEFINE_int64(pb_total_bytes_limit_MB, 1024, "pb_total_bytes_limit_MB");
DEFINE_int64(pb_warning_threshold_MB, 256, "pb_warning_threshold_MB");
DEFINE_int64(rewrite_retry_times, 5, "rewrite retry times");
DEFINE_int64(diff_scan_interval_ns, 1, "scan interval(ns)");
DEFINE_int64(diff_scan_count_per_interval, 1000, "scan count per interval");
DEFINE_bool(readable, true, "readable input");
DEFINE_bool(enable_copy_schema, true, "enable copy schema from src cluster to dest cluster");
DEFINE_bool(enable_write_dfs_diff_only_in_src, true, "enable write dfs file");
DEFINE_bool(enable_write_dfs_diff_only_in_dest, true, "enable write dfs file");
DEFINE_bool(enable_write_dfs_diff_both_have_but_diff, true, "enable write dfs file");
DEFINE_bool(enable_write_dfs_diffbin_only_in_src, true, "enable write dfs file");
DEFINE_bool(enable_write_diff_only_in_src_to_dest, false,
            "enable write diff only_in_src data to dest");
DEFINE_int64(write_only_in_src_to_dest_concurrent_limit, 500, "the qps limit of unit job to write");

using namespace tera;

typedef std::pair<std::string, TableMeta> TablePair;
typedef std::pair<std::string, TabletMeta> TabletPair;

namespace {

common::Semaphore* g_sem;
leveldb::Dfs* g_dfs = NULL;
}

struct Progress {
  int finish_range_num;
  int total_range_num;
};

struct DstTableCf {
  std::string dst_table_name;
  std::vector<std::string> cf_list;
};

struct DiffStatData {
  unsigned long only_in_src;
  unsigned long only_in_dest;
  unsigned long both_have_but_diff;
  unsigned long both_have_and_same;  // 交集
  unsigned long in_src_or_in_dest;   // 并集

  void reset() {
    only_in_src = 0;
    only_in_dest = 0;
    both_have_but_diff = 0;
    both_have_and_same = 0;
    in_src_or_in_dest = 0;
  };
};

const char* terautil_builtin_cmds[] = {
    "dump",
    "dump <operation>                                                           \n\
            prepare_safe                                                        \n\
            prepare                                                             \n\
            load                                                                \n\
                load a --tables_map_file to nexus                               \n\
            prepare_tables                                                      \n\
                dump src_tables to dest_tables according tables_map             \n\
            run                                                                 \n\
                running dump specify by tables_map in nexus                     \n\
            rewrite                                                             \n\
                rewrite failed kv_pairs stored in afs to dest tera              \n\
            progress                                                            \n\
                show the dump job completing status                             \n\
            clean                                                               \n\
                clean nexus & afs useless data in dump root dir",
    "diff",
    "diff <operation>                                                           \n\
            prepare                                                             \n\
                generate some dicts and put one to ins for diff                 \n\
            run                                                                 \n\
                run the diff job                                                \n\
            progress                                                            \n\
                show the diff job completing status                             \n\
            result                                                              \n\
                stat the diff result and show the result                        \n\
            clean                                                               \n\
                clean nexus & afs useless data in diff root dir",
    "help",
    "help [cmd]                                                                 \n\
          show manual for a or all cmd(s)",

    "version",
    "version                                                                    \n\
             show version info",
};

static void ShowCmdHelpInfo(const char* msg) {
  if (msg == NULL) {
    return;
  }
  int count = sizeof(terautil_builtin_cmds) / sizeof(char*);
  for (int i = 0; i < count; i += 2) {
    if (strncmp(msg, terautil_builtin_cmds[i], 32) == 0) {
      std::cout << terautil_builtin_cmds[i + 1] << std::endl;
      return;
    }
  }
}

static void ShowAllCmd() {
  std::cout << "there is cmd list:" << std::endl;
  int count = sizeof(terautil_builtin_cmds) / sizeof(char*);
  bool newline = false;
  for (int i = 0; i < count; i += 2) {
    std::cout << std::setiosflags(std::ios::left) << std::setw(20) << terautil_builtin_cmds[i];
    if (newline) {
      std::cout << std::endl;
      newline = false;
    } else {
      newline = true;
    }
  }
  std::cout << std::endl
            << "help [cmd] for details." << std::endl;
}

int32_t HelpOp(int32_t argc, char** argv) {
  if (argc == 2) {
    ShowAllCmd();
  } else if (argc == 3) {
    ShowCmdHelpInfo(argv[2]);
  } else {
    ShowCmdHelpInfo("help");
  }
  return 0;
}

int32_t InitDfsClient() {
  if (g_dfs != NULL) {
    return 0;
  }
  if (FLAGS_tera_leveldb_env_dfs_type == "nfs") {
    if (access(FLAGS_tera_leveldb_env_nfs_conf_path.c_str(), R_OK) == 0) {
      LOG(INFO) << "init nfs system: use configure file" << FLAGS_tera_leveldb_env_nfs_conf_path;
      leveldb::Nfs::Init(FLAGS_tera_leveldb_env_nfs_mountpoint,
                         FLAGS_tera_leveldb_env_nfs_conf_path);
      g_dfs = leveldb::Nfs::GetInstance();
    } else {
      LOG(INFO) << "init nfs system: no configure file found";
      return -1;
    }
  } else if (FLAGS_tera_leveldb_env_dfs_type == "hdfs2") {
    LOG(INFO) << "hdfs2 system support currently, please use hadoop-client";
    g_dfs = new leveldb::Hdfs2(FLAGS_tera_leveldb_env_hdfs2_nameservice_list);
  } else if (FLAGS_tera_leveldb_env_dfs_type == "hdfs") {
    g_dfs = new leveldb::Hdfs();
  } else {
    LOG(INFO) << "init dfs system: " << FLAGS_tera_dfs_so_path << "(" << FLAGS_tera_dfs_conf << ")";
    g_dfs = leveldb::Dfs::NewDfs(FLAGS_tera_dfs_so_path, FLAGS_tera_dfs_conf);
  }
  return 0;
}

int PutMapInNexus(const std::string& path, galaxy::ins::sdk::InsSDK* ins_sdk,
                  const std::map<std::string, std::string>& tables_map) {
  galaxy::ins::sdk::SDKError ins_err;
  for (auto it = tables_map.cbegin(); it != tables_map.cend(); ++it) {
    std::string key = path + "/" + it->first;
    std::string value = it->second;
    if (!ins_sdk->Put(key, value, &ins_err)) {
      LOG(WARNING) << "ins put: key[" << key << "], value[" << value << "], error " << ins_err;
      return -1;
    }
  }
  return 0;
}

std::string InitInsValueFirstPartForDump(const std::string& tablet_id) {
  return "0," + tablet_id + ":";
}

std::string InitInsValueFirstPartForDiff(const std::string& tablet_id) {
  // 一共6个数字：
  // 位置0: 是否已经比较完了，0:没比较完，1:比较完了
  // 位置1: 只在原表中
  // 位置2: 只在新表中
  // 位置3: 原表新表都在，但是不同
  // 位置4: 相同的(交集)
  // 位置5: 并集
  // 位置6：tablet_id字符串
  std::string value_prefix = "0,0,0,0,0,0," + tablet_id + ":";
  return value_prefix;
}

int DumpRange(const std::string& ins_cluster_root_path, galaxy::ins::sdk::InsSDK* ins_sdk,
              const tera::TableMetaList& table_list, const tera::TabletMetaList& tablet_list,
              std::function<std::string(const std::string&)> init_ins_value_first_part) {
  int res = 0;
  galaxy::ins::sdk::SDKError ins_err;
  std::string table_path = ins_cluster_root_path + "/table";
  std::string tablet_path = ins_cluster_root_path + "/tablet";

  for (int32_t i = 0; i < table_list.meta_size(); i++) {
    const tera::TableMeta& meta = table_list.meta(i);
    if (meta.table_name() == FLAGS_tera_master_meta_table_name) {
      continue;
    }
    std::string key = table_path + "/" + meta.table_name();
    if (!ins_sdk->Put(key, meta.table_name(), &ins_err)) {
      LOG(WARNING) << "ins put: " << key << ", error " << ins_err;
      return -1;
    }
  }

  for (int32_t i = 0; i < tablet_list.meta_size(); i++) {
    const tera::TabletMeta& meta = tablet_list.meta(i);
    if (meta.table_name() == FLAGS_tera_master_meta_table_name) {
      continue;
    }
    std::string key_start = meta.key_range().key_start();
    std::string key_end = meta.key_range().key_end();

    std::string debug_key_start;
    std::string debug_key_end;
    if (FLAGS_readable) {
      debug_key_start = DebugString(key_start);
      debug_key_end = DebugString(key_end);
      if (debug_key_start != key_start || debug_key_end != key_end) {
        LOG(INFO) << "debug_key_start[" << debug_key_start << "] <=> key_start[" << key_start
                  << "], debug_key_end[" << debug_key_end << "] <=> key_end[" << key_end << "]";
      }
    } else {
      debug_key_start = key_start;
      debug_key_end = key_end;
    }

    std::string table_name = meta.table_name();

    const std::string& tablet_id_path = meta.path();
    std::size_t pos = tablet_id_path.find('/');
    CHECK(pos != std::string::npos);
    std::string tablet_id = tablet_id_path.substr(pos + 1);
    CHECK(tablet_id.length() > 0);

    std::string key = tablet_path + "/" + table_name + "/" + debug_key_start;
    std::string val = init_ins_value_first_part(tablet_id);
    val.append(debug_key_end);
    if (!ins_sdk->Put(key, val, &ins_err)) {
      LOG(WARNING) << "ins put: " << key << ", error " << ins_err;
      return -1;
    }
  }
  return res;
}

int ScanAndDumpMeta(const std::string& src_meta_tablet_addr,
                    const std::string& dest_meta_tablet_addr, tera::TableMetaList* table_list,
                    tera::TabletMetaList* tablet_list) {
  uint64_t seq_id = 0;
  tera::ScanTabletRequest request;
  tera::ScanTabletResponse response;
  tera::WriteTabletRequest write_request;
  tera::WriteTabletResponse write_response;
  uint64_t request_size = 0;
  write_request.set_sequence_id(seq_id++);
  write_request.set_tablet_name(FLAGS_tera_master_meta_table_name);
  write_request.set_is_sync(true);
  write_request.set_is_instant(true);

  request.set_sequence_id(seq_id++);
  request.set_table_name(FLAGS_tera_master_meta_table_name);
  request.set_start("");
  request.set_end("");
  common::ThreadPool thread_pool(2);
  tera::tabletnode::TabletNodeClient src_meta_node_client(&thread_pool, src_meta_tablet_addr);
  bool success = true;
  while ((success = src_meta_node_client.ScanTablet(&request, &response))) {
    if (response.status() != tera::kTabletNodeOk) {
      LOG(WARNING) << "dump: fail to load meta table: " << StatusCodeToString(response.status());
      return -1;
    }
    int32_t record_size = response.results().key_values_size();
    LOG(INFO) << "scan meta table: " << record_size << " records";

    bool need_dump = false;
    std::string last_record_key;
    for (int32_t i = 0; i < record_size; i++) {
      const tera::KeyValuePair& record = response.results().key_values(i);
      last_record_key = record.key();
      char first_key_char = record.key()[0];

      TableMeta table_meta;
      TabletMeta tablet_meta;
      if (first_key_char == '~') {
        LOG(INFO) << "(user: " << record.key().substr(1) << ")";
      } else if (first_key_char == '|') {
        // user&passwd&role&permission
      } else if (first_key_char == '@') {
        // ParseMetaTableKeyValue(record.key(), record.value(),
        // table_list->add_meta());
        table_meta.Clear();
        ParseMetaTableKeyValue(record.key(), record.value(), &table_meta);

        std::string key, val;
        // table_meta.set_status(kTableDisable);
        table_meta.mutable_schema()->set_merge_size(0);         // never merge during dump
        table_meta.mutable_schema()->set_split_size(10000000);  // never split during dump
        MakeMetaTableKeyValue(table_meta, &key, &val);

        RowMutationSequence* mu_seq = write_request.add_row_list();
        mu_seq->set_row_key(record.key());
        Mutation* mutation = mu_seq->add_mutation_sequence();
        mutation->set_type(tera::kPut);
        mutation->set_value(val);
        request_size += mu_seq->ByteSize();
        if (request_size >= kMaxRpcSize) {  // write req too large,dump into new tera cluster
          need_dump = true;
        }

        TableMeta* table_meta2 = table_list->add_meta();
        table_meta2->CopyFrom(table_meta);
      } else if (first_key_char > '@') {
        // ParseMetaTableKeyValue(record.key(), record.value(),
        // tablet_list->add_meta());
        tablet_meta.Clear();
        ParseMetaTableKeyValue(record.key(), record.value(), &tablet_meta);

        std::string key, val;
        tablet_meta.clear_parent_tablets();
        // tablet_meta.set_status(kTabletDisable);
        MakeMetaTableKeyValue(tablet_meta, &key, &val);

        RowMutationSequence* mu_seq = write_request.add_row_list();
        mu_seq->set_row_key(record.key());
        Mutation* mutation = mu_seq->add_mutation_sequence();
        mutation->set_type(tera::kPut);
        mutation->set_value(val);
        request_size += mu_seq->ByteSize();
        if (request_size >= kMaxRpcSize) {  // write req too large,dump into new tera cluster
          need_dump = true;
        }

        TabletMeta* tablet_meta2 = tablet_list->add_meta();
        tablet_meta2->CopyFrom(tablet_meta);
      } else {
        LOG(WARNING) << "dump: invalid meta record: " << record.key();
      }
    }

    if ((need_dump || record_size <= 0) && write_request.row_list_size() > 0) {
      tabletnode::TabletNodeClient dest_meta_node_client(&thread_pool, dest_meta_tablet_addr);
      if (!dest_meta_node_client.WriteTablet(&write_request, &write_response)) {
        LOG(WARNING) << "dump: fail to dump meta tablet: " << StatusCodeToString(kRPCError);
        return -1;
      }
      tera::StatusCode status = write_response.status();
      if (status == tera::kTabletNodeOk && write_response.row_status_list_size() > 0) {
        status = write_response.row_status_list(0);
      }
      if (status != kTabletNodeOk) {
        LOG(WARNING) << "dump: fail to dump meta tablet: " << StatusCodeToString(status);
        return -1;
      }
      write_request.clear_row_list();
      write_response.Clear();
      request_size = 0;
    }
    if (record_size <= 0) {
      response.Clear();
      LOG(INFO) << "dump: scan meta table success";
      break;
    }

    std::string next_record_key = tera::NextKey(last_record_key);
    request.set_start(next_record_key);
    request.set_end("");
    request.set_sequence_id(seq_id++);
    response.Clear();
  }
  return success ? 0 : -1;
}

int DumpPrepareOp() {
  int res = 0;
  std::string tera_src_conf = FLAGS_dump_tera_src_conf;
  std::string tera_src_root = FLAGS_dump_tera_src_root_path;
  std::string tera_dest_conf = FLAGS_dump_tera_dest_conf;
  std::string tera_dest_root = FLAGS_dump_tera_dest_root_path;

  // read src meta ts addr and dest meta ts addr
  std::string src_meta_addr, dest_meta_addr;
  src_meta_addr = FLAGS_dump_tera_src_meta_addr;
  dest_meta_addr = FLAGS_dump_tera_dest_meta_addr;

  // scan and dump meta
  tera::TableMetaList table_list;
  tera::TabletMetaList tablet_list;

  if ((res = ScanAndDumpMeta(src_meta_addr, dest_meta_addr, &table_list, &tablet_list)) >= 0) {
    // create key range in nexus
    std::string ins_cluster_root_path = FLAGS_ins_cluster_dump_root_path;
    std::string ins_cluster_addr = FLAGS_ins_cluster_addr;
    galaxy::ins::sdk::InsSDK ins_sdk(ins_cluster_addr);
    auto init_ins_value_first_part = std::bind(InitInsValueFirstPartForDump, std::placeholders::_1);
    res = DumpRange(ins_cluster_root_path, &ins_sdk, table_list, tablet_list,
                    init_ins_value_first_part);
  }
  return res;
}

int GetMapFromNexus(const std::string& path, galaxy::ins::sdk::InsSDK* ins_sdk,
                    std::map<std::string, std::string>* nexus_map) {
  int res = 0;
  std::string nexus_start_key = path + "/";
  std::string nexus_end_key = path + "/";
  nexus_start_key.append(1, '\0');
  nexus_end_key.append(1, '\255');
  std::string delimiter("/");
  galaxy::ins::sdk::ScanResult* result = ins_sdk->Scan(nexus_start_key, nexus_end_key);
  while (!result->Done()) {
    if (result->Error() != galaxy::ins::sdk::kOK) {
      LOG(WARNING) << "scan fail: start " << nexus_start_key << ", end " << nexus_end_key
                   << ", err " << result->Error();
      res = -1;
      break;
    }
    std::string key = result->Key();
    std::vector<std::string> nexus_path;
    SplitString(key, delimiter, &nexus_path);
    std::string nexus_key = nexus_path[nexus_path.size() - 1];
    std::string nexus_value = result->Value();

    (*nexus_map)[nexus_key] = nexus_value;
    result->Next();
  }
  delete result;
  return res;
}

std::string GetEndKey(const std::string& val) {
  std::size_t pos = val.find(':');
  CHECK(pos != std::string::npos);
  return val.substr(pos + 1);
}

std::string GetTabletIdForDiff(const std::string& val) {
  std::size_t pos = val.find(':');
  CHECK(pos != std::string::npos);
  std::string prefix = val.substr(0, pos);
  std::vector<std::string> prefix_datas;
  SplitString(prefix, ",", &prefix_datas);
  return prefix_datas[6];
}

std::string GetTabletIdForDump(const std::string& val) {
  std::size_t pos = val.find(':');
  CHECK(pos != std::string::npos);
  std::string prefix = val.substr(0, pos);
  std::vector<std::string> prefix_datas;
  SplitString(prefix, ",", &prefix_datas);
  return prefix_datas[1];
}
int GetAndLockDumpRange(const std::string& ins_cluster_root_path, std::string* table_name,
                        std::string* tablet_id, std::string* start_key, std::string* end_key,
                        galaxy::ins::sdk::InsSDK* ins_sdk,
                        std::function<std::string(const std::string&)> get_tablet_id_func) {
  int res = -1;
  galaxy::ins::sdk::SDKError ins_err;

  std::string tablet_path = ins_cluster_root_path + "/tablet";
  std::string lock_path = ins_cluster_root_path + "/lock";

  std::string start = tablet_path + "/";
  std::string end = tablet_path + "/";
  if (table_name->size()) {
    start.append(*table_name);
    start.append("/");
    start.append(*start_key);
    // the start_key is the last end_key
    if (*start_key == "") {
      // when we finish scaning all ins tablet range, the last end_key is "",
      // and here we restart a new scan loop
      // and going on, untill all tablet range's result is 1 (finish dump or diff)
      start.append(1, '\0');
    }
    // if the last end_key is not "", it means we have not finished one scan loop,
    // so we continue scaning ins tablet range,
    // for speeding up scaning ins, here we set start by last end_key
  }
  end.append(1, '\255');
  galaxy::ins::sdk::ScanResult* result = ins_sdk->Scan(start, end);
  while (!result->Done()) {
    if (result->Error() != galaxy::ins::sdk::kOK) {
      LOG(WARNING) << "scan fail: start " << start << ", end " << end << ", err "
                   << result->Error();
      res = -1;
      break;
    }
    std::string key = result->Key();
    std::string val = result->Value();
    std::string has_done = val.substr(0, 1);
    if (has_done == "1") {  // someone has copy it
      result->Next();
      VLOG(1) << "key = " << key << ", value = " << val << ", has done";
      continue;
    }

    std::string str = key.substr(tablet_path.length() + 1);
    std::size_t pos = str.find('/');
    *table_name = str.substr(0, pos);
    *start_key = str.substr(pos + 1);
    *end_key = GetEndKey(val);
    *tablet_id = get_tablet_id_func(val);

    VLOG(1) << "start_key = " << *start_key << ", end_key = " << *end_key << ", try lock";

    std::string lock_key = lock_path + "/" + *table_name + "/" + *start_key + "/";
    if (!ins_sdk->TryLock(lock_key, &ins_err)) {
      LOG(WARNING) << "ins: TryLock fail: " << lock_key << ", err " << ins_err;
      result->Next();
      continue;
    }

    std::string val1;
    if (ins_sdk->Get(key, &val1, &ins_err)) {
      has_done = val1.substr(0, 1);
    } else {
      LOG(WARNING) << "ins: get fail: " << key << ", err " << ins_err;
      if (!ins_sdk->UnLock(lock_key, &ins_err)) {
        LOG(WARNING) << "ins: unlock fail: " << lock_key << ", err " << ins_err;
      }
      abort();
    }
    if (has_done == "1") {  // someone has copy it
      if (!ins_sdk->UnLock(lock_key, &ins_err)) {
        LOG(WARNING) << "ins: unlock fail: " << lock_key << ", err " << ins_err;
      }
      result->Next();
      continue;
    }

    res = 0;
    break;  // begin to scan
  }
  delete result;
  return res;
}

int ReleaseAndUnlockDumpRange(const std::string& ins_cluster_root_path,
                              const std::string& src_table_name, const std::string& tablet_id,
                              const std::string& start_key, const std::string& end_key,
                              galaxy::ins::sdk::InsSDK* ins_sdk) {
  int res = 0;
  galaxy::ins::sdk::SDKError ins_err;
  // std::string table_path = ins_cluster_root_path + "/table";
  std::string tablet_path = ins_cluster_root_path + "/tablet";
  std::string lock_path = ins_cluster_root_path + "/lock";

  std::string key = tablet_path + "/" + src_table_name + "/" + start_key;
  std::string val = "1," + tablet_id + ":";
  val.append(end_key);

  if (!ins_sdk->Put(key, val, &ins_err)) {
    LOG(WARNING) << "ins put: " << key << ", error " << ins_err;
  }

  std::string lock_key = lock_path + "/" + src_table_name + "/" + start_key + "/";
  if (!ins_sdk->UnLock(lock_key, &ins_err)) {
    LOG(WARNING) << "ins unlock fail: " << lock_key << ", error " << ins_err;
  }
  return res;
}

struct ScanDumpContext {
  virtual ~ScanDumpContext() { row_result.clear_key_values(); }
  Counter counter;
  RowResult row_result;
  Mutex mutex;
  volatile bool fail;
  std::string reason;
};

void ScanAndDumpCallBack(RowMutation* mu) {
  g_sem->Release();
  ScanDumpContext* ctx = (ScanDumpContext*)mu->GetContext();
  if (mu->GetError().GetType() != tera::ErrorCode::kOK) {
    if (ctx->fail == false) {
      ctx->fail = true;
      ctx->reason = mu->GetError().ToString();
    }
    if (FLAGS_enable_dump_failed_kv) {
      MutexLock l(&ctx->mutex);
      RowMutationImpl* mu_impl = dynamic_cast<RowMutationImpl*>(mu);
      for (uint32_t index = 0; index < mu_impl->MutationNum(); ++index) {
        KeyValuePair* kv_pair = ctx->row_result.add_key_values();
        kv_pair->set_key(mu_impl->RowKey());
        kv_pair->set_column_family(mu_impl->GetMutation(index).family);
        kv_pair->set_qualifier(mu_impl->GetMutation(index).qualifier);
        kv_pair->set_value(mu_impl->GetMutation(index).value);
        kv_pair->set_timestamp(mu_impl->GetMutation(index).timestamp);
      }
    }
  }
  delete mu;

  ctx->counter.Dec();
  return;
}

struct RewriteContext {
  Table* target_table;
  KeyValuePair* kv_pair;
  bool hold_kv_pair;
  Counter* counter;
  Counter failed_times;

  RewriteContext() : target_table(NULL), kv_pair(NULL), hold_kv_pair(false), counter(NULL) {}
};

void RewriteCallBack(RowMutation* mu) {
  g_sem->Release();
  RewriteContext* ctx = (RewriteContext*)mu->GetContext();
  if (mu->GetError().GetType() != tera::ErrorCode::kOK) {
    ctx->failed_times.Inc();
    if (ctx->failed_times.Get() <= FLAGS_rewrite_retry_times) {
      // Retry write this mu
      g_sem->Acquire();
      mu->Reset(ctx->kv_pair->key());
      mu->Put(ctx->kv_pair->column_family(), ctx->kv_pair->qualifier(), ctx->kv_pair->value(),
              ctx->kv_pair->timestamp());
      mu->SetContext(ctx);
      mu->SetCallBack(RewriteCallBack);
      ctx->target_table->ApplyMutation(mu);
      return;
    }
    LOG(WARNING) << "failed write key[" << ctx->kv_pair->key() << "], cf["
                 << ctx->kv_pair->column_family() << "], qu[" << ctx->kv_pair->qualifier()
                 << "], value[" << ctx->kv_pair->value() << "], timestamp["
                 << ctx->kv_pair->timestamp() << "], error : " << mu->GetError().ToString()
                 << std::endl;
  }
  delete mu;
  if (ctx->hold_kv_pair) {
    delete ctx->kv_pair;
  }
  ctx->counter->Dec();
  delete ctx;
  return;
}

bool DeserializationRowResult(const char* data, ssize_t data_len, RowResult* row_result) {
  ::google::protobuf::io::ArrayInputStream input(data, data_len);
  ::google::protobuf::io::CodedInputStream decoder(&input);
  decoder.SetTotalBytesLimit(FLAGS_pb_total_bytes_limit_MB * 1024 * 1024,
                             FLAGS_pb_warning_threshold_MB * 1024 * 1024);
  return (row_result->ParseFromCodedStream(&decoder) && decoder.ConsumedEntireMessage());
}

void WriteToDfs(const std::string& file_path, const std::string& write_string) {
  int32_t to_write_len = write_string.length();
  if (to_write_len > 0) {
    leveldb::DfsFile* file = g_dfs->OpenFile(file_path, leveldb::WRONLY);
    int32_t len = file->Write(write_string.c_str(), to_write_len);
    if (len == -1) {
      LOG(WARNING) << "Write afs failed [" << file_path << "]";
    } else if (len != to_write_len) {
      LOG(WARNING) << "Write afs miss some data [" << file_path << "]";
    }
    file->CloseFile();
  } else {
    LOG(WARNING) << "string len for Writing afs is 0, file_path: " << file_path;
  }
  return;
}

bool SerializationRowResult(const RowResult& row_result, std::string* data) {
  ::google::protobuf::io::StringOutputStream output(data);
  ::google::protobuf::io::CodedOutputStream coder(&output);
  return row_result.SerializeToCodedStream(&coder);
}

int ScanAndDumpData(Table* src, Table* dest, const std::string& table_name,
                    const std::string& tablet_id, const std::string& start_key,
                    const std::string& end_key,
                    const std::map<std::string, std::string>& tables_cf_map) {
  int res = 0;
  ErrorCode err;

  std::string raw_start_str;
  std::string raw_end_str;
  if (FLAGS_readable) {
    if (!ParseDebugString(start_key, &raw_start_str) || !ParseDebugString(end_key, &raw_end_str)) {
      LOG(WARNING) << "Parse debug string failed!";
      return -1;
    }
  } else {
    raw_start_str = start_key;
    raw_end_str = end_key;
  }
  VLOG(1) << "Start scan start_key[" << start_key << "], end_key[" << end_key << "]";

  ScanDescriptor desc(raw_start_str);
  desc.SetEnd(raw_end_str);
  desc.SetMaxVersions(std::numeric_limits<int>::max());
  if (FLAGS_dump_endtime != 0) {
    desc.SetTimeRange(FLAGS_dump_endtime, FLAGS_dump_startime);
  }

  ScanDumpContext* ctx = new ScanDumpContext;
  ctx->counter.Set(1);
  ctx->fail = false;

  // Deal with specifing cfs
  std::vector<std::string> cfs;
  auto it = tables_cf_map.find(table_name);
  if (it != tables_cf_map.cend()) {
    std::string delimiter(FLAGS_lg_and_cf_delimiter);
    SplitString(it->second, delimiter, &cfs);
  }
  std::for_each(cfs.cbegin(), cfs.cend(),
                [&desc](const std::string& cf) { desc.AddColumnFamily(cf); });

  ResultStream* result_stream;
  if ((result_stream = src->Scan(desc, &err)) == NULL) {
    LOG(WARNING) << "scan dump fail(new scan): " << table_name << ", start " << start_key
                 << ", end " << end_key;
    delete ctx;
    return -1;
  }
  while (!result_stream->Done(&err)) {
    g_sem->Acquire();
    RowMutation* mu = dest->NewRowMutation(result_stream->RowName());
    mu->Put(result_stream->Family(), result_stream->Qualifier(), result_stream->Value(),
            result_stream->Timestamp());
    ctx->counter.Inc();
    mu->SetContext(ctx);
    mu->SetCallBack(ScanAndDumpCallBack);
    dest->ApplyMutation(mu);

    result_stream->Next();
  }
  delete result_stream;
  ctx->counter.Dec();

  while (ctx->counter.Get() > 0) {
    sleep(3);
  }

  VLOG(1) << "Finish scan start_key[" << start_key << "], end_key[" << end_key << "]";

  if (err.GetType() != tera::ErrorCode::kOK) {
    LOG(WARNING) << "scan dump fail: " << table_name << ", start " << start_key << ", end "
                 << end_key << ", reason " << err.GetReason();
    res = -1;
  }

  if (FLAGS_enable_dump_failed_kv && ctx->fail == true) {
    LOG(WARNING) << "scan dump fail: " << table_name << ", start " << start_key << ", end "
                 << end_key << ", reason " << ctx->reason;
    // Write the RowResult to afs file
    if (ctx->row_result.key_values_size() > 0) {
      std::string row_result_str;
      // ctx->row_result.SerializeToString(&row_result_str);
      if (!SerializationRowResult(ctx->row_result, &row_result_str)) {
        LOG(WARNING) << "row_result serilize failed!";
      } else {
        std::string file_path =
            FLAGS_dump_failed_kv_afs_path + "/" + table_name + "/" + tablet_id + ".pbtxt";
        WriteToDfs(file_path, row_result_str);
      }
    }
  }
  delete ctx;
  return res;
}

int DumpRunOp() {
  int res = 0;
  std::string ins_cluster_addr = FLAGS_ins_cluster_addr;
  std::string ins_cluster_root_path = FLAGS_ins_cluster_dump_root_path;
  std::string tera_src_conf = FLAGS_dump_tera_src_conf;
  std::string tera_dest_conf = FLAGS_dump_tera_dest_conf;

  // get and lock range
  ErrorCode err;
  std::unique_ptr<Client> src_client(Client::NewClient(tera_src_conf, &err));
  if (src_client == nullptr) {
    LOG(WARNING) << "open src client fail: " << tera_src_conf << ", err " << err.ToString();
    return -1;
  }
  std::unique_ptr<Client> dest_client(Client::NewClient(tera_dest_conf, &err));
  if (dest_client == nullptr) {
    LOG(WARNING) << "open dest client fail: " << tera_dest_conf << ", err " << err.ToString();
    return -1;
  }
  std::unique_ptr<Table> src_table;
  std::unique_ptr<Table> dest_table;

  galaxy::ins::sdk::InsSDK ins_sdk(ins_cluster_addr);
  std::string src_table_name, start_key, end_key, last_table_name, tablet_id;

  std::map<std::string, std::string> tables_map;
  std::string path = ins_cluster_root_path + "/" + FLAGS_dump_tables_map_path;
  if (-1 == GetMapFromNexus(path, &ins_sdk, &tables_map)) {
    LOG(WARNING) << "GetMapFromNexus failed in DumpRun";
    return -1;
  }
  for (auto it = tables_map.cbegin(); it != tables_map.cend(); ++it) {
    LOG(INFO) << "src table[" << it->first << "] => dest table[" << it->second << "]";
  }

  std::map<std::string, std::string> tables_cf_map;
  path = ins_cluster_root_path + "/" + FLAGS_dump_tables_cf_map_path;
  if (-1 == GetMapFromNexus(path, &ins_sdk, &tables_cf_map)) {
    LOG(WARNING) << "GetMapFromNexus failed in DumpRun";
    return -1;
  }
  for (auto it = tables_cf_map.cbegin(); it != tables_cf_map.cend(); ++it) {
    LOG(INFO) << "src table[" << it->first << "] => src cf[" << it->second << "]";
  }

  if (-1 == InitDfsClient()) {
    LOG(WARNING) << "init dfs client failed";
    return -1;
  }
  auto get_tablet_id_func = std::bind(GetTabletIdForDump, std::placeholders::_1);
  while (GetAndLockDumpRange(ins_cluster_root_path, &src_table_name, &tablet_id, &start_key,
                             &end_key, &ins_sdk, get_tablet_id_func) == 0) {
    if (last_table_name != src_table_name) {  // table change
      src_table.reset();
      dest_table.reset();
      src_table.reset(src_client->OpenTable(src_table_name, &err));
      if (src_table == nullptr) {
        LOG(WARNING) << "open src table fail: " << src_table_name << ", err " << err.ToString();
        continue;
      }
      std::string dest_table_name = src_table_name;
      if (tables_map.size() != 0) {
        if (tables_map.find(src_table_name) != tables_map.cend()) {
          dest_table_name = tables_map[src_table_name];
        } else {
          LOG(WARNING) << "Couldn't find src_table_name[" << src_table_name << "] in tables_map";
          return -1;
        }
      }
      dest_table.reset(dest_client->OpenTable(dest_table_name, &err));
      if (dest_table == nullptr) {
        src_table.reset();
        LOG(WARNING) << "open dest table fail: " << dest_table_name << ", err " << err.ToString();
        continue;
      }
    }
    last_table_name = src_table_name;

    if ((res = ScanAndDumpData(src_table.get(), dest_table.get(), src_table_name, tablet_id,
                               start_key, end_key, tables_cf_map)) < 0) {
      LOG(WARNING) << "scan dump data fail: " << src_table_name << ", start " << start_key
                   << ", end " << end_key;
    } else {
      VLOG(1) << "Set has_done for start_key[" << start_key << "], end_key[" << end_key << "]";
      ReleaseAndUnlockDumpRange(ins_cluster_root_path, src_table_name, tablet_id, start_key,
                                end_key, &ins_sdk);
    }
    start_key = end_key;
  }
  LOG(INFO) << "Finish DumpRunOp";
  return res;
}

void GetTableKeyRange(const std::string& table_name, const TabletMetaList& tablet_list,
                      std::vector<std::string>* delimiters) {
  for (int32_t i = 0; i < tablet_list.meta_size(); i++) {
    const tera::TabletMeta& meta = tablet_list.meta(i);
    if (table_name == meta.table_name() && meta.key_range().key_start().size() > 0) {
      delimiters->push_back(meta.key_range().key_start());
    }
  }
}

int ManualCreateTable(std::shared_ptr<tera::ClientImpl> client, const std::string& table_name,
                      const TableSchema& schema, const std::vector<std::string>& delimiters) {
  ErrorCode err;
  TableDescriptor table_desc;
  table_desc.SetTableName(table_name);
  TableSchemaToDesc(schema, &table_desc);
  table_desc.SetSplitSize(10000000);
  table_desc.SetMergeSize(0);
  if (!client->CreateTable(table_desc, delimiters, &err)) {
    LOG(WARNING) << "manual create error: " << table_name << ", err: " << err.ToString();
    return -1;
  }
  return 0;
}

int ManualSplitTable(std::shared_ptr<tera::ClientImpl> client, const std::string& table_name,
                     const std::vector<std::string>& delimiters) {
  ErrorCode err;
  std::vector<std::string> arg_list;
  arg_list.push_back("split");
  arg_list.push_back(table_name);
  for (uint32_t i = 0; i < delimiters.size(); i++) {
    arg_list.push_back(delimiters[i]);
    if (!client->CmdCtrl("table", arg_list, NULL, NULL, &err)) {
      LOG(WARNING) << "manual split table fail(ignore old master):  " << table_name
                   << ", delimiters_size: " << delimiters.size() << ", err: " << err.ToString();
    }
    usleep(FLAGS_dump_manual_split_interval);
    arg_list.pop_back();
  }
  return 0;
}

bool SchemaCompare(const TableSchema& src, const TableSchema& dest) {
  return ((src.raw_key() == dest.raw_key()) && (src.kv_only() == dest.kv_only()) &&
          (src.name() == dest.name()) && (!IsSchemaCfDiff(src, dest)) &&
          (!IsSchemaLgDiff(src, dest)));
}

int GetOrSetTabletLocationSafe(Client* src_client, Client* dest_client, TableMetaList* table_list,
                               TabletMetaList* tablet_list) {
  // get src and dest tablet location
  ErrorCode err;
  TableMetaList src_table_list;
  TabletMetaList src_tablet_list;
  std::shared_ptr<tera::ClientImpl> src_client_impl(
      (static_cast<ClientWrapper*>(src_client))->GetClientImpl());
  if (!src_client_impl->ShowTablesInfo(&src_table_list, &src_tablet_list, false, &err)) {
    LOG(WARNING) << "tera_master show src cluster fail: " << err.ToString();
    return -1;
  }

  TableMetaList dest_table_list;
  TabletMetaList dest_tablet_list;
  std::shared_ptr<tera::ClientImpl> dest_client_impl(
      (static_cast<ClientWrapper*>(dest_client))->GetClientImpl());
  if (!dest_client_impl->ShowTablesInfo(&dest_table_list, &dest_tablet_list, false, &err)) {
    LOG(WARNING) << "tera_master show dest cluster fail: " << err.ToString();
    return -1;
  }

  // get table meta set
  std::map<std::string, TableSchema> src_table_set;
  for (int32_t i = 0; i < src_table_list.meta_size(); i++) {
    const tera::TableMeta& meta = src_table_list.meta(i);
    TableSchema& schema = src_table_set[meta.table_name()];
    schema.CopyFrom(meta.schema());
  }
  std::map<std::string, TableSchema> dest_table_set;
  for (int32_t i = 0; i < dest_table_list.meta_size(); i++) {
    const tera::TableMeta& meta = dest_table_list.meta(i);
    TableSchema& schema = dest_table_set[meta.table_name()];
    schema.CopyFrom(meta.schema());
  }

  // create or split table, and filter schema not match meta
  for (int32_t i = 0; i < src_table_list.meta_size(); i++) {
    const tera::TableMeta& meta = src_table_list.meta(i);
    if (meta.table_name() == FLAGS_tera_master_meta_table_name) {
      continue;
    }
    std::vector<std::string> delimiters;
    GetTableKeyRange(meta.table_name(), src_tablet_list, &delimiters);
    if (dest_table_set.find(meta.table_name()) == dest_table_set.end()) {
      if (ManualCreateTable(dest_client_impl, meta.table_name(), meta.schema(), delimiters) < 0) {
        return -1;
      }
    } else if (SchemaCompare(dest_table_set[meta.table_name()], meta.schema())) {
      if (FLAGS_dump_enable_manual_split &&
          ManualSplitTable(dest_client_impl, meta.table_name(), delimiters) < 0) {
        return -1;
      }
    } else {
      LOG(WARNING) << "table schema not match: " << meta.table_name()
                   << ", src schema: " << meta.schema().ShortDebugString()
                   << ", dest schema: " << dest_table_set[meta.table_name()].ShortDebugString();
      src_table_set.erase(meta.table_name());
      continue;
    }
    tera::TableMeta* meta2 = table_list->add_meta();
    meta2->CopyFrom(meta);
  }

  // filter key range
  for (int32_t i = 0; i < src_tablet_list.meta_size(); i++) {
    const tera::TabletMeta& meta = src_tablet_list.meta(i);
    if (src_table_set.find(meta.table_name()) == src_table_set.end()) {
      continue;
    }
    tera::TabletMeta* meta2 = tablet_list->add_meta();
    meta2->CopyFrom(meta);
  }
  return 0;
}

int LoadTablesMapFile(const std::string& file_name, std::map<std::string, std::string>* tables_map,
                      std::map<std::string, std::string>* tables_lg_map) {
  std::fstream fin(file_name.c_str());
  std::string line;
  while (getline(fin, line)) {
    // line format :
    // test1,test2
    // test3:lg1|lg2|lg3,test5:lg1|lg2|lg3
    std::vector<std::string> tables;
    std::string delimiter(",");
    SplitString(line, delimiter, &tables);
    if (tables.size() != 2) {
      return -1;
    }
    if (tables_map->find(tables[0]) != tables_map->end()) {
      LOG(WARNING) << "Reduplicative table name";
      return -1;
    }
    std::string src_table = tables[0];
    std::string dest_table = tables[1];
    std::string lg_delimiter(":");
    std::size_t pos = src_table.find(lg_delimiter);
    if (pos != std::string::npos) {
      std::string src_table_no_lg = src_table.substr(0, pos);
      std::string lg = src_table.substr(pos + 1);

      pos = dest_table.find(lg_delimiter);
      if (pos == std::string::npos) {
        LOG(WARNING) << "Wrong arguement in specifing lg";
        return -1;
      }
      std::string dest_table_no_lg = dest_table.substr(0, pos);
      std::string dest_lg = dest_table.substr(pos + 1);
      if (lg != dest_lg) {
        LOG(WARNING) << "Mismatch lg in src_table & dest_table is forbidden";
        return -1;
      }
      (*tables_map)[src_table_no_lg] = dest_table_no_lg;
      (*tables_lg_map)[src_table_no_lg] = lg;
    } else {
      (*tables_map)[src_table] = dest_table;
    }
  }
  return 0;
}

int CheckTablesMapSensible(const TableMetaList& src_table_list,
                           const TableMetaList& dest_table_list,
                           const std::map<std::string, std::string>& tables_map) {
  // get table meta set
  std::set<std::string> src_table_set;

  LOG(INFO) << "print src tables : ";
  for (int32_t i = 0; i < src_table_list.meta_size(); i++) {
    const tera::TableMeta& meta = src_table_list.meta(i);
    src_table_set.insert(meta.table_name());
    LOG(INFO) << "table = " << meta.table_name();
  }

  std::set<std::string> dest_table_set;
  LOG(INFO) << "print dest tables : ";
  for (int32_t i = 0; i < dest_table_list.meta_size(); i++) {
    const tera::TableMeta& meta = dest_table_list.meta(i);
    dest_table_set.insert(meta.table_name());
    LOG(INFO) << "table = " << meta.table_name();
  }

  // make sure src_table in src_table_set
  // dest_table not in dest_table_set
  for (auto it = tables_map.cbegin(); it != tables_map.cend(); ++it) {
    // Not work for specify lg
    if (src_table_set.find(it->first) == src_table_set.cend()) {
      LOG(WARNING) << "The src_table " << it->first << " not in src_table_set";
      return -1;
    }
    if (dest_table_set.find(it->second) != dest_table_set.cend()) {
      LOG(WARNING) << "The dest_table " << it->second << " in dest_table_set";
      return -1;
    }
  }
  return 0;
}

void MayBeAddCfMapByLgMap(const TableMeta& src_meta,
                          const std::map<std::string, std::string>& tables_lg_map,
                          std::map<std::string, std::string>* tables_cf_map) {
  std::string src_table_name = src_meta.table_name();

  auto it = tables_lg_map.find(src_table_name);
  if (it != tables_lg_map.cend()) {
    const TableSchema& src_schema = src_meta.schema();

    // lg0|lg1|lg2
    std::string lg_str = it->second;
    std::vector<std::string> lgs;
    std::string delimiter(FLAGS_lg_and_cf_delimiter);
    SplitString(lg_str, delimiter, &lgs);

    std::vector<std::string> cfs;
    for (int cf_index = 0; cf_index < src_schema.column_families_size(); ++cf_index) {
      auto result = std::find(lgs.cbegin(), lgs.cend(),
                              src_schema.column_families(cf_index).locality_group());
      if (result != lgs.cend()) {
        cfs.emplace_back(src_schema.column_families(cf_index).name());
      }
    }
    if (cfs.size() > 0) {
      (*tables_cf_map)[src_table_name] =
          std::accumulate(cfs.cbegin(), cfs.cend(), std::string(),
                          [&delimiter](const std::string& a, const std::string& b)
                              -> std::string { return a + (a.length() > 0 ? delimiter : "") + b; });
    }
  }

  return;
}

void MayBeChangeSchemaByLgMap(TableMeta& src_meta,
                              const std::map<std::string, std::string>& tables_lg_map) {
  std::string src_table_name = src_meta.table_name();

  // Remove lg not in tables_lg_map if this tabls' exist
  auto it = tables_lg_map.find(src_table_name);
  if (it != tables_lg_map.cend()) {
    TableSchema* src_schema = src_meta.mutable_schema();

    TableSchema* src_schema_tmp = new TableSchema;
    src_schema_tmp->CopyFrom(*src_schema);
    src_schema->clear_locality_groups();

    // lg0|lg1|lg2
    std::string lg_str = it->second;
    std::vector<std::string> lgs;
    std::string delimiter(FLAGS_lg_and_cf_delimiter);
    SplitString(lg_str, delimiter, &lgs);

    for (int lg_index = 0; lg_index < src_schema_tmp->locality_groups_size(); ++lg_index) {
      auto result =
          std::find(lgs.cbegin(), lgs.cend(), src_schema_tmp->locality_groups(lg_index).name());
      if (result != lgs.cend()) {
        LocalityGroupSchema* lg_schema = src_schema->add_locality_groups();
        lg_schema->CopyFrom(src_schema_tmp->locality_groups(lg_index));
      }
    }

    src_schema->clear_column_families();
    std::vector<std::string> cfs;
    for (int cf_index = 0; cf_index < src_schema_tmp->column_families_size(); ++cf_index) {
      auto result = std::find(lgs.cbegin(), lgs.cend(),
                              src_schema_tmp->column_families(cf_index).locality_group());
      if (result != lgs.cend()) {
        ColumnFamilySchema* cf_schema = src_schema->add_column_families();
        cf_schema->CopyFrom(src_schema_tmp->column_families(cf_index));
      }
    }
    delete src_schema_tmp;
  }

  return;
}

int GetAndSetTableSchema(Client* src_client, Client* dest_client, TableMetaList* table_list,
                         TabletMetaList* tablet_list,
                         const std::map<std::string, std::string>& tables_map,
                         const std::map<std::string, std::string>& tables_lg_map,
                         std::map<std::string, std::string>* tables_cf_map) {
  ErrorCode err;
  TableMetaList src_table_list;
  TabletMetaList src_tablet_list;
  std::shared_ptr<tera::ClientImpl> src_client_impl(
      (static_cast<ClientWrapper*>(src_client))->GetClientImpl());
  if (!src_client_impl->ShowTablesInfo(&src_table_list, &src_tablet_list, false, &err)) {
    LOG(WARNING) << "tera_master show src cluster fail: " << err.ToString();
    return -1;
  }

  TableMetaList dest_table_list;
  TabletMetaList dest_tablet_list;
  std::shared_ptr<tera::ClientImpl> dest_client_impl(
      (static_cast<ClientWrapper*>(dest_client))->GetClientImpl());
  if (!dest_client_impl->ShowTablesInfo(&dest_table_list, &dest_tablet_list, false, &err)) {
    LOG(WARNING) << "tera_master show dest cluster fail: " << err.ToString();
    return -1;
  }
  if (FLAGS_enable_copy_schema &&
      (-1 == CheckTablesMapSensible(src_table_list, dest_table_list, tables_map))) {
    LOG(WARNING) << "TablesMap not sensible!";
    return -1;
  }
  for (int32_t src_list_index = 0; src_list_index < src_table_list.meta_size(); ++src_list_index) {
    TableMeta src_meta = src_table_list.meta(src_list_index);
    std::string src_table_name = src_meta.table_name();
    auto it = tables_map.find(src_table_name);
    if (it == tables_map.cend()) {
      continue;
    }

    MayBeChangeSchemaByLgMap(src_meta, tables_lg_map);
    MayBeAddCfMapByLgMap(src_meta, tables_lg_map, tables_cf_map);

    std::vector<std::string> delimiters;
    GetTableKeyRange(src_table_name, src_tablet_list, &delimiters);
    TableMeta dest_meta(src_meta);
    dest_meta.set_table_name(it->second);
    TableSchema* dest_table_schema = new TableSchema;
    dest_table_schema->CopyFrom(dest_meta.schema());
    dest_table_schema->set_name(it->second);
    dest_table_schema->set_alias(it->second);
    dest_meta.release_schema();
    dest_meta.set_allocated_schema(dest_table_schema);
    if (FLAGS_enable_copy_schema &&
        ManualCreateTable(dest_client_impl, dest_meta.table_name(), dest_meta.schema(),
                          delimiters) < 0) {
      LOG(WARNING) << "Create table[" << dest_meta.table_name() << "] in dest cluster failed!";
      return -1;
    }
    TableMeta* meta2 = table_list->add_meta();
    meta2->CopyFrom(src_meta);
  }
  for (int32_t src_list_index = 0; src_list_index < src_tablet_list.meta_size(); ++src_list_index) {
    const TabletMeta& meta = src_tablet_list.meta(src_list_index);
    if (tables_map.find(meta.table_name()) == tables_map.end()) {
      continue;
    }
    TabletMeta* meta2 = tablet_list->add_meta();
    meta2->CopyFrom(meta);
  }
  return 0;
}

int LoadTablesMapOp() {
  std::string ins_cluster_addr = FLAGS_ins_cluster_addr;
  std::string ins_cluster_root_path = FLAGS_ins_cluster_dump_root_path;

  if (FLAGS_tables_map_file == "") {
    LOG(WARNING) << "Should set --tables_map_file before use prepare_tables!";
    return -1;
  }
  std::map<std::string, std::string> tables_map;
  std::map<std::string, std::string> tables_lg_map;
  if (-1 == LoadTablesMapFile(FLAGS_tables_map_file, &tables_map, &tables_lg_map)) {
    LOG(WARNING) << "Load tables_map_file failed!";
    return -1;
  }

  // Put t1=>t2 in nexus
  galaxy::ins::sdk::InsSDK ins_sdk(ins_cluster_addr);
  std::string path = ins_cluster_root_path + "/" + FLAGS_dump_tables_map_path;
  if (-1 == PutMapInNexus(path, &ins_sdk, tables_map)) {
    LOG(WARNING) << "PutMapInNexus failed";
    return -1;
  }

  // Put src_t1=>lg in nexus
  path = ins_cluster_root_path + "/" + FLAGS_dump_tables_lg_map_path;
  if (-1 == PutMapInNexus(path, &ins_sdk, tables_lg_map)) {
    LOG(WARNING) << "DumpTables lg Map failed";
    return -1;
  }
  return 0;
}

int CreateDfsPath(const std::map<std::string, std::string>& tables_map,
                  const std::string& dfs_parent_path) {
  int ret = -1;
  if (g_dfs == NULL) {
    LOG(WARNING) << "Init afs client before create afs path";
    return ret;
  }

  // Make sure dfs parent path exist
  ret = g_dfs->CreateDirectory(dfs_parent_path);
  if (0 != ret) {
    LOG(WARNING) << "create parent path failed, errno = " << errno;
    return ret;
  }

  for (auto it = tables_map.cbegin(); it != tables_map.cend(); ++it) {
    std::string src_table_path = dfs_parent_path + "/" + it->first;
    LOG(INFO) << "create path[" << src_table_path << "] in dfs";
    ret = g_dfs->CreateDirectory(src_table_path);
    if (0 != ret) {
      LOG(WARNING) << "create dir[" << src_table_path << "] in dfs failed!";
      break;
    }
  }
  return ret;
}

int64_t ReadAfsFile(const std::string& file_path, char* data, ssize_t max_size) {
  leveldb::DfsFile* file = g_dfs->OpenFile(file_path, leveldb::RDONLY);
  if (file == NULL) {
    LOG(WARNING) << "Open file[" << file_path << "] failed!  errno : " << errno;
    return -1;
  }

  ssize_t buf_len = 128 * 1024;
  char buf[buf_len];
  memset(buf, 0, buf_len);
  ssize_t ret_size = 0;
  int64_t sum = 0;
  while ((ret_size = file->Read(buf, sizeof(buf))) > 0) {
    memcpy(data, buf, ret_size);
    sum += ret_size;
    data += ret_size;
    memset(buf, 0, buf_len);
  }
  file->CloseFile();
  //*data = '\0';
  return sum;
}

int GetRowResultFromAfsFile(const std::string& file_path, RowResult* row_result) {
  ssize_t max_size = FLAGS_pb_total_bytes_limit_MB * 1024 * 1024;
  char* data = new char[max_size];
  FuncScopeGuard on_exit([&data] { delete[] data; });
  int64_t read_len = ReadAfsFile(file_path, data, max_size);
  if (read_len <= 0) {
    LOG(WARNING) << "Read afs file failed!";
    return -1;
  }
  LOG(INFO) << "file[" << file_path << "] data size = " << read_len;
  if (!DeserializationRowResult(data, read_len, row_result)) {
    LOG(WARNING) << "Parse afs kv file[" << file_path << "] failed!";
    return -1;
  }
  return 0;
}

// Re-write failed kv_pairs
int DumpRewriteOp() {
  std::string ins_cluster_addr = FLAGS_ins_cluster_addr;
  std::string ins_cluster_root_path = FLAGS_ins_cluster_dump_root_path;
  std::string tera_dest_conf = FLAGS_dump_tera_dest_conf;

  // Get tables_map from nexus
  galaxy::ins::sdk::InsSDK ins_sdk(ins_cluster_addr);
  std::map<std::string, std::string> tables_map;
  std::string path = ins_cluster_root_path + "/" + FLAGS_dump_tables_map_path;
  if (-1 == GetMapFromNexus(path, &ins_sdk, &tables_map)) {
    LOG(WARNING) << "GetMapFromNexus failed";
    return -1;
  }
  for (auto it = tables_map.cbegin(); it != tables_map.cend(); ++it) {
    LOG(INFO) << it->first << " => " << it->second;
  }

  // target cluster op
  ErrorCode err;
  std::unique_ptr<Client> target_client(Client::NewClient(tera_dest_conf, &err));
  if (target_client == nullptr) {
    LOG(WARNING) << "open dest client fail: " << tera_dest_conf << ", err " << err.ToString();
    return -1;
  }

  // Afs op
  if (-1 == InitDfsClient()) {
    LOG(WARNING) << "init dfs client failed!";
    return -1;
  }
  std::string dfs_parent_path = FLAGS_dump_failed_kv_afs_path;
  struct stat fstat;
  memset(&fstat, 0, sizeof(struct stat));
  std::map<std::string, std::vector<std::string>> src_table_files_map;
  if (0 == g_dfs->Stat(dfs_parent_path, &fstat)) {
    std::vector<std::string> sub_paths;
    if (0 != g_dfs->ListDirectory(dfs_parent_path, &sub_paths)) {
      LOG(WARNING) << "dfs list dir[" << dfs_parent_path << "] failed!";
      return -1;
    }
    std::ostringstream fullpath;
    for (const auto& sub_path : sub_paths) {
      auto it = tables_map.find(sub_path);
      if (it == tables_map.cend()) {
        LOG(WARNING) << "Wrong nexus record!!!";
        return -1;
      }
      std::vector<std::string>& file_paths = src_table_files_map[sub_path];

      fullpath << dfs_parent_path << "/" << sub_path;

      std::vector<std::string> files;
      if (0 != g_dfs->ListDirectory(fullpath.str(), &files)) {
        LOG(WARNING) << "dfs list dir[" << fullpath.str() << "] failed!";
        return -1;
      }
      for_each(files.cbegin(), files.cend(), [&](const std::string& file) {
        file_paths.emplace_back(dfs_parent_path + "/" + sub_path + "/" + file);
      });
      fullpath.str("");
      fullpath.clear();
    }
  }

  std::unique_ptr<RowResult> row_result(new RowResult);
  for (auto it = src_table_files_map.cbegin(); it != src_table_files_map.cend(); ++it) {
    const std::string& target_table_name = it->first;
    Table* target_table = target_client->OpenTable(tables_map[target_table_name], &err);
    if (target_table == nullptr) {
      LOG(WARNING) << "Open table[" << target_table_name << "] failed!";
      continue;
    }
    for (const auto& file_path : it->second) {
      row_result->clear_key_values();
      if (-1 == GetRowResultFromAfsFile(file_path, row_result.get())) {
        LOG(WARNING) << "GetRowResultFromAfsFile[" << file_path << "] failed!";
        continue;
      }

      // Write Rowresult to target table
      Counter counter;
      counter.Inc();
      for (int kv_index = 0; kv_index < row_result->key_values_size(); ++kv_index) {
        g_sem->Acquire();
        RewriteContext* ctx = new RewriteContext;
        KeyValuePair* kv_pair = row_result->mutable_key_values(kv_index);
        RowMutation* mu = target_table->NewRowMutation(kv_pair->key());
        mu->Put(kv_pair->column_family(), kv_pair->qualifier(), kv_pair->value(),
                kv_pair->timestamp());
        counter.Inc();
        ctx->counter = &counter;
        ctx->target_table = target_table;
        ctx->kv_pair = kv_pair;
        mu->SetContext(ctx);
        mu->SetCallBack(RewriteCallBack);
        target_table->ApplyMutation(mu);
      }
      counter.Dec();
      while (counter.Get() > 0) {
        sleep(3);
      }
      LOG(INFO) << "finish write diff file: " << file_path;
    }
    delete target_table;
  }
  return 0;
}

// Read write failed kv_pairs
int DumpReadOp(const std::string& afs_file_path) {
  if (-1 == InitDfsClient()) {
    LOG(WARNING) << "init dfs client failed!";
    return -1;
  }
  std::unique_ptr<RowResult> row_result(new RowResult);
  if (-1 == GetRowResultFromAfsFile(afs_file_path, row_result.get())) {
    LOG(WARNING) << "GetRowResultFromAfsFile[" << afs_file_path << "] failed!";
    return -1;
  }
  std::cout << "afs file[" << afs_file_path << "] kv size = " << row_result->key_values_size()
            << std::endl;
  for (int kv_index = 0; kv_index < row_result->key_values_size(); ++kv_index) {
    const KeyValuePair& kv_pair = row_result->key_values(kv_index);
    std::cout << kv_pair.key() << ":" << kv_pair.column_family() << ":" << kv_pair.qualifier()
              << ":" << kv_pair.timestamp() << "=>" << kv_pair.value() << std::endl;
  }
  return 0;
}

int DeleteKeyRangeInNexus(const std::string& start_key, const std::string& end_key,
                          galaxy::ins::sdk::InsSDK* ins_sdk) {
  int res = 0;
  galaxy::ins::sdk::ScanResult* result = ins_sdk->Scan(start_key, end_key);

  while (!result->Done()) {
    if (result->Error() != galaxy::ins::sdk::kOK) {
      LOG(WARNING) << "scan fail: start " << start_key << ", end " << end_key << ", err "
                   << result->Error();
      res = -1;
      break;
    }
    galaxy::ins::sdk::SDKError err;
    ins_sdk->Delete(result->Key(), &err);
    if (err != galaxy::ins::sdk::kOK) {
      LOG(WARNING) << "Delete failed[key = " << result->Key() << ", value = " << result->Value()
                   << "]";
      res = -1;
    }
    result->Next();
  }
  delete result;
  return res;
}

std::string FormatPath(const std::string& pathname) {
  std::string result;
  bool need_strip = false;
  for (std::string::size_type i = 0; i < pathname.length(); ++i) {
    if (pathname.at(i) == '/') {
      if (need_strip) {
        continue;
      } else {
        result.push_back(pathname.at(i));
        need_strip = true;
      }
    } else {
      need_strip = false;
      result.push_back(pathname.at(i));
    }
  }
  if (result.at(result.length() - 1) == '/') {
    result.pop_back();
  }
  return result;
}

int32_t DfsPrintPath(const char* pathname, struct stat* st) {
  printf("%s", FormatPath(pathname).c_str());
  if (S_IFDIR & st->st_mode) {
    printf("/");
  }
  printf("\n");
  return 0;
}

int ShowAfsDir(const std::string& dir_name) {
  std::vector<std::string> sub_paths;
  if (0 != g_dfs->ListDirectory(dir_name, &sub_paths)) {
    return -1;
  }

  struct stat st;
  std::ostringstream fullpath;
  for (std::size_t i = 0; i < sub_paths.size(); ++i) {
    fullpath.str("");
    fullpath.clear();
    fullpath << dir_name << "/" << sub_paths[i];
    memset(&st, 0, sizeof(struct stat));
    if (g_dfs->Stat(fullpath.str(), &st) < 0) {
      perror("Stat failed");
      continue;
    }
    DfsPrintPath(fullpath.str().c_str(), &st);
  }
  return 0;
}

bool Confirm() {
  std::cout << "[Y/N] ";
  std::string ensure;
  if (!std::getline(std::cin, ensure)) {
    std::cout << "Get input error" << std::endl;
    return false;
  }
  if (ensure != "Y") {
    return false;
  }
  return true;
}

int CleanAfsData(const std::string& dfs_parent_path) {
  int res = 0;
  struct stat fstat;
  if (0 == g_dfs->Stat(dfs_parent_path, &fstat)) {
    if (S_IFDIR & fstat.st_mode) {
      if (-1 == ShowAfsDir(dfs_parent_path)) {
        LOG(WARNING) << "List afs dir failed!";
        res = -1;
      }
      std::cout << "Are you sure DELETE AFS PATH[" << dfs_parent_path << "]?" << std::endl;
      if (Confirm()) {
        // Delete afs path
        res = g_dfs->DeleteDirectory(dfs_parent_path);
        if (0 != res) {
          LOG(WARNING) << "RmDir[" << dfs_parent_path << "] fail";
        }
      }
    } else {
      LOG(WARNING) << "Please make sure input the right parent path for delete";
      res = -1;
    }
  } else {
    LOG(WARNING) << "dfs stat failed!";
    res = -1;
  }
  return res;
}

// Clean nexus&afs path data
int CleanUpDumpData(const std::string& ins_cluster_root_path, const std::string& dfs_parent_path) {
  int res = 0;
  // Clean nexus data
  std::string ins_cluster_addr = FLAGS_ins_cluster_addr;
  std::string start = ins_cluster_root_path + "/";
  std::string end = ins_cluster_root_path + "/";

  start.append(1, '\0');
  end.append(1, '\255');

  galaxy::ins::sdk::InsSDK ins_sdk(ins_cluster_addr);
  res = DeleteKeyRangeInNexus(start, end, &ins_sdk);

  // Clean afs data
  if (-1 == InitDfsClient()) {
    LOG(WARNING) << "init dfs client failed!";
    return -1;
  }
  res = CleanAfsData(dfs_parent_path);
  return res;
}

// Clean nexus&afs path data
int CleanUpDiffData(const std::string& ins_cluster_root_path, const std::string& dfs_diff_path,
                    const std::string& dfs_diffbin_path) {
  int res = 0;
  // Clean nexus data
  std::string ins_cluster_addr = FLAGS_ins_cluster_addr;
  std::string start = ins_cluster_root_path + "/";
  std::string end = ins_cluster_root_path + "/";

  start.append(1, '\0');
  end.append(1, '\255');

  galaxy::ins::sdk::InsSDK ins_sdk(ins_cluster_addr);
  res = DeleteKeyRangeInNexus(start, end, &ins_sdk);

  // Clean afs data
  if (-1 == InitDfsClient()) {
    LOG(WARNING) << "init dfs client failed!";
    return -1;
  }
  res = CleanAfsData(dfs_diff_path);
  if (res != 0) {
    return res;
  }
  res = CleanAfsData(dfs_diffbin_path);
  return res;
}

int DumpCleanOp() {
  return CleanUpDumpData(FLAGS_ins_cluster_dump_root_path, FLAGS_dump_failed_kv_afs_path);
}

int DiffCleanOp() {
  return CleanUpDiffData(FLAGS_ins_cluster_diff_root_path, FLAGS_diff_data_afs_path,
                         FLAGS_diff_bin_data_afs_path);
}

int DumpPrepareTablesOp() {
  std::string ins_cluster_addr = FLAGS_ins_cluster_addr;
  std::string ins_cluster_root_path = FLAGS_ins_cluster_dump_root_path;
  std::string tera_src_conf = FLAGS_dump_tera_src_conf;
  std::string tera_dest_conf = FLAGS_dump_tera_dest_conf;

  galaxy::ins::sdk::InsSDK ins_sdk(ins_cluster_addr);

  // Get tables_map from nexus
  std::map<std::string, std::string> tables_map;
  std::string path = ins_cluster_root_path + "/" + FLAGS_dump_tables_map_path;
  if (-1 == GetMapFromNexus(path, &ins_sdk, &tables_map)) {
    LOG(WARNING) << "GetMapFromNexus failed";
    return -1;
  }
  for (auto it = tables_map.cbegin(); it != tables_map.cend(); ++it) {
    LOG(INFO) << it->first << " => " << it->second;
  }

  // Get tables_lg_map from nexus
  std::map<std::string, std::string> tables_lg_map;
  path = ins_cluster_root_path + "/" + FLAGS_dump_tables_lg_map_path;
  if (-1 == GetMapFromNexus(path, &ins_sdk, &tables_lg_map)) {
    LOG(WARNING) << "GetTablesLgMap failed";
    return -1;
  }
  for (auto it = tables_lg_map.cbegin(); it != tables_lg_map.cend(); ++it) {
    LOG(INFO) << it->first << " => " << it->second;
  }

  ErrorCode err;
  std::unique_ptr<Client> src_client(Client::NewClient(tera_src_conf, &err));
  if (src_client == nullptr) {
    LOG(WARNING) << "open src client fail: " << tera_src_conf << ", err " << err.ToString();
    return -1;
  }
  LOG(INFO) << "Open src client " << tera_src_conf << " success";
  std::unique_ptr<Client> dest_client(Client::NewClient(tera_dest_conf, &err));
  if (dest_client == nullptr) {
    LOG(WARNING) << "open dest client fail: " << tera_dest_conf << ", err " << err.ToString();
    return -1;
  }
  LOG(INFO) << "Open dest client " << tera_dest_conf << " success";
  // dump src cluster range into ins
  TableMetaList table_list;
  TabletMetaList tablet_list;
  std::map<std::string, std::string> tables_cf_map;
  if (GetAndSetTableSchema(src_client.get(), dest_client.get(), &table_list, &tablet_list,
                           tables_map, tables_lg_map, &tables_cf_map) < 0) {
    LOG(WARNING) << "GetAndSetTableSchema faield";
    return -1;
  }

  // Put src_t1=>cf1|cf2 in nexus
  path = ins_cluster_root_path + "/" + FLAGS_dump_tables_cf_map_path;
  if (-1 == PutMapInNexus(path, &ins_sdk, tables_cf_map)) {
    LOG(WARNING) << "Dump Tables cf Map failed";
    return -1;
  }

  auto init_ins_value_first_part = std::bind(InitInsValueFirstPartForDump, std::placeholders::_1);
  if (-1 == DumpRange(ins_cluster_root_path, &ins_sdk, table_list, tablet_list,
                      init_ins_value_first_part)) {
    LOG(WARNING) << "Dump range faield";
    return -1;
  }

  if (-1 == InitDfsClient()) {
    LOG(WARNING) << "init dfs client failed!";
    return -1;
  }

  if (-1 == CreateDfsPath(tables_map, FLAGS_dump_failed_kv_afs_path)) {
    LOG(WARNING) << "init dfs path failed for storing failed kv";
    return -1;
  }

  return 0;
}

int DumpPrepareSafeOp() {
  int res = 0;
  std::string ins_cluster_addr = FLAGS_ins_cluster_addr;
  std::string ins_cluster_root_path = FLAGS_ins_cluster_dump_root_path;
  std::string tera_src_conf = FLAGS_dump_tera_src_conf;
  std::string tera_dest_conf = FLAGS_dump_tera_dest_conf;

  ErrorCode err;
  std::unique_ptr<Client> src_client(Client::NewClient(tera_src_conf, &err));
  if (src_client == nullptr) {
    LOG(WARNING) << "open src client fail: " << tera_src_conf << ", err " << err.ToString();
    return -1;
  }
  std::unique_ptr<Client> dest_client(Client::NewClient(tera_dest_conf, &err));
  if (dest_client == nullptr) {
    src_client = nullptr;
    LOG(WARNING) << "open dest client fail: " << tera_dest_conf << ", err " << err.ToString();
    return -1;
  }

  // dump src cluster range into ins
  TableMetaList table_list;
  TabletMetaList tablet_list;
  if (GetOrSetTabletLocationSafe(src_client.get(), dest_client.get(), &table_list, &tablet_list) <
      0) {
    return -1;
  }
  galaxy::ins::sdk::InsSDK ins_sdk(ins_cluster_addr);
  auto init_ins_value_first_part = std::bind(InitInsValueFirstPartForDump, std::placeholders::_1);
  res = DumpRange(ins_cluster_root_path, &ins_sdk, table_list, tablet_list,
                  init_ins_value_first_part);
  return res;
}

int DumpUtOp() {
  // Ut for pb convert
  RowResult row_result;
  int kv_num = 1000 * 10000;
  for (int i = 0; i < kv_num; i++) {
    KeyValuePair* kv_pair = row_result.add_key_values();
    kv_pair->set_key("aaa");
    kv_pair->set_column_family("bbb");
    kv_pair->set_qualifier("ccc");
    kv_pair->set_value("ddd");
    kv_pair->set_timestamp(111);
  }

  std::string seri_str;
  if (!SerializationRowResult(row_result, &seri_str)) {
    std::cout << "SerializationRowResult failed" << std::endl;
    return -1;
  }

  if (-1 == InitDfsClient()) {
    LOG(WARNING) << "InitDfsClient FAILED";
    return -1;
  }
  int ret = CleanAfsData(FLAGS_dump_ut_kv_afs_path);
  if (0 != ret) {
    LOG(WARNING) << "CleanAfsData failed";
    return ret;
  }
  ret = g_dfs->CreateDirectory(FLAGS_dump_ut_kv_afs_path);
  if (0 != ret) {
    LOG(WARNING) << "create parent path failed, errno = " << errno;
    return ret;
  }
  std::string file_path = FLAGS_dump_ut_kv_afs_path + "/test.pbtxt";
  WriteToDfs(file_path, seri_str);

  ssize_t max_size = FLAGS_pb_total_bytes_limit_MB * 1024 * 1024;
  char* data = new char[max_size];
  FuncScopeGuard on_exit([&data] { delete[] data; });
  int64_t read_len = ReadAfsFile(file_path, data, max_size);
  if (read_len <= 0) {
    LOG(WARNING) << "Read afs file failed!";
    return -1;
  }

  std::unique_ptr<RowResult> other_row_result(new RowResult);
  if (!DeserializationRowResult(data, read_len, other_row_result.get())) {
    std::cout << "DeserializationRowResult failed!" << std::endl;
    return -1;
  }

  if (other_row_result->key_values_size() != kv_num) {
    std::cout << "DisMatch in convert pb, kv_num not match" << std::endl;
    return -1;
  } else {
    std::cout << "kv_num Match" << std::endl;
  }

  int dismatch_num = 0;
  int match_num = 0;
  for (int i = 0; i < kv_num; i++) {
    const KeyValuePair& kv_pair = row_result.key_values(i);
    const KeyValuePair& other_kv_pair = other_row_result->key_values(i);
    if ((other_kv_pair.key() != kv_pair.key()) ||
        (other_kv_pair.column_family() != kv_pair.column_family()) ||
        (other_kv_pair.qualifier() != kv_pair.qualifier()) ||
        (other_kv_pair.value() != kv_pair.value()) ||
        (other_kv_pair.timestamp() != kv_pair.timestamp())) {
      dismatch_num++;
    } else {
      match_num++;
    }
  }
  if (match_num != kv_num) {
    std::cout << "DisMatch in convert pb" << std::endl;
    return -1;
  } else {
    std::cout << "Match, correct" << std::endl;
  }
  return 0;
}

std::string GetIsMultiVersionFlag(const ColumnFamilySchema& cf_schema) {
  if (cf_schema.max_versions() > 1) {
    return "1";
  } else {
    return "0";
  }
}

void GetTablesCfVersionMap(const TableMetaList& table_list,
                           const std::map<std::string, std::string>& tables_cf_map,
                           std::map<std::string, std::string>* tables_cf_version_map) {
  for (int32_t i = 0; i < table_list.meta_size(); i++) {
    const TableMeta& meta = table_list.meta(i);
    std::string table_name = meta.table_name();

    const TableSchema& schema = meta.schema();
    auto it = tables_cf_map.find(table_name);
    if (it == tables_cf_map.cend()) {
      if (schema.column_families_size() == 0) {
        (*tables_cf_version_map)[table_name + "::"] = "0";
      } else {
        for (int j = 0; j < schema.column_families_size(); ++j) {
          (*tables_cf_version_map)[table_name + "::" + schema.column_families(j).name()] =
              GetIsMultiVersionFlag(schema.column_families(j));
        }
      }
    } else {
      std::vector<std::string> cfs;
      std::string delimiter(FLAGS_lg_and_cf_delimiter);
      SplitString(it->second, delimiter, &cfs);
      for (int j = 0; j < schema.column_families_size(); ++j) {
        auto result = std::find(cfs.cbegin(), cfs.cend(), schema.column_families(j).name());
        if (result != cfs.cend()) {
          (*tables_cf_version_map)[table_name + "::" + schema.column_families(j).name()] =
              GetIsMultiVersionFlag(schema.column_families(j));
        }
      }
    }
  }

  return;
}

int ShowTablesInfo(const std::string& tera_src_conf, TableMetaList* table_list,
                   TabletMetaList* tablet_list) {
  ErrorCode err;

  std::unique_ptr<Client> src_client(Client::NewClient(tera_src_conf, &err));
  if (src_client == nullptr) {
    LOG(WARNING) << "open src client fail: " << tera_src_conf << ", err " << err.ToString();
    return -1;
  }

  std::shared_ptr<tera::ClientImpl> src_client_impl(
      (static_cast<ClientWrapper*>(src_client.get()))->GetClientImpl());
  if (!src_client_impl->ShowTablesInfo(table_list, tablet_list, false, &err)) {
    LOG(WARNING) << "ShowTablesInfo fail: " << err.ToString();
    return -1;
  }

  return 0;
}

void FilterTables(const TableMetaList& all_tables,
                  const std::map<std::string, std::string>& tables_map, TableMetaList* table_list) {
  for (int32_t i = 0; i < all_tables.meta_size(); i++) {
    const tera::TableMeta& meta = all_tables.meta(i);
    if (tables_map.find(meta.table_name()) == tables_map.cend()) {
      continue;
    }
    tera::TableMeta* meta2 = table_list->add_meta();
    meta2->CopyFrom(meta);
  }

  return;
}

void FilterTablets(const TabletMetaList& all_tablets,
                   const std::map<std::string, std::string>& tables_map,
                   TabletMetaList* tablet_list) {
  for (int32_t i = 0; i < all_tablets.meta_size(); i++) {
    const tera::TabletMeta& meta = all_tablets.meta(i);
    if (tables_map.find(meta.table_name()) == tables_map.cend()) {
      continue;
    }
    tera::TabletMeta* meta2 = tablet_list->add_meta();
    meta2->CopyFrom(meta);
  }

  return;
}

void GetTablesCfMap(const TableMetaList& table_list,
                    const std::map<std::string, std::string>& tables_lg_map,
                    std::map<std::string, std::string>* tables_cf_map) {
  for (int32_t i = 0; i < table_list.meta_size(); ++i) {
    const TableMeta& meta = table_list.meta(i);
    MayBeAddCfMapByLgMap(meta, tables_lg_map, tables_cf_map);
  }
  return;
}

int DiffPrepareOp() {
  std::string ins_cluster_addr = FLAGS_ins_cluster_addr;
  std::string ins_cluster_root_path = FLAGS_ins_cluster_diff_root_path;
  std::string tera_src_conf = FLAGS_dump_tera_src_conf;

  if (FLAGS_diff_tables_map_file == "") {
    LOG(WARNING) << "Should set --diff_tables_map_file before use diff prepare!";
    return -1;
  }
  std::map<std::string, std::string> tables_map;
  std::map<std::string, std::string> tables_lg_map;
  if (-1 == LoadTablesMapFile(FLAGS_diff_tables_map_file, &tables_map, &tables_lg_map)) {
    LOG(WARNING) << "LoadTablesMapFile FAILED";
    return -1;
  }
  LOG(INFO) << "[STEP_1] LoadTablesMapFile OK";

  TableMetaList src_all_tables;
  TabletMetaList src_all_tablets;
  if (ShowTablesInfo(tera_src_conf, &src_all_tables, &src_all_tablets) < 0) {
    return -1;
  }

  TableMetaList table_list;
  FilterTables(src_all_tables, tables_map, &table_list);
  LOG(INFO) << "[STEP_2] FilterTables OK, table_list.meta_size(): " << table_list.meta_size();

  TabletMetaList tablet_list;
  FilterTablets(src_all_tablets, tables_map, &tablet_list);
  CHECK(tablet_list.meta_size() > 0);
  LOG(INFO) << "[STEP_3] FilterTablets OK, tablet_list.meta_size(): " << tablet_list.meta_size();

  std::map<std::string, std::string> tables_cf_map;
  GetTablesCfMap(table_list, tables_lg_map, &tables_cf_map);
  LOG(INFO) << "[STEP_4] GetTablesCfMap OK, tables_cf_map.size(): " << tables_cf_map.size();

  std::map<std::string, std::string> tables_cf_version_map;
  GetTablesCfVersionMap(table_list, tables_cf_map, &tables_cf_version_map);
  LOG(INFO) << "[STEP_5] GetTablesCfVersionMap OK, tables_cf_version_map.size(): "
            << tables_cf_version_map.size();

  galaxy::ins::sdk::InsSDK ins_sdk(ins_cluster_addr);

  std::string path = ins_cluster_root_path + "/" + FLAGS_dump_tables_map_path;
  if (-1 == PutMapInNexus(path, &ins_sdk, tables_map)) {
    LOG(WARNING) << "PutMapInNexus tables_map FAILED";
    return -1;
  }
  LOG(INFO) << "[STEP_6] PutMapInNexus tables_map OK";

  path = ins_cluster_root_path + "/" + FLAGS_dump_tables_lg_map_path;
  if (-1 == PutMapInNexus(path, &ins_sdk, tables_lg_map)) {
    LOG(WARNING) << "PutMapInNexus tables_lg_map FAILED";
    return -1;
  }
  LOG(INFO) << "[STEP_7] PutMapInNexus tables_lg_map OK";

  path = ins_cluster_root_path + "/" + FLAGS_dump_tables_cf_map_path;
  if (-1 == PutMapInNexus(path, &ins_sdk, tables_cf_map)) {
    LOG(WARNING) << "PutMapInNexus tables_cf_map FAILED";
    return -1;
  }
  LOG(INFO) << "[STEP_8] PutMapInNexus tables_cf_map OK";

  path = ins_cluster_root_path + "/" + FLAGS_dump_tables_cf_version_map_path;
  if (-1 == PutMapInNexus(path, &ins_sdk, tables_cf_version_map)) {
    LOG(WARNING) << "PutMapInNexus tables_cf_version_map FAILED";
    return -1;
  }
  LOG(INFO) << "[STEP_9] PutMapInNexus tables_cf_version_map OK";

  auto init_ins_value_first_part = std::bind(InitInsValueFirstPartForDiff, std::placeholders::_1);
  if (DumpRange(ins_cluster_root_path, &ins_sdk, table_list, tablet_list,
                init_ins_value_first_part) < 0) {
    LOG(WARNING) << "DumpDiffRange FAILED";
    return -1;
  }
  LOG(INFO) << "[STEP_10] DumpDiffRange OK";

  if (-1 == InitDfsClient()) {
    LOG(WARNING) << "InitDfsClient FAILED";
    return -1;
  }
  LOG(INFO) << "[STEP_11] InitDfsClient OK";

  if (-1 == CreateDfsPath(tables_map, FLAGS_diff_data_afs_path)) {
    LOG(WARNING) << "CreateDfsPath FAILED";
    return -1;
  }
  LOG(INFO) << "[STEP_12] CreateDfsPath OK";

  if (-1 == CreateDfsPath(tables_map, FLAGS_diff_bin_data_afs_path)) {
    LOG(WARNING) << "CreateDfsBinDataPath FAILED";
    return -1;
  }
  LOG(INFO) << "[STEP_13] CreateDfsBinDataPath OK";

  LOG(INFO) << "diff prepare finish";

  return 0;
}

bool IsMultiVersion(const std::string& src_table_name, const std::string& cf,
                    const std::map<std::string, std::string>& tables_cf_version_map) {
  std::string table_cf_key = src_table_name + "::" + cf;
  auto it = tables_cf_version_map.find(table_cf_key);
  CHECK(it != tables_cf_version_map.cend());
  if (it->second == "1") {
    return true;
  } else {
    return false;
  }
}

void WriteDiffDataToStream(std::ostringstream& diffdata_stream, ResultStream* result_stream) {
  diffdata_stream << "--------------------" << std::endl;
  diffdata_stream << "[rk] " << DebugString(result_stream->RowName()) << std::endl;
  diffdata_stream << "[cf] " << result_stream->Family() << std::endl;
  diffdata_stream << "[qu] " << DebugString(result_stream->Qualifier()) << std::endl;
  diffdata_stream << "[ts] " << result_stream->Timestamp() << std::endl;
}

void AddDiffRowResult(RowResult& row_result, ResultStream* result_stream) {
  KeyValuePair* kv_pair = row_result.add_key_values();
  kv_pair->set_key(result_stream->RowName());
  kv_pair->set_column_family(result_stream->Family());
  kv_pair->set_qualifier(result_stream->Qualifier());
  kv_pair->set_timestamp(result_stream->Timestamp());
  kv_pair->set_value(result_stream->Value());
}

struct CompareContext {
  ResultStream* src_result_stream;
  ResultStream* dest_result_stream;
  std::ostringstream& diffdata_only_in_src;
  std::ostringstream& diffdata_only_in_dest;
  std::ostringstream& diffdata_both_have_but_diff;
  DiffStatData* diff_stat_data;
  const std::string& src_table_name;
  const std::map<std::string, std::string>& tables_cf_version_map;
  RowResult row_result_only_in_src;
  Table* dest_table;
  Counter counter;

  CompareContext(ResultStream* src_result_stream, ResultStream* dest_result_stream,
                 std::ostringstream& diffdata_only_in_src,
                 std::ostringstream& diffdata_only_in_dest,
                 std::ostringstream& diffdata_both_have_but_diff, DiffStatData* diff_stat_data,
                 const std::string& src_table_name,
                 const std::map<std::string, std::string>& tables_cf_version_map, Table* dest_table)
      : src_result_stream(src_result_stream),
        dest_result_stream(dest_result_stream),
        diffdata_only_in_src(diffdata_only_in_src),
        diffdata_only_in_dest(diffdata_only_in_dest),
        diffdata_both_have_but_diff(diffdata_both_have_but_diff),
        diff_stat_data(diff_stat_data),
        src_table_name(src_table_name),
        tables_cf_version_map(tables_cf_version_map),
        dest_table(dest_table) {}
  virtual ~CompareContext() { row_result_only_in_src.clear_key_values(); }
};

void WriteDiffToDest(CompareContext& ctx) {
  g_sem->Acquire();

  RewriteContext* wctx = new RewriteContext;

  wctx->hold_kv_pair = true;
  KeyValuePair* kv_pair = new KeyValuePair();

  kv_pair->set_key(ctx.src_result_stream->RowName());
  kv_pair->set_column_family(ctx.src_result_stream->Family());
  kv_pair->set_qualifier(ctx.src_result_stream->Qualifier());
  kv_pair->set_timestamp(ctx.src_result_stream->Timestamp());
  kv_pair->set_value(ctx.src_result_stream->Value());

  RowMutation* mu = ctx.dest_table->NewRowMutation(kv_pair->key());
  mu->Put(kv_pair->column_family(), kv_pair->qualifier(), kv_pair->value(), kv_pair->timestamp());
  ctx.counter.Inc();
  wctx->counter = &(ctx.counter);
  wctx->target_table = ctx.dest_table;
  wctx->kv_pair = kv_pair;
  mu->SetContext(wctx);
  mu->SetCallBack(RewriteCallBack);
  ctx.dest_table->ApplyMutation(mu);
}

void SrcRecordAndNext(CompareContext& ctx) {
  if (FLAGS_enable_write_dfs_diff_only_in_src) {
    WriteDiffDataToStream(ctx.diffdata_only_in_src, ctx.src_result_stream);
  }
  if (FLAGS_enable_write_dfs_diffbin_only_in_src) {
    AddDiffRowResult(ctx.row_result_only_in_src, ctx.src_result_stream);
  }
  if (FLAGS_enable_write_diff_only_in_src_to_dest) {
    WriteDiffToDest(ctx);
  }
  ctx.diff_stat_data->only_in_src++;
  ctx.src_result_stream->Next();
}

void DestRecordAndNext(CompareContext& ctx) {
  if (FLAGS_enable_write_dfs_diff_only_in_dest) {
    WriteDiffDataToStream(ctx.diffdata_only_in_dest, ctx.dest_result_stream);
  }
  ctx.diff_stat_data->only_in_dest++;
  ctx.dest_result_stream->Next();
}

void HandleNotEqual(int comp_res, CompareContext& ctx) {
  if (comp_res < 0) {
    SrcRecordAndNext(ctx);
  } else if (comp_res > 0) {
    DestRecordAndNext(ctx);
  }
}

void CompareValue(CompareContext& ctx) {
  if (ctx.src_result_stream->Value().compare(ctx.dest_result_stream->Value()) != 0) {
    if (FLAGS_enable_write_dfs_diff_both_have_but_diff) {
      WriteDiffDataToStream(ctx.diffdata_both_have_but_diff, ctx.src_result_stream);
    }
    ctx.diff_stat_data->both_have_but_diff++;
  } else {
    ctx.diff_stat_data->both_have_and_same++;
  }
  ctx.src_result_stream->Next();
  ctx.dest_result_stream->Next();
}

void CompareTimestamp(CompareContext& ctx) {
  int comp_res = 0;
  if (ctx.src_result_stream->Timestamp() > ctx.dest_result_stream->Timestamp()) {
    comp_res = -1;
  } else if (ctx.src_result_stream->Timestamp() < ctx.dest_result_stream->Timestamp()) {
    comp_res = 1;
  }
  if (comp_res != 0) {
    HandleNotEqual(comp_res, ctx);
    VLOG(1) << "[diff] timestamp is diff";
  } else {
    CompareValue(ctx);
  }
}

void CompareQualifier(CompareContext& ctx) {
  int comp_res = ctx.src_result_stream->Qualifier().compare(ctx.dest_result_stream->Qualifier());
  if (comp_res != 0) {
    HandleNotEqual(comp_res, ctx);
    VLOG(1) << "[diff] qualifier is diff";
  } else {
    if (IsMultiVersion(ctx.src_table_name, ctx.src_result_stream->Family(),
                       ctx.tables_cf_version_map)) {
      CompareTimestamp(ctx);
    } else {
      CompareValue(ctx);
    }
  }
}

void CompareFamily(CompareContext& ctx) {
  int comp_res = ctx.src_result_stream->Family().compare(ctx.dest_result_stream->Family());
  if (comp_res != 0) {
    HandleNotEqual(comp_res, ctx);
    VLOG(1) << "[diff] family is diff";
  } else {
    CompareQualifier(ctx);
  }
}

void CompareRowName(CompareContext& ctx) {
  int comp_res = ctx.src_result_stream->RowName().compare(ctx.dest_result_stream->RowName());
  if (comp_res != 0) {
    HandleNotEqual(comp_res, ctx);
    VLOG(1) << "[diff] rowkey is diff";
  } else {
    CompareFamily(ctx);
  }
}

int StartScan(Table* table, const std::string& table_name, const std::string& start_key,
              const std::string& end_key, const std::vector<std::string>& cfs,
              const std::string& cluster, ResultStream** result_stream) {
  ErrorCode err;

  std::string raw_start_str;
  std::string raw_end_str;
  if (FLAGS_readable) {
    if (!ParseDebugString(start_key, &raw_start_str) || !ParseDebugString(end_key, &raw_end_str)) {
      LOG(WARNING) << "Parse debug string failed!";
      return -1;
    }
  } else {
    raw_start_str = start_key;
    raw_end_str = end_key;
  }

  ScanDescriptor desc(raw_start_str);
  desc.SetEnd(raw_end_str);
  desc.SetMaxVersions(std::numeric_limits<int>::max());
  if (FLAGS_dump_endtime != 0) {
    desc.SetTimeRange(FLAGS_dump_endtime, FLAGS_dump_startime);
  }
  std::for_each(cfs.cbegin(), cfs.cend(),
                [&desc](const std::string& cf) { desc.AddColumnFamily(cf); });
  if ((*result_stream = table->Scan(desc, &err)) == NULL) {
    LOG(WARNING) << cluster << " start scan fail: " << table_name << ", start " << start_key
                 << ", end " << end_key << ", reason " << err.GetReason();
    return -1;
  }
  return 0;
}

bool IsScanFinish(const std::string& src_table_name, const std::string& dest_table_name,
                  const std::string& start_key, const std::string& end_key,
                  ResultStream* src_result_stream, ResultStream* dest_result_stream, bool* src_done,
                  bool* dest_done, int* res) {
  ErrorCode src_err;
  ErrorCode dest_err;

  *src_done = src_result_stream->Done(&src_err);
  *dest_done = dest_result_stream->Done(&dest_err);

  if (src_err.GetType() != tera::ErrorCode::kOK) {
    LOG(WARNING) << "src scan fail: " << src_table_name << ", start " << start_key << ", end "
                 << end_key << ", reason " << src_err.GetReason();
    *res = -1;
    return true;
  }
  if (dest_err.GetType() != tera::ErrorCode::kOK) {
    LOG(WARNING) << "dest scan fail: " << dest_table_name << ", start " << start_key << ", end "
                 << end_key << ", reason " << dest_err.GetReason();
    *res = -1;
    return true;
  }

  if (*src_done && *dest_done) {
    return true;
  }

  return false;
}

int ScanAndDiffData(Table* src, Table* dest, const std::string& src_table_name,
                    const std::string& dest_table_name, const std::string& tablet_id,
                    const std::string& start_key, const std::string& end_key,
                    const std::map<std::string, std::string>& tables_cf_map,
                    const std::map<std::string, std::string>& tables_cf_version_map,
                    DiffStatData* diff_stat_data) {
  int res = 0;

  diff_stat_data->reset();

  std::vector<std::string> cfs;
  auto it = tables_cf_map.find(src_table_name);
  if (it != tables_cf_map.cend()) {
    std::string delimiter(FLAGS_lg_and_cf_delimiter);
    SplitString(it->second, delimiter, &cfs);
  }

  ResultStream* src_result_stream = NULL;
  ResultStream* dest_result_stream = NULL;
  if (StartScan(src, src_table_name, start_key, end_key, cfs, "src", &src_result_stream) < 0) {
    return -1;
  }
  if (StartScan(dest, dest_table_name, start_key, end_key, cfs, "dest", &dest_result_stream) < 0) {
    delete src_result_stream;
    return -1;
  }

  std::ostringstream diffdata_only_in_src, diffdata_only_in_dest, diffdata_both_have_but_diff;

  CompareContext ctx(src_result_stream, dest_result_stream, diffdata_only_in_src,
                     diffdata_only_in_dest, diffdata_both_have_but_diff, diff_stat_data,
                     src_table_name, tables_cf_version_map, dest);
  ctx.counter.Inc();
  uint64_t cnt = 0;
  while (true) {
    bool src_done = false;
    bool dest_done = false;

    if (FLAGS_diff_scan_count_per_interval > 0) {
      cnt++;
      if (cnt % FLAGS_diff_scan_count_per_interval == 0) {
        ThisThread::Sleep(FLAGS_diff_scan_interval_ns);
      }
    }

    if (IsScanFinish(src_table_name, dest_table_name, start_key, end_key, src_result_stream,
                     dest_result_stream, &src_done, &dest_done, &res)) {
      break;
    }

    diff_stat_data->in_src_or_in_dest++;
    if (src_done && !dest_done) {
      VLOG(1) << "dest: [" << dest_result_stream->RowName() << "] [" << dest_result_stream->Family()
              << "] [" << dest_result_stream->Qualifier() << "] ["
              << dest_result_stream->Timestamp() << "]";
      DestRecordAndNext(ctx);
      VLOG(1) << "[diff] src_done";
    } else if (!src_done && dest_done) {
      VLOG(1) << "src: [" << src_result_stream->RowName() << "] [" << src_result_stream->Family()
              << "] [" << src_result_stream->Qualifier() << "] [" << src_result_stream->Timestamp()
              << "]";
      SrcRecordAndNext(ctx);
      VLOG(1) << "[diff] dest_done";
    } else {  // !src_done && !dest_done
      VLOG(1) << "src: [" << src_result_stream->RowName() << "] [" << src_result_stream->Family()
              << "] [" << src_result_stream->Qualifier() << "] [" << src_result_stream->Timestamp()
              << "], dest: [" << dest_result_stream->RowName() << "] ["
              << dest_result_stream->Family() << "] [" << dest_result_stream->Qualifier() << "] ["
              << dest_result_stream->Timestamp() << "]";
      CompareRowName(ctx);
    }
  }
  delete src_result_stream;
  delete dest_result_stream;
  ctx.counter.Dec();
  while (ctx.counter.Get() > 0) {
    sleep(3);
  }

  if (res == 0) {
    std::string file_path;
    if (FLAGS_enable_write_dfs_diff_only_in_src) {
      file_path =
          FLAGS_diff_data_afs_path + "/" + src_table_name + "/" + tablet_id + ".only_in_src";
      WriteToDfs(file_path, diffdata_only_in_src.str());
    }
    if (FLAGS_enable_write_dfs_diff_only_in_dest) {
      file_path =
          FLAGS_diff_data_afs_path + "/" + src_table_name + "/" + tablet_id + ".only_in_dest";
      WriteToDfs(file_path, diffdata_only_in_dest.str());
    }
    if (FLAGS_enable_write_dfs_diff_both_have_but_diff) {
      file_path =
          FLAGS_diff_data_afs_path + "/" + src_table_name + "/" + tablet_id + ".both_have_but_diff";
      WriteToDfs(file_path, diffdata_both_have_but_diff.str());
    }

    if (FLAGS_enable_write_dfs_diffbin_only_in_src) {
      std::string row_result_str;
      if (!SerializationRowResult(ctx.row_result_only_in_src, &row_result_str)) {
        LOG(WARNING) << "row_result_only_in_src serilize failed!";
      } else {
        file_path = FLAGS_diff_bin_data_afs_path + "/" + src_table_name + "/" + tablet_id +
                    ".only_in_src.pbtxt";
        WriteToDfs(file_path, row_result_str);
      }
    }
  }

  return res;
}

int ScanDest(Table* dest, const std::string& src_table_name, const std::string& dest_table_name,
             const std::string& start_key, const std::string& end_key,
             const std::map<std::string, std::string>& tables_cf_map) {
  int res = 0;

  std::vector<std::string> cfs;
  auto it = tables_cf_map.find(src_table_name);
  if (it != tables_cf_map.cend()) {
    std::string delimiter(FLAGS_lg_and_cf_delimiter);
    SplitString(it->second, delimiter, &cfs);
  }

  ResultStream* dest_result_stream = NULL;
  if (StartScan(dest, dest_table_name, start_key, end_key, cfs, "dest", &dest_result_stream) < 0) {
    return -1;
  }

  uint64_t cnt = 0;
  while (true) {
    bool dest_done = false;

    if (FLAGS_diff_scan_count_per_interval > 0) {
      cnt++;
      if (cnt % FLAGS_diff_scan_count_per_interval == 0) {
        ThisThread::Sleep(FLAGS_diff_scan_interval_ns);
      }
    }

    ErrorCode dest_err;
    dest_done = dest_result_stream->Done(&dest_err);
    if (dest_err.GetType() != tera::ErrorCode::kOK) {
      LOG(WARNING) << "dest scan fail: " << dest_table_name << ", start " << start_key << ", end "
                   << end_key << ", reason " << dest_err.GetReason();
      res = -1;
      break;
    }
    if (dest_done) {
      break;
    }
    dest_result_stream->Next();
  }
  delete dest_result_stream;
  return res;
}

int ReleaseAndUnlockDiffRange(const std::string& ins_cluster_root_path,
                              const std::string& src_table_name, const std::string& tablet_id,
                              const std::string& start_key, const std::string& end_key,
                              galaxy::ins::sdk::InsSDK* ins_sdk,
                              const DiffStatData& diff_stat_data) {
  int res = 0;
  galaxy::ins::sdk::SDKError ins_err;
  std::string range_path = ins_cluster_root_path + "/tablet";
  std::string lock_path = ins_cluster_root_path + "/lock";

  std::string key = range_path + "/" + src_table_name + "/" + start_key;
  char stat_str[1024];
  snprintf(stat_str, 1024, "1,%lu,%lu,%lu,%lu,%lu,%s:", diff_stat_data.only_in_src,
           diff_stat_data.only_in_dest, diff_stat_data.both_have_but_diff,
           diff_stat_data.both_have_and_same, diff_stat_data.in_src_or_in_dest, tablet_id.c_str());
  LOG(INFO) << "range diff stat: " << stat_str;
  std::string val = stat_str;
  val.append(end_key);

  if (!ins_sdk->Put(key, val, &ins_err)) {
    LOG(WARNING) << "ins put FAILED: " << key << ", error " << ins_err;
  } else {
    LOG(INFO) << "ins put OK: " << key;
  }

  std::string lock_key = lock_path + "/" + src_table_name + "/" + start_key + "/";
  if (!ins_sdk->UnLock(lock_key, &ins_err)) {
    LOG(WARNING) << "ins unlock FAILED: " << lock_key << ", error " << ins_err;
  } else {
    LOG(INFO) << "ins unlock OK: " << lock_key;
  }
  return res;
}

int DiffRunOp() {
  int res = 0;
  std::string ins_cluster_addr = FLAGS_ins_cluster_addr;
  std::string ins_cluster_root_path = FLAGS_ins_cluster_diff_root_path;
  std::string tera_src_conf = FLAGS_dump_tera_src_conf;
  std::string tera_dest_conf = FLAGS_dump_tera_dest_conf;

  ErrorCode err;
  std::unique_ptr<Client> src_client(Client::NewClient(tera_src_conf, &err));
  if (src_client == nullptr) {
    LOG(WARNING) << "DiffRun open src client fail: " << tera_src_conf << ", err " << err.ToString();
    return -1;
  }
  std::unique_ptr<Client> dest_client(Client::NewClient(tera_dest_conf, &err));
  if (dest_client == nullptr) {
    LOG(WARNING) << "DiffRun open dest client fail: " << tera_dest_conf << ", err "
                 << err.ToString();
    return -1;
  }

  LOG(INFO) << "[DiffRun_1] NewClient src and dest OK";

  std::unique_ptr<Table> src_table;
  std::unique_ptr<Table> dest_table;

  galaxy::ins::sdk::InsSDK ins_sdk(ins_cluster_addr);
  std::string src_table_name, dest_table_name, start_key, end_key, last_table_name, tablet_id;

  std::map<std::string, std::string> tables_map;
  std::string path = ins_cluster_root_path + "/" + FLAGS_dump_tables_map_path;
  if (-1 == GetMapFromNexus(path, &ins_sdk, &tables_map)) {
    LOG(WARNING) << "DiffRun GetMapFromNexus tables_map FAILED";
    return -1;
  }

  CHECK(tables_map.size() > 0);

  LOG(INFO) << "[DiffRun_2] GetMapFromNexus tables_map OK";
  LOG(INFO) << "DiffRun tables_map.size(): " << tables_map.size();
  for (auto it = tables_map.cbegin(); it != tables_map.cend(); ++it) {
    LOG(INFO) << "DiffRun src table[" << it->first << "] => dest table[" << it->second << "]";
  }

  std::map<std::string, std::string> tables_cf_map;
  path = ins_cluster_root_path + "/" + FLAGS_dump_tables_cf_map_path;
  if (-1 == GetMapFromNexus(path, &ins_sdk, &tables_cf_map)) {
    LOG(WARNING) << "DiffRun GetMapFromNexus tables_cf_map FAILED";
    return -1;
  }
  LOG(INFO) << "[DiffRun_3] GetMapFromNexus tables_cf_map OK";
  LOG(INFO) << "DiffRun tables_cf_map.size(): " << tables_cf_map.size();
  for (auto it = tables_cf_map.cbegin(); it != tables_cf_map.cend(); ++it) {
    LOG(INFO) << "DiffRun src table[" << it->first << "] => src cf[" << it->second << "]";
  }

  std::map<std::string, std::string> tables_cf_version_map;
  path = ins_cluster_root_path + "/" + FLAGS_dump_tables_cf_version_map_path;
  if (-1 == GetMapFromNexus(path, &ins_sdk, &tables_cf_version_map)) {
    LOG(WARNING) << "DiffRun GetMapFromNexus tables_cf_version_map FAILED";
    return -1;
  }
  CHECK(tables_cf_version_map.size() > 0);
  LOG(INFO) << "[DiffRun_4] GetMapFromNexus tables_cf_version_map OK";
  LOG(INFO) << "DiffRun tables_cf_version_map.size(): " << tables_cf_version_map.size();
  for (auto it = tables_cf_version_map.cbegin(); it != tables_cf_version_map.cend(); ++it) {
    LOG(INFO) << "DiffRun table_cf[" << it->first << "] => is_multi_version[" << it->second << "]";
  }

  if (-1 == InitDfsClient()) {
    LOG(WARNING) << "[DiffRun_5] InitDfsClient FAILED";
    return -1;
  }

  LOG(INFO) << "[DiffRun_5] InitDfsClient OK";

  auto get_tablet_id_func = std::bind(GetTabletIdForDiff, std::placeholders::_1);
  LOG(INFO) << "[DiffRun_6] start diff range by range";
  while (GetAndLockDumpRange(ins_cluster_root_path, &src_table_name, &tablet_id, &start_key,
                             &end_key, &ins_sdk, get_tablet_id_func) == 0) {
    if (last_table_name != src_table_name) {  // table change
      src_table.reset();
      dest_table.reset();
      src_table.reset(src_client->OpenTable(src_table_name, &err));
      if (src_table == nullptr) {
        LOG(WARNING) << "open src table fail: " << src_table_name << ", err " << err.ToString();
        continue;
      }
      if (tables_map.find(src_table_name) != tables_map.cend()) {
        dest_table_name = tables_map[src_table_name];
      } else {
        LOG(WARNING) << "Couldn't find src_table_name[" << src_table_name << "] in tables_map";
        return -1;
      }
      dest_table.reset(dest_client->OpenTable(dest_table_name, &err));
      if (dest_table == nullptr) {
        src_table.reset();
        LOG(WARNING) << "open dest table fail: " << dest_table_name << ", err " << err.ToString();
        continue;
      }
      LOG(INFO) << "start diff new table: " << src_table_name << " vs " << dest_table_name;
    }
    last_table_name = src_table_name;
    DiffStatData diff_stat_data;
    LOG(INFO) << "start diff table " << src_table_name << " vs " << dest_table_name
              << ", new range: start " << start_key << ", end  " << end_key;
    if ((res = ScanAndDiffData(src_table.get(), dest_table.get(), src_table_name, dest_table_name,
                               tablet_id, start_key, end_key, tables_cf_map, tables_cf_version_map,
                               &diff_stat_data)) < 0) {
      LOG(WARNING) << "scan and diff data fail: " << src_table_name << " vs " << dest_table_name
                   << ", start " << start_key << ", end " << end_key;
    } else {
      LOG(INFO) << "Set has_done for start_key[" << start_key << "], end_key[" << end_key << "]";
      ReleaseAndUnlockDiffRange(ins_cluster_root_path, src_table_name, tablet_id, start_key,
                                end_key, &ins_sdk, diff_stat_data);
    }
    start_key = end_key;
  }
  LOG(INFO) << "Finish DiffRunOp";
  return res;
}

int DiffScanDestOp() {
  int res = 0;
  std::string ins_cluster_addr = FLAGS_ins_cluster_addr;
  std::string ins_cluster_root_path = FLAGS_ins_cluster_diff_root_path;
  std::string tera_dest_conf = FLAGS_dump_tera_dest_conf;

  ErrorCode err;
  std::unique_ptr<Client> dest_client(Client::NewClient(tera_dest_conf, &err));
  if (dest_client == nullptr) {
    LOG(WARNING) << "DiffScanDest open dest client fail: " << tera_dest_conf << ", err "
                 << err.ToString();
    return -1;
  }

  LOG(INFO) << "[DiffScanDest_1] NewClient OK";

  std::unique_ptr<Table> dest_table;

  galaxy::ins::sdk::InsSDK ins_sdk(ins_cluster_addr);
  std::string src_table_name, dest_table_name, start_key, end_key, last_table_name, tablet_id;

  std::map<std::string, std::string> tables_map;
  std::string path = ins_cluster_root_path + "/" + FLAGS_dump_tables_map_path;
  if (-1 == GetMapFromNexus(path, &ins_sdk, &tables_map)) {
    LOG(WARNING) << "DiffScanDest GetMapFromNexus tables_map FAILED";
    return -1;
  }

  CHECK(tables_map.size() > 0);

  LOG(INFO) << "[DiffScanDest_2] GetMapFromNexus tables_map OK";
  LOG(INFO) << "DiffScanDest tables_map.size(): " << tables_map.size();
  for (auto it = tables_map.cbegin(); it != tables_map.cend(); ++it) {
    LOG(INFO) << "DiffScanDest src table[" << it->first << "] => dest table[" << it->second << "]";
  }

  std::map<std::string, std::string> tables_cf_map;
  path = ins_cluster_root_path + "/" + FLAGS_dump_tables_cf_map_path;
  if (-1 == GetMapFromNexus(path, &ins_sdk, &tables_cf_map)) {
    LOG(WARNING) << "DiffScanDest GetMapFromNexus tables_cf_map FAILED";
    return -1;
  }
  LOG(INFO) << "[DiffScanDest_3] GetMapFromNexus tables_cf_map OK";
  LOG(INFO) << "DiffScanDest tables_cf_map.size(): " << tables_cf_map.size();
  for (auto it = tables_cf_map.cbegin(); it != tables_cf_map.cend(); ++it) {
    LOG(INFO) << "DiffScanDest src table[" << it->first << "] => src cf[" << it->second << "]";
  }

  if (-1 == InitDfsClient()) {
    LOG(WARNING) << "[DiffScanDest_4] InitDfsClient FAILED";
    return -1;
  }

  LOG(INFO) << "[DiffScanDest_4] InitDfsClient OK";

  auto get_tablet_id_func = std::bind(GetTabletIdForDiff, std::placeholders::_1);
  LOG(INFO) << "[DiffScanDest_5] start diff range by range";
  while (GetAndLockDumpRange(ins_cluster_root_path, &src_table_name, &tablet_id, &start_key,
                             &end_key, &ins_sdk, get_tablet_id_func) == 0) {
    if (last_table_name != src_table_name) {  // table change
      dest_table.reset();
      if (tables_map.find(src_table_name) != tables_map.cend()) {
        dest_table_name = tables_map[src_table_name];
      } else {
        LOG(WARNING) << "Couldn't find src_table_name[" << src_table_name << "] in tables_map";
        return -1;
      }
      dest_table.reset(dest_client->OpenTable(dest_table_name, &err));
      if (dest_table == nullptr) {
        LOG(WARNING) << "open dest table fail: " << dest_table_name << ", err " << err.ToString();
        continue;
      }
      LOG(INFO) << "start scan new table: " << dest_table_name;
    }
    last_table_name = src_table_name;
    DiffStatData diff_stat_data;
    diff_stat_data.reset();
    LOG(INFO) << "start scan table " << dest_table_name << ", new range: start " << start_key
              << ", end  " << end_key;
    if ((res = ScanDest(dest_table.get(), src_table_name, dest_table_name, start_key, end_key,
                        tables_cf_map)) < 0) {
      LOG(WARNING) << "scan fail: " << dest_table_name << ", start " << start_key << ", end "
                   << end_key;
    } else {
      LOG(INFO) << "Set has_done for start_key[" << start_key << "], end_key[" << end_key << "]";
      ReleaseAndUnlockDiffRange(ins_cluster_root_path, src_table_name, tablet_id, start_key,
                                end_key, &ins_sdk, diff_stat_data);
    }
    start_key = end_key;
  }
  LOG(INFO) << "Finish DiffScanDestOp";
  return res;
}

int StatProgress(const std::string& ins_cluster_root_path, galaxy::ins::sdk::InsSDK* ins_sdk,
                 const std::map<std::string, std::string>& tables_map,
                 std::map<std::string, Progress>* stat_res) {
  int res = 0;
  stat_res->clear();

  std::string range_path = ins_cluster_root_path + "/tablet";

  std::string start = range_path + "/";
  std::string end = range_path + "/";
  end.append(1, '\255');

  galaxy::ins::sdk::ScanResult* result = ins_sdk->Scan(start, end);
  while (!result->Done()) {
    if (result->Error() != galaxy::ins::sdk::kOK) {
      LOG(INFO) << "scan fail: start " << start << ", end " << end << ", err " << result->Error();
      res = -1;
      break;
    }

    std::string key = result->Key();
    std::string val = result->Value();

    std::string str = key.substr(range_path.length() + 1);
    std::size_t pos = str.find('/');
    std::string src_table_name = str.substr(0, pos);

    if (tables_map.find(src_table_name) == tables_map.cend()) {
      result->Next();
      continue;
    }

    std::string has_done = val.substr(0, 1);
    if (stat_res->find(src_table_name) == stat_res->cend()) {
      (*stat_res)[src_table_name].finish_range_num = 0;
      (*stat_res)[src_table_name].total_range_num = 1;
    } else {
      (*stat_res)[src_table_name].total_range_num += 1;
    }

    if (has_done == "1") {
      (*stat_res)[src_table_name].finish_range_num += 1;
    }
    result->Next();
  }
  delete result;
  return res;
}

int PrintStatProgrssInfo(const std::string& ins_cluster_root_path) {
  std::string ins_cluster_addr = FLAGS_ins_cluster_addr;
  galaxy::ins::sdk::InsSDK ins_sdk(ins_cluster_addr);
  std::map<std::string, std::string> tables_map;
  std::string path = ins_cluster_root_path + "/" + FLAGS_dump_tables_map_path;
  if (-1 == GetMapFromNexus(path, &ins_sdk, &tables_map)) {
    LOG(WARNING) << "GetTablesMap failed in ShowProgress";
    return -1;
  }

  CHECK(tables_map.size() > 0);

  std::map<std::string, Progress> stat_res;
  if (StatProgress(ins_cluster_root_path, &ins_sdk, tables_map, &stat_res) < 0) {
    return -1;
  }
  for (auto it = stat_res.cbegin(); it != stat_res.cend(); ++it) {
    printf("%-20s%6d / %-6d\n", it->first.c_str(), it->second.finish_range_num,
           it->second.total_range_num);
  }

  return 0;
}

int DumpProgressOp() {
  std::string ins_cluster_root_path = FLAGS_ins_cluster_dump_root_path;
  if (-1 == PrintStatProgrssInfo(ins_cluster_root_path)) {
    LOG(WARNING) << "Print stat progress failed";
    return -1;
  }
  return 0;
}

int DiffProgressOp() {
  std::string ins_cluster_root_path = FLAGS_ins_cluster_diff_root_path;
  if (-1 == PrintStatProgrssInfo(ins_cluster_root_path)) {
    LOG(WARNING) << "Print stat progress failed";
    return -1;
  }
  return 0;
}

int StatDiffResult(const std::string& ins_cluster_root_path, galaxy::ins::sdk::InsSDK* ins_sdk,
                   const std::map<std::string, std::string>& tables_map,
                   std::map<std::string, DiffStatData>* stat_res) {
  int res = 0;
  stat_res->clear();

  std::string range_path = ins_cluster_root_path + "/tablet";

  std::string start = range_path + "/";
  std::string end = range_path + "/";
  end.append(1, '\255');

  galaxy::ins::sdk::ScanResult* result = ins_sdk->Scan(start, end);
  while (!result->Done()) {
    if (result->Error() != galaxy::ins::sdk::kOK) {
      LOG(INFO) << "scan fail: start " << start << ", end " << end << ", err " << result->Error();
      res = -1;
      break;
    }

    std::string key = result->Key();
    std::string val = result->Value();

    std::string str = key.substr(range_path.length() + 1);
    std::size_t pos = str.find('/');
    std::string src_table_name = str.substr(0, pos);

    if (tables_map.find(src_table_name) == tables_map.cend()) {
      result->Next();
      continue;
    }

    std::string has_done = val.substr(0, 1);
    if (has_done != "1") {
      LOG(WARNING) << "still have range not finish diff running";
      res = -1;
      break;
    }

    if (stat_res->find(src_table_name) == stat_res->cend()) {
      (*stat_res)[src_table_name].reset();
    }

    pos = val.find(':');
    std::string stat_res_str = val.substr(0, pos);
    std::vector<std::string> res_datas;
    SplitString(stat_res_str, ",", &res_datas);
    (*stat_res)[src_table_name].only_in_src += strtoul(res_datas[1].c_str(), NULL, 10);
    (*stat_res)[src_table_name].only_in_dest += strtoul(res_datas[2].c_str(), NULL, 10);
    (*stat_res)[src_table_name].both_have_but_diff += strtoul(res_datas[3].c_str(), NULL, 10);
    (*stat_res)[src_table_name].both_have_and_same += strtoul(res_datas[4].c_str(), NULL, 10);
    (*stat_res)[src_table_name].in_src_or_in_dest += strtoul(res_datas[5].c_str(), NULL, 10);

    result->Next();
  }
  delete result;
  return res;
}

int DiffResultOp() {
  int res = 0;
  std::string ins_cluster_addr = FLAGS_ins_cluster_addr;
  std::string ins_cluster_root_path = FLAGS_ins_cluster_diff_root_path;

  galaxy::ins::sdk::InsSDK ins_sdk(ins_cluster_addr);
  std::map<std::string, std::string> tables_map;
  std::string path = ins_cluster_root_path + "/" + FLAGS_dump_tables_map_path;
  if (-1 == GetMapFromNexus(path, &ins_sdk, &tables_map)) {
    LOG(WARNING) << "GetTablesMap failed in ShowProgress";
    return -1;
  }

  CHECK(tables_map.size() > 0);

  std::map<std::string, DiffStatData> stat_res;
  if (StatDiffResult(ins_cluster_root_path, &ins_sdk, tables_map, &stat_res) < 0) {
    return -1;
  }
  printf("%-20s%20s%20s%20s%20s%20s%20s\n", "table_name", "only_in_src", "only_in_dest",
         "both_have_but_diff", "both_have_and_same", "in_src_or_in_dest", "diff_rate");
  for (auto it = stat_res.cbegin(); it != stat_res.cend(); ++it) {
    double diff_rate = 0.0;
    if (it->second.in_src_or_in_dest) {
      diff_rate = 1.0 - 1.0 * it->second.both_have_and_same / it->second.in_src_or_in_dest;
    }
    printf("%-20s%20lu%20lu%20lu%20lu%20lu%19.6f\n", it->first.c_str(), it->second.only_in_src,
           it->second.only_in_dest, it->second.both_have_but_diff, it->second.both_have_and_same,
           it->second.in_src_or_in_dest, diff_rate);
  }

  return res;
}

int main(int argc, char* argv[]) {
  FLAGS_minloglevel = 2;
  ::google::ParseCommandLineFlags(&argc, &argv, true);
  if (FLAGS_flagfile == "") {
    FLAGS_flagfile = "../conf/terautil.flag";
    if (access(FLAGS_flagfile.c_str(), R_OK) != 0) {
      FLAGS_flagfile = "./terautil.flag";
    }
    utils::LoadFlagFile(FLAGS_flagfile);
  }

  std::string log_prefix = "terautil";
  ::google::InitGoogleLogging(log_prefix.c_str());
  utils::SetupLog(log_prefix);
  if (FLAGS_tera_info_log_clean_enable) {
    common::LogCleaner::StartCleaner();
    LOG(INFO) << "start log cleaner";
  } else {
    LOG(INFO) << "log cleaner is disable";
  }
  Client::SetGlogIsInitialized();

  if (argc == 2 && std::string(argv[1]) == "version") {
    PrintSystemVersion();
  } else if (argc > 2) {
    std::string op(argv[1]);
    std::string cmd(argv[2]);
    if (op == "dump" && cmd == "prepare") {
      return DumpPrepareOp();
    } else if (op == "dump" && cmd == "prepare_safe") {
      return DumpPrepareSafeOp();
    } else if (op == "dump" && cmd == "load") {
      return LoadTablesMapOp();
    } else if (op == "dump" && cmd == "prepare_tables") {
      return DumpPrepareTablesOp();
    } else if (op == "dump" && cmd == "run") {
      g_sem = new common::Semaphore(FLAGS_dump_concurrent_limit);
      return DumpRunOp();
    } else if (op == "dump" && cmd == "rewrite") {
      g_sem = new common::Semaphore(FLAGS_dump_concurrent_limit);
      return DumpRewriteOp();
    } else if (op == "dump" && cmd == "read") {
      if (argc != 4) {
        std::cout << "leak argument" << std::endl;
        return -1;
      }
      std::string afs_file_path(argv[3]);
      return DumpReadOp(afs_file_path);
    } else if (op == "dump" && cmd == "clean") {
      return DumpCleanOp();
    } else if (op == "dump" && cmd == "progress") {
      return DumpProgressOp();
    } else if (op == "dump" && cmd == "ut") {
      return DumpUtOp();
    } else if (op == "diff" && cmd == "prepare") {
      return DiffPrepareOp();
    } else if (op == "diff" && cmd == "run") {
      g_sem = new common::Semaphore(FLAGS_write_only_in_src_to_dest_concurrent_limit);
      return DiffRunOp();
    } else if (op == "diff" && cmd == "scan_dest") {
      return DiffScanDestOp();
    } else if (op == "diff" && cmd == "progress") {
      return DiffProgressOp();
    } else if (op == "diff" && cmd == "result") {
      return DiffResultOp();
    } else if (op == "diff" && cmd == "clean") {
      return DiffCleanOp();
    } else {
      HelpOp(argc, argv);
      return -1;
    }
  } else {
    HelpOp(argc, argv);
    return -1;
  }
  return 0;
}
