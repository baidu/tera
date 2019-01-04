// Copyright (c) 2018, Baidu.com, Inc. All Rights Reserved
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
#include <stdexcept>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "ins_sdk.h"
#include "common/base/string_ext.h"
#include "common/base/string_number.h"
#include "common/base/string_format.h"
#include "common/console/progress_bar.h"
#include "common/file/file_path.h"
#include "common/timer.h"
#include "io/coding.h"
#include "proto/kv_helper.h"
#include "proto/proto_helper.h"
#include "proto/tabletnode.pb.h"
#include "proto/tabletnode_client.h"
#include "proto/table_meta.pb.h"
#include <readline/history.h>
#include <readline/readline.h>
#include "db/dbformat.h"
#include "db/filename.h"
#include "db/log_reader.h"
#include "db/log_writer.h"
#include "db/version_set.h"
#include "db/version_edit.h"
#include "db/write_batch_internal.h"
#include "leveldb/env.h"
#include "leveldb/env_dfs.h"
#include "leveldb/iterator.h"
#include "leveldb/options.h"
#include "leveldb/status.h"
#include "leveldb/table.h"
#include "sdk/client_impl.h"
#include "sdk/cookie.h"
#include "sdk/sdk_utils.h"
#include "sdk/sdk_zk.h"
#include "sdk/table_impl.h"
#include "master/master_impl.h"
#include "tera.h"
#include "types.h"
#include "leveldb/dfs.h"
#include "util/nfs.h"
#include "util/hdfs.h"
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
DECLARE_string(tera_tabletnode_path_prefix);

DEFINE_string(meta_cli_token, "",
              "Only be executed for the guys who has the token. \
                                   Please figure out what metacli is before use it.");
DEFINE_bool(readable, true, "readable input");
DEFINE_bool(make_sure_manual, true, "input Y manual");

DECLARE_string(tera_leveldb_env_dfs_type);
DECLARE_string(tera_leveldb_env_nfs_mountpoint);
DECLARE_string(tera_leveldb_env_nfs_conf_path);
DECLARE_string(tera_leveldb_env_hdfs2_nameservice_list);
DECLARE_string(tera_dfs_so_path);
DECLARE_string(tera_dfs_conf);

using namespace tera;

#define TABLET_NUM_LEN 8
#define TABLET_NAME_LEN 14
#define MANIFEST_LEN 16

std::vector<std::string> g_tables;
// map_bak - backup, map_scan - scan, map_diff - diff
// <path,<startkey,endkey>> map_bak
// <path,<startkey,endkey>> map_diff
// <startkey,<endkey,path>> map_scan
std::map<std::string, std::pair<std::string, std::string>> g_map_bak;
Mutex g_output_lock;
uint32_t g_thread_count = 0;
uint32_t g_diff_count = 0;
leveldb::Dfs* g_dfs = NULL;
leveldb::Env* g_env = NULL;

class RestoreReporter : public leveldb::log::Reader::Reporter {
  std::string s_;

 public:
  virtual void Corruption(size_t bytes, const leveldb::Status& s) { s_ = s.ToString(); }
};

enum class CliStatus { kOk, kError, kNotFound };

using CommandTable =
    std::map<std::string, CliStatus (*)(Client*, int32_t, std::string*, ErrorCode*)>;

namespace {
volatile uint64_t g_sequence_id = 0;
const std::string g_metacli_token("2862933555777941757");
}

const char* builtin_cmd_list[] = {
    "get",
    "get                                                                                          \n\
        <table_name> <start_key>                                                                  \n\
           get one tablet meta info in meta table                                                 \n\
        inmem <table_name> <start_key>                                                            \n\
           get one tablet meta info in master memory",
    "show",
    "show                                                                                         \n\
        [start_key] [end_key]                                                                     \n\
           show meta info in meta table, support specify KeyRange                                 \n\
           e.g. show \"table1#\\\\x00\'n\\\\x842\" \"table1#\\\\x00K\\\\x85\"                     \n\
        inmem [start_key] [end_key]                                                               \n\
           show meta info in master memory, support specify KeyRange                              \n\
           e.g. check \"table1#\\\\x00\'n\\\\x842\" \"table1#\\\\x00K\\\\x85\"",
    "healthcheck",
    "healthcheck                                                                                  \n\
        [start_key] [end_key]                                                                     \n\
           health check all meta info in meta table, support specify KeyRange                     \n\
        inmem                                                                                     \n\
           health check all meta info in master memory",
    "backup",
    "backup                                                                                       \n\
        [filename]                                                                                \n\
           backup meta info in meta table to file, default to meta.bak with timestamp             \n\
        inmem [filename]                                                                          \n\
           backup meta info in meta table to file, default to inmem_meta.bak with timestamp",
    "modify",
    "modify                                                                                       \n\
        <table_name> <row_key> endkey <end_key>                                                   \n\
           modify the value of key_end                                                            \n\
        <table_name> <row_key>  dest_ts <hostname>                                                \n\
           modify the host of an tablet, hostname should with port                                \n\
           e.g. modify test_table \'\\x00abc\'  dest_ts  yq01.baidu.com:2002",
    "delete",
    "delete                                                                                       \n\
        delete <table_name> <start_key>                                                           \n\
           delete the table_name+row_key in meta_table                                            \n\
           e.g. delete test_table1 \'\\x00abc\' ",
    "put",
    "put                                                                                          \n\
        put <tablet_path> <start_key> <end_key> <server_addr>                                     \n\
           insert one TabletMeta into meta table                                                  \n\
           e.g. put table1/tablet00000019 \'\' \'\\x00abc\' yq01.baidu.com:2002",
    "diff",
    "diff                                                                                         \n\
         <filename>                                                                               \n\
           scan all the tables and diff with meta backup file                                     \n\
           e.g. diff meta.bak_20180926-20:55:32                                                   \n\
         <table_name>  <filename>                                                                 \n\
           scan the given table and diff with meta backup file                                    \n\
           e.g. diff test1 meta.bak_20180926-20:55:32                                             \n\
         <table_name/tablet_name> <filename>                                                      \n\
           scan the given tablet and diff with meta backup file                                   \n\
           e.g. diff test1/tablet00000001 meta.bak_20180926-20:55:32",
    "conv",
    "conv                                                                                         \n\
         <unreadable_key>                                                                         \n\
           unreadable key convert to readable key",
    "ugi",
    "ugi                                                                                          \n\
        update <user_name> <passwd>                                                               \n\
            add/update ugi(user_name&passwd)                                                      \n\
        del <user_name>                                                                           \n\
            delete ugi                                                                            \n\
        show                                                                                      \n\
            list ugis",

    "role",
    "role                                                                                         \n\
        add <role_name>                                                                           \n\
            add a role account                                                                    \n\
        del <role_name>                                                                           \n\
            delete a role account                                                                 \n\
        grant <role_name> <user_name>                                                             \n\
            grant the role to user                                                                \n\
        revoke <role_name> <user_name>                                                            \n\
            revoke the role from user                                                             \n\
        show                                                                                      \n\
            list roles",

    "auth",
    "auth                                                                                         \n\
        set <table_name> <auth_policy>                                                            \n\
            auth_policy=none/ugi/giano, which would decide Table how to access.                   \n\
        show                                                                                      \n\
            list all table=>auth_policy",

    "procedure-limit",
    "procedure-limit                                                                              \n\
        get                                                                                       \n\
            show the current limit of all procedures                                              \n\
        set <procedure> <limit>                                                                   \n\
            procedure = [kMerge, kSplit, kMove, kLoad, kUnload]                                   \n\
            limit shoud be a non-negative number",

    "help",
    "help [cmd]                                                                                   \n\
          show manual for a or all cmd(s)",

    "version",
    "version                                                                                      \n\
             show version info",

    "dfs-throughput-limit",
    "dfs-throughput-limit <cmd> [args]                                                                  \n\
                    get   Get current dfs hard limit info                                         \n\
                    write <limit_value>                                                           \n\
                    read <limit_value>                                                            \n\
                    limit_value: [limit bytes|'reset'] cluster dfs hard limit in bytes or reset limit.",
};

static void PrintCmdHelpInfo(const char* msg) {
  if (msg == NULL) {
    return;
  }
  int count = sizeof(builtin_cmd_list) / sizeof(char*);
  for (int i = 0; i < count; i += 2) {
    if (strncmp(msg, builtin_cmd_list[i], 32) == 0) {
      std::cout << builtin_cmd_list[i + 1] << std::endl;
      return;
    }
  }
}

bool ParseCommand(int argc, char** arg_list, std::vector<std::string>* parsed_arg_list) {
  for (int i = 0; i < argc; i++) {
    std::string parsed_arg = arg_list[i];
    if (FLAGS_readable && !ParseDebugString(arg_list[i], &parsed_arg)) {
      std::cout << "invalid debug format of argument: " << arg_list[i] << std::endl;
      return false;
    }
    parsed_arg_list->push_back(parsed_arg);
  }
  return true;
}

static void PrintCmdHelpInfo(const std::string& msg) { PrintCmdHelpInfo(msg.c_str()); }

static CommandTable& GetCommandTable() {
  static CommandTable command_table;
  return command_table;
}

static void PrintAllCmd() {
  std::cout << "there is cmd list:" << std::endl;
  int count = sizeof(builtin_cmd_list) / sizeof(char*);
  bool newline = false;
  for (int i = 0; i < count; i += 2) {
    std::cout << std::setiosflags(std::ios::left) << std::setw(20) << builtin_cmd_list[i];
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

CliStatus HelpOp(Client*, int32_t argc, std::string* argv, ErrorCode*) {
  if (argc == 2) {
    PrintAllCmd();
  } else if (argc == 3) {
    PrintCmdHelpInfo(argv[2]);
  } else {
    PrintCmdHelpInfo("help");
  }
  return CliStatus::kOk;
}

CliStatus HelpOp(int32_t argc, char** argv) {
  std::vector<std::string> argv_svec(argv, argv + argc);
  return HelpOp(NULL, argc, &argv_svec[0], NULL);
}

CliStatus CheckAndParseDebugString(const std::string& debug_str, std::string* raw_str) {
  if (FLAGS_readable) {
    raw_str->clear();
    if (!ParseDebugString(debug_str, raw_str)) {
      LOG(ERROR) << "invalid debug format: " << debug_str;
      return CliStatus::kError;
    }
  } else {
    *raw_str = debug_str;
  }
  return CliStatus::kOk;
}

static void PrintMetaInfo(const TabletMeta* meta) {
  std::cout << "tablet: " << meta->table_name() << " [" << meta->key_range().key_start() << " ("
            << DebugString(meta->key_range().key_start()) << "), " << meta->key_range().key_end()
            << " (" << DebugString(meta->key_range().key_end()) << ")], " << meta->path() << ", "
            << meta->server_addr() << ", " << meta->size() << ", "
            << StatusCodeToString(meta->status()) << ", "
            << StatusCodeToString(meta->compact_status()) << std::endl;
}

CliStatus GetMetaValue(const std::string& meta_server, common::ThreadPool* thread_pool,
                       const std::string& table_name, const std::string& start_key,
                       TableMeta* table_meta, TabletMeta* tablet_meta) {
  tabletnode::TabletNodeClient read_meta_client(thread_pool, meta_server);
  ReadTabletRequest read_request;
  ReadTabletResponse read_response;
  read_request.set_sequence_id(g_sequence_id++);
  read_request.set_tablet_name(FLAGS_tera_master_meta_table_name);
  RowReaderInfo* row_info = read_request.add_row_info_list();
  MakeMetaTableKey(table_name, start_key, row_info->mutable_key());
  if (!read_meta_client.ReadTablet(&read_request, &read_response)) {
    std::cout << "read tablet failed" << std::endl;
    return CliStatus::kError;
  }
  StatusCode err = read_response.status();
  if (err != tera::kTabletNodeOk) {
    std::cerr << "Read meta table response not kTabletNodeOk!";
    return CliStatus::kError;
  }
  if (read_response.detail().row_result_size() <= 0 ||
      read_response.detail().row_result(0).key_values_size() <= 0) {
    std::cout << "Couldn't read table[" << table_name << "] start_key[" << start_key
              << "], suitable for put tablet_meta" << std::endl;
    return CliStatus::kNotFound;
  }
  const KeyValuePair& record = read_response.detail().row_result(0).key_values(0);
  char first_key_char = record.key()[0];
  if (first_key_char == '~') {
    std::cout << "(user: " << record.key().substr(1) << ")" << std::endl;
  } else if (first_key_char == '|') {
    // user&passwd&role&permission
  } else if (first_key_char == '@') {
    ParseMetaTableKeyValue(record.key(), record.value(), table_meta);
    std::cout << "ok, you find a table meta info" << std::endl;
  } else if (first_key_char > '@') {
    ParseMetaTableKeyValue(record.key(), record.value(), tablet_meta);
  } else {
    std::cerr << "invalid record: " << record.key();
  }

  if (first_key_char <= '@') {
    std::cout << "couldn't find tablet meta" << std::endl;
    return CliStatus::kNotFound;
  }
  return CliStatus::kOk;
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

CliStatus GetMeta(const std::string& meta_server, common::ThreadPool* thread_pool,
                  const std::string& table_name, const std::string& start_key) {
  TabletMeta tablet_meta;
  TableMeta table_meta;
  if (GetMetaValue(meta_server, thread_pool, table_name, start_key, &table_meta, &tablet_meta) !=
      CliStatus::kOk) {
    std::cout << "wrong tablet input" << std::endl;
    return CliStatus::kError;
  }
  PrintMetaInfo(&tablet_meta);
  return CliStatus::kOk;
}

CliStatus DeleteMetaTablet(const std::string& meta_server, common::ThreadPool* thread_pool,
                           const std::string& table_name, const std::string& start_key) {
  TabletMeta tablet_meta;
  TableMeta table_meta;
  if (GetMetaValue(meta_server, thread_pool, table_name, start_key, &table_meta, &tablet_meta) !=
      CliStatus::kOk) {
    std::cout << "wrong tablet input" << std::endl;
    return CliStatus::kError;
  }
  tabletnode::TabletNodeClient write_meta_client(thread_pool, meta_server);
  WriteTabletRequest write_request;
  WriteTabletResponse write_response;
  write_request.set_sequence_id(g_sequence_id++);
  write_request.set_tablet_name(FLAGS_tera_master_meta_table_name);
  RowMutationSequence* mu_seq = write_request.add_row_list();

  std::cout << "Are you sure delete the tablet meta info?" << std::endl;
  PrintMetaInfo(&tablet_meta);
  if (FLAGS_make_sure_manual && !Confirm()) {
    return CliStatus::kError;
  }

  std::string row_key;
  MakeMetaTableKey(table_name, start_key, &row_key);
  mu_seq->set_row_key(row_key);
  tera::Mutation* mutation = mu_seq->add_mutation_sequence();
  mutation->set_type(tera::kDeleteRow);
  mutation->set_timestamp(kLatestTimestamp);
  if (!write_meta_client.WriteTablet(&write_request, &write_response)) {
    std::cout << "write tablet failed" << std::endl;
    return CliStatus::kError;
  }
  StatusCode err = write_response.status();
  if (err != tera::kTabletNodeOk) {
    std::cerr << "Write meta table response not kTabletNodeOk!";
    return CliStatus::kError;
  }
  return CliStatus::kOk;
}

CliStatus ModifyMetaValue(const std::string& meta_server, common::ThreadPool* thread_pool,
                          const std::string& table_name, const std::string& start_key,
                          const std::string& type, const std::string& value) {
  TabletMeta tablet_meta;
  TableMeta table_meta;
  if (GetMetaValue(meta_server, thread_pool, table_name, start_key, &table_meta, &tablet_meta) !=
      CliStatus::kOk) {
    std::cout << "wrong tablet input" << std::endl;
    return CliStatus::kError;
  }

  tabletnode::TabletNodeClient write_meta_client(thread_pool, meta_server);
  WriteTabletRequest write_request;
  WriteTabletResponse write_response;
  write_request.set_sequence_id(g_sequence_id++);
  write_request.set_tablet_name(FLAGS_tera_master_meta_table_name);
  RowMutationSequence* mu_seq = write_request.add_row_list();

  if (type == "endkey") {
    std::string end_key = value;
    std::cout << "Are you sure modify key_end?" << std::endl;
    std::cout << "[" << tablet_meta.key_range().key_start() << " ("
              << DebugString(tablet_meta.key_range().key_start()) << "), "
              << tablet_meta.key_range().key_end() << " ("
              << DebugString(tablet_meta.key_range().key_end()) << ")] => ";
    tera::KeyRange* key_range = new tera::KeyRange();
    key_range->set_key_start(tablet_meta.key_range().key_start());
    key_range->set_key_end(end_key);

    tablet_meta.clear_key_range();
    tablet_meta.set_allocated_key_range(key_range);
    std::cout << "[" << tablet_meta.key_range().key_start() << " ("
              << DebugString(tablet_meta.key_range().key_start()) << "), "
              << tablet_meta.key_range().key_end() << " ("
              << DebugString(tablet_meta.key_range().key_end()) << ")]" << std::endl;
  } else {
    std::string host = value;
    std::cout << "[" << tablet_meta.key_range().key_start() << "("
              << DebugString(tablet_meta.key_range().key_start()) << "), "
              << tablet_meta.key_range().key_end() << "("
              << DebugString(tablet_meta.key_range().key_end()) << ")]" << std::endl;
    tablet_meta.set_server_addr(host);
  }

  if (FLAGS_make_sure_manual && !Confirm()) {
    return CliStatus::kError;
  }

  std::string row_key;
  MakeMetaTableKey(table_name, start_key, &row_key);
  mu_seq->set_row_key(row_key);
  tera::Mutation* mutation = mu_seq->add_mutation_sequence();
  mutation->set_type(tera::kPut);

  std::string modify_value;
  MakeMetaTableValue(tablet_meta, &modify_value);
  mutation->set_value(modify_value);
  mutation->set_timestamp(kLatestTimestamp);

  if (!write_meta_client.WriteTablet(&write_request, &write_response)) {
    std::cout << "write tablet failed" << std::endl;
    return CliStatus::kError;
  }
  StatusCode err = write_response.status();
  if (err != tera::kTabletNodeOk) {
    std::cerr << "Write meta table response not kTabletNodeOk!";
    return CliStatus::kError;
  }
  return CliStatus::kOk;
}

CliStatus PutTabletMeta(const std::string& meta_server, common::ThreadPool* thread_pool,
                        const std::string& table_name, const std::string& start_key,
                        const std::string& end_key, const std::string& tablet_path,
                        const std::string& server_addr) {
  TabletMeta tablet_meta;
  TableMeta table_meta;
  if (CliStatus::kNotFound !=
      GetMetaValue(meta_server, thread_pool, table_name, start_key, &table_meta, &tablet_meta)) {
    std::cout << "The table#start_key[" << table_name << "#" << DebugString(start_key)
              << "] has exist, wrong tablet input" << std::endl;
    return CliStatus::kError;
  }

  tabletnode::TabletNodeClient write_meta_client(thread_pool, meta_server);
  WriteTabletRequest write_request;
  WriteTabletResponse write_response;
  write_request.set_sequence_id(g_sequence_id++);
  write_request.set_tablet_name(FLAGS_tera_master_meta_table_name);
  RowMutationSequence* mu_seq = write_request.add_row_list();

  tablet_meta.set_table_name(table_name);
  tablet_meta.set_path(tablet_path);
  tablet_meta.set_server_addr(server_addr);
  tablet_meta.set_status(TabletMeta::kTabletOffline);
  tablet_meta.set_size(1000);
  tablet_meta.add_lg_size(1000);

  tera::KeyRange* key_range = new tera::KeyRange();
  key_range->set_key_start(start_key);
  key_range->set_key_end(end_key);
  tablet_meta.set_allocated_key_range(key_range);

  std::string row_key;
  MakeMetaTableKey(table_name, start_key, &row_key);
  mu_seq->set_row_key(row_key);
  tera::Mutation* mutation = mu_seq->add_mutation_sequence();
  mutation->set_type(tera::kPut);

  std::string tablet_meta_value;
  MakeMetaTableValue(tablet_meta, &tablet_meta_value);
  mutation->set_value(tablet_meta_value);
  mutation->set_timestamp(kLatestTimestamp);

  if (!write_meta_client.WriteTablet(&write_request, &write_response)) {
    std::cout << "write tablet failed" << std::endl;
    return CliStatus::kError;
  }
  StatusCode err = write_response.status();
  if (err != tera::kTabletNodeOk) {
    std::cerr << "Write meta table response not kTabletNodeOk!";
    return CliStatus::kError;
  }

  return CliStatus::kOk;
}

void WriteToStream(std::ofstream& ofs, const std::string& key, const std::string& value) {
  uint32_t key_size = key.size();
  uint32_t value_size = value.size();
  ofs.write((char*)&key_size, sizeof(key_size));
  ofs.write(key.data(), key_size);
  ofs.write((char*)&value_size, sizeof(value_size));
  ofs.write(value.data(), value_size);
}

void WriteTable(const TableMeta& meta, std::ofstream& ofs) {
  std::string key, value;
  MakeMetaTableKeyValue(meta, &key, &value);
  WriteToStream(ofs, key, value);
}

void WriteTablet(const TabletMeta& meta, std::ofstream& ofs) {
  std::string key, value;
  MakeMetaTableKeyValue(meta, &key, &value);
  WriteToStream(ofs, key, value);
}

CliStatus ProcessMeta(const std::string& op, const TableMetaList& table_list,
                      const TabletMetaList& tablet_list, const std::string& start_key,
                      const std::string& end_key, const std::string& filename) {
  int32_t table_num = table_list.meta_size();
  int32_t tablet_num = tablet_list.meta_size();
  if (table_num == 0 && tablet_num == 0) {
    std::cout << "meta table is empty" << std::endl;
    return CliStatus::kOk;
  }

  std::ofstream bak;
  if (op == "backup") {
    bak.open(filename, std::ofstream::trunc | std::ofstream::binary);
  }

  for (int32_t i = 0; i < table_num; ++i) {
    const tera::TableMeta& meta = table_list.meta(i);
    if (op == "show") {
      std::cout << "table: " << meta.table_name() << std::endl;
      int32_t lg_size = meta.schema().locality_groups_size();
      for (int32_t lg_id = 0; lg_id < lg_size; lg_id++) {
        const tera::LocalityGroupSchema& lg = meta.schema().locality_groups(lg_id);
        std::cout << " lg" << lg_id << ": " << lg.name() << " (" << lg.store_type() << ", "
                  << lg.compress_type() << ", " << lg.block_size() << ")" << std::endl;
      }
      int32_t cf_size = meta.schema().column_families_size();
      for (int32_t cf_id = 0; cf_id < cf_size; cf_id++) {
        const tera::ColumnFamilySchema& cf = meta.schema().column_families(cf_id);
        std::cout << " cf" << cf_id << ": " << cf.name() << " (" << cf.locality_group() << ", "
                  << cf.type() << ", " << cf.max_versions() << ", " << cf.time_to_live() << ")"
                  << std::endl;
      }
    }
    if (op == "backup") {
      WriteTable(meta, bak);
    }
  }

  tera::TabletMeta last;
  bool table_start = true;
  for (int32_t i = 0; i < tablet_num; ++i) {
    const tera::TabletMeta& meta = tablet_list.meta(i);
    if (op == "show") {
      std::string internal_startkey = meta.table_name() + "#" + meta.key_range().key_start();
      std::string internal_endkey = meta.table_name() + "#" + meta.key_range().key_end();
      if ((start_key == "" && end_key == "") ||
          (start_key <= internal_startkey &&
           (internal_startkey <= end_key || meta.table_name() + "#" == end_key))) {
        std::cout << "tablet: " << meta.table_name() << " ["
                  << DebugString(meta.key_range().key_start()) << ","
                  << DebugString(meta.key_range().key_end()) << "], " << meta.path() << ", "
                  << meta.server_addr() << ", " << meta.size() << ", "
                  << StatusCodeToString(meta.status()) << ", "
                  << StatusCodeToString(meta.compact_status()) << std::endl;
      }
    }
    if (op == "backup") {
      WriteTablet(meta, bak);
    }
    // check self range
    if (!meta.key_range().key_end().empty() &&
        meta.key_range().key_start() >= meta.key_range().key_end()) {
      std::cerr << "invalid tablet " << meta.table_name() << " ["
                << DebugString(meta.key_range().key_start()) << ","
                << DebugString(meta.key_range().key_end()) << "], " << meta.path() << ", "
                << meta.server_addr() << ", " << meta.size() << ", "
                << StatusCodeToString(meta.status()) << ", "
                << StatusCodeToString(meta.compact_status()) << std::endl;
      // ignore invalid tablet
      continue;
    }

    bool covered = false;
    // check miss/cover/overlap with previous tablet
    if (!table_start) {
      assert(!last.key_range().key_end().empty());
      if (meta.table_name() != last.table_name()) {
        std::cerr << "miss tablet: " << last.table_name() << " path " << last.path() << " ["
                  << DebugString(last.key_range().key_end()) << ",-]" << std::endl;
        table_start = true;
      } else if (meta.key_range().key_start() > last.key_range().key_end()) {
        std::cerr << "miss tablet " << last.table_name() << " last path " << last.path()
                  << " curr path " << meta.path() << " [" << DebugString(last.key_range().key_end())
                  << "," << DebugString(meta.key_range().key_start()) << "]" << std::endl;
      } else if (meta.key_range().key_start() == last.key_range().key_end()) {
      } else if (!meta.key_range().key_end().empty() &&
                 meta.key_range().key_end() <= last.key_range().key_end()) {
        std::cerr << "tablet " << meta.table_name() << " path " << meta.path() << " ["
                  << DebugString(meta.key_range().key_start()) << ","
                  << DebugString(meta.key_range().key_end()) << "] is coverd by tablet "
                  << last.table_name() << " path " << last.path() << " ["
                  << DebugString(last.key_range().key_start()) << ","
                  << DebugString(last.key_range().key_end()) << "]" << std::endl;
        covered = true;
      } else {
        std::cerr << "tablet " << meta.table_name() << " path " << meta.path() << " ["
                  << DebugString(meta.key_range().key_start()) << ","
                  << DebugString(meta.key_range().key_end()) << "] overlap with tablet "
                  << last.table_name() << " path " << last.path() << " ["
                  << DebugString(last.key_range().key_start()) << ","
                  << DebugString(last.key_range().key_end()) << "]" << std::endl;
      }
    }
    if (table_start) {
      if (meta.table_name() == last.table_name()) {
        std::cerr << "tablet " << meta.table_name() << " path " << meta.path() << " ["
                  << DebugString(meta.key_range().key_start()) << ","
                  << DebugString(meta.key_range().key_end()) << "] is coverd by tablet "
                  << last.table_name() << " path " << last.path() << " ["
                  << DebugString(last.key_range().key_start()) << ","
                  << DebugString(last.key_range().key_end()) << "]" << std::endl;
        covered = true;
      } else {
        if (!meta.key_range().key_start().empty()) {
          std::cerr << "Please check the whole KeyRange, maybe miss tablet " << meta.table_name()
                    << " path " << meta.path() << " [-,"
                    << DebugString(meta.key_range().key_start()) << "]" << std::endl;
        }
      }
    }
    // ignore covered tablet
    if (!covered) {
      last.CopyFrom(meta);
      table_start = meta.key_range().key_end().empty();
    }
  }
  if (op == "backup") {
    bak.close();
  }
  return CliStatus::kOk;
}

bool ReadMetaFromStream(std::ifstream& ifs, std::string* key, std::string* value) {
  uint32_t key_size = 0, value_size = 0;
  ifs.read((char*)&key_size, sizeof(key_size));
  if (ifs.eof() && ifs.gcount() == 0) {
    key->clear();
    value->clear();
    return true;
  }
  key->resize(key_size);
  ifs.read((char*)key->data(), key_size);
  if (ifs.fail()) {
    return false;
  }
  ifs.read((char*)&value_size, sizeof(value_size));
  if (ifs.fail()) {
    return false;
  }
  value->resize(value_size);
  ifs.read((char*)value->data(), value_size);
  if (ifs.fail()) {
    return false;
  }
  return true;
}

int ReadMetaTabletFromFile(const std::string& filename) {
  std::ifstream ifs(filename.c_str(), std::ofstream::binary);
  if (!ifs.is_open()) {
    LOG(INFO) << "fail to open file " << filename << " for read";
    return -1;
  }

  std::string key, value;
  TabletMeta meta;
  std::pair<std::string, std::string> keyrange_bak;
  std::string path_bak;
  std::string startkey_bak;
  std::string endkey_bak;

  while (ReadMetaFromStream(ifs, &key, &value)) {
    if (key.empty()) {
      return 0;
    }
    char first_key_char = key[0];
    if (first_key_char == '~' || first_key_char == '@' || first_key_char == '|') {
      continue;
    } else if (first_key_char > '@') {
      ParseMetaTableKeyValue(key, value, &meta);

      if (meta.table_name() == FLAGS_tera_master_meta_table_name) {
        LOG(INFO) << "ignore meta tablet record in meta table";
      } else {
        path_bak = meta.path();
        startkey_bak = meta.key_range().key_start();
        endkey_bak = meta.key_range().key_end();

        auto it = g_map_bak.find(path_bak);
        if (it == g_map_bak.end()) {
          keyrange_bak = make_pair(startkey_bak, endkey_bak);
          g_map_bak.insert(make_pair(path_bak, keyrange_bak));
          LOG(INFO) << "read from meta bak file: path" << path_bak << "start" << startkey_bak
                    << "end" << endkey_bak;
        }
      }
    }
  }

  ifs.close();
  LOG(INFO) << "restore meta tablet from meta bak file succ";
  return 0;
}

int DfsListDir(const std::string& dir_name, std::vector<std::string>* paths) {
  struct stat fstat;
  if (0 == g_dfs->Stat(dir_name.c_str(), &fstat) && !(S_IFDIR & fstat.st_mode)) {
    LOG(INFO) << "stat dir:" << dir_name << "fail";
    return 0;
  }

  if (0 != g_dfs->ListDirectory(dir_name.c_str(), paths)) {
    LOG(INFO) << "list dir:" << dir_name << "fail";
    return -1;
  }

  return 0;
}

int DfsGetManifest(const std::string& manifest_path, std::string* start_key, std::string* end_key) {
  if (manifest_path == "") {
    LOG(INFO) << "fail, Invalid arguments";
    return -1;
  }
  leveldb::SequentialFile* file;
  leveldb::Status s = g_env->NewSequentialFile(manifest_path, &file);
  if (!s.ok()) {
    LOG(INFO) << "open fail:" << manifest_path << "status: " << s.ToString();
    return -1;
  }

  RestoreReporter reporter;
  leveldb::log::Reader reader(file, &reporter, true, 0);
  // just read the first record
  leveldb::Slice record;
  std::string scratch;
  while (reader.ReadRecord(&record, &scratch)) {
    leveldb::VersionEdit edit;
    s = edit.DecodeFrom(record);
    if (!s.ok()) {
      LOG(INFO) << "fail, decode record status:" << s.ToString().c_str();
    } else {
      if (edit.HasStartKey() && edit.HasEndKey()) {
        *start_key = edit.GetStartKey().c_str();
        *end_key = edit.GetEndKey().c_str();
        break;
      }
    }
  }
  delete file;
  return 0;
}

int DfsGetCurrent(const std::string& current_path, std::string* content) {
  if (current_path == "") {
    LOG(INFO) << "fail, Invalid arguments";
    return -1;
  }

  leveldb::DfsFile* file = g_dfs->OpenFile(current_path, leveldb::RDONLY);
  if (NULL == file) {
    LOG(INFO) << "fail, open current path(" << current_path.c_str() << ") fail";
    return errno;
  }

  // MANIFEST-000000, just 15 char
  char buf[MANIFEST_LEN] = {0};
  ssize_t ret_size = 0;
  ret_size = file->Read(buf, sizeof(buf));
  if (ret_size > 0) {
    buf[MANIFEST_LEN - 1] = '\0';
    *content = buf;
  } else {
    *content = "";
    LOG(INFO) << "fail, read current fail";
  }

  file->CloseFile();
  return 0;
}

int MapOutPutDiff(std::map<std::string, std::vector<std::string>>* map_diff) {
  // output mutex
  MutexLock locker(&g_output_lock);

  std::ofstream ofs("meta.diff", std::ofstream::app);
  if (!ofs.is_open()) {
    LOG(INFO) << "output diff open file ("
              << "meta.diff"
              << ") fail";
    return -1;
  }

  auto it = map_diff->begin();
  while (it != map_diff->end()) {
    g_diff_count++;
    // 4 -- keyrange diff, else path diff
    if (it->second.size() == 4) {
      ofs << "tablet keyrange err: " << it->first;
      ofs << "[" << DebugString(it->second[0]) << "," << DebugString(it->second[1]) << "]";
      ofs << "---[" << DebugString(it->second[2]) << "," << DebugString(it->second[3]) << "]";
    } else {
      ofs << "tablet miss: " << it->first;
      ofs << "[" << DebugString(it->second[0]) << "," << DebugString(it->second[1]) << "]";
    }
    ofs << "\n";
    ++it;
  }
  ofs.close();
  return 0;
}

uint64_t GetTabletNumFromName(const std::string& tabletname) {
  if (tabletname.size() != TABLET_NAME_LEN || tabletname.substr(0, 6).compare("tablet") != 0) {
    return 0;
  }
  std::string num = tabletname.substr(6, TABLET_NUM_LEN);

  uint64_t v = 0;
  uint32_t i = 0;
  for (; i < num.size(); i++) {
    char c = num[i];
    if (c >= '0' && c <= '9') {
      const int delta = (c - '0');
      static const uint64_t kMaxUint64 = ~static_cast<uint64_t>(0);
      if (v > kMaxUint64 / 10 ||
          (v == kMaxUint64 / 10 && static_cast<uint64_t>(delta) > kMaxUint64 % 10)) {
        // Overflow
        return false;
      }
      v = (v * 10) + delta;
    } else {
      break;
    }
  }

  return v;
}

void CompareToDiff(std::map<std::string, std::pair<std::string, std::string>>& map_scan,
                   std::map<std::string, std::vector<std::string>>* map_diff) {
  std::vector<std::string> keyrange_diff;
  // input: <path,<startkey,endkey>> map_bak
  // input: <startkey,<endkey,path>> map_scan
  auto it_scan = map_scan.begin();

  while (it_scan != map_scan.end()) {
    auto it_bak = g_map_bak.find(it_scan->second.second);
    if (it_bak != g_map_bak.end()) {
      if (it_scan->first != it_bak->second.first ||
          it_scan->second.first != it_bak->second.second) {
        // output: keyrange err -- <path,<startkey_scan,endkey_scan,startkey_bak,endkey_bak>>
        // map_diff
        keyrange_diff.clear();
        // scan keyrange
        keyrange_diff.push_back(it_scan->first);
        keyrange_diff.push_back(it_scan->second.first);
        // bak keyrange
        keyrange_diff.push_back(it_bak->second.first);
        keyrange_diff.push_back(it_bak->second.second);

        map_diff->insert(make_pair(it_scan->second.second, keyrange_diff));
      }
    } else {
      // output: tablet   err -- <path,<startkey_scan,endkey_scan>> map_diff
      keyrange_diff.clear();
      keyrange_diff.push_back(it_scan->first);
      keyrange_diff.push_back(it_scan->second.first);
      map_diff->insert(make_pair(it_scan->second.second, keyrange_diff));
    }
    ++it_scan;
  }
}

void FixTabletOverlap(std::map<std::string, std::pair<std::string, std::string>>& map_scan) {
  auto it = map_scan.begin();
  while (it != map_scan.end()) {
    // it_pre->first always < it->first
    auto it_pre = it;
    ++it;
    if (it != map_scan.end()) {
      /************* remove the overlap ************
       *  <it->first, <it->second.first, it->second.second>>
       *  <startkey,  <endkey,           path>>
       *
       *    it_pre    |------------)
       *    it             |----)
       */
      if (it->first < it_pre->second.first) {
        LOG(INFO) << "overlap --"
                  << " tablet path1:" << it_pre->second.second << "startkey1" << it_pre->first
                  << "endkey1" << it_pre->second.first << "tablet path2" << it->second.second
                  << "startkey2" << it->first << "endkey2" << it->second.first;
        if (it->second.second > it_pre->second.second) {
          map_scan.erase(it_pre->first);
        } else {
          map_scan.erase(it->first);
        }
      }
    }
  }
}

std::string GetTabletName(const std::vector<std::string>& fail_tablet, uint32_t& index,
                          uint64_t& tablet_num) {
  std::string tablet_name;
  std::string tablet_str;
  if (index < fail_tablet.size()) {  // reuse abnormal tablet num
    tablet_name = fail_tablet[index];
    index++;
  } else {
    tablet_num++;
    tablet_str = std::to_string(tablet_num);
    // add 0 before num to TABLET_NUM_LEN
    int i, gap = TABLET_NUM_LEN - tablet_str.size();
    for (i = 0; i < gap; i++) {
      tablet_str = "0" + tablet_str;
    }
    tablet_name = "tablet" + tablet_str;
  }
  return tablet_name;
}

void FixTabletGap(std::map<std::string, std::pair<std::string, std::string>>& map_scan,
                  const std::vector<std::string>& fail_tablet, const std::string& table_name,
                  uint64_t tablet_num) {
  uint32_t index = 0;
  bool table_start = true;
  std::string path_gap;
  std::pair<std::string, std::string> keyrange_scan;

  auto it = map_scan.begin();

  if (map_scan.size() == 1) {
    // start not -
    if (!it->first.empty()) {
      path_gap = table_name + "/" + GetTabletName(fail_tablet, index, tablet_num);
      keyrange_scan = make_pair(it->first, path_gap);
      map_scan.insert(make_pair("", keyrange_scan));
    }
    // end not -
    if (!it->second.first.empty()) {
      path_gap = table_name + "/" + GetTabletName(fail_tablet, index, tablet_num);
      keyrange_scan = make_pair("", path_gap);
      map_scan.insert(make_pair(it->second.first, keyrange_scan));
    }
    return;
  }

  while (it != map_scan.end()) {
    // it_pre->first always < it->first
    auto it_pre = it;
    ++it;
    if (it != map_scan.end()) {
      if (table_start) {  // start not -
        table_start = false;
        if (!it_pre->first.empty()) {
          path_gap = table_name + "/" + GetTabletName(fail_tablet, index, tablet_num);
          keyrange_scan = make_pair(it_pre->first, path_gap);
          map_scan.insert(make_pair("", keyrange_scan));
        }
      }
      /************* fix the gap *******************
       *  <it->first, <it->second.first, it->second.second>>
       *  <startkey,  <endkey,           path>>
       *
       *  it_pre   |------)
       *  it                        |------)
       *   ----------------------------------
       *  new             |---------)
       */
      if (it->first > it_pre->second.first) {
        path_gap = table_name + "/" + GetTabletName(fail_tablet, index, tablet_num);
        keyrange_scan = make_pair(it->first, path_gap);
        map_scan.insert(make_pair(it_pre->second.first, keyrange_scan));

        LOG(INFO) << "gap - path:" << path_gap << "startkey" << it_pre->second.first << "endkey"
                  << it->first;
      }
    } else {  // end not -
      if (!it_pre->second.first.empty()) {
        path_gap = table_name + "/" + GetTabletName(fail_tablet, index, tablet_num);
        keyrange_scan = make_pair("", path_gap);
        map_scan.insert(make_pair(it_pre->second.first, keyrange_scan));
      }
    }
  }
  return;
}

int GetTabletKeyRange(const std::string& prefix_path, const std::string& table_name,
                      const std::string& tablet_name, std::string& startkey, std::string& endkey) {
  char full_path[4096] = {0};
  std::string manifest_name;

  // read current, eg: prefix/table/tablet/0/CURRENT
  snprintf(full_path, sizeof(full_path), "%s/%s/%s/0/CURRENT", prefix_path.c_str(),
           table_name.c_str(), tablet_name.c_str());
  int ret = DfsGetCurrent(full_path, &manifest_name);
  if (0 != ret) {
    LOG(INFO) << "get current(" << full_path << ") fail";
    return ret;
  }

  // read manifest, prefix/table/tablet/0/MANIFEST-*
  snprintf(full_path, sizeof(full_path), "%s/%s/%s/0/%s", prefix_path.c_str(), table_name.c_str(),
           tablet_name.c_str(), manifest_name.c_str());
  ret = DfsGetManifest(full_path, &startkey, &endkey);
  if (0 != ret) {
    LOG(INFO) << "get manifest(" << full_path << ") fail";
    return ret;
  }

  LOG(INFO) << "get manifest(" << full_path << ") succ"
            << "table(" << table_name << ")"
            << "tablet(" << tablet_name << ")"
            << "startkey(" << startkey << ")"
            << "endkey(" << endkey << ")";
  return 0;
}

// get tablet key range
int ScanTabletMeta(const std::string& table_name, const std::string& prefix_path,
                   std::map<std::string, std::pair<std::string, std::string>>* map_scan,
                   std::vector<std::string>* fail_tablet, uint64_t* tablet_num) {
  std::vector<std::string> tablets;
  char table_path[512] = {0};

  snprintf(table_path, sizeof(table_path), "%s/%s", prefix_path.c_str(), table_name.c_str());
  int ret = DfsListDir(table_path, &tablets);
  if (0 != ret) {
    LOG(INFO) << "get table path fail:" << table_path;
    return ret;
  }
  *tablet_num = GetTabletNumFromName(tablets[tablets.size() - 1]);
  LOG(INFO) << "table:" << table_name << "tablet count" << tablets.size()
            << "largest tablet_num:" << *tablet_num;

  std::string startkey_scan;
  std::string endkey_scan;
  std::pair<std::string, std::string> keyrange_scan;

  for (size_t i = 0; i < tablets.size(); i++) {
    ret = GetTabletKeyRange(prefix_path, table_name, tablets[i], startkey_scan, endkey_scan);
    if (0 != ret) {
      LOG(INFO) << "fail table:" << table_name << "tablet:" << tablets[i];
      fail_tablet->push_back(tablets[i]);
      continue;
    }
    // add to tablet scan map
    keyrange_scan = make_pair(endkey_scan, table_name + "/" + tablets[i]);

    auto it = map_scan->find(startkey_scan);
    if (it == map_scan->end()) {
      map_scan->insert(make_pair(startkey_scan, keyrange_scan));
    } else {
      // the same startkey, save the tablet num bigger one
      LOG(INFO) << "start key overlap : 1." << tablets[i] << "2." << it->second.second;
      if (keyrange_scan.second > it->second.second) {
        map_scan->erase(it->first);
        map_scan->insert(make_pair(startkey_scan, keyrange_scan));
      }
    }
  }

  LOG(INFO) << "table:" << table_name << "abnormal count" << fail_tablet->size();
  return 0;
}

// get tablet key range
int ScanAndDiff(const std::string& table_name, const std::string& prefix_path) {
  std::map<std::string, std::pair<std::string, std::string>> map_scan;
  std::vector<std::string> fail_tablet;
  uint64_t tablet_num;

  int ret = ScanTabletMeta(table_name, prefix_path, &map_scan, &fail_tablet, &tablet_num);
  if (0 != ret || map_scan.size() == 0) {
    g_thread_count--;
    return 0;
  }

  FixTabletOverlap(map_scan);
  FixTabletGap(map_scan, fail_tablet, table_name, tablet_num);

  // compare the scan meta with the read meta
  // <path,<startkey,endkey>> map_diffiff
  std::map<std::string, std::vector<std::string>> map_diff;
  CompareToDiff(map_scan, &map_diff);

  // output diff
  ret = MapOutPutDiff(&map_diff);
  if (0 != ret) {
    LOG(ERROR) << "output diff of table(" << table_name << ") fail";
  }

  g_thread_count--;
  return 0;
}

int InitDfsClient() {
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
      LOG(ERROR) << "init nfs system: no configure file found";
      return -1;
    }
  } else if (FLAGS_tera_leveldb_env_dfs_type == "hdfs2") {
    LOG(INFO) << "init hdfs2 file system";
    g_dfs = new leveldb::Hdfs2(FLAGS_tera_leveldb_env_hdfs2_nameservice_list);
  } else if (FLAGS_tera_leveldb_env_dfs_type == "hdfs") {
    g_dfs = new leveldb::Hdfs();
  } else {
    LOG(INFO) << "init dfs system: " << FLAGS_tera_dfs_so_path << "(" << FLAGS_tera_dfs_conf << ")";
    g_dfs = leveldb::Dfs::NewDfs(FLAGS_tera_dfs_so_path, FLAGS_tera_dfs_conf);
  }

  if (g_dfs == NULL) {
    return -1;
  }
  return 0;
}

CliStatus AllDiff(const std::string& prefix_path) {
  int ret = DfsListDir(prefix_path, &g_tables);
  if (0 != ret) {
    LOG(ERROR) << "all check fail, get table path fail";
    return CliStatus::kError;
  }

  if (g_tables.size() > 0) {
    // scan tablet meta and compare, thread pool parallel
    g_thread_count = g_tables.size();
    const std::string gc_table("#trackable_gc_trash");
    const std::string trash_table("#trash");
    const std::string meta_table("meta");
    const std::string stat_table("stat_table");

    ThreadPool thread_pool(g_tables.size());
    for (size_t i = 0; i < g_tables.size(); ++i) {
      LOG(INFO) << "table is:(" << g_tables[i] << ")";
      if (g_tables[i] == gc_table || g_tables[i] == trash_table || g_tables[i] == meta_table ||
          g_tables[i] == stat_table) {
        g_thread_count--;
        continue;
      }
      ThreadPool::Task task = std::bind(&ScanAndDiff, g_tables[i], prefix_path);
      thread_pool.AddTask(task);
    }

    while (g_thread_count > 0) {
      LOG(INFO) << get_time_str(time(NULL)) << " " << g_thread_count << "scan and diff ......";
      sleep(5);
    }
    thread_pool.Stop(true);
  }

  if (g_diff_count == 0) {
    std::cout << "tables no diff" << std::endl;
  } else {
    std::cout << "table diff num:" << g_diff_count << ", check the details in ./meta.diff"
              << std::endl;
  }
  return CliStatus::kOk;
}

CliStatus TableDiff(const std::string& prefix_path, const std::string& table_name) {
  int ret = ScanAndDiff(table_name, prefix_path);
  if (ret != 0) {
    return CliStatus::kError;
  }

  if (g_diff_count == 0) {
    std::cout << "table:" << table_name << " no diff:" << std::endl;
  } else {
    std::cout << "table:" << table_name << "diff num:" << g_diff_count
              << ", check the details in ./meta.diff" << std::endl;
  }
  return CliStatus::kOk;
}

CliStatus TabletDiff(const std::string& prefix_path, const std::string& table_name,
                     const std::string& tablet_name) {
  std::string startkey;
  std::string endkey;
  std::string path;
  std::vector<std::string> record_diff;

  int ret = GetTabletKeyRange(prefix_path, table_name, tablet_name, startkey, endkey);
  if (ret != 0) {
    LOG(ERROR) << "tablet check fail:" << table_name << "/" << tablet_name;
    return CliStatus::kError;
  }

  path = table_name + "/" + tablet_name;
  auto it = g_map_bak.find(path);
  if (it != g_map_bak.end()) {
    if (startkey != it->second.first || endkey != it->second.second) {
      // find but keyrange not match
      // scan keyrange [startkey,endkey] <--> bak keyrange [it->second.first, it->second.second]
      record_diff.push_back(it->second.first);
      record_diff.push_back(it->second.second);
      std::cout << "tablet keyrange not match:" << path << " [" << startkey << "("
                << DebugString(startkey) << ")," << endkey << "(" << DebugString(endkey) << ") ]"
                << "<---> [" << it->second.first << "(" << DebugString(it->second.first) << "),"
                << it->second.second << "(" << DebugString(it->second.second) << ") ]" << std::endl;
    } else {
      std::cout << "tablet no diff:" << table_name << "/" << tablet_name << " [" << startkey << "("
                << DebugString(startkey) << ")," << endkey << "(" << DebugString(endkey) << ") ]"
                << std::endl;
    }
  } else {
    // not find
    std::cout << "tablet miss: " << path << " [" << startkey << "(" << DebugString(startkey) << "),"
              << endkey << "(" << DebugString(endkey) << ") ]" << std::endl;
  }
  return CliStatus::kOk;
}

CliStatus MetaInternalOp(const std::string& meta_server, common::ThreadPool* thread_pool,
                         const std::string& op, const std::string& start_key,
                         const std::string& end_key, const std::string& filename) {
  tabletnode::TabletNodeClient meta_client(thread_pool, meta_server);
  TableMeta table_meta;
  TableSchema table_schema;
  TableMetaList table_list;
  TabletMetaList tablet_list;
  ScanTabletRequest request;
  ScanTabletResponse response;
  request.set_sequence_id(g_sequence_id++);
  request.set_table_name(FLAGS_tera_master_meta_table_name);
  request.set_start(start_key);
  request.set_end(end_key);
  while (meta_client.ScanTablet(&request, &response)) {
    StatusCode err = response.status();
    if (err != tera::kTabletNodeOk) {
      std::cerr << "Read meta table response not kTabletNodeOk!";
      return CliStatus::kError;
    }

    int32_t record_size = response.results().key_values_size();
    if (record_size <= 0) {
      std::cout << "scan meta table success" << std::endl;
      break;
    }
    std::cout << "recode size = " << record_size << std::endl;
    std::string last_record_key;
    for (int i = 0; i < record_size; ++i) {
      const tera::KeyValuePair& record = response.results().key_values(i);
      last_record_key = record.key();
      char first_key_char = record.key()[0];
      if (first_key_char == '~') {
        std::cout << "(user: " << record.key().substr(1) << ")" << std::endl;
      } else if (first_key_char == '|') {
        // user&passwd&role&permission
      } else if (first_key_char == '@') {
        ParseMetaTableKeyValue(record.key(), record.value(), table_list.add_meta());
      } else if (first_key_char > '@') {
        ParseMetaTableKeyValue(record.key(), record.value(), tablet_list.add_meta());
      } else {
        std::cerr << "invalid record: " << record.key();
      }
    }
    std::string next_record_key = tera::NextKey(last_record_key);
    request.set_start(next_record_key);
    request.set_end(end_key);
    request.set_sequence_id(g_sequence_id++);
    response.Clear();
  }
  return ProcessMeta(op, table_list, tablet_list, "", "", filename);
}

// diff [table_name|tablet_name] <backup_filename>
CliStatus MetaDiffOp(Client* client, int32_t argc, std::string* argv, ErrorCode* err) {
  if (0 != InitDfsClient()) {
    LOG(ERROR) << "Init Dfs Client fail";
    return CliStatus::kError;
  }
  g_env = new leveldb::DfsEnv(g_dfs);

  std::ofstream diff_file;
  diff_file.open("meta.diff", std::ofstream::trunc);
  diff_file.close();

  // read meta tablets from meta bak file
  if (argc == 3) {
    const std::string backup_filename = argv[2];
    int ret = ReadMetaTabletFromFile(backup_filename);
    if (0 != ret) {
      LOG(INFO) << "restore meta from meta bak file fail";
      return CliStatus::kError;
    }
    return AllDiff(FLAGS_tera_tabletnode_path_prefix);
  } else if (argc == 4) {
    const std::string backup_filename = argv[3];
    int ret = ReadMetaTabletFromFile(backup_filename);
    if (0 != ret) {
      LOG(INFO) << "restore meta from meta bak file fail";
      return CliStatus::kError;
    }

    std::vector<std::string> arg_list;
    SplitString(argv[2], "/", &arg_list);
    if (arg_list.size() == 1) {
      const std::string table_name = arg_list[0];
      return TableDiff(FLAGS_tera_tabletnode_path_prefix, table_name);
    } else if (arg_list.size() == 2) {
      const std::string table_name = arg_list[0];
      const std::string tablet_name = arg_list[1];
      return TabletDiff(FLAGS_tera_tabletnode_path_prefix, table_name, tablet_name);
    }
  } else {
    PrintCmdHelpInfo(argv[1]);
  }
  return CliStatus::kOk;
}

// put <tablet_path> <start_key> <end_key> <server_addr>
CliStatus MetaPutOp(Client* client, int32_t argc, std::string* argv, ErrorCode* err) {
  scoped_ptr<tera::sdk::ClusterFinder> finder(tera::sdk::NewClusterFinder());
  const std::string meta_server = finder->RootTableAddr();
  if (argc == 6) {
    common::ThreadPool thread_pool(1);
    const std::string& tablet_path = argv[2];
    std::vector<std::string> arg_list;
    SplitString(tablet_path, "/", &arg_list);
    const std::string& table_name = arg_list[0];
    const std::string& start_key = argv[3];
    const std::string& end_key = argv[4];
    const std::string& server_addr = argv[5];
    return PutTabletMeta(meta_server, &thread_pool, table_name, start_key, end_key, tablet_path,
                         server_addr);
  } else {
    PrintCmdHelpInfo(argv[1]);
  }
  return CliStatus::kOk;
}

// delete <table_name> <start_key>
CliStatus MetaDeleteOp(Client* client, int32_t argc, std::string* argv, ErrorCode* err) {
  scoped_ptr<tera::sdk::ClusterFinder> finder(tera::sdk::NewClusterFinder());
  const std::string meta_server = finder->RootTableAddr();
  if (argc == 4) {
    common::ThreadPool thread_pool(1);
    const std::string& table_name = argv[2];
    const std::string& start_key = argv[3];
    return DeleteMetaTablet(meta_server, &thread_pool, table_name, start_key);
  } else {
    PrintCmdHelpInfo(argv[1]);
  }
  return CliStatus::kOk;
}

// modify <table_name> <start_key> <type> <end_key|dest_ts>
CliStatus MetaModifyOp(Client* client, int32_t argc, std::string* argv, ErrorCode* err) {
  scoped_ptr<tera::sdk::ClusterFinder> finder(tera::sdk::NewClusterFinder());
  const std::string meta_server = finder->RootTableAddr();
  if (argc == 6) {
    common::ThreadPool thread_pool(1);
    const std::string& table_name = argv[2];
    const std::string& start_key = argv[3];
    const std::string type = argv[4];
    const std::string value = argv[5];
    return ModifyMetaValue(meta_server, &thread_pool, table_name, start_key, type, value);
  } else {
    PrintCmdHelpInfo(argv[1]);
  }
  return CliStatus::kOk;
}

// get [inmem] <table_name> <start_key>
CliStatus MetaGetOp(Client* client, int32_t argc, std::string* argv, ErrorCode* err) {
  scoped_ptr<tera::sdk::ClusterFinder> finder(tera::sdk::NewClusterFinder());
  const std::string meta_server = finder->RootTableAddr();
  if (argc == 4) {
    common::ThreadPool thread_pool(1);
    const std::string& table_name = argv[2];
    const std::string& start_key = argv[3];
    return GetMeta(meta_server, &thread_pool, table_name, start_key);
  } else if (argc == 5 && argv[2] == "inmem") {
    const std::string& table_name = argv[3];
    const std::string& start_key = argv[4];

    TableMeta table_meta;
    TabletMetaList tablet_list;
    std::shared_ptr<tera::ClientImpl> client_impl(
        (static_cast<ClientWrapper*>(client))->GetClientImpl());
    client_impl->ShowTablesInfo(table_name, &table_meta, &tablet_list, err);

    for (int i = 0; i < tablet_list.meta_size(); ++i) {
      if (tablet_list.meta(i).key_range().key_start() == start_key) {
        PrintMetaInfo(&tablet_list.meta(i));
        break;
      }
    }
  } else {
    PrintCmdHelpInfo(argv[1]);
  }
  return CliStatus::kOk;
}

// show [inmem] [start_key] [end_key]
CliStatus MetaShowOp(Client* client, int32_t argc, std::string* argv, ErrorCode* err) {
  scoped_ptr<tera::sdk::ClusterFinder> finder(tera::sdk::NewClusterFinder());
  const std::string meta_server = finder->RootTableAddr();
  std::string start_key;
  std::string end_key;
  if (argc == 4 || argc == 2) {
    if (argc == 4) {
      start_key = argv[2];
      end_key = argv[3];
    } else {
      start_key = "";
      end_key = "";
    }
    common::ThreadPool thread_pool(1);
    return MetaInternalOp(meta_server, &thread_pool, "show", start_key, end_key, "");
  } else if ((argc == 5 || argc == 3) && argv[2] == "inmem") {
    if (argc == 5) {
      start_key = argv[3];
      end_key = argv[4];
    } else {
      start_key = "";
      end_key = "";
    }
    TableMetaList table_list;
    TabletMetaList tablet_list;
    std::shared_ptr<tera::ClientImpl> client_impl(
        (static_cast<ClientWrapper*>(client))->GetClientImpl());
    if (!client_impl->ShowTablesInfo(&table_list, &tablet_list, false, err)) {
      LOG(ERROR) << "fail to get meta data from tera.";
      return CliStatus::kError;
    }
    return ProcessMeta("show", table_list, tablet_list, start_key, end_key, "");
  } else {
    PrintCmdHelpInfo(argv[1]);
  }
  return CliStatus::kOk;
}

// backup [inmem] [backup_filename]
CliStatus MetaBackUpOp(Client* client, int32_t argc, std::string* argv, ErrorCode* err) {
  std::string filename;
  if ((argc == 3 || argc == 4) && argv[2] == "inmem") {
    if (argc == 3) {
      filename = "inmem_meta.bak_" + get_curtime_str();
    } else {
      filename = argv[3] + "_" + get_curtime_str();
    }
    std::shared_ptr<tera::ClientImpl> client_impl(
        (static_cast<ClientWrapper*>(client))->GetClientImpl());
    std::vector<std::string> arg_list;
    arg_list.push_back("backup");
    arg_list.push_back(filename);
    if (!client->CmdCtrl("meta", arg_list, NULL, NULL, err)) {
      LOG(ERROR) << "fail to backup meta";
      return CliStatus::kError;
    }
  } else if (argc == 2 || argc == 3) {
    scoped_ptr<tera::sdk::ClusterFinder> finder(tera::sdk::NewClusterFinder());
    const std::string meta_server = finder->RootTableAddr();
    if (argc == 2) {
      filename = "meta.bak_" + get_curtime_str();
    } else {
      filename = argv[2] + "_" + get_curtime_str();
    }
    common::ThreadPool thread_pool(1);
    std::string start_key("");
    std::string end_key("");
    MetaInternalOp(meta_server, &thread_pool, "backup", start_key, end_key, filename);
  } else {
    PrintCmdHelpInfo(argv[1]);
  }
  return CliStatus::kOk;
}

// healthcheck [inmem] [start_key] [end_key]
CliStatus MetaHealthCheckOp(Client* client, int32_t argc, std::string* argv, ErrorCode* err) {
  if (0 != InitDfsClient()) {
    LOG(ERROR) << "Init Dfs Client fail";
    return CliStatus::kError;
  }
  g_env = new leveldb::DfsEnv(g_dfs);

  scoped_ptr<tera::sdk::ClusterFinder> finder(tera::sdk::NewClusterFinder());
  const std::string meta_server = finder->RootTableAddr();
  std::string start_key;
  std::string end_key;

  if (argc == 4 || argc == 2) {
    if (argc == 4) {
      start_key = argv[2];
      end_key = argv[3];
    } else {
      start_key = "";
      end_key = "";
    }
    common::ThreadPool thread_pool(1);
    return MetaInternalOp(meta_server, &thread_pool, "healtchcheck", start_key, end_key, "");
  } else if ((argc == 5 || argc == 3) && argv[2] == "inmem") {
    if (argc == 5) {
      start_key = argv[3];
      end_key = argv[4];
    } else {
      start_key = "";
      end_key = "";
    }

    TableMetaList table_list;
    TabletMetaList tablet_list;
    std::shared_ptr<tera::ClientImpl> client_impl(
        (static_cast<ClientWrapper*>(client))->GetClientImpl());
    if (!client_impl->ShowTablesInfo(&table_list, &tablet_list, false, err)) {
      LOG(ERROR) << "fail to get meta data from tera.";
      return CliStatus::kError;
    }
    return ProcessMeta("healthcheck", table_list, tablet_list, start_key, end_key, "");
  } else {
    PrintCmdHelpInfo(argv[1]);
  }
  return CliStatus::kOk;
}

// conv <readable_key>
CliStatus MetaConvOp(Client* client, int32_t argc, std::string* argv, ErrorCode* err) {
  if (argc == 3 && argv[1] == "conv") {
    const std::string& readable_key = argv[2];
    std::cout << DebugString(readable_key) << " => " << readable_key << std::endl;
  } else {
    PrintCmdHelpInfo(argv[1]);
  }
  return CliStatus::kOk;
}

CliStatus ShowUgiOp(std::shared_ptr<tera::ClientImpl> client_impl, int32_t argc, std::string* argv,
                    ErrorCode* err) {
  tera::UserVerificationInfoList user_verification_info_list;
  if (!client_impl->ShowUgi(&user_verification_info_list, err)) {
    LOG(ERROR) << "show ugi failed!" << err->ToString();
    return CliStatus::kError;
  }
  std::cout << "Show ugi : " << std::endl;
  std::cout << "\tuser_name\tpasswd\troles" << std::endl;
  for (auto it = user_verification_info_list.begin(); it != user_verification_info_list.end();
       ++it) {
    std::cout << "\t" << it->first << "\t" << it->second.first << "\t";
    for (auto& role : it->second.second) {
      std::cout << role << ",";
    }
    std::cout << std::endl;
  }
  return CliStatus::kOk;
}

CliStatus UgiOp(Client* client, int32_t argc, std::string* argv, ErrorCode* err) {
  if (argc < 3) {
    PrintCmdHelpInfo(argv[1]);
    return CliStatus::kError;
  }
  const std::string& op = argv[2];
  std::shared_ptr<tera::ClientImpl> client_impl(
      (static_cast<ClientWrapper*>(client))->GetClientImpl());

  if (argc == 5 && op == "update") {
    const std::string& user_name = argv[3];
    const std::string& passwd = argv[4];
    if (!client_impl->UpdateUgi(user_name, passwd, err)) {
      LOG(ERROR) << "update ugi failed!" << err->ToString();
      return CliStatus::kError;
    }
  } else if (argc == 4 && op == "del") {
    const std::string& user_name = argv[3];
    if (!client_impl->DelUgi(user_name, err)) {
      LOG(ERROR) << "delete ugi failed!" << err->ToString();
      return CliStatus::kError;
    }
  } else if (argc == 3 && op == "show") {
    return ShowUgiOp(client_impl, argc, argv, err);
  } else {
    PrintCmdHelpInfo(argv[1]);
    return CliStatus::kError;
  }
  std::cout << "Ugi " << op << " success" << std::endl;
  return CliStatus::kOk;
}

CliStatus ShowRoleOp(const std::shared_ptr<tera::ClientImpl>& client_impl, int32_t argc,
                     std::string* argv, ErrorCode* err) {
  std::vector<std::string> roles_list;
  if (!client_impl->ShowRole(&roles_list, err)) {
    LOG(ERROR) << "show roles failed!" << err->ToString();
    return CliStatus::kError;
  }
  std::cout << "Show role : " << std::endl;
  for (auto it = roles_list.begin(); it != roles_list.end(); ++it) {
    std::cout << *it << std::endl;
  }
  return CliStatus::kOk;
}

CliStatus RoleOp(Client* client, int32_t argc, std::string* argv, ErrorCode* err) {
  if (argc < 3) {
    PrintCmdHelpInfo(argv[1]);
    return CliStatus::kError;
  }
  const std::string& op = argv[2];
  std::shared_ptr<tera::ClientImpl> client_impl(
      (static_cast<ClientWrapper*>(client))->GetClientImpl());

  if (argc == 5) {
    const std::string& role_name = argv[3];
    const std::string& user_name = argv[4];
    if (op == "grant") {
      if (!client_impl->GrantRole(role_name, user_name, err)) {
        LOG(ERROR) << "grant role failed!" << err->ToString();
        return CliStatus::kError;
      }
    } else if (op == "revoke") {
      if (!client_impl->RevokeRole(role_name, user_name, err)) {
        LOG(ERROR) << "revoke role failed!" << err->ToString();
        return CliStatus::kError;
      }
    } else {
      PrintCmdHelpInfo(argv[1]);
      return CliStatus::kError;
    }
  } else if (argc == 4) {
    const std::string& role_name = argv[3];
    if (op == "add") {
      if (!client_impl->AddRole(role_name, err)) {
        LOG(ERROR) << "add role failed!" << err->ToString();
        return CliStatus::kError;
      }
    } else if (op == "del") {
      if (!client_impl->DelRole(role_name, err)) {
        LOG(ERROR) << "del role failed!" << err->ToString();
        return CliStatus::kError;
      }
    } else {
      PrintCmdHelpInfo(argv[1]);
      return CliStatus::kError;
    }
  } else if (argc == 3 && op == "show") {
    return ShowRoleOp(client_impl, argc, argv, err);
  } else {
    PrintCmdHelpInfo(argv[1]);
    return CliStatus::kError;
  }
  std::cout << "role " << op << " success" << std::endl;
  return CliStatus::kOk;
}

CliStatus ShowAuthPolicyOp(const std::shared_ptr<tera::ClientImpl>& client_impl, ErrorCode* err) {
  std::map<std::string, std::string> table_auth_policy_list;
  if (!client_impl->ShowAuthPolicy(&table_auth_policy_list, err)) {
    LOG(ERROR) << "show auth_policy failed!" << err->ToString();
    return CliStatus::kError;
  }
  std::cout << "TableName\tAuthType" << std::endl;
  for (auto it = table_auth_policy_list.begin(); it != table_auth_policy_list.end(); ++it) {
    std::cout << it->first << "\t" << it->second << std::endl;
  }
  return CliStatus::kOk;
}

CliStatus AuthOp(Client* client, int32_t argc, std::string* argv, ErrorCode* err) {
  if (argc < 3) {
    PrintCmdHelpInfo(argv[1]);
    return CliStatus::kError;
  }
  const std::string& op = argv[2];
  std::shared_ptr<tera::ClientImpl> client_impl(
      (static_cast<ClientWrapper*>(client))->GetClientImpl());
  if (argc == 5 && op == "set") {
    const std::string& table_name = argv[3];
    const std::string& auth_policy = argv[4];
    if (!client_impl->SetAuthPolicy(table_name, auth_policy, err)) {
      LOG(ERROR) << "set auth policy failed!" << err->ToString();
      return CliStatus::kError;
    }
  } else if (argc == 3 && op == "show") {
    return ShowAuthPolicyOp(client_impl, err);
  } else {
    PrintCmdHelpInfo(argv[1]);
    return CliStatus::kError;
  }
  std::cout << "auth " << op << " success" << std::endl;
  return CliStatus::kOk;
}

CliStatus DfsThroughputHardLimitOp(Client* client, int32_t argc, std::string* argv,
                                   ErrorCode* err) {
  if (argc != 4 && argc != 3) {
    PrintCmdHelpInfo(argv[1]);
    return CliStatus::kError;
  }

  std::string op = argv[2];
  if (argc == 4 && op != "write" && op != "read") {
    PrintCmdHelpInfo(argv[1]);
    return CliStatus::kError;
  }

  if (argc == 3 && op != "get") {
    PrintCmdHelpInfo(argv[1]);
    return CliStatus::kError;
  }

  std::vector<std::string> arg_list;
  arg_list.push_back(op);
  if (op != "get") {
    std::string value = argv[3];
    if (value == "reset") {
      arg_list.push_back("-1");
    } else {
      try {
        // check argument valid
        std::stol(argv[3]);
      } catch (...) {
        std::cout << "Convert " << argv[3] << " to number failed.";
        PrintCmdHelpInfo(argv[1]);
        return CliStatus::kError;
      }
      arg_list.push_back(argv[3]);
    }
  }
  std::string result;
  if (!client->CmdCtrl("dfs-hard-limit", arg_list, nullptr, &result, err)) {
    std::cout << "Fail to run dfs-quota " << op << ": " << result << std::endl;
    return CliStatus::kError;
  }

  std::cout << result << std::endl;
  return CliStatus::kOk;
}

CliStatus ProcedureLimitOp(Client* client, int32_t argc, std::string* argv, ErrorCode* err) {
  if (argc != 3 && argc != 5) {
    PrintCmdHelpInfo(argv[1]);
    return CliStatus::kError;
  }
  std::string op = argv[2];
  if (argc == 3 && op != "get") {
    PrintCmdHelpInfo(argv[1]);
    return CliStatus::kError;
  }
  if (argc == 5 && op != "set") {
    PrintCmdHelpInfo(argv[1]);
    return CliStatus::kError;
  }
  std::vector<std::string> arg_list;
  arg_list.push_back(op);
  if (op == "set") {
    arg_list.push_back(argv[3]);
    arg_list.push_back(argv[4]);
  }

  std::string result;
  if (!client->CmdCtrl("procedure-limit", arg_list, nullptr, &result, err)) {
    std::cout << "Fail to run procudure-limit " << op << ": " << result << std::endl;
    return CliStatus::kError;
  }
  std::cout << result << std::endl;
  return CliStatus::kOk;
}

// return false if similar command(s) not found
static bool PromptSimilarCmd(const char* msg) {
  if (msg == NULL) {
    return false;
  }
  bool found = false;
  int64_t len = strlen(msg);
  int64_t threshold = int64_t((len * 0.3 < 3) ? 3 : len * 0.3);
  int count = sizeof(builtin_cmd_list) / sizeof(char*);
  for (int i = 0; i < count; i += 2) {
    if (EditDistance(msg, builtin_cmd_list[i]) <= threshold) {
      if (!found) {
        std::cout << "Did you mean:" << std::endl;
        found = true;
      }
      std::cout << "    " << builtin_cmd_list[i] << std::endl;
    }
  }
  return found;
}

static void PrintUnknownCmdHelpInfo(const char* msg) {
  if (msg != NULL) {
    std::cout << "'" << msg << "' is not a valid command." << std::endl
              << std::endl;
  }
  if ((msg != NULL) && PromptSimilarCmd(msg)) {
    return;
  }
  PrintAllCmd();
}

static void InitializeCommandTable() {
  CommandTable& command_table = GetCommandTable();
  command_table["diff"] = MetaDiffOp;
  command_table["put"] = MetaPutOp;
  command_table["delete"] = MetaDeleteOp;
  command_table["modify"] = MetaModifyOp;
  command_table["get"] = MetaGetOp;
  command_table["show"] = MetaShowOp;
  command_table["backup"] = MetaBackUpOp;
  command_table["healthcheck"] = MetaHealthCheckOp;
  command_table["conv"] = MetaConvOp;
  command_table["ugi"] = UgiOp;
  command_table["role"] = RoleOp;
  command_table["auth"] = AuthOp;
  command_table["procedure-limit"] = ProcedureLimitOp;
  command_table["help"] = HelpOp;
  command_table["dfs-throughput-limit"] = DfsThroughputHardLimitOp;
}

CliStatus ExecuteCommand(Client* client, int argc, char** arg_list) {
  CliStatus ret = CliStatus::kOk;
  ErrorCode error_code;

  std::vector<std::string> parsed_arg_list;
  if (!ParseCommand(argc, arg_list, &parsed_arg_list)) {
    return CliStatus::kError;
  }
  std::string* argv = &parsed_arg_list[0];

  CommandTable& command_table = GetCommandTable();
  std::string cmd = argv[1];
  if (cmd == "version") {
    PrintSystemVersion();
  } else if (command_table.find(cmd) != command_table.end()) {
    ret = command_table[cmd](client, argc, argv, &error_code);
  } else {
    PrintUnknownCmdHelpInfo(argv[1].c_str());
    ret = CliStatus::kError;
  }

  if (error_code.GetType() != ErrorCode::kOK) {
    LOG(ERROR) << "fail reason: " << error_code.ToString();
  }
  return ret;
}

int main(int argc, char* argv[]) {
  FLAGS_minloglevel = 2;
  ::google::ParseCommandLineFlags(&argc, &argv, true);

  if (argc > 1 && std::string(argv[1]) == "version") {
    PrintSystemVersion();
    return 0;
  } else if (argc > 1 && std::string(argv[1]) == "help") {
    HelpOp(argc, argv);
    return 0;
  }

  if (FLAGS_flagfile == "") {
    FLAGS_flagfile = "../conf/tera.flag";
    if (access(FLAGS_flagfile.c_str(), R_OK) != 0) {
      FLAGS_flagfile = "./tera.flag";
    }
    utils::LoadFlagFile(FLAGS_flagfile);
  }
  if (FLAGS_meta_cli_token != g_metacli_token) {
    std::cout << "Please figure out what metacli is before use it." << std::endl;
    return -1;
  }

  Client* client = Client::NewClient(FLAGS_flagfile, NULL);
  if (client == NULL) {
    LOG(ERROR) << "client instance not exist";
    return -1;
  }

  InitializeCommandTable();

  CliStatus ret = CliStatus::kOk;
  if (argc == 1) {
    char* line = NULL;
    while ((line = readline("meta> ")) != NULL) {
      char* line_copy = strdup(line);
      std::vector<char*> arg_list;
      arg_list.push_back(argv[0]);
      char* tmp = NULL;
      char* token = strtok_r(line, " \t", &tmp);
      while (token != NULL) {
        arg_list.push_back(token);
        token = strtok_r(NULL, " \t", &tmp);
      }
      if (arg_list.size() == 2 &&
          (strcmp(arg_list[1], "quit") == 0 || strcmp(arg_list[1], "exit") == 0)) {
        free(line_copy);
        free(line);
        break;
      }
      if (arg_list.size() > 1) {
        add_history(line_copy);
        ret = ExecuteCommand(client, arg_list.size(), &arg_list[0]);
      }
      free(line_copy);
      free(line);
    }
  } else {
    ret = ExecuteCommand(client, argc, argv);
  }

  delete client;
  return ((ret == CliStatus::kOk) ? 0 : -1);
}
