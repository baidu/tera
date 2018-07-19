// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <readline/history.h>
#include <readline/readline.h>

#include <fstream>
#include <fcntl.h>
#include <iomanip>
#include <iostream>
#include <limits>
#include <map>
#include <memory>
#include <sstream>
#include <errno.h>
#include <gflags/gflags.h>
#include <glog/logging.h>

#include "common/thread_pool.h"
#include "common/base/string_ext.h"
#include "common/base/string_number.h"
#include "common/console/progress_bar.h"
#include "common/file/file_path.h"
#include "io/coding.h"
#include "io/utils_leveldb.h"
#include "leveldb/dfs.h"
#include "util/nfs.h"
#include "util/hdfs.h"
#include "proto/kv_helper.h"
#include "proto/proto_helper.h"
#include "proto/tabletnode.pb.h"
#include "proto/tabletnode_client.h"
#include "sdk/client_impl.h"
#include "sdk/cookie.h"
#include "sdk/sdk_utils.h"
#include "sdk/sdk_zk.h"
#include "sdk/table_impl.h"
#include "sdk/stat_table.h"
#include "tera.h"
#include "types.h"
#include "utils/crypt.h"
#include "utils/string_util.h"
#include "utils/tprinter.h"
#include "utils/utils_cmd.h"
#include "version.h"

DECLARE_string(flagfile);
DECLARE_string(log_dir);
DECLARE_string(tera_master_meta_table_name);
DECLARE_string(tera_zk_addr_list);
DECLARE_string(tera_zk_root_path);
DECLARE_bool(tera_sdk_batch_scan_enabled);
DECLARE_int64(tera_sdk_status_timeout);

DECLARE_string(tera_leveldb_env_type);
DECLARE_string(tera_leveldb_env_dfs_type);
DECLARE_string(tera_leveldb_env_nfs_mountpoint);
DECLARE_string(tera_leveldb_env_nfs_conf_path);
DECLARE_string(tera_leveldb_env_hdfs2_nameservice_list);
DECLARE_string(tera_dfs_so_path);
DECLARE_string(tera_dfs_conf);
DECLARE_uint64(tera_sdk_read_max_qualifiers);

DEFINE_int32(tera_client_batch_put_num, 1000, "num of each batch in batch put mode");
DEFINE_int32(tera_client_scan_package_size, 1024, "the package size (in KB) of each scan request");

DEFINE_int64(snapshot, 0, "read | scan snapshot");

DEFINE_int32(lg, -1, "locality group number.");
DEFINE_int32(concurrency, 1, "concurrency for compact table.");
DEFINE_int32(compact_timeout, 120000, "tablet compact timeout(ms), default 20min");
DEFINE_int64(timestamp, -1, "timestamp.");
DEFINE_string(tablets_file, "", "tablet set file");

DEFINE_bool(readable, true, "readable input");
DEFINE_bool(printable, true, "printable output");
DEFINE_bool(print_data, true, "is print data when scan");
DEFINE_bool(rowkey_count, false, "is print rowkey count when scan");

// using FLAGS instead of isatty() for compatibility
DEFINE_bool(stdout_is_tty, true, "is stdout connected to a tty");
DEFINE_bool(reorder_tablets, false, "reorder tablets by ts list");

// dfs related FLAGS
DEFINE_bool(asowner, false, "become owner and execute the command");
DEFINE_bool(e, false, "test dfs file exist or not");
DEFINE_bool(z, false, "test dfs file is zero or not");
DEFINE_bool(d, false, "test dfs file is directory or not");
DEFINE_bool(override, false, "dfs put file override the existing one");
DEFINE_bool(attribute, false, "dfs list file detail attribute");
DEFINE_bool(recursive, false, "dfs remove file recursively");

volatile int32_t g_start_time = 0;
volatile int32_t g_end_time = 0;
volatile int32_t g_used_time = 0;
volatile int32_t g_last_time  = 0;
volatile int64_t g_total_size = 0;
volatile int32_t g_key_num = 0;
Mutex g_stat_lock;

volatile int32_t g_cur_batch_num = 0;

tera::TPrinter::PrintOpt g_printer_opt;

using namespace tera;

typedef std::shared_ptr<Table> TablePtr;
typedef std::shared_ptr<TableImpl> TableImplPtr;
typedef std::map<std::string, int32_t(*)(Client*, int32_t, std::string*, ErrorCode*)> CommandTable;
// FileSystem command table
typedef std::map<std::string, int32_t(*)(int32_t, std::string*, ErrorCode*)> FSCommandTable;
//typedef std::map<std::string, std::function<int32_t(int32_t, std::string*, ErrorCode*)> > FSCommandTable;
/// global variables of single-row-txn used in interactive mode
tera::Transaction* g_row_txn = NULL;
Table* g_row_txn_table = NULL;

leveldb::Dfs* g_dfs = NULL;

static CommandTable& GetCommandTable(){
    static CommandTable command_table;
    return command_table;
}

static FSCommandTable& GetFSCommandTable() {
    static FSCommandTable fs_command_table;
    return fs_command_table;
}

const char* builtin_cmd_list[] = {
    "create",
    "create   <schema> [<delimiter_file>]                              \n\
         - schema syntax (all properties are optional):                \n\
               tablename <splitsize=1024,...> {                        \n\
                   lg1 <storage=flash,...> {                           \n\
                       cf1 <maxversion=3>,                             \n\
                       cf2...},                                        \n\
                   lg2...                                              \n\
               }                                                       \n\
         - kv mode schema:                                             \n\
               tablename <splitsize=1024, storage=memory, ...>         \n\
         - simple mode schema:                                         \n\
               tablename{cf1, cf2, cf3, ...}",

    "createbyfile",
    "createbyfile <schema_file> [<delimiter_file>]",

    "update",
    "update <schema>                                                   \n\
         - kv schema:                                                  \n\
               e.g. tablename<splitsize=512,storage=disk>              \n\
         - table schema:                                               \n\
           - update properties                                         \n\
               e.g. tablename<splitsize=512>                           \n\
               e.g. tablename{lg0{cf0<ttl=123>}}                       \n\
               e.g. tablename<mergesize=233>{lg0{cf0<ttl=123>}}        \n\
           - add new cf                                                \n\
               e.g. tablename{lg0{cf0<ttl=250>,new_cf<op=add,ttl=233>}}\n\
           - delete cf                                                 \n\
               e.g. tablename{lg0{cf0<op=del>}}",

    "update-check",
    "update-check <tablename>                                          \n\
                  check status of last online-schema-update",

    "enable",
    "enable <tablename>",

    "disable",
    "disable <tablename>",

    "drop",
    "drop <tablename>",

    "put",
    "put <tablename> <rowkey> [<columnfamily:qualifier>] <value>",

    "put-ttl",
    "put-ttl <tablename> <rowkey> [<columnfamily:qualifier>] <value> <ttl(second)>",

    "putif",
    "putif <tablename> <rowkey> [<columnfamily:qualifier>] <value>",

    "get",
    "get<tablename> <rowkey> [<columnfamily:qualifier>]",

    "scan",
    "scan[allv] <tablename> <startkey> <endkey> [<\"cf1|cf2\">]               \n\
                scan table from startkey to endkey.                           \n\
                (return all qulifier version when using suffix \"allv\")",

    "delete",
    "delete[1v] <tablename> <rowkey> [<columnfamily:qualifier>]               \n\
                delete row/columnfamily/qualifiers.                           \n\
                (only delete latest version when using suffix \"1v\")",

    "put_counter",
    "put_counter <tablename> <rowkey> [<columnfamily:qualifier>] <integer(int64_t)>",

    "get_counter",
    "get_counter <tablename> <rowkey> [<columnfamily:qualifier>]",

    "add",
    "add <tablename> <rowkey> <columnfamily:qualifier>   delta                 \n\
         add 'delta'(int64_t) to specified cell",

    "putint64",
    "putint64 <tablename> <rowkey> [<columnfamily:qualifier>] <integer(int64_t)>",

    "getint64",
    "getint64 <tablename> <rowkey> [<columnfamily:qualifier>]",

    "addint64",
    "addint64 <tablename> <rowkey> <columnfamily:qualifier>  delta              \n\
              add 'delta'(int64_t) to specified cell",

    "append",
    "append <tablename> <rowkey> [<columnfamily:qualifier>] <value>",

    "batchput",
    "batchput <tablename> <input file>",

    "batchget",
    "batchget <tablename> <input file>",

    "show",
    "show[x] [<tablename>]                                               \n\
             show table list or tablets info.                            \n\
             (show more detail when using suffix \"x\")",

    "showschema",
    "showschema[x] <tablename>                                            \n\
                   show table schema (show more detail when using suffix \"x\")",

    "showts",
    "showts[x] [<tabletnode addr>]                                        \n\
               show all tabletnodes or single tabletnode info.            \n\
               (show more detail when using suffix \"x\")",

    "range",
    "range[x]  <tablename>                                                \n\
               get all tablets range.                                     \n\
               --reorder_tablets=true ordered tablets by ts addr          \n\
               (show more detail when using suffix \"x\")",

    "txn",
    "txn <operation> <params>                                             \n\
         start      <tablename> <row_key>                                 \n\
         commit                                                           \n\
         (only support single row transaction)",

    "cas",
    "cas <tablename> <rowkey> <columnfamily:qualifier> <old_value> <new_value>                     \n\
         Compare and set a value atomically. (The txn value of table schema must be 'on')          \n\
         This command will compare the value at rowkey:columnfamily:qualifier with <old_value>:    \n\
         -> equal    : put <new_value> to this location.                                           \n\
         -> not equal: do nothing.",

    "user",
    "user <operation> <params>                                            \n\
          create          <username> <password>                           \n\
          changepwd       <username> <new-password>                       \n\
          show            <username>                                      \n\
          delete          <username>                                      \n\
          addtogroup      <username> <groupname>                          \n\
          deletefromgroup <username> <groupname>",

    "tablet",
    "tablet <operation> <params>                                          \n\
            move    <tablet_path> <target_addr>                           \n\
            movex   <tablet_path> <target_addr> <lg_list>                 \n\
                    * only for force move tablet ignore error             \n\
            reload  <tablet_path>                                         \n\
                    force to unload and load on the same ts               \n\
            reloadx <tablet_path> <lg_list>                               \n\
                    force to unload and load on the same ts               \n\
                    * only for force reload tablet ignore error           \n\
                    lg_list : lg1:lg2:lg3                                 \n\
            compact <tablet_path>                                         \n\
            split   <tablet_path>                                         \n\
            merge   <tablet_path>                                         \n\
            scan    <tablet_path>",

    "compact",
    "compact <tablename> [--lg=] [--concurrency=] [--tablets_file=]       \n\
             run manual compaction on a table.                            \n\
             --lg: only run compact on specified lg number.               \n\
             --concurrency: compacting tablets number at the same time.   \n\
             --tablets_file: specify tablet set, one tablet_path each line.",

    "safemode",
    "safemode [get|enter|leave]",

    "meta",
    "meta[2] [backup|check|repair|show]                                   \n\
             meta for master memory, meta2 for meta table.",

    "findmaster",
    "findmaster                                                           \n\
                find the address of master",

    "findts",
    "findts <tablename> [rowkey]                                          \n\
            find the specify tabletnode serving 'rowkey'.                 \n\
            if 'rowkey' is omited, read from stdin with one rowkey per line.",

    "reload",
    "reload config hostname:port                                          \n\
            notify master | ts reload flag file                           \n\
            *** at your own risk ***",

    "kick",
    "kick hostname:port                                                   \n\
          ask master to kick a tabletserver                               \n\
          *** at your own risk ***",


    "findtablet",
    "findtablet <tablename> <rowkey-prefix>                               \n\
                <tablename> <start-key> <end-key>",
    
    "stat",
    "stat <command>   <args>                                              \n\
          corruption  [tabletnode] [tabletpath]                           \n\
          -- show tablet load or compaction corruption detail message     \n\
                                                                          \n\
          example:                                                        \n\
            ./teracli stat corruption hostname::port t1/tablet00000001",

    "cookie",
    "cookie <command> <args>                                              \n\
            dump     cookie-file     -- dump contents of specified files  \n\
            findkey  cookie-file key -- find a key's info",

    "help",
    "help [cmd]                                                           \n\
          show manual for a or all cmd(s)",

    "dfs",
    "dfs [cmd]        args                                                \n\
         mkdir        $NFS_PATH                                           \n\
         touchz       $NFS_PATH                                           \n\
         test         [-e|-z|-d]     $NFS_PATH                            \n\
         get          $NFS_PATH      $LOCAL_PATH                          \n\
         put          [--override]   $LOCAL_PATH    $NFS_PATH             \n\
         ls           [--attribute]  $NFS_PATH                            \n\
         lsr          [--attribute]  $NFS_PATH                            \n\
         dus          $NFS_PATH                                           \n\
         rm           [--recursive]  $NFS_PATH                            \n\
         stat         $NFS_PATH                                           \n\
         rename       $NFS_PATH_SRC  $NFS_PATH_DEST                       \n\
         unlockdir    $NFS_PATH                                           \n\
         checksum     $NFS_PATH      $OFFSET        $LENGTH               \n\
         forcerelease $NFS_PATH",

    "gtxn",
    "gtxn                                                                 \n\
         complete a global transaction with input from std input or file. \n\
         pay attention to set flags '--tera_sdk_client_for_gtxn' and      \n\
         '--tera_sdk_tso_client_enabled' equal to true.                   \n\
                                                                          \n\
         input file format:                                               \n\
         BEGIN                                                            \n\
             statement [\\n statement] ...                                \n\
         COMMIT                                                           \n\
                                                                          \n\
         statement:                                                       \n\
             PUT|GET|DEL <tablename> <rowkey> <cf:qu> [put_value]         \n\
                                                                          \n\
         example: cat input_file | ./teracli gtxn                         \n\
                                                                          \n\
         input_file example:                                              \n\
         BEGIN                                                            \n\
             PUT table1 key1 cf1:qu1 value1                               \n\
             DEL table2 key2 cf2:qu2                                      \n\
             GET table3 key3 cf3:qu3                                      \n\
         COMMIT",
 
    "version",
    "version                                                              \n\
             show version info",

};

static void PrintCmdHelpInfo(const char* msg) {
    if (msg == NULL) {
        return;
    }
    int count = sizeof(builtin_cmd_list)/sizeof(char*);
    for (int i = 0; i < count; i+=2) {
        if(strncmp(msg, builtin_cmd_list[i], 32) == 0) {
            std::cout << builtin_cmd_list[i + 1] << std::endl;
            return;
        }
    }
}

static void PrintCmdHelpInfo(const std::string& msg) {
    PrintCmdHelpInfo(msg.c_str());
}

static void PrintAllCmd() {
    std::cout << "there is cmd list:" << std::endl;
    int count = sizeof(builtin_cmd_list)/sizeof(char*);
    bool newline = false;
    for (int i = 0; i < count; i+=2) {
        std::cout << std::setiosflags(std::ios::left) << std::setw(20) << builtin_cmd_list[i];
        if (newline) {
            std::cout << std::endl;
            newline = false;
        } else {
            newline = true;
        }
    }

    std::cout << std::endl << "help [cmd] for details." << std::endl;
}

// return false if similar command(s) not found
static bool PromptSimilarCmd(const char* msg) {
    if (msg == NULL) {
        return false;
    }
    bool found = false;
    int64_t len = strlen(msg);
    int64_t threshold = int64_t((len * 0.3 < 3) ? 3 : len * 0.3);
    int count = sizeof(builtin_cmd_list)/sizeof(char*);
    for (int i = 0; i < count; i+=2) {
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
        std::cout << "'" << msg << "' is not a valid command." << std::endl << std::endl;
    }
    if ((msg != NULL)
        && PromptSimilarCmd(msg)) {
        return;
    }
    PrintAllCmd();
}

int32_t CreateOp(Client* client, int32_t argc, std::string* argv, ErrorCode* err) {
    if (argc < 3) {
        PrintCmdHelpInfo(argv[1]);
        return -1;
    }

    TableDescriptor table_desc;
    std::vector<std::string> delimiters;
    std::string schema = argv[2];
    if (!ParseTableSchema(schema, &table_desc, err)) {
        LOG(ERROR) << "fail to parse input table schema.";
        return -1;
    }
    if (argc == 4) {
        // have tablet delimiters
        if (!ParseDelimiterFile(argv[3], &delimiters)) {
            LOG(ERROR) << "fail to parse delimiter file.";
            return -1;
        }
    } else if (argc > 4) {
        PrintCmdHelpInfo("create");
        return -1;
    }
    if (!client->CreateTable(table_desc, delimiters, err)) {
        LOG(ERROR) << "fail to create table, " << err->ToString();
        return -1;
    }
    ShowTableDescriptor(table_desc);
    return 0;
}

int32_t CreateByFileOp(Client* client, int32_t argc, std::string* argv, ErrorCode* err) {
    if (argc < 3) {
        PrintCmdHelpInfo(argv[1]);
        return -1;
    }

    TableDescriptor table_desc;
    if (!ParseTableSchemaFile(argv[2], &table_desc, err)) {
        LOG(ERROR) << "fail to parse input table schema.";
        return -1;
    }

    std::vector<std::string> delimiters;
    if (argc == 4) {
        // have tablet delimiters
        if (!ParseDelimiterFile(argv[3], &delimiters)) {
            LOG(ERROR) << "fail to parse delimiter file.";
            return -1;
        }
    } else if (argc > 4) {
        LOG(ERROR) << "too many args: " << argc;
        return -1;
    }
    if (!client->CreateTable(table_desc, delimiters, err)) {
        LOG(ERROR) << "fail to create table, " << err->ToString();
        return -1;
    }
    ShowTableDescriptor(table_desc);
    return 0;
}

int32_t UpdateCheckOp(Client* client, int32_t argc, std::string* argv, ErrorCode* err) {
    if (argc != 3) {
        PrintCmdHelpInfo(argv[1]);
        return -1;
    }
    bool done = false;
    if (!client->UpdateCheck(argv[2], &done, err)) {
        std::cerr << err->ToString() << std::endl;
        return -1;
    }
    std::cout << "update " << (done ? "successed" : "is running...") << std::endl;
    return 0;
}

int32_t UpdateOp(Client* client, int32_t argc, std::string* argv, ErrorCode* err) {
    if (argc != 3) {
        PrintCmdHelpInfo(argv[1]);
        return -1;
    }
    std::string schema = argv[2];
    PropTree schema_tree;
    if (!schema_tree.ParseFromString(schema)) {
        LOG(ERROR) << "[update] invalid schema: " << schema;
        LOG(ERROR) << "[update] state: " << schema_tree.State();
        return -1;
    }
    std::string tablename = schema_tree.GetRootNode()->name_;
    TableDescriptor* table_desc = client->GetTableDescriptor(tablename, err);
    if (table_desc == NULL) {
        LOG(ERROR) << "[update] can't get the TableDescriptor of table: " << tablename;
        return -1;
    }

    if (!UpdateTableDescriptor(schema_tree, table_desc, err)) {
        LOG(ERROR) << "[update] update failed";
        return -1;
    }

    if (!client->UpdateTable(*table_desc, err)) {
        LOG(ERROR) << "[update] fail to update table, " << err->ToString();
        return -1;
    }
    ShowTableDescriptor(*table_desc);
    delete table_desc;
    return 0;
}

int32_t DropOp(Client* client, int32_t argc, std::string* argv, ErrorCode* err) {
    if (argc < 3) {
        PrintCmdHelpInfo(argv[1]);
        return -1;
    }

    std::string tablename = argv[2];
    if (!client->DeleteTable(tablename, err)) {
        LOG(ERROR) << "fail to delete table, " << err->ToString();
        return -1;
    }
    return 0;
}

int32_t EnableOp(Client* client, int32_t argc, std::string* argv, ErrorCode* err) {
    if (argc < 3) {
        PrintCmdHelpInfo(argv[1]);
        return -1;
    }

    std::string tablename = argv[2];
    if (!client->EnableTable(tablename, err)) {
        LOG(ERROR) << "fail to enable table";
        return -1;
    }
    return 0;
}

int32_t DisableOp(Client* client, int32_t argc, std::string* argv, ErrorCode* err) {
    if (argc < 3) {
        PrintCmdHelpInfo(argv[1]);
        return -1;
    }

    std::string tablename = argv[2];
    if (!client->DisableTable(tablename, err)) {
        LOG(ERROR) << "fail to disable table";
        return -1;
    }
    TableMeta table_meta;
    TabletMetaList tablet_list;
    std::shared_ptr<tera::ClientImpl> client_impl((static_cast<ClientWrapper*>(client))->GetClientImpl());
    if (!client_impl->ShowTablesInfo(tablename, &table_meta, &tablet_list, err)) {
        LOG(ERROR) << "table not exist: " << tablename;
        return -1;
    }

    uint64_t tablet_num = tablet_list.meta_size();
    common::ProgressBar progress_bar(common::ProgressBar::ENHANCED, tablet_num, 100);
    while (true) {
        if (!client_impl->ShowTablesInfo(tablename, &table_meta, &tablet_list, err)) {
            LOG(ERROR) << "table not exist: " << tablename;
            return -1;
        }
        uint64_t tablet_cnt = 0;
        for (int32_t i = 0; i < tablet_list.meta_size(); ++i) {
            const TabletMeta& tablet = tablet_list.meta(i);
            VLOG(10) << "tablet status: " << StatusCodeToString(tablet.status());
            if (tablet.status() == TabletMeta::kTabletDisable || 
                tablet.status() == TabletMeta::kTabletOffline) {
                tablet_cnt++;
            }
        }
        progress_bar.Refresh(tablet_cnt);
        if (tablet_cnt == tablet_num) {
            // disable finish
            progress_bar.Done();
            break;
        }
        sleep(1);
    }
    return 0;
}

void ParseCfQualifier(const std::string& input, std::string* columnfamily,
                      std::string* qualifier, bool *has_qualifier = NULL) {
    std::string::size_type pos = input.find(":", 0);
    if (pos != std::string::npos) {
        *columnfamily = input.substr(0, pos);
        *qualifier = input.substr(pos + 1);
        if (has_qualifier) {
            *has_qualifier = true;
        }
    } else {
        *columnfamily = input;
        if (has_qualifier) {
            *has_qualifier = false;
        }
    }
}

int32_t PutInt64Op(Client* client, int32_t argc, std::string* argv, ErrorCode* err) {
    if (argc != 5 && argc != 6) {
        LOG(ERROR) << "args number error: " << argc << ", need 5 | 6.";
        PrintCmdHelpInfo(argv[1]);
        return -1;
    }

    std::string tablename = argv[2];
    TablePtr table(client->OpenTable(tablename, err));
    if (table == NULL) {
        LOG(ERROR) << "fail to open table";
        return -1;
    }

    std::string rowkey = argv[3];
    std::string columnfamily = "";
    std::string qualifier = "";
    std::string value;
    if (argc == 5) {
        // use table as kv
        value = argv[4];
    } else if (argc == 6) {
        ParseCfQualifier(argv[4], &columnfamily, &qualifier);
        value = argv[5];
    }
    int64_t value_int;
    if (!StringToNumber(value.c_str(), &value_int)) {
       LOG(ERROR) << "invalid Integer number Got: " << value;
       return -1;
    }
    if (!table->Put(rowkey, columnfamily, qualifier, value_int, err)) {
        LOG(ERROR) << "fail to put record to table: " << tablename;
        return -1;
    }
    return 0;
}

int32_t PutCounterOp(Client* client, int32_t argc, std::string* argv, ErrorCode* err) {
    if (argc != 5 && argc != 6) {
        LOG(ERROR) << "args number error: " << argc << ", need 5 | 6.";
        PrintCmdHelpInfo(argv[1]);
        return -1;
    }

    std::string tablename = argv[2];
    TablePtr table(client->OpenTable(tablename, err));
    if (table == NULL) {
        LOG(ERROR) << "fail to open table";
        return -1;
    }

    std::string rowkey = argv[3];
    std::string columnfamily = "";
    std::string qualifier = "";
    std::string value;
    if (argc == 5) {
        // use table as kv
        value = argv[4];
    } else if (argc == 6) {
        ParseCfQualifier(argv[4], &columnfamily, &qualifier);
        value = argv[5];
    }
    int64_t counter;
    if (!StringToNumber(value.c_str(), &counter)) {
       LOG(ERROR) << "invalid Integer number Got: " << value;
       return -1;
    }

    std::string s_counter = tera::CounterCoding::EncodeCounter(counter);
    if (!table->Put(rowkey, columnfamily, qualifier, s_counter, err)) {
        LOG(ERROR) << "fail to put record to table: " << tablename;
        return -1;
    }
    return 0;
}

int32_t PutOp(Client* client, int32_t argc, std::string* argv, ErrorCode* err) {
    if (argc != 5 && argc != 6) {
        LOG(ERROR) << "args number error: " << argc << ", need 5 | 6.";
        PrintCmdHelpInfo(argv[1]);
        return -1;
    }

    std::string tablename = argv[2];
    TablePtr table(client->OpenTable(tablename, err));
    if (table == NULL) {
        LOG(ERROR) << "fail to open table";
        return -1;
    }

    std::string rowkey = argv[3];
    std::string columnfamily = "";
    std::string qualifier = "";
    std::string value;
    if (argc == 5) {
        // use table as kv
        value = argv[4];
    } else if (argc == 6) {
        ParseCfQualifier(argv[4], &columnfamily, &qualifier);
        value = argv[5];
    }

    std::unique_ptr<RowMutation> mutation(table->NewRowMutation(rowkey));
    if (FLAGS_timestamp == -1) {
        mutation->Put(columnfamily, qualifier, value);
    } else {
        mutation->Put(columnfamily, qualifier, FLAGS_timestamp, value);
    }
    if (g_row_txn != NULL) {
        g_row_txn->ApplyMutation(mutation.get());
    } else {
        table->ApplyMutation(mutation.get());
    }
    if (mutation->GetError().GetType() != tera::ErrorCode::kOK) {
        std::cout << mutation->GetError().ToString() << std::endl;
        return -1;
    }
    return 0;
}

int32_t PutTTLOp(Client* client, int32_t argc, std::string* argv, ErrorCode* err) {
    if (argc != 6 && argc != 7) {
        LOG(ERROR) << "args number error: " << argc << ", need 5 | 6.";
        PrintCmdHelpInfo(argv[1]);
        return -1;
    }

    std::string tablename = argv[2];
    TablePtr table(client->OpenTable(tablename, err));
    if (table == NULL) {
        LOG(ERROR) << "fail to open table";
        return -1;
    }

    std::string rowkey = argv[3];
    std::string columnfamily = "";
    std::string qualifier = "";
    std::string value;
    int32_t ttl = -1;
    if (argc == 6) {
        // use table as kv
        value = argv[4];
        ttl = atoi(argv[5].c_str());
    } else if (argc == 7) {
        ParseCfQualifier(argv[4], &columnfamily, &qualifier);
        value = argv[5];
        ttl = atoi(argv[6].c_str());
    }
    if (!table->Put(rowkey, columnfamily, qualifier, value, ttl, err)) {
        LOG(ERROR) << "fail to put record to table: " << tablename;
        return -1;
    }
    return 0;
}

int32_t AppendOp(Client* client, int32_t argc, std::string* argv, ErrorCode* err) {
    if (argc != 5 && argc != 6) {
        LOG(ERROR) << "args number error: " << argc << ", need 5 | 6.";
        PrintCmdHelpInfo(argv[1]);
        return -1;
    }

    std::string tablename = argv[2];
    TablePtr table(client->OpenTable(tablename, err));
    if (table == NULL) {
        LOG(ERROR) << "fail to open table";
        return -1;
    }

    std::string rowkey = argv[3];
    std::string columnfamily = "";
    std::string qualifier = "";
    std::string value;
    if (argc == 5) {
        // use table as kv
        value = argv[4];
    } else if (argc == 6) {
        ParseCfQualifier(argv[4], &columnfamily, &qualifier);
        value = argv[5];
    }
    if (!table->Append(rowkey, columnfamily, qualifier, value, err)) {
        LOG(ERROR) << "fail to append record to table: " << tablename;
        return -1;
    }
    return 0;
}

int32_t PutIfAbsentOp(Client* client, int32_t argc, std::string* argv, ErrorCode* err) {
    if (argc != 5 && argc != 6) {
        LOG(ERROR) << "args number error: " << argc << ", need 5 | 6.";
        PrintCmdHelpInfo(argv[1]);
        return -1;
    }

    std::string tablename = argv[2];
    TablePtr table(client->OpenTable(tablename, err));
    if (table == NULL) {
        LOG(ERROR) << "fail to open table";
        return -1;
    }

    std::string rowkey = argv[3];
    std::string columnfamily = "";
    std::string qualifier = "";
    std::string value;
    if (argc == 5) {
        // use table as kv
        value = argv[4];
    } else if (argc == 6) {
        ParseCfQualifier(argv[4], &columnfamily, &qualifier);
        value = argv[5];
    }
    if (!table->PutIfAbsent(rowkey, columnfamily, qualifier, value, err)) {
        LOG(ERROR) << "fail to put record to table: " << tablename;
        return -1;
    }
    return 0;
}

int32_t AddOp(Client* client, int32_t argc, std::string* argv, ErrorCode* err) {
    if (argc != 5 && argc != 6) {
        LOG(ERROR)<< "args number error: " << argc << ", need 5 | 6.";
        PrintCmdHelpInfo(argv[1]);
        return -1;
    }

    std::string tablename = argv[2];
    TablePtr table(client->OpenTable(tablename, err));
    if (table == NULL) {
        LOG(ERROR) << "fail to open table";
        return -1;
    }

    std::string rowkey = argv[3];
    std::string columnfamily = "";
    std::string qualifier = "";
    std::string value;
    if (argc == 5) {
        // use table as kv
        value = argv[4];
    } else if (argc == 6) {
        ParseCfQualifier(argv[4], &columnfamily, &qualifier);
        value = argv[5];
    }
    int64_t delta;
    if (!StringToNumber(value.c_str(), &delta)) {
        LOG(ERROR) << "invalid Integer number Got: " << value;
        return -1;
    }
    if (!table->Add(rowkey, columnfamily, qualifier, delta, err)) {
        LOG(ERROR) << "fail to add record to table: " << tablename;
        return -1;
    }
    return 0;
}

int32_t AddInt64Op(Client* client, int32_t argc, std::string* argv, ErrorCode* err) {
    if (argc != 5 && argc != 6) {
        LOG(ERROR)<< "args number error: " << argc << ", need 5 | 6.";
        PrintCmdHelpInfo(argv[1]);
        return -1;
    }

    std::string tablename = argv[2];
    TablePtr table(client->OpenTable(tablename, err));
    if (table == NULL) {
        LOG(ERROR) << "fail to open table";
        return -1;
    }

    std::string rowkey = argv[3];
    std::string columnfamily = "";
    std::string qualifier = "";
    std::string value;
    if (argc == 5) {
        // use table as kv
        value = argv[4];
    } else if (argc == 6) {
        ParseCfQualifier(argv[4], &columnfamily, &qualifier);
        value = argv[5];
    }
    int64_t delta;
    if (!StringToNumber(value.c_str(), &delta)) {
        LOG(ERROR) << "invalid Integer number Got: " << value;
        return -1;
    }
    if (!table->AddInt64(rowkey, columnfamily, qualifier, delta, err)) {
        LOG(ERROR) << "fail to add record to table: " << tablename;
        return -1;
    }
    return 0;
}

int32_t GetInt64Op(Client* client, int32_t argc, std::string* argv, ErrorCode* err) {
    if (argc != 4 && argc != 5) {
        LOG(ERROR) << "args number error: " << argc << ", need 5 | 6.";
        PrintCmdHelpInfo(argv[1]);
        return -1;
    }

    std::string tablename = argv[2];
    TablePtr table(client->OpenTable(tablename, err));
    if (table == NULL) {
        LOG(ERROR) << "fail to open table";
        return -1;
    }

    std::string rowkey = argv[3];
    std::string columnfamily = "";
    std::string qualifier = "";
    int64_t value;
    if (argc == 4) {
        // use table as kv
    } else if (argc == 5) {
        ParseCfQualifier(argv[4], &columnfamily, &qualifier);
    }

    if (!table->Get(rowkey, columnfamily, qualifier, &value, err, FLAGS_snapshot)) {
        LOG(ERROR) << "fail to get record from table: " << tablename;
        return -1;
    }

    std::cout << value << std::endl;
    return 0;
}

std::string PrintableFormatter(const std::string& value) {
    if (FLAGS_printable) {
        return DebugString(value);
    } else {
        return value;
    }
}

int32_t GetOp(Client* client, int32_t argc, std::string* argv, ErrorCode* err) {
    if (argc != 4 && argc != 5) {
        LOG(ERROR) << "args number error: " << argc << ", need 5 | 6.";
        PrintCmdHelpInfo(argv[1]);
        return -1;
    }

    std::string tablename = argv[2];
    TablePtr table(client->OpenTable(tablename, err));
    if (table == NULL) {
        LOG(ERROR) << "fail to open table";
        return -1;
    }

    std::string rowkey = argv[3];
    std::string columnfamily = "";
    std::string qualifier = "";
    std::string value;
    std::unique_ptr<RowReader> reader(table->NewRowReader(rowkey));
    if (argc == 4) {
        // use table as kv or get row
    } else if (argc == 5) {
        bool has_qu;
        ParseCfQualifier(argv[4], &columnfamily, &qualifier, &has_qu);
        if (has_qu) {
            reader->AddColumn(columnfamily, qualifier);
        } else {
            reader->AddColumnFamily(columnfamily);
        }
    }
    reader->SetMaxQualifiers(FLAGS_tera_sdk_read_max_qualifiers);
    if (g_row_txn != NULL) {
        g_row_txn->Get(reader.get());
    } else {
        table->Get(reader.get());
    }
    while (!reader->Done()) {
        std::cout << PrintableFormatter(reader->RowName()) << ":"
            << PrintableFormatter(reader->ColumnName()) << ":"
            << reader->Timestamp() << ":"
            << PrintableFormatter(reader->Value()) << std::endl;
        reader->Next();
    }
    if (reader->GetError().GetType() != tera::ErrorCode::kOK
        && reader->GetError().GetType() != tera::ErrorCode::kNotFound) {
        std::cout << reader->GetError().ToString() << std::endl;
        return -1;
    }
    return 0;
}

int32_t GetCounterOp(Client* client, int32_t argc, std::string* argv, ErrorCode* err) {
    if (argc != 4 && argc != 5) {
        LOG(ERROR) << "args number error: " << argc << ", need 5 | 6.";
        PrintCmdHelpInfo(argv[1]);
        return -1;
    }

    std::string tablename = argv[2];
    TablePtr table(client->OpenTable(tablename, err));
    if (table == NULL) {
        LOG(ERROR) << "fail to open table";
        return -1;
    }

    std::string rowkey = argv[3];
    std::string columnfamily = "";
    std::string qualifier = "";
    std::string value;
    if (argc == 4) {
        // use table as kv
    } else if (argc == 5) {
        ParseCfQualifier(argv[4], &columnfamily, &qualifier);
    }

    if (!table->Get(rowkey, columnfamily, qualifier, &value, err)) {
        LOG(ERROR) << "fail to get record from table: " << tablename;
        return -1;
    }

    int64_t counter = 0;
    bool ret = tera::CounterCoding::DecodeCounter(value, &counter);
    if (!ret) {
        LOG(ERROR) << "invalid counter read, fail to parse";
    } else {
        std::cout << counter << std::endl;
    }
    return 0;
}


int32_t DeleteOp(Client* client, int32_t argc, std::string* argv, ErrorCode* err) {
    if (argc != 4 && argc != 5) {
        PrintCmdHelpInfo("delete");
        return -1;
    }

    std::string tablename = argv[2];
    std::string rowkey = argv[3];
    TablePtr table(client->OpenTable(tablename, err));
    if (table == NULL) {
        LOG(ERROR) << "fail to open table";
        return -1;
    }

    std::string op = argv[1];
    RowMutation* mutation = table->NewRowMutation(rowkey);
    if (argc == 4) {
        // delete a row
        mutation->DeleteRow();
    } else if (argc == 5) {
        // delete a family or column
        std::string input(argv[4]);
        if (input.find(":", 0) == std::string::npos) {
            // delete a family
            if (FLAGS_timestamp == -1) {
                mutation->DeleteFamily(input);
            } else {
                mutation->DeleteFamily(input, FLAGS_timestamp);
            }
        } else {
            std::string family;
            std::string qualifier;
            ParseCfQualifier(input, &family, &qualifier);
            if (op == "delete") {
                // delete a column (all versions)
                if (FLAGS_timestamp == -1) {
                    mutation->DeleteColumns(family, qualifier);
                } else {
                    mutation->DeleteColumns(family, qualifier, FLAGS_timestamp);
                }
            } else if (op == "delete1v") {
                // delete the newest version
                if (FLAGS_timestamp == -1) {
                    mutation->DeleteColumn(family, qualifier);
                } else {
                    mutation->DeleteColumn(family, qualifier, FLAGS_timestamp);
                }
            }
        }
    } else {
        LOG(FATAL) << "should not run here.";
    }
    if (g_row_txn != NULL) {
        g_row_txn->ApplyMutation(mutation);
    } else {
        table->ApplyMutation(mutation);
    }
    if (mutation->GetError().GetType() != tera::ErrorCode::kOK) {
        std::cout << mutation->GetError().ToString() << std::endl;
    }
    delete mutation;
    return 0;
}

int32_t ScanRange(TablePtr& table, ScanDescriptor& desc, ErrorCode* err) {
    desc.SetBufferSize(FLAGS_tera_client_scan_package_size << 10);
    desc.SetAsync(FLAGS_tera_sdk_batch_scan_enabled);
    desc.SetSnapshot(FLAGS_snapshot);
    desc.SetMaxQualifiers(FLAGS_tera_sdk_read_max_qualifiers);

    ResultStream* result_stream;
    if ((result_stream = table->Scan(desc, err)) == NULL) {
        LOG(ERROR) << "fail to scan records from table: " << table->GetName();
        return -7;
    }
    g_start_time = time(NULL);

    std::string last_key = "";
    int64_t found_num = 0;
    while (!result_stream->Done(err)) {
        if (result_stream->RowName() != last_key) {
            found_num++;
        }
        last_key = result_stream->RowName();

        int32_t len = result_stream->RowName().size()
            + result_stream->ColumnName().size()
            + sizeof(result_stream->Timestamp())
            + result_stream->Value().size();
        g_total_size += len;
        g_key_num ++;
        g_cur_batch_num ++;
        if (FLAGS_print_data) {
            std::cout << PrintableFormatter(result_stream->RowName()) << ":"
                << PrintableFormatter(result_stream->ColumnName()) << ":"
                << result_stream->Timestamp() << ":"
                << PrintableFormatter(result_stream->Value()) << std::endl;
        }

        result_stream->Next();
        if (g_cur_batch_num >= FLAGS_tera_client_batch_put_num) {
            int32_t time_cur=time(NULL);
            uint32_t time_used = time_cur - g_start_time;
            LOG(INFO) << "Scaning " << g_key_num << " keys " << g_key_num/(time_used?time_used:1)
                   << " keys/S " << g_total_size/1024.0/1024/(time_used?time_used:1) << " MB/S ";
            g_cur_batch_num = 0;
            g_last_time = time_cur;
        }
    }
    if (FLAGS_rowkey_count) {
        std::cout << found_num << std::endl;
    }
    delete result_stream;
    if (err->GetType() != ErrorCode::kOK) {
        LOG(ERROR) << "fail to finish scan: " << err->ToString();
        return -1;
    }
    g_end_time = time(NULL);
    g_used_time = g_end_time - g_start_time;
    LOG(INFO) << "Scan done " << g_key_num << " keys " << g_key_num/(g_used_time?g_used_time:1)
            <<" keys/S " << g_total_size/1024.0/1024/(g_used_time?g_used_time:1) << " MB/S ";
    return 0;
}

int32_t ScanOp(Client* client, int32_t argc, std::string* argv, ErrorCode* err) {
    if (argc != 5 && argc != 6) {
        PrintCmdHelpInfo("scan");
        return -1;
    }

    std::string op = argv[1];
    std::string tablename = argv[2];
    TablePtr table(client->OpenTable(tablename, err));
    if (table == NULL) {
        LOG(ERROR) << "fail to open table";
        return -1;
    }

    std::string start_rowkey = argv[3];
    std::string end_rowkey = argv[4];
    if (start_rowkey == "\"\"") {
        start_rowkey = "";
    }
    if (end_rowkey == "\"\"") {
        end_rowkey = "";
    }
    LOG(INFO) << "start_key=" << start_rowkey << " end_key=" << end_rowkey;
    ScanDescriptor desc(start_rowkey);
    desc.SetEnd(end_rowkey);
    if (op == "scanallv") {
        desc.SetMaxVersions(std::numeric_limits<int>::max());
    }
    if (argc == 6 && !desc.SetFilter(argv[5])) {
        LOG(ERROR) << "fail to parse scan schema: " << argv[5];
        return -1;
    }
    return ScanRange(table, desc, err);
}

static std::string DoubleToStr(double value)
{
    const int len_max = 32;
    char buffer[len_max];
    int len = snprintf(buffer, len_max, "%.2g", value);
    return std::string(buffer, len);
}

std::string BytesNumberToString(const uint64_t size) {
    if (FLAGS_stdout_is_tty) {
        // 1024 -> 1K
        // 1024*1024 -> 1M
        return utils::ConvertByteToString(size);
    }
    return NumberToString(size);
}

std::string DateNumberToString(int64_t ts) {
    if (FLAGS_stdout_is_tty) {
        return get_time_str(ts);
    }
    return NumberToString(ts);
}

static std::string GetTabletStatusString(const TabletMetaList& tablet_list, int64_t now, int32_t i) {
    // old tera master will not return timestamp #963
    if ((tablet_list.timestamp_size() > 0)) {
        // new tera master
        int64_t delta = now - tablet_list.timestamp(i);
        TabletMeta::TabletStatus status = tablet_list.meta(i).status();
        TabletMeta::TabletStatus db_status = tablet_list.counter(i).db_status();
        if (db_status == TabletMeta::kTabletCorruption) {
            return StatusCodeToString(db_status);
        }
        if ((status == TabletMeta::kTabletReady) && (delta > FLAGS_tera_sdk_status_timeout * 1000000)) {
            return "kUnknown";
        } else {
            return StatusCodeToString(tablet_list.meta(i).status());
        }
    } else {
        // old tera master
        return StatusCodeToString(tablet_list.meta(i).status());
    }
}

int32_t ShowTabletList(const TabletMetaList& tablet_list, bool is_server_addr, bool is_x) {
    TPrinter printer;
    int cols;
    std::vector<string> row;
    int64_t now = get_micros();
    if (is_x) {
        if (is_server_addr) {
            cols = 14;
            printer.Reset(cols,
                           " ", "server_addr", "path", "status", "size",
                           "lread", "read", "rspeed", "write", "wspeed",
                           "scan", "sspeed", "wwl", "startkey");
        } else {
            cols = 13;
            printer.Reset(cols,
                           " ", "path", "status", "size", "lread",
                           "read", "rspeed", "write", "wspeed",
                           "scan", "sspeed", "wwl", "startkey");
        }

        for (int32_t i = 0; i < tablet_list.meta_size(); ++i) {
            const TabletMeta& meta = tablet_list.meta(i);
            row.clear();
            row.push_back(NumberToString(i));
            if (is_server_addr) {
                row.push_back(meta.server_addr());
            }
            row.push_back(meta.path());
            row.push_back(GetTabletStatusString(tablet_list, now, i));

            uint64_t size = meta.size();
            std::string size_str =
                BytesNumberToString(size) +
                "[";
            for (int l = 0; l < meta.lg_size_size(); ++l) {
                size_str += BytesNumberToString(meta.lg_size(l));
                if (l < meta.lg_size_size() - 1) {
                    size_str += ",";
                }
            }
            size_str += "]";
            row.push_back(size_str);

            if (tablet_list.counter_size() > 0) {
                const TabletCounter& counter = tablet_list.counter(i);
                row.push_back(NumberToString(counter.low_read_cell()));
                row.push_back(NumberToString(counter.read_rows()));
                row.push_back(BytesNumberToString(counter.read_size()) + "B/s");
                row.push_back(NumberToString(counter.write_rows()));
                row.push_back(BytesNumberToString(counter.write_size()) + "B/s");
                row.push_back(NumberToString(counter.scan_rows()));
                row.push_back(BytesNumberToString(counter.scan_size()) + "B/s");
                row.push_back(DoubleToStr(counter.write_workload()));
            }
            row.push_back(DebugString(meta.key_range().key_start().substr(0, 20)));
            printer.AddRow(row);
        }
    } else {
        cols = 7;
        printer.Reset(cols,
                       " ", "server_addr", "path", "status",
                       "size", "startkey", "endkey");
        for (int32_t i = 0; i < tablet_list.meta_size(); ++i) {
            const TabletMeta& meta = tablet_list.meta(i);
            row.clear();
            row.push_back(NumberToString(i));
            row.push_back(meta.server_addr());
            row.push_back(meta.path());
            row.push_back(GetTabletStatusString(tablet_list, now, i));

            uint64_t size = meta.size();
            row.push_back(BytesNumberToString(size));
            row.push_back(DebugString(meta.key_range().key_start()));
            row.push_back(DebugString(meta.key_range().key_end()));
            printer.AddRow(row);
        }
    }
    printer.Print(g_printer_opt);
    return 0;
}

void SetTableCounter(const std::string& table_name,
                     const TabletMetaList& tablet_list,
                     TableCounter* counter) {
    int64_t size = 0;
    int64_t tablet = 0;
    int64_t notready = 0;
    int64_t lread = 0;
    int64_t read = 0;
    int64_t rmax = 0;
    int64_t rspeed = 0;
    int64_t write = 0;
    int64_t wmax = 0;
    int64_t wspeed = 0;
    int64_t scan = 0;
    int64_t smax = 0;
    int64_t sspeed = 0;
    int64_t lg_num = 0;
    std::vector<int64_t> lg_size;
    for (int32_t i = 0; i < tablet_list.meta_size(); ++i) {
        if (tablet_list.meta(i).table_name() != table_name) {
            continue;
        }
        size += tablet_list.meta(i).size();
        tablet++;
        if (tablet_list.meta(i).status() != TabletMeta::kTabletReady) {
            notready++;
        }
        lread += tablet_list.counter(i).low_read_cell();
        read += tablet_list.counter(i).read_rows();
        if (tablet_list.counter(i).read_rows() > rmax) {
            rmax = tablet_list.counter(i).read_rows();
        }
        rspeed += tablet_list.counter(i).read_size();
        write += tablet_list.counter(i).write_rows();
        if (tablet_list.counter(i).write_rows() > wmax) {
            wmax = tablet_list.counter(i).write_rows();
        }
        wspeed += tablet_list.counter(i).write_size();
        scan += tablet_list.counter(i).scan_rows();
        if (tablet_list.counter(i).scan_rows() > smax) {
            smax = tablet_list.counter(i).scan_rows();
        }
        sspeed += tablet_list.counter(i).scan_size();

        if (lg_num == 0) {
            lg_num = tablet_list.meta(i).lg_size_size();
            lg_size.resize(lg_num, 0);
        }
        for (int l = 0; l < lg_num; ++l) {
            if (tablet_list.meta(i).lg_size_size() > l) {
                lg_size[l] += tablet_list.meta(i).lg_size(l);
            }
        }
    }
    counter->set_size(size);
    counter->set_tablet_num(tablet);
    counter->set_notready_num(notready);
    counter->set_lread(lread);
    counter->set_read_rows(read);
    counter->set_read_max(rmax);
    counter->set_read_size(rspeed);
    counter->set_write_rows(write);
    counter->set_write_max(wmax);
    counter->set_write_size(wspeed);
    counter->set_scan_rows(scan);
    counter->set_scan_max(smax);
    counter->set_scan_size(sspeed);
    for (int l = 0; l < lg_num; ++l) {
        counter->add_lg_size(lg_size[l]);
    }
}

int32_t ShowAllTables(Client* client, bool is_x, bool show_all, ErrorCode* err) {
    TableMetaList table_list;
    TabletMetaList tablet_list;
    std::shared_ptr<tera::ClientImpl> client_impl((static_cast<ClientWrapper*>(client))->GetClientImpl());
    if (!client_impl->ShowTablesInfo(&table_list, &tablet_list, !show_all, err)) {
        LOG(ERROR) << "fail to get meta data from tera.";
        return -1;
    }

    TPrinter printer;
    int64_t sum_size = 0;
    int64_t sum_tablet = 0;
    int64_t sum_notready = 0;
    int64_t sum_lread = 0;
    int64_t sum_read = 0;
    int64_t sum_rspeed = 0;
    int64_t sum_write = 0;
    int64_t sum_wspeed = 0;
    int64_t sum_scan = 0;
    int64_t sum_sspeed = 0;
    int cols;
    if (is_x) {
        cols = 17;
        printer.Reset(cols,
                       " ", "tablename", "status", "size", "lg_size",
                       "tablet", "notready", "lread", "read",
                       "rmax", "rspeed", "write", "wmax", "wspeed",
                       "scan", "smax", "sspeed");
    } else {
        cols = 7;
        printer.Reset(cols,
                       " ", "tablename", "status", "size", "lg_size",
                       "tablet", "notready");
    }
    for (int32_t table_no = 0; table_no < table_list.meta_size(); ++table_no) {
        std::string tablename = table_list.meta(table_no).table_name();
        TableCounter counter;
        if (table_list.counter_size() > 0) {
            counter = table_list.counter(table_no);
        } else {
            SetTableCounter(tablename, tablet_list, &counter);
        }
        TableStatus status = table_list.meta(table_no).status();
        std::string lg_size_str = "";
        for (int l = 0; l < counter.lg_size_size(); ++l) {
            lg_size_str += BytesNumberToString(counter.lg_size(l));
            if (l < counter.lg_size_size() - 1) {
                lg_size_str += ",";
            }
        }
        if (lg_size_str.empty()) {
            lg_size_str = "-";
        }
        int64_t notready;
        std::string print_size;
        if (status == kTableDisable) {
            notready = 0;
            print_size = "-";
            lg_size_str = "-";
        } else {
            notready = counter.notready_num();
            print_size = BytesNumberToString(counter.size());
        }
        sum_size += counter.size();
        sum_tablet += counter.tablet_num();
        sum_notready += notready;
        sum_lread += counter.lread();
        sum_read += counter.read_rows();
        sum_rspeed += counter.read_size();
        sum_write += counter.write_rows();
        sum_wspeed += counter.write_size();
        sum_scan += counter.scan_rows();
        sum_sspeed += counter.scan_size();
        if (is_x) {
            printer.AddRow(cols,
                           NumberToString(table_no).data(),
                           tablename.data(),
                           StatusCodeToString(status).data(),
                           print_size.data(),
                           lg_size_str.data(),
                           NumberToString(counter.tablet_num()).data(),
                           NumberToString(notready).data(),
                           BytesNumberToString(counter.lread()).data(),
                           BytesNumberToString(counter.read_rows()).data(),
                           BytesNumberToString(counter.read_max()).data(),
                           (BytesNumberToString(counter.read_size()) + "B/s").data(),
                           BytesNumberToString(counter.write_rows()).data(),
                           BytesNumberToString(counter.write_max()).data(),
                           (BytesNumberToString(counter.write_size()) + "B/s").data(),
                           BytesNumberToString(counter.scan_rows()).data(),
                           BytesNumberToString(counter.scan_max()).data(),
                           (BytesNumberToString(counter.scan_size()) + "B/s").data());
        } else {
            printer.AddRow(cols,
                           NumberToString(table_no).data(),
                           tablename.data(),
                           StatusCodeToString(status).data(),
                           print_size.data(),
                           lg_size_str.data(),
                           NumberToString(counter.tablet_num()).data(),
                           NumberToString(notready).data());
        }
    }
    if (!FLAGS_stdout_is_tty) {
        // we don't need total infos
    } else if (is_x) {
        printer.AddRow(cols,
                       "-",
                       "total",
                       "-",
                       BytesNumberToString(sum_size).data(),
                       "-",
                       NumberToString(sum_tablet).data(),
                       NumberToString(sum_notready).data(),
                       BytesNumberToString(sum_lread).data(),
                       BytesNumberToString(sum_read).data(),
                       "-",
                       (BytesNumberToString(sum_rspeed) + "B/s").data(),
                       BytesNumberToString(sum_write).data(),
                       "-",
                       (BytesNumberToString(sum_wspeed) + "B/s").data(),
                       BytesNumberToString(sum_scan).data(),
                       "-",
                       (BytesNumberToString(sum_sspeed) + "B/s").data());
    } else {
        printer.AddRow(cols,
                       "-",
                       "total",
                       "-",
                       BytesNumberToString(sum_size).data(),
                       "-",
                       NumberToString(sum_tablet).data(),
                       NumberToString(sum_notready).data());
    }
    printer.Print(g_printer_opt);
    std::cout << std::endl;
    if (show_all) {
        ShowTabletList(tablet_list, true, true);
    }
    return 0;
}

int32_t ShowSingleTable(Client* client, const string& table_name,
                        bool is_x, ErrorCode* err) {
    TableMeta table_meta;
    TabletMetaList tablet_list;

    std::shared_ptr<tera::ClientImpl> client_impl((static_cast<ClientWrapper*>(client))->GetClientImpl());
    if (!client_impl->ShowTablesInfo(table_name, &table_meta, &tablet_list, err)) {
        LOG(ERROR) << "table not exist: " << table_name;
        return -1;
    }

    if (FLAGS_stdout_is_tty) {
        std::cout << std::endl;
        std::cout << "create time: "
            << get_time_str(table_meta.create_time()) << std::endl;
        std::cout << std::endl;
    }
    ShowTabletList(tablet_list, true, is_x);
    std::cout << std::endl;
    return 0;
}

int32_t ShowSingleTabletNodeInfo(Client* client, const string& addr,
                                 bool is_x, ErrorCode* err) {
    TabletNodeInfo info;
    TabletMetaList tablet_list;
    std::shared_ptr<tera::ClientImpl> client_impl((static_cast<ClientWrapper*>(client))->GetClientImpl());
    if (!client_impl->ShowTabletNodesInfo(addr, &info, &tablet_list, err)) {
        LOG(ERROR) << "fail to show tabletnode: " << addr;
        return -1;
    }

    std::cout << "\nTabletNode Info:\n";
    std::cout << "  address:  " << info.addr() << std::endl;
    std::cout << "  status:   " << info.status_m() << std::endl;
    std::cout << "  update time:   "
        << get_time_str(info.timestamp() / 1000000) << "\n\n";

    int cols = 5;
    TPrinter printer(cols, "workload", "tablets", "load", "split", "unload");
    std::vector<string> row;
    row.push_back(BytesNumberToString(info.load()));
    row.push_back(NumberToString(info.tablet_total()));
    row.push_back(NumberToString(info.tablet_onload()));
    row.push_back(NumberToString(info.tablet_onsplit()));
    row.push_back(NumberToString(info.tablet_unloading()));
    printer.AddRow(row);
    printer.Print(g_printer_opt);

    std::cout << std::endl;
    cols = 7;
    printer.Reset(cols, "lread", "read", "rspeed", "write", "wspeed", "scan", "sspeed");
    row.clear();
    row.push_back(NumberToString(info.low_read_cell()));
    row.push_back(NumberToString(info.read_rows()));
    row.push_back(BytesNumberToString(info.read_size()) + "B/s");
    row.push_back(NumberToString(info.write_rows()));
    row.push_back(BytesNumberToString(info.write_size()) + "B/s");
    row.push_back(NumberToString(info.scan_rows()));
    row.push_back(BytesNumberToString(info.scan_size()) + "B/s");
    printer.AddRow(row);
    printer.Print(g_printer_opt);

    std::cout << "\nHardware Info:\n";
    cols = 8;
    printer.Reset(cols, "cpu", "mem_used", "net_tx", "net_rx", "dfs_r", "dfs_w", "local_r", "local_w");
    row.clear();
    row.push_back(NumberToString(info.cpu_usage()));
    row.push_back(BytesNumberToString(info.mem_used()));
    row.push_back(BytesNumberToString(info.net_tx()) + "B/s");
    row.push_back(BytesNumberToString(info.net_rx()) + "B/s");
    row.push_back(BytesNumberToString(info.dfs_io_r()) + "B/s");
    row.push_back(BytesNumberToString(info.dfs_io_w()) + "B/s");
    row.push_back(BytesNumberToString(info.local_io_r()) + "B/s");
    row.push_back(BytesNumberToString(info.local_io_w()) + "B/s");
    printer.AddRow(row);
    printer.Print(g_printer_opt);

    std::cout << "\nOther Infos:\n";
    cols = info.extra_info_size();
    row.clear();
    for (int i = 0; i < cols; ++i) {
        row.push_back(info.extra_info(i).name());
    }
    printer.Reset(row);
    std::vector<int64_t> row_int;
    for (int i = 0; i < cols; ++i) {
        row_int.push_back(info.extra_info(i).value());
    }
    printer.AddRow(row_int);
    printer.Print(g_printer_opt);

    std::cout << "\nTablets In this TabletNode:\n";
    ShowTabletList(tablet_list, false, is_x);
    return 0;
}

int32_t ShowTabletNodesInfo(Client* client, bool is_x, ErrorCode* err) {
    std::vector<TabletNodeInfo> infos;
    std::shared_ptr<tera::ClientImpl> client_impl((static_cast<ClientWrapper*>(client))->GetClientImpl());
    if (!client_impl->ShowTabletNodesInfo(&infos, err)) {
        LOG(ERROR) << "fail to get meta data from tera.";
        return -1;
    }

    int64_t now = get_micros();
    int cols;
    TPrinter printer;
    if (is_x) {
        cols = 25;
        printer.Reset(cols,
                       " ", "address", "status", "size", "num",
                       "lread", "r", "rspd", "w", "wspd",
                       "s", "sspd", "rdly", "rp", "wp",
                       "sp", "ld", "bs", "mem", "cpu",
                       "net_tx", "net_rx", "dfs_r", "dfs_w", "start_time");
        std::vector<string> row;
        for (size_t i = 0; i < infos.size(); ++i) {
            std::map<string, string> extra;
            for (int j = 0; j < infos[i].extra_info_size(); ++j) {
                extra[infos[i].extra_info(j).name()] =
                    NumberToString(infos[i].extra_info(j).value());
            }

            row.clear();
            row.push_back(NumberToString(i));
            row.push_back(infos[i].addr());
            if (now - (int64_t)infos[i].timestamp() > FLAGS_tera_sdk_status_timeout * 1000000) {
                // tabletnode status timeout
                row.push_back("kZombie");
            } else {
                row.push_back(infos[i].status_m());
            }
            row.push_back(BytesNumberToString(infos[i].load()));
            row.push_back(NumberToString(infos[i].tablet_total()));
            row.push_back(NumberToString(infos[i].low_read_cell()));
            row.push_back(NumberToString(infos[i].read_rows()));
            row.push_back(BytesNumberToString(infos[i].read_size()) + "B");
            row.push_back(NumberToString(infos[i].write_rows()));
            row.push_back(BytesNumberToString(infos[i].write_size()) + "B");
            row.push_back(NumberToString(infos[i].scan_rows()));
            row.push_back(BytesNumberToString(infos[i].scan_size()) + "B");
            row.push_back(extra["rand_read_delay"] + "ms");
            row.push_back(extra["read_pending"]);
            row.push_back(extra["write_pending"]);
            row.push_back(extra["scan_pending"]);
            row.push_back(NumberToString(infos[i].tablet_onload()));
            row.push_back(NumberToString(infos[i].tablet_onbusy()));
            row.push_back(BytesNumberToString(infos[i].mem_used()));
            row.push_back(NumberToString(infos[i].cpu_usage()));
            row.push_back(BytesNumberToString(infos[i].net_tx()));
            row.push_back(BytesNumberToString(infos[i].net_rx()));
            row.push_back(BytesNumberToString(infos[i].dfs_io_r()));
            row.push_back(BytesNumberToString(infos[i].dfs_io_w()));
            row.push_back(DateNumberToString(infos[i].process_start_time() / 1000 / 1000));
            printer.AddRow(row);
        }
    } else {
        cols = 8;
        printer.Reset(cols,
                       " ", "address", "status", "workload",
                       "tablet", "load", "busy", "unload");
        std::vector<string> row;
        for (size_t i = 0; i < infos.size(); ++i) {
            row.clear();
            row.push_back(NumberToString(i));
            row.push_back(infos[i].addr());
            if (now - (int64_t)infos[i].timestamp() > FLAGS_tera_sdk_status_timeout * 1000000) {
                row.push_back("kZombie");
            } else {
                row.push_back(infos[i].status_m());
            }
            row.push_back(BytesNumberToString(infos[i].load()));
            row.push_back(NumberToString(infos[i].tablet_total()));
            row.push_back(NumberToString(infos[i].tablet_onload()));
            row.push_back(NumberToString(infos[i].tablet_onbusy()));
            row.push_back(NumberToString(infos[i].tablet_unloading()));
            printer.AddRow(row);
        }
    }
    printer.Print(g_printer_opt);
    std::cout << std::endl;
    return 0;
}

int32_t ShowTabletNodesOp(Client* client, int32_t argc, std::string* argv, ErrorCode* err) {
    if (argc < 2) {
        LOG(ERROR) << "args number error: " << argc << ", need >2.";
        PrintCmdHelpInfo(argv[1]);
        return -1;
    }

    int32_t ret_val;
    string cmd = argv[1];

    if (argc == 3) {
        ret_val = ShowSingleTabletNodeInfo(client, argv[2], cmd == "showtsx", err);
    } else {
        ret_val = ShowTabletNodesInfo(client, cmd == "showtsx", err);
    }
    return ret_val;
}

int32_t ShowOp(Client* client, int32_t argc, std::string* argv, ErrorCode* err) {
    if (argc < 2) {
        LOG(ERROR) << "args number error: " << argc << ", need >2.";
        PrintCmdHelpInfo(argv[1]);
        return -1;
    }

    int32_t ret_val;
    std::string cmd = argv[1];
    if (argc == 3) {
        ret_val = ShowSingleTable(client, argv[2], cmd == "showx", err);
    } else if (argc == 2 && (cmd == "show" || cmd == "showx" || cmd == "showall")) {
        ret_val = ShowAllTables(client, cmd == "showx", cmd == "showall", err);
    } else {
        ret_val = -1;
        LOG(ERROR) << "error: arg num: " << argc;
    }
    return ret_val;
}

int32_t ShowSchemaOp(Client* client, int32_t argc, std::string* argv, ErrorCode* err) {
    if (argc < 3) {
        PrintCmdHelpInfo("showschema");
        return -1;
    }

    std::string cmd = argv[1];
    std::string table_name = argv[2];
    TableSchema table_schema;

    std::shared_ptr<tera::ClientImpl> client_impl((static_cast<ClientWrapper*>(client))->GetClientImpl());
    if (!client_impl->ShowTableSchema(table_name, &table_schema, err)) {
        LOG(ERROR) << "table not exist: " << table_name;
        return -1;
    }
    ShowTableSchema(table_schema, cmd == "showschemax");
    return 0;
}

void BatchPutCallBack(RowMutation* mutation) {
    const ErrorCode& error_code = mutation->GetError();
    if (error_code.GetType() != ErrorCode::kOK) {
        LOG(ERROR) << "exception occured, reason:" << error_code.ToString();
    }
    int32_t mut_num = mutation->MutationNum();

    {
        // for performance testing
        MutexLock locker(&g_stat_lock);
        g_total_size += mutation->Size();
        g_key_num += mut_num;
        int32_t time_cur = time(NULL);
        int32_t time_used = time_cur - g_start_time;
        if (time_cur > g_last_time) {
            g_last_time = time_cur;
            LOG(INFO) << "Write file  "<<g_key_num<<" keys "<<g_key_num/(time_used?time_used:1)
                <<" keys/S "<<g_total_size/1024.0/1024/(time_used?time_used:1)<<" MB/S ";
        }
    }

    delete mutation;
}

int32_t BatchPutOp(Client* client, int32_t argc, std::string* argv, ErrorCode* err) {
    if (argc != 4) {
        LOG(ERROR) << "args number error: " << argc << ", need 4.";
        PrintCmdHelpInfo(argv[1]);
        return -1;
    }

    std::string tablename = argv[2];
    std::string record_file = argv[3];
    TablePtr table(client->OpenTable(tablename, err));
    if (table == NULL) {
        LOG(ERROR) << "fail to open table";
        return -1;
    }
    const int32_t buf_size = 1024 * 1024;
    char buf[buf_size];
    std::ifstream stream(record_file.c_str());

    // input record format: rowkey columnfamily:qualifier value
    // or: key:value
    std::vector<std::string> input_v;
    g_start_time = time(NULL);
    while (stream.getline(buf, buf_size)) {
        SplitString(buf, " ", &input_v);
        if (input_v.size() != 3 && input_v.size() != 2) {
            LOG(ERROR) << "input file format error, skip it: " << buf;
            continue;
        }
        std::string rowkey = input_v[0];
        if (FLAGS_readable && !ParseDebugString(input_v[0], &rowkey)) {
            LOG(ERROR) << "input file format error, skip it: " << buf;
            continue;
        }
        std::string family;
        std::string qualifier;
        std::string value = input_v[input_v.size() - 1];
        if (FLAGS_readable && !ParseDebugString(input_v[input_v.size() - 1], &value)) {
            LOG(ERROR) << "input file format error, skip it: " << buf;
            continue;
        }
        RowMutation* mutation = table->NewRowMutation(rowkey);
        if (input_v.size() == 2) {
            // for kv mode
            mutation->Put(value);
        } else {
            // for table mode, put(family, qulifier, value)
            ParseCfQualifier(input_v[1], &family, &qualifier);
            mutation->Put(family, qualifier, value);
        }
        mutation->SetCallBack(BatchPutCallBack);
        table->ApplyMutation(mutation);
    }
    while (!table->IsPutFinished()) {
        usleep(100000);
    }

    g_end_time = time(NULL);
    g_used_time = g_end_time-g_start_time;
    LOG(INFO) << "Write done,write_key_num=" << g_key_num << " used_time=" << g_used_time <<std::endl;
    return 0;
}

int32_t BatchPutInt64Op(Client* client, int32_t argc, std::string* argv, ErrorCode* err) {
    if (argc != 4) {
        LOG(ERROR) << "args number error: " << argc << ", need 4.";
        PrintCmdHelpInfo(argv[1]);
        return -1;
    }

    std::string tablename = argv[2];
    std::string record_file = argv[3];
    TablePtr table(client->OpenTable(tablename, err));
    if (table == NULL) {
        LOG(ERROR) << "fail to open table";
        return -1;
    }
    const int32_t buf_size = 1024 * 1024;
    char buf[buf_size];
    std::ifstream stream(record_file.c_str());

    // input record format: rowkey columnfamily:qualifier value
    // or: key:value
    std::vector<std::string> input_v;
    g_start_time = time(NULL);
    while (stream.getline(buf, buf_size)) {
        SplitString(buf, " ", &input_v);
        if (input_v.size() != 3 && input_v.size() != 2) {
            LOG(ERROR) << "input file format error, skip it: " << buf;
            continue;
        }
        std::string rowkey = input_v[0];
        if (FLAGS_readable && !ParseDebugString(input_v[0], &rowkey)) {
            LOG(ERROR) << "input file format error, skip it: " << buf;
            continue;
        }
        std::string family;
        std::string qualifier;
        std::string value = input_v[input_v.size() - 1];
        if (FLAGS_readable && !ParseDebugString(input_v[input_v.size() - 1], &value)) {
            LOG(ERROR) << "input file format error, skip it: " << buf;
            continue;
        }
        RowMutation* mutation = table->NewRowMutation(rowkey);
        int64_t value_int;
        if (!StringToNumber(value.c_str(), &value_int)) {
           LOG(ERROR) << "invalid Integer number Got: " << value;
           return -1;
        }
        if (input_v.size() == 2) {
            // for kv mode
            mutation->Put(value_int);
        } else {
            // for table mode, put(family, qulifier, value)
            ParseCfQualifier(input_v[1], &family, &qualifier);
            mutation->Put(family, qualifier, value_int);
        }
        mutation->SetCallBack(BatchPutCallBack);
        table->ApplyMutation(mutation);
    }
    while (!table->IsPutFinished()) {
        usleep(100000);
    }

    g_end_time = time(NULL);
    g_used_time = g_end_time-g_start_time;
    LOG(INFO) << "Write done,write_key_num=" << g_key_num << " used_time=" << g_used_time <<std::endl;
    return 0;
}

void BatchGetCallBack(RowReader* reader) {
    while (!reader->Done()) {
        {
            // for performance testing
            MutexLock locker(&g_stat_lock);
            g_key_num ++;
            g_total_size += reader->RowName().size()
                + reader->ColumnName().size()
                + sizeof(reader->Timestamp())
                + reader->Value().size();
            int32_t time_cur = time(NULL);
            int32_t time_used = time_cur - g_start_time;
            if (time_cur > g_last_time) {
                g_last_time = time_cur;
                LOG(INFO) << "Read file  "<<g_key_num<<" keys "<<g_key_num/(time_used?time_used:1)
                    <<" keys/S "<<g_total_size/1024.0/1024/(time_used?time_used:1)<<" MB/S ";
            }
        }
        std::cout << reader->RowName() << ":"
            << reader->ColumnName() << ":"
            << reader->Timestamp() << ":"
            << reader->Value() << std::endl;
        reader->Next();
    }
    delete reader;
}

int32_t BatchGetOp(Client* client, int32_t argc, std::string* argv, ErrorCode* err) {
    if (argc != 4 && argc != 5) {
        LOG(ERROR) << "args number error: " << argc << ", need 4 | 5.";
        PrintCmdHelpInfo(argv[1]);
        return -1;
    }

    uint64_t snapshot = 0;
    if (argc == 5) {
        std::stringstream is;
        is << std::string(argv[4]);
        is >> snapshot;
    }

    std::string tablename = argv[2];
    std::string input_file = argv[3];
    TablePtr table(client->OpenTable(tablename, err));
    if (table == NULL) {
        LOG(ERROR) << "fail to open table";
        return -1;
    }
    const int32_t buf_size = 1024 * 1024;
    char buf[buf_size];
    std::ifstream stream(input_file.c_str());

    // input file format: rowkey [columnfamily|cf:qualifier]...
    // std::cout << "rowkey:columnfamily:qualifier:timestamp:value" << std::endl;
    std::vector<std::string> input_v;
    while (stream.getline(buf, buf_size)) {
        SplitString(buf, " ", &input_v);
        if (input_v.size() <= 0) {
            LOG(ERROR) << "input file format error: " << buf;
            continue;
        }
        std::string rowkey = input_v[0];
        if (FLAGS_readable && !ParseDebugString(input_v[0], &rowkey)) {
            LOG(ERROR) << "input file format error, skip it: " << buf;
            continue;
        }
        RowReader* reader = table->NewRowReader(rowkey);
        for (size_t i = 1; i < input_v.size(); ++i) {
            std::string& cfqu = input_v[i];
            std::string::size_type pos = cfqu.find(":", 0);
            if (pos != std::string::npos) {
                // add column
                reader->AddColumn(cfqu.substr(0, pos), cfqu.substr(pos + 1));
            } else {
                // add columnfamily
                reader->AddColumnFamily(cfqu);
            }
            reader->SetSnapshot(snapshot);
        }
        reader->SetCallBack(BatchGetCallBack);
        table->Get(reader);
    }
    while (!table->IsGetFinished()) {
        // waiting async get finishing
        usleep(100000);
    }
    g_end_time = time(NULL);
    g_used_time = g_end_time-g_start_time;
    LOG(INFO) << "Read done,write_key_num=" << g_key_num << " used_time=" << g_used_time <<std::endl;
    return 0;
}

void BatchGetInt64CallBack(RowReader* reader) {
    while (!reader->Done()) {
        {
            // for performance testing
            MutexLock locker(&g_stat_lock);
            g_key_num ++;
            g_total_size += reader->RowName().size()
                + reader->ColumnName().size()
                + sizeof(reader->Timestamp())
                + reader->Value().size();
            int32_t time_cur = time(NULL);
            int32_t time_used = time_cur - g_start_time;
            if (time_cur > g_last_time) {
                g_last_time = time_cur;
                LOG(INFO) << "Read file  "<<g_key_num<<" keys "<<g_key_num/(time_used?time_used:1)
                    <<" keys/S "<<g_total_size/1024.0/1024/(time_used?time_used:1)<<" MB/S ";
            }
        }
        uint64_t tmp_data = io::DecodeBigEndain(reader->Value().c_str());
        int64_t value_int = tmp_data - std::numeric_limits<int64_t>::max();
        std::cout << reader->RowName() << ":"
            << reader->ColumnName() << ":"
            << reader->Timestamp() << ":"
            << value_int << std::endl;
        reader->Next();
    }
    delete reader;
}

int32_t BatchGetInt64Op(Client* client, int32_t argc, std::string* argv, ErrorCode* err) {
    if (argc != 4 && argc != 5) {
        LOG(ERROR) << "args number error: " << argc << ", need 4 | 5.";
        PrintCmdHelpInfo(argv[1]);
        return -1;
    }

    uint64_t snapshot = 0;
    if (argc == 5) {
        std::stringstream is;
        is << std::string(argv[4]);
        is >> snapshot;
    }

    std::string tablename = argv[2];
    std::string input_file = argv[3];
    TablePtr table(client->OpenTable(tablename, err));
    if (table == NULL) {
        LOG(ERROR) << "fail to open table";
        return -1;
    }
    const int32_t buf_size = 1024 * 1024;
    char buf[buf_size];
    std::ifstream stream(input_file.c_str());

    // input file format: rowkey [columnfamily|cf:qualifier]...
    // std::cout << "rowkey:columnfamily:qualifier:timestamp:value" << std::endl;
    std::vector<std::string> input_v;
    while (stream.getline(buf, buf_size)) {
        SplitString(buf, " ", &input_v);
        if (input_v.size() <= 0) {
            LOG(ERROR) << "input file format error: " << buf;
            continue;
        }
        std::string rowkey = input_v[0];
        if (FLAGS_readable && !ParseDebugString(input_v[0], &rowkey)) {
            LOG(ERROR) << "input file format error, skip it: " << buf;
            continue;
        }
        if (input_v.size() == 1) {
            // only rowkey explicit, scan all records out
            ScanDescriptor desc(rowkey);
            ResultStream* result_stream;
            desc.SetEnd(rowkey);
            if ((result_stream = table->Scan(desc, err)) == NULL) {
                LOG(ERROR) << "fail to get records from table: " << tablename;
                return -1;
            }

            while (!result_stream->Done()) {
                {
                    // for performance testing
                    MutexLock locker(&g_stat_lock);
                    g_key_num ++;
                    g_total_size += result_stream->RowName().size()
                        + result_stream->ColumnName().size()
                        + sizeof(result_stream->Timestamp())
                        + result_stream->Value().size();
                    int32_t time_cur = time(NULL);
                    int32_t time_used = time_cur - g_start_time;
                    if (time_cur > g_last_time) {
                        g_last_time = time_cur;
                        LOG(INFO) << "Read file  "<<g_key_num<<" keys "<<g_key_num/(time_used?time_used:1)
                            <<" keys/S "<<g_total_size/1024.0/1024/(time_used?time_used:1)<<" MB/S ";
                    }
                }

                uint64_t tmp_data = io::DecodeBigEndain(result_stream->Value().c_str());
                int value_int = tmp_data - std::numeric_limits<int64_t>::max();
                std::cout << result_stream->RowName() << ":"
                    << result_stream->ColumnName() << ":"
                    << result_stream->Timestamp() << ":"
                    << value_int << std::endl;
                result_stream->Next();
            }
        } else {
            // get specific records with RowReader
            RowReader* reader = table->NewRowReader(rowkey);
            for (size_t i = 1; i < input_v.size(); ++i) {
                std::string& cfqu = input_v[i];
                std::string::size_type pos = cfqu.find(":", 0);
                if (pos != std::string::npos) {
                    // add column
                    reader->AddColumn(cfqu.substr(0, pos), cfqu.substr(pos + 1));
                } else {
                    // add columnfamily
                    reader->AddColumnFamily(cfqu);
                }
                reader->SetSnapshot(snapshot);
            }
            reader->SetCallBack(BatchGetInt64CallBack);
            table->Get(reader);
        }
    }
    while (!table->IsGetFinished()) {
        // waiting async get finishing
        usleep(100000);
    }
    g_end_time = time(NULL);
    g_used_time = g_end_time-g_start_time;
    LOG(INFO) << "Read done,write_key_num=" << g_key_num << " used_time=" << g_used_time <<std::endl;
    return 0;
}

int32_t GetRandomNumKey(int32_t key_size,std::string *p_key){
    std::stringstream ss;
    std::string temp_str;
    int32_t temp_num;
    *p_key = "";
    for(int i=0;i!=key_size;++i) {
        temp_num=rand()%10;
        ss << temp_num;
        ss >> temp_str;
        *p_key += temp_str;
        ss.clear();
    }
    return 0;
}

int32_t SafeModeOp(Client* client, int32_t argc, std::string* argv, ErrorCode* err) {
    if (argc < 3) {
        PrintCmdHelpInfo(argv[1]);
        return -1;
    }

    std::string op = argv[2];
    if (op != "get" && op != "leave" && op != "enter") {
        PrintCmdHelpInfo(argv[1]);
        return -1;
    }

    bool is_safemode = false;
    std::vector<std::string> arg_list;
    arg_list.push_back(op);
    if (!client->CmdCtrl("safemode", arg_list, &is_safemode, NULL, err)) {
        std::cout << "fail to " << op << " safemode" << std::endl;
        return -1;
    }
    if (op == "get") {
        if (is_safemode) {
            std::cout << "master is in safemode" << std::endl;
        } else {
            std::cout << "master is not in safemode" << std::endl;
        }
    } else {
        std::cout << "master " << op << " safemode success" << std::endl;
    }

    return 0;
}

int32_t CookieOp(Client*, int32_t argc, std::string* argv, ErrorCode*) {
    std::string command;
    if (argc == 4) {
        command = argv[2];
        if (command == "dump") {
            return ::tera::sdk::DumpCookieFile(argv[3]);
        }
    } else if (argc == 5) {
        command = argv[2];
        if (command == "findkey") {
            return ::tera::sdk::FindKeyInCookieFile(argv[3], argv[4]);
        }
    }
    PrintCmdHelpInfo(argv[1]);
    return -1;
}

// e.g. ./teracli kick <hostname>:<port>
int32_t KickTabletServerOp(Client* client, int32_t argc, std::string* argv, ErrorCode* err) {
    if ((argc != 3)) {
        PrintCmdHelpInfo(argv[1]);
        return -1;
    }
    std::string addr(argv[2]);
    std::vector<std::string> arg_list;
    arg_list.push_back(addr);
    if (!client->CmdCtrl("kick", arg_list, NULL, NULL, err)) {
        LOG(ERROR) << "fail to kick tabletserver: " << addr;
        return -1;
    }
    std::cout << "master will kick: " << addr << std::endl;
    return 0;
}

int32_t ReloadConfigOp(Client* client, int32_t argc, std::string* argv, ErrorCode* err) {
    if ((argc != 4) || (std::string(argv[2]) != "config")) {
        PrintCmdHelpInfo(argv[1]);
        return -1;
    }
    std::string addr(argv[3]);

    scoped_ptr<tera::sdk::ClusterFinder> finder(tera::sdk::NewClusterFinder());
    if (finder->MasterAddr() == addr) {
        // master
        std::vector<std::string> arg_list;
        if (!client->CmdCtrl("reload config", arg_list, NULL, NULL, err)) {
            LOG(ERROR) << "fail to reload config: " << addr;
            return -1;
        }
    } else {
        // tabletnode
        TsCmdCtrlRequest request;
        TsCmdCtrlResponse response;
        request.set_sequence_id(0);
        request.set_command("reload config");
        common::ThreadPool thread_pool(FLAGS_concurrency);
        tabletnode::TabletNodeClient tabletnode_client(&thread_pool, addr, 3600000);
        if (!tabletnode_client.CmdCtrl(&request, &response)
            || (response.status() != kTabletNodeOk)) {
            LOG(ERROR) << "fail to reload config: " << addr;
            return -1;
        }
    }
    std::cout << "reload config success" << std::endl;
    return 0;
}

int32_t CompactTablet(TabletInfo& tablet, int lg) {
    CompactTabletRequest request;
    CompactTabletResponse response;
    request.set_sequence_id(0);
    request.set_tablet_name(tablet.table_name);
    request.mutable_key_range()->set_key_start(tablet.start_key);
    request.mutable_key_range()->set_key_end(tablet.end_key);
    common::ThreadPool thread_pool(FLAGS_concurrency);
    tabletnode::TabletNodeClient tabletnode_client(&thread_pool, tablet.server_addr, FLAGS_compact_timeout);

    std::string path;
    if (lg >= 0) {
        request.set_lg_no(lg);
        path = tablet.path + "/" + NumberToString(lg);
    } else {
        path = tablet.path;
    }

    std::cout << "try compact tablet: " << path
        << " on " << tabletnode_client.GetConnectAddr() << std::endl;

    if (!tabletnode_client.CompactTablet(&request, &response)) {
        LOG(ERROR) << "no response from ["
            << tabletnode_client.GetConnectAddr() << "]";
        return -7;
    }

    if (response.status() != kTabletNodeOk) {
        LOG(ERROR) << "fail to compact tablet: " << path
            << ", status: " << StatusCodeToString(response.status());
        return -1;
    }

    if (response.compact_status() != kTableCompacted) {
        LOG(ERROR) << "fail to compact tablet: " << path
            << ", status: " << StatusCodeToString(response.compact_status());
        return -1;
    }

    std::cout << "compact tablet success: " << path << ", data size: "
        << BytesNumberToString(response.compact_size()) << std::endl;
    return 0;
}

static bool ComputeCompactInsertKeys(RawKey rawkey, std::string* start_key, std::string* end_key) {
    static std::string x0("\x0", 1);
    static std::string x1("\x1", 1);
    *start_key = (rawkey == Readable ? *start_key + x1 : *start_key + x0);

    // pop all '\x0' charcters at the tailing of end_key. Note that Readable should not contain any
    // '\x0' characters but here we do not
    while (end_key->size() > 0) {
        unsigned char last = end_key->at(end_key->size() - 1);
        if (last == '\x0') {
            end_key->pop_back();
        }
        // for Readable key, if the last nonzero character of end_key is '\x1', the wanted key that
        // is barely smaller than end_key is computed as: end_key.substr(0, end_key.rfind('\x1'));
        // eg: end_key: abcde'\x1' -> wanted key: abcde
        else if (rawkey == Readable && last == '\x1'){
            end_key->pop_back();
            return true;
        }
        else {
            break;
        }
    }
    // for other case, the wanted key that is barely smaller than end_key is computed as:  minus the
    // last char of end_key with 1 and append '\x255' to end key until it reaches the max keysize
    // allowed. Notice that the last char of end_key will not be '\x0' for Binary key and not be
    // '\x0' nor '\x1' for Readable key here
    if (end_key->size() > 0) {
        (*end_key)[end_key->size() - 1] = char((*end_key)[end_key->size() - 1] - 1);
    }
    end_key->resize(kRowkeySize - 1, char(255));
    return true;
}

void CompactPreprocess(TableImplPtr table, const std::vector<TabletInfo>& tablet_infos) {
    std::vector<RowReader*> readers;
    for (std::size_t i = 0; i < tablet_infos.size(); ++i) {
        const TabletInfo& tablet_info = tablet_infos[i];
        std::string start_key(tablet_info.start_key);
        std::string end_key(tablet_info.end_key);
        ComputeCompactInsertKeys(table->GetTableSchema().raw_key(), &start_key, &end_key);
        std::vector<RowReader*> readers;
        RowReader* start_reader = table->NewRowReader(start_key);
        RowReader* end_reader = table->NewRowReader(end_key);
        readers.push_back(start_reader);
        readers.push_back(end_reader);
    }
    if (readers.size() > 0) {
        table->Get(readers);
    }
    std::vector<RowMutation*> mutations;
    for (std::size_t i = 0; i < readers.size(); ++i) {
        if (readers[i]->GetError().GetType() == tera::ErrorCode::kNotFound) {
            RowMutation* mutation = table->NewRowMutation(readers[i]->RowKey());
            mutation->DeleteRow();
            mutations.push_back(mutation);
        }
        delete readers[i];
    }
    if (mutations.size() > 0) {
        table->ApplyMutation(mutations);
        for (std::size_t i = 0; i < mutations.size(); ++i) {
            if (mutations[i]->GetError().GetType() != tera::ErrorCode::kOK) {
                LOG(WARNING) <<"write key " << DebugString(mutations[i]->RowKey())
                    << " failed, error: " << mutations[i]->GetError().ToString();
            }
            delete mutations[i];
        }
    }
}

int32_t CompactTabletOp(Client* client, int32_t argc, std::string* argv, ErrorCode* err) {
    if (argc != 4) {
        PrintCmdHelpInfo(argv[1]);
        return -1;
    }

    std::vector<std::string> subs;
    std::string table, tablet, tablet_path;
    int lg = -1;
    SplitString(argv[3], "/", &subs);
    if (subs.size() == 2) {
        table = subs[0];
        tablet = subs[1];
        tablet_path = table + "/" + tablet;
    } else if (subs.size() == 3) {
        table = subs[0];
        tablet = subs[1];
        tablet_path = table + "/" + tablet;
        if (!StringToNumber(subs[2], &lg)) {
            LOG(ERROR) << "lg no error: " << subs[2];
            return -5;
        }
    } else if (subs.size() != 2 && subs.size() != 3) {
        LOG(ERROR) << "tablet path error, format [table/tablet] "
            << "or [table/tablet/lg]: " << tablet_path;
        return -2;
    }

    std::vector<TabletInfo> tablet_list;
    if (!client->GetTabletLocation(table, &tablet_list, err)) {
        LOG(ERROR) << "fail to list tablet info";
        return -3;
    }

    std::vector<TabletInfo>::iterator tablet_it = tablet_list.begin();
    for (; tablet_it != tablet_list.end(); ++tablet_it) {
        if (tablet_it->path == tablet_path) {
            break;
        }
    }
    if (tablet_it == tablet_list.end()) {
        LOG(ERROR) << "fail to find tablet: " << tablet_path
            << ", total tablets: " << tablet_list.size();
        return -4;
    }
    std::string command = argv[1];
    if (command == "compactx")
    {
        TablePtr table_ptr;
        table_ptr.reset(client->OpenTable(table, err));
        TableImplPtr table_impl(static_cast<TableWrapper*>(table_ptr.get())->GetTableImpl());
        if (table_impl == NULL) {
            LOG(ERROR) << "fail to open table: " << table;
            return -5;
        }
        std::vector<TabletInfo> tablet_infos(1, *tablet_it);
        CompactPreprocess(table_impl, tablet_infos);
    }

    return CompactTablet(*tablet_it, lg);
}

bool GetTabletInfo(Client* client, const std::string& tablename,
                   const std::string& tablet_path, TabletInfo* tablet,
                   ErrorCode* err) {
    std::vector<TabletInfo> tablet_list;
    if (!client->GetTabletLocation(tablename, &tablet_list, err)) {
        LOG(ERROR) << "fail to list tablet info";
        return false;
    }

    std::vector<TabletInfo>::iterator tablet_it = tablet_list.begin();
    for (; tablet_it != tablet_list.end(); ++tablet_it) {
        if (tablet_it->path == tablet_path) {
            *tablet = *tablet_it;
            break;
        }
    }
    if (tablet_it == tablet_list.end()) {
        LOG(ERROR) << "fail to find tablet: " << tablet_path
            << ", total tablets: " << tablet_list.size();
        return false;
    }
    return true;
}

int32_t ScanTabletOp(Client* client, int32_t argc, std::string* argv, ErrorCode* err) {
    if (argc < 4) {
        PrintCmdHelpInfo(argv[1]);
        return -1;
    }

    std::vector<std::string> subs;
    std::string op = argv[2];
    std::string tablet_path = argv[3];
    SplitString(tablet_path, "/", &subs);
    if (subs.size() != 2) {
        LOG(ERROR) << "tablet path error, format [table/tablet]: " << tablet_path;
        return -2;
    }

    TablePtr table(client->OpenTable(subs[0], err));
    if (table == NULL) {
        LOG(ERROR) << "fail to open table: " << subs[0];
        return -3;
    }

    TabletInfo tablet;
    if (!GetTabletInfo(client, subs[0], tablet_path, &tablet, err)) {
        LOG(ERROR) << "fail to parse tablet: " << tablet_path;
        return -4;
    }

    ScanDescriptor desc(tablet.start_key);
    desc.SetEnd(tablet.end_key);

    if (op == "scanallv") {
        desc.SetMaxVersions(std::numeric_limits<int>::max());
    }

    if (argc > 4 && !desc.SetFilter(argv[4])) {
        LOG(ERROR) << "fail to parse scan schema: " << argv[4];
        return -5;
    }

    int32_t ret = ScanRange(table, desc, err);
    return ret;
}

int32_t TabletOp(Client* client, int32_t argc, std::string* argv, ErrorCode* err) {
    if ((argc != 4) && (argc != 5) && (argc != 6)) {
        PrintCmdHelpInfo(argv[1]);
        return -1;
    }

    std::string op = argv[2];
    std::string tablet_id = argv[3];
    std::string server_addr;

    std::vector<std::string> arg_list;
    arg_list.push_back(op);
    arg_list.push_back(tablet_id);
    if (op == "compact" || op == "compactx") {
        return CompactTabletOp(client, argc, argv, err);
    } else if (op == "scan" || op == "scanallv") {
        return ScanTabletOp(client, argc, argv, err);
    } else if (argc == 4 && (op == "reload" || op == "merge" || op == "split")) {
        // nothing to do
    } else if (argc == 5 && (op == "reloadx" || op == "move" || op == "split")) {
        // reloadx->lg_list  move->server_addr  split->split_key
        arg_list.push_back(argv[4]);
    } else if (argc == 6 && op == "movex") {
        arg_list.push_back(argv[4]); // server_addr
        arg_list.push_back(argv[5]); // lg_list
    } else {
        PrintCmdHelpInfo(argv[1]);
        return -1;
    }
    if (!client->CmdCtrl("tablet", arg_list, NULL, NULL, err)) {
        LOG(ERROR) << "fail to " << op << " tablet " << tablet_id;
        return -1;
    }
    std::cout << op << " tablet " << tablet_id << " success" << std::endl;

    return 0;
}

void ReorderTabletList(std::vector<TabletInfo>* tablets) {
    if (tablets->size() <= 1) {
        return;
    }

    // ordered all tablets by ts
    std::map<std::string, std::vector<TabletInfo> > tablet_map;
    size_t max_tablet = 0;
    for (size_t i = 0; i < tablets->size(); ++i) {
        std::vector<TabletInfo>& v = tablet_map[tablets->at(i).server_addr];
        v.push_back(tablets->at(i));
        if (v.size() > max_tablet) {
            max_tablet = v.size();
        }
    }

    size_t ts_num = tablet_map.size();
    std::vector<std::vector<TabletInfo> > tablet_vector;
    tablet_vector.resize(ts_num);
    std::map<std::string, std::vector<TabletInfo> >::iterator it =
        tablet_map.begin();
    for (size_t i = 0; it != tablet_map.end(); ++it, ++i) {
        tablet_vector[i].swap(it->second);
    }

    // recover tablet list
    std::vector<TabletInfo> tablets_t;
    for (size_t y = 0; y < max_tablet; y++) {
        for (size_t x = 0; x < ts_num; x++) {
            if (y < tablet_vector[x].size()) {
                tablets_t.push_back(tablet_vector[x][y]);
            }
        }
    }
    CHECK(tablets_t.size() == tablets->size());
    tablets->swap(tablets_t);
}

bool FiltrateTabletsByFile(std::vector<TabletInfo>& tablet_list) {
    if (FLAGS_tablets_file.empty()) {
        return true;
    }
    std::ifstream fin(FLAGS_tablets_file.c_str());
    if (fin.fail()) {
        LOG(ERROR) << "fail to read tablets file: " << FLAGS_tablets_file;
        return false;
    }
    std::set<string> tablets_filter;
    string str;
    while (fin >> str) {
        tablets_filter.insert(str);
    }

    std::vector<TabletInfo> tablets;
    std::vector<TabletInfo>::iterator tablet_it = tablet_list.begin();
    for (; tablet_it != tablet_list.end(); ++tablet_it) {
        if (tablets_filter.find(tablet_it->path) != tablets_filter.end()) {
            tablets.push_back(*tablet_it);
        }
    }
    tablet_list.swap(tablets);
    return true;
}

int32_t CompactOp(Client* client, int32_t argc, std::string* argv, ErrorCode* err) {
    if (argc != 3) {
        PrintCmdHelpInfo(argv[1]);
        return -1;
    }

    std::string tablename = argv[2];
    std::vector<TabletInfo> tablet_list;
    if (!client->GetTabletLocation(tablename, &tablet_list, err)) {
        LOG(ERROR) << "fail to list tablets info: " << tablename;
        return -3;
    }
    if (!FiltrateTabletsByFile(tablet_list)) {
        return -4;
    }
    ReorderTabletList(&tablet_list);

    std::string command = argv[1];
    if (command == "compactx")
    {
        TablePtr table;
        table.reset(client->OpenTable(tablename, err));
        TableImplPtr table_impl(static_cast<TableWrapper*>(table.get())->GetTableImpl());
        if (table_impl == NULL) {
            LOG(ERROR) << "fail to open table: " << tablename;
            return -5;
        }
        std::cout << "begin compact preprocess tablet: " << tablename << std::endl;
        CompactPreprocess(table_impl, tablet_list);
    }

    int conc = FLAGS_concurrency;
    if (conc <= 0 || conc > 1000) {
        LOG(ERROR) << "compact concurrency illegal: " << conc;
    }

    ThreadPool thread_pool(conc);
    std::vector<TabletInfo>::iterator tablet_it = tablet_list.begin();
    for (; tablet_it != tablet_list.end(); ++tablet_it) {
        ThreadPool::Task task =
                std::bind(&CompactTablet, *tablet_it, FLAGS_lg);
        thread_pool.AddTask(task);
    }
    while (thread_pool.PendingNum() > 0) {
        std::cerr << get_time_str(time(NULL)) << " "
            << thread_pool.PendingNum()
            << " tablets waiting for compact ..." << std::endl;
        sleep(5);
    }
    thread_pool.Stop(true);
    return 0;
}

int32_t FindMasterOp(Client* client, int32_t argc, std::string* argv, ErrorCode* err) {
    if (argc != 2) {
        PrintCmdHelpInfo(argv[1]);
        return -1;
    }
    scoped_ptr<tera::sdk::ClusterFinder> finder(tera::sdk::NewClusterFinder());
    std::cout << "master addr:< " << finder->MasterAddr() << " >\n";
    return 0;
}

int32_t FindTsOp(Client* client, int32_t argc, std::string* argv, ErrorCode* err) {
    if (argc != 3 && argc != 4) {
        PrintCmdHelpInfo(argv[1]);
        return -1;
    }

    std::string tablename = argv[2];
    TablePtr table;
    table.reset(client->OpenTable(tablename, err));
    TableImplPtr table_impl(static_cast<TableWrapper*>(table.get())->GetTableImpl());
    if (table_impl == NULL) {
        LOG(ERROR) << "fail to open table";
        return -1;
    }

    if (argc == 4) {
        std::string rowkey = argv[3];
        table_impl->ScanMetaTable(rowkey, rowkey + '\0');

        TabletMeta meta;
        if (!table_impl->GetTabletMetaForKey(rowkey, &meta)) {
            LOG(ERROR) << "fail to get tablet meta for " << rowkey;
            return -1;
        }
        std::cout << meta.server_addr() << "\t" << meta.path() << std::endl;
        return 0;
    }

    table_impl->ScanMetaTable("", "");
    const int32_t buf_size = 1024 * 1024;
    char rowkey[buf_size];
    while (std::cin.getline(rowkey, buf_size)) {
        TabletMeta meta;
        if (!table_impl->GetTabletMetaForKey(rowkey, &meta)) {
            LOG(ERROR) << "fail to get tablet meta for " << rowkey;
            continue;
        }
        std::cout << rowkey << "\t" << meta.server_addr() << "\t" << meta.path() << std::endl;
    }

    return 0;
}

void WriteToStream(std::ofstream& ofs,
                   const std::string& key,
                   const std::string& value) {
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

int32_t ProcessMeta(const std::string& op, const TableMetaList& table_list,
                    const TabletMetaList& tablet_list) {
    int32_t table_num = table_list.meta_size();
    int32_t tablet_num = tablet_list.meta_size();
    if (table_num == 0 && tablet_num == 0) {
        std::cout << "meta table is empty" << std::endl;
        return 0;
    }

    std::ofstream bak;
    if (op == "backup" || op == "repair") {
        bak.open("meta.bak", std::ofstream::trunc|std::ofstream::binary);
    }

    for (int32_t i = 0; i < table_num; ++i) {
        const tera::TableMeta& meta = table_list.meta(i);
        if (op == "show") {
            std::cout << "table: " << meta.table_name() << std::endl;
            int32_t lg_size = meta.schema().locality_groups_size();
            for (int32_t lg_id = 0; lg_id < lg_size; lg_id++) {
                const tera::LocalityGroupSchema& lg =
                    meta.schema().locality_groups(lg_id);
                std::cout << " lg" << lg_id << ": " << lg.name() << " ("
                    << lg.store_type() << ", "
                    << lg.compress_type() << ", "
                    << lg.block_size() << ")" << std::endl;
            }
            int32_t cf_size = meta.schema().column_families_size();
            for (int32_t cf_id = 0; cf_id < cf_size; cf_id++) {
                const tera::ColumnFamilySchema& cf =
                    meta.schema().column_families(cf_id);
                std::cout << " cf" << cf_id << ": " << cf.name() << " ("
                    << cf.locality_group() << ", "
                    << cf.type() << ", "
                    << cf.max_versions() << ", "
                    << cf.time_to_live() << ")" << std::endl;
            }
        }
        if (op == "backup" || op == "repair") {
            WriteTable(meta, bak);
        }
    }

    tera::TabletMeta last;
    bool table_start = true;
    for (int32_t i = 0; i < tablet_num; ++i) {
        const tera::TabletMeta& meta = tablet_list.meta(i);
        if (op == "show") {
            std::cout << "tablet: " << meta.table_name() << " ["
                << meta.key_range().key_start() << ","
                << meta.key_range().key_end() << "], "
                << meta.path() << ", " << meta.server_addr() << ", "
                << meta.size() << ", "
                << StatusCodeToString(meta.status()) << ", "
                << StatusCodeToString(meta.compact_status()) << std::endl;
        }
        if (op == "backup") {
            WriteTablet(meta, bak);
        }
        // check self range
        if (!meta.key_range().key_end().empty() &&
            meta.key_range().key_start() >= meta.key_range().key_end()) {
            std::cerr << "invalid tablet " << meta.table_name() << " ["
                << meta.key_range().key_start() << ","
                << meta.key_range().key_end() << "], "
                << meta.path() << ", " << meta.server_addr() << ", "
                << meta.size() << ", "
                << StatusCodeToString(meta.status()) << ", "
                << StatusCodeToString(meta.compact_status()) << std::endl;
            // ignore invalid tablet
            continue;
        }

        tera::TabletMeta repair_meta;
        bool covered = false;
        // check miss/cover/overlap with previous tablet
        if (!table_start) {
            assert(!last.key_range().key_end().empty());
            if (meta.table_name() != last.table_name()) {
                std::cerr << "miss tablet: " << last.table_name() << ", ["
                    << last.key_range().key_end() << ",-]" << std::endl;
                if (op == "repair") {
                    tera::TabletMeta miss_meta;
                    miss_meta.set_table_name(last.table_name());
                    miss_meta.mutable_key_range()->set_key_start(last.key_range().key_end());
                    miss_meta.mutable_key_range()->set_key_end("");
                    WriteTablet(miss_meta, bak);
                }
                table_start = true;
            } else if (meta.key_range().key_start() > last.key_range().key_end()) {
                std::cerr << "miss tablet " << last.table_name() << " ["
                    << last.key_range().key_end() << ","
                    << meta.key_range().key_start() << "]" << std::endl;
                if (op == "repair") {
                    tera::TabletMeta miss_meta;
                    miss_meta.set_table_name(last.table_name());
                    miss_meta.mutable_key_range()->set_key_start(last.key_range().key_end());
                    miss_meta.mutable_key_range()->set_key_end(meta.key_range().key_start());
                    WriteTablet(miss_meta, bak);
                    WriteTablet(meta, bak);
                }
            } else if (meta.key_range().key_start() == last.key_range().key_end()) {
                if (op == "repair") {
                    WriteTablet(meta, bak);
                }
            } else if (!meta.key_range().key_end().empty()
                            && meta.key_range().key_end() <= last.key_range().key_end()) {
                std::cerr << "tablet " << meta.table_name() << " ["
                    << meta.key_range().key_start() << ","
                    << meta.key_range().key_end() << "] is coverd by tablet "
                    << last.table_name() << " ["
                    << last.key_range().key_start() << ","
                    << last.key_range().key_end() << "]" << std::endl;
                covered = true;
            } else {
                std::cerr << "tablet " << meta.table_name() << " ["
                    << meta.key_range().key_start() << ","
                    << meta.key_range().key_end() << "] overlap with tablet "
                    << last.table_name() << " ["
                    << last.key_range().key_start() << ","
                    << last.key_range().key_end() << "]" << std::endl;
                if (op == "repair") {
                    tera::TabletMeta repair_meta = meta;
                    repair_meta.mutable_key_range()->set_key_start(last.key_range().key_end());
                    WriteTablet(repair_meta, bak);
                }
            }
        }
        if (table_start) {
            if (meta.table_name() == last.table_name()) {
                std::cerr << "tablet " << meta.table_name() << " ["
                    << meta.key_range().key_start() << ","
                    << meta.key_range().key_end() << "] is coverd by tablet "
                    << last.table_name() << " ["
                    << last.key_range().key_start() << ","
                    << last.key_range().key_end() << "]" << std::endl;
                covered = true;
            } else {
                if (!meta.key_range().key_start().empty()) {
                    std::cerr << "miss tablet " << meta.table_name() << " [-,"
                        << meta.key_range().key_start() << "]" << std::endl;
                    if (op == "repair") {
                        tera::TabletMeta miss_meta;
                        miss_meta.set_table_name(meta.table_name());
                        miss_meta.mutable_key_range()->set_key_start("");
                        miss_meta.mutable_key_range()->set_key_end(meta.key_range().key_start());
                        WriteTablet(miss_meta, bak);
                    }
                }
                if (op == "repair") {
                    WriteTablet(meta, bak);
                }
            }
        }

        // ignore covered tablet
        if (!covered) {
            last.CopyFrom(meta);
            table_start = meta.key_range().key_end().empty();
        }
    }
    if (op == "backup" || op == "repair") {
        bak.close();
    }
    return 0;
}

int32_t MetaOp(Client* client, int32_t argc, std::string* argv, ErrorCode* err) {
    if (argc != 4 && argc != 3) {
        PrintCmdHelpInfo(argv[1]);
        return -1;
    }

    std::string op = argv[2];
    if (op == "backup") {
        if (argc < 4) {
            LOG(ERROR) << "need backup file name.";
            return -1;
        }
        std::string filename = argv[3];
        std::vector<std::string> arg_list;
        arg_list.push_back(op);
        arg_list.push_back(filename);
        if (!client->CmdCtrl("meta", arg_list, NULL, NULL, err)) {
            LOG(ERROR) << "fail to " << op << " meta";
            return -5;
        }
    } else if (op == "check" || op == "repair" || op == "show") {
        TableMetaList table_list;
        TabletMetaList tablet_list;
        std::shared_ptr<tera::ClientImpl> client_impl((static_cast<ClientWrapper*>(client))->GetClientImpl());
        if (!client_impl->ShowTablesInfo(&table_list, &tablet_list, false, err)) {
            LOG(ERROR) << "fail to get meta data from tera.";
            return -3;
        }
        ProcessMeta(op, table_list, tablet_list);
    } else {
        PrintCmdHelpInfo(argv[1]);
        return -2;
    }

    std::cout << op << " tablet success" << std::endl;
    return 0;
}

int32_t FindTabletOp(Client*, int32_t argc, std::string* argv, ErrorCode* err) {
    if ((argc != 4) && (argc != 5)) {
        PrintCmdHelpInfo(argv[1]);
        return -1;
    }
    if (argc == 5) {
        std::string s = argv[3];
        std::string e = argv[4];
        if ((e != "") && (s.compare(e) >= 0)) {
            if (err != NULL) {
                err->SetFailed(ErrorCode::kBadParam, "note: start_key <= end_key");
            }
            return -1;
        }
    }
    // get meta address
    scoped_ptr<tera::sdk::ClusterFinder> finder(tera::sdk::NewClusterFinder());
    std::string meta_tablet_addr = finder->RootTableAddr();
    if (meta_tablet_addr.empty()) {
        if (err != NULL) {
            err->SetFailed(ErrorCode::kBadParam, "read root addr from zk fail");
        }
        return -1;
    }
    uint64_t seq_id = 0;
    tera::TabletMetaList tablet_list;
    tera::ScanTabletRequest request;
    tera::ScanTabletResponse response;
    request.set_sequence_id(seq_id++);
    request.set_table_name(FLAGS_tera_master_meta_table_name);
    request.set_start(std::string(argv[2]) + '#');
    request.set_end(std::string(argv[2]) + '$');
    common::ThreadPool thread_pool(FLAGS_concurrency);
    tera::tabletnode::TabletNodeClient meta_node_client(&thread_pool, meta_tablet_addr);
    while (meta_node_client.ScanTablet(&request, &response)) {
        if (response.status() != tera::kTabletNodeOk) {
            std::stringstream ss;
            ss << "fail to load meta table: "
                << StatusCodeToString(response.status()) << std::endl;
            if (err != NULL) {
                err->SetFailed(ErrorCode::kBadParam, ss.str());
            }
            return -1;
        }
        int32_t record_size = response.results().key_values_size();
        if (record_size <= 0) {
            break;
        }

        std::string last_record_key;
        for (int32_t i = 0; i < record_size; i++) {
            const tera::KeyValuePair& record = response.results().key_values(i);
            last_record_key = record.key();
            ParseMetaTableKeyValue(record.key(), record.value(), tablet_list.add_meta());
        }
        std::string next_record_key = tera::NextKey(last_record_key);
        request.set_start(next_record_key);
        request.set_end(std::string(argv[2]) + '$');
        request.set_sequence_id(seq_id++);
        response.Clear();
    }
    std::string start_key;
    std::string end_key;
    if (argc == 4) {
        start_key = std::string(argv[3]);
        end_key = tera::NextKey(start_key);
    } else {
        start_key = std::string(argv[3]);
        end_key = std::string(argv[4]);
    }
    int32_t tablet_num = tablet_list.meta_size();
    for (int32_t i = 0; i < tablet_num; ++i) {
        const tera::TabletMeta& meta = tablet_list.meta(i);
        if ((meta.key_range().key_end() != "")
            && (start_key.compare(meta.key_range().key_end()) >= 0)) {
            continue;
        }
        if ((end_key != "")
            && (end_key.compare(meta.key_range().key_start()) < 0)) {
            break;
        }
        std::cout << meta.path() << " " << meta.server_addr() << std::endl;
    }
    return 0;
}

int32_t StatOp(Client*, int32_t argc, std::string* argv, ErrorCode*) {
    if (argc < 3) {
        PrintCmdHelpInfo("stat");
        return -1;
    }
    std::string op = argv[2];
    if (op != "corruption") {
        PrintCmdHelpInfo(argv[1]);
        return -1;
    }
    std::string tabletpath = "";
    if (argc == 4) {
        tabletpath = argv[3];
    }
    std::string tabletnode = "";
    if (argc == 5) {
        tabletnode = argv[4];
    }
    int conc = FLAGS_concurrency;
    if (conc <= 0 || conc > 1000) {
        LOG(ERROR) << "compact concurrency illegal: " << conc;
        return -1;
    }
    ThreadPool thread_pool(conc);
    std::unique_ptr<tera::sdk::StatTable> stat_table(
            new tera::sdk::StatTable(&thread_pool,
                                     tera::sdk::StatTableCustomer::kClient));
    stat_table->OpenStatTable();
    stat_table->SelectTabletsFailMessages(tabletpath, tabletnode);
    return 0;
}

int32_t Meta2Op(Client*, int32_t argc, std::string* argv, ErrorCode*) {
    if (argc < 3) {
        PrintCmdHelpInfo("meta");
        return -1;
    }

    std::string op = argv[2];
    if (op != "check" && op != "show" && op != "backup" && op != "repair") {
        PrintCmdHelpInfo(argv[1]);
        return -1;
    }

    // get meta address
    scoped_ptr<tera::sdk::ClusterFinder> finder(tera::sdk::NewClusterFinder());
    std::string meta_tablet_addr = finder->RootTableAddr();
    if (meta_tablet_addr.empty()) {
        std::cerr << "read root addr from zk fail";
        return -1;
    }

    // scan meta
    uint64_t seq_id = 0;
    tera::TableMetaList table_list;
    tera::TabletMetaList tablet_list;
    tera::ScanTabletRequest request;
    tera::ScanTabletResponse response;
    request.set_sequence_id(seq_id++);
    request.set_table_name(FLAGS_tera_master_meta_table_name);
    request.set_start("");
    request.set_end("");
    common::ThreadPool thread_pool(FLAGS_concurrency);
    tera::tabletnode::TabletNodeClient meta_node_client(&thread_pool, meta_tablet_addr);
    while (meta_node_client.ScanTablet(&request, &response)) {
        if (response.status() != tera::kTabletNodeOk) {
            std::cerr << "fail to load meta table: "
                << StatusCodeToString(response.status()) << std::endl;
            return -1;
        }
        int32_t record_size = response.results().key_values_size();
        if (record_size <= 0) {
            std::cout << "scan meta table success" << std::endl;
            break;
        }
        std::cerr << "scan meta table: " << record_size << " records" << std::endl;

        std::string last_record_key;
        for (int32_t i = 0; i < record_size; i++) {
            const tera::KeyValuePair& record = response.results().key_values(i);
            last_record_key = record.key();
            char first_key_char = record.key()[0];
            if (first_key_char == '~') {
                std::cout << "(user: " << record.key().substr(1) << ")" << std::endl;
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
        request.set_end("");
        request.set_sequence_id(seq_id++);
        response.Clear();
    }

    return ProcessMeta(op, table_list, tablet_list);
}

static int32_t CreateUser(Client* client, const std::string& user,
                          const std::string& password, ErrorCode* err) {
    if (!client->CreateUser(user, password, err)) {
        LOG(ERROR) << "fail to create user: " << user << ", " << err->ToString();
        return -1;
    }
    return 0;
}

static int32_t DeleteUser(Client* client, const std::string& user, ErrorCode* err) {
    if (!client->DeleteUser(user, err)) {
        LOG(ERROR) << "fail to delete user: " << user << ", " << err->ToString();
        return -1;
    }
    return 0;
}

static int32_t ChangePwd(Client* client, const std::string& user,
                         const std::string& password, ErrorCode* err) {
    if (!client->ChangePwd(user, password, err)) {
        LOG(ERROR) << "fail to update user: " << user << ", " << err->ToString();
        return -1;
    }
    return 0;
}

static int32_t ShowUser(Client* client, const std::string& user, ErrorCode* err) {
    std::vector<std::string> user_infos;
    if (!client->ShowUser(user, user_infos, err)) {
        LOG(ERROR) << "fail to show user: " << user << ", " << err->ToString();
        return -1;
    }
    if (user_infos.size() < 1) {
        return -1;
    }
    std::cout << "user:" << user_infos[0]
        << "\ngroups (" << user_infos.size() - 1 << "):";
    for (size_t i = 1; i < user_infos.size(); ++i) {
        std::cout << user_infos[i] << " ";
    }
    std::cout << std::endl;
    return 0;
}

static int32_t AddUserToGroup(Client* client, const std::string& user,
                                const std::string& group, ErrorCode* err) {
    if (!client->AddUserToGroup(user, group, err)) {
        LOG(ERROR) << "fail to add user: " << user
            << " to group:" << group << err->ToString();
        return -1;
    }
    return 0;
}

static int32_t DeleteUserFromGroup(Client* client, const std::string& user,
                                     const std::string& group, ErrorCode* err) {
    if (!client->DeleteUserFromGroup(user, group, err)) {
        LOG(ERROR) << "fail to delete user: " << user
            << " from group: " << group << err->ToString();
        return -1;
    }
    return 0;
}

int32_t UserOp(Client* client, int32_t argc, std::string* argv, ErrorCode* err) {
    if (argc < 4) {
        PrintCmdHelpInfo(argv[1]);
        return -1;
    }
    std::string op = argv[2];
    if ((argc == 5) && (op == "create")) {
        return CreateUser(client, argv[3], argv[4], err);
    } else if ((argc == 5) && (op == "changepwd")) {
        return ChangePwd(client, argv[3], argv[4], err);
    } else if ((argc == 4) && (op == "show")) {
        return ShowUser(client, argv[3], err);
    } else if ((argc == 4) && (op == "delete")) {
        return DeleteUser(client, argv[3], err);
    } else if ((argc == 5) && (op == "addtogroup")) {
        return AddUserToGroup(client, argv[3], argv[4], err);
    } else if ((argc == 5) && (op == "deletefromgroup")) {
        return DeleteUserFromGroup(client, argv[3], argv[4], err);
    }
    PrintCmdHelpInfo(argv[1]);
    return -1;
}

int32_t RangeOp(Client* client, int32_t argc, std::string* argv, ErrorCode* err) {
    if (argc != 3) {
        PrintCmdHelpInfo(argv[1]);
        return -1;
    }

    bool is_x = (std::string(argv[1]) == "rangex");
    std::string tablename = argv[2];

    std::vector<TabletInfo> tablet_list;
    if (!client->GetTabletLocation(tablename, &tablet_list, err)) {
        LOG(ERROR) << "fail to list tablet info: " << tablename;
        return -2;
    }

    if (FLAGS_reorder_tablets) {
        ReorderTabletList(&tablet_list);
    }

    std::vector<TabletInfo>::iterator it = tablet_list.begin();
    for (; it != tablet_list.end(); ++it) {
        if (it->start_key.empty()) {
            it->start_key = "-";
        }
        if (it->end_key.empty()) {
            it->end_key = "-";
        }
        if (is_x) {
            std::cout << it->server_addr << "\t" << it->path << "\t"
                << it->start_key << "\t" << it->end_key << std::endl;
        } else {
            std::cout << it->start_key << "\t" << it->end_key << std::endl;
        }
    }
    return 0;
}

int StartRowTxnOp(Client* client, int32_t argc, std::string* argv, ErrorCode* err) {
    if (argc != 5) {
        PrintCmdHelpInfo(argv[1]);
        return -1;
    }
    if (g_row_txn != NULL) {
        LOG(ERROR) << "txn has started";
        return -1;
    }

    CHECK(g_row_txn_table == NULL);
    std::string tablename = argv[3];
    g_row_txn_table = client->OpenTable(tablename, err);
    if (g_row_txn_table == NULL) {
        LOG(ERROR) << "fail to open table";
        return -1;
    }

    std::string row_key = argv[4];
    g_row_txn = g_row_txn_table->StartRowTransaction(row_key);
    if (g_row_txn == NULL) {
        LOG(ERROR) << "fail to start row txn";
        delete g_row_txn_table;
        g_row_txn_table = NULL;
        return -1;
    }
    return 0;
}

int CommitRowTxnOp(Client* client, int32_t argc, std::string* argv, ErrorCode* err) {
    if (argc != 3) {
        PrintCmdHelpInfo(argv[1]);
        return -1;
    }
    if (g_row_txn == NULL) {
        LOG(ERROR) << "txn has not started";
        return -1;
    }
    g_row_txn_table->CommitRowTransaction(g_row_txn);
    std::cout << g_row_txn->GetError().ToString() << std::endl;

    delete g_row_txn;
    g_row_txn = NULL;
    delete g_row_txn_table;
    g_row_txn_table = NULL;
    return 0;
}

int TxnOp(Client* client, int32_t argc, std::string* argv, ErrorCode* err) {
    if (argc < 3) {
        LOG(ERROR) << "args number error: " << argc << ", need > 2";
        PrintCmdHelpInfo(argv[1]);
        return -1;
    }

    std::string txn_op = argv[2];
    if (txn_op == "start") {
        return StartRowTxnOp(client, argc, argv, err);
    } else if (txn_op == "commit") {
        return CommitRowTxnOp(client, argc, argv, err);
    } else {
        PrintCmdHelpInfo(argv[1]);
        return -1;
    }
}

int GTxnOp(Client* client, int32_t argc, std::string* argv, ErrorCode* err) {
    std::vector<std::vector<std::string>> statements;
    std::string line;
    while (getline(std::cin, line)) {
        bool on_skipping = false;
        uint32_t pos = 0;
        std::vector<std::string> terms;
        std::string term;
        while (pos < line.length()) {
            char c = line[pos];
            if (!on_skipping) {
                if (c == ' ' || c == '\t') {
                    on_skipping = true;
                    if (term.length() > 0) {
                        terms.push_back(term);
                        term.clear();
                    }
                } else {
                    term.push_back(c); 
                }
            } else {
                if (c != ' ' && c != '\t') {
                    term.push_back(c);
                    on_skipping = false;
                }
            }
            ++pos;
        }
        if (term.length() > 0) {
            terms.push_back(term);
            term.clear();
        }
        if (terms.size() > 0) {
            statements.push_back(terms);
        }
    }
    bool begin_txn = false;
    std::unique_ptr<Transaction> txn;
    std::vector<TablePtr> tables; 
    for (auto statement : statements) {
        if (!begin_txn) {
            if (statement.size() == 1 && (statement[0] == "BEGIN")) {
                txn.reset(client->NewGlobalTransaction());
                if (txn != NULL) {
                    begin_txn = true;
                } else {
                    std::cerr << "new global txn failed" << std::endl;
                    return -1;
                }
            } else {
                std::cerr << "intput syntax error @ " << statement[0] << std::endl;
                return -1;
            }
        } else {
            std::string family;
            std::string qualifier;
            switch (statement.size()) {
                case 1: {
                    // COMMIT
                    if (statement[0] != "COMMIT") {
                        std::cerr << "input syntax error @ " << statement[0] << std::endl;
                        return -1;
                    }
                    txn->Commit();
                    begin_txn = false;
                    if (txn->GetError().GetType() != ErrorCode::kOK) {
                        std::cerr << "txn commit failed" 
                                  << txn->GetError().ToString() << std::endl;
                        return -1;
                    } else {
                        std::cout << "ok" << std::endl;
                    }
                    break;
                }
                case 5: {
                    // PUT tablename rowkey cf:qu value
                    if (statement[0] != "PUT") {
                        std::cerr << "input syntax error @ " << statement[0] << std::endl;
                        return -1;
                    }
                    TablePtr mu_table(client->OpenTable(statement[1], err));
                    if (!mu_table) {
                        LOG(ERROR) << "fail to open table";
                        return -1;
                    }
                    tables.push_back(mu_table);
                    std::unique_ptr<RowMutation> mu(mu_table->NewRowMutation(statement[2]));
                    ParseCfQualifier(statement[3], &family, &qualifier);
                    mu->Put(family, qualifier, statement[4]);
                    txn->ApplyMutation(mu.get());
                    break;
                }
                case 4: {
                    // DEL tablename rowkey cf:qu
                    if (statement[0] == "DEL") {
                        TablePtr del_table(client->OpenTable(statement[1], err));
                        if (!del_table) {
                            LOG(ERROR) << "fail to open table";
                            return -1;
                        }
                        tables.push_back(del_table);
                        std::unique_ptr<RowMutation> mu(del_table->NewRowMutation(statement[2]));
                        ParseCfQualifier(statement[3], &family, &qualifier);
                        mu->DeleteColumn(family, qualifier);
                        txn->ApplyMutation(mu.get());
                    } else if (statement[0] == "GET") {
                        // GET tablename rowkey cf:qu
                        TablePtr r_table(client->OpenTable(statement[1], err));
                        if (!r_table) {
                            LOG(ERROR) << "fail to open table";
                            return -1;
                        }
                        tables.push_back(r_table);
                        std::unique_ptr<RowReader> reader(r_table->NewRowReader(statement[2]));
                        ParseCfQualifier(statement[3], &family, &qualifier);
                        reader->AddColumn(family, qualifier);
                        txn->Get(reader.get());
                        if (reader->GetError().GetType() != ErrorCode::kOK) {
                            std::cerr << "txn get failed" 
                                      << reader->GetError().ToString() << std::endl;
                            return -1;
                        }
                        while (!reader->Done()) {
                            std::cout << reader->Value() << std::endl;
                            reader->Next();
                        }
                    } else {
                        std::cerr << "input syntax error @ " << statement[0] << std::endl;
                        return -1;
                    }
                    break;
                }
                default:
                    std::cerr << "input syntax error : statement size should 1,4,5 " << std::endl;
                    return -1;
            }
        }
    }
    return 0;
}

int32_t CasOp(Client* client, int32_t argc, std::string* argv, ErrorCode* err) {
    if (argc != 7) {
        LOG(ERROR) << "args number error: " << argc << ", need 7";
        PrintCmdHelpInfo(argv[1]);
        return -1;
    }

    const std::string& tablename = argv[2];
    TablePtr table(client->OpenTable(tablename, err));
    if (!table) {
        LOG(ERROR) << "fail to open table";
        return -1;
    }

    const std::string& rowkey = argv[3];
    const std::string& old_val = argv[5];
    const std::string& new_val = argv[6];
    std::string columnfamily = "";
    std::string qualifier = "";
    ParseCfQualifier(argv[4], &columnfamily, &qualifier);

    std::unique_ptr<tera::Transaction> txn(table->StartRowTransaction(rowkey));
    if (!txn) {
        LOG(ERROR) << "fail to start row txn";
        return -1;
    }

    std::unique_ptr<tera::RowReader> reader(table->NewRowReader(rowkey));
    reader->AddColumn(columnfamily, qualifier);
    txn->Get(reader.get());
    if (reader->Done()) {
        std::cout << "cas failed: NotFound" << std::endl;
        return -1;
    }
    std::string cur_val = reader->Value();
    if (cur_val != old_val) {
        std::cout << "cas failed: NotEqual" << std::endl;
        return -1;
    }

    std::unique_ptr<tera::RowMutation> mutation(table->NewRowMutation(rowkey));
    mutation->Put(columnfamily, qualifier, new_val);
    txn->ApplyMutation(mutation.get());
    if (mutation->GetError().GetType() != tera::ErrorCode::kOK) {
        std::cout << "cas failed: " << tera::strerr(mutation->GetError()) << std::endl;
        return -1;
    }

    auto error_code = txn->Commit();
    if (error_code.GetType() != tera::ErrorCode::kOK) {
        std::cout << "cas failed: " << tera::strerr(error_code) << std::endl;
        return -1;
    } else {
        std::cout << "cas success" << std::endl;
    }

    return 0;
}

int32_t HelpOp(Client*, int32_t argc, std::string* argv, ErrorCode*) {
    if (argc == 2) {
        PrintAllCmd();
    } else if (argc == 3) {
        PrintCmdHelpInfo(argv[2]);
    } else {
        PrintCmdHelpInfo("help");
    }
    return 0;
}

int32_t HelpOp(int32_t argc, char** argv) {
    std::vector<std::string> argv_svec(argv, argv + argc);
    return HelpOp(NULL, argc, &argv_svec[0], NULL);
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


int32_t InitDfsClient() {
    if (g_dfs != NULL) {
        return 0;
    }
    if (FLAGS_tera_leveldb_env_dfs_type == "nfs") {
        if (access(FLAGS_tera_leveldb_env_nfs_conf_path.c_str(), R_OK) == 0) {
            LOG(INFO) << "init nfs system: use configure file" << FLAGS_tera_leveldb_env_nfs_conf_path;
            leveldb::Nfs::Init(FLAGS_tera_leveldb_env_nfs_mountpoint, FLAGS_tera_leveldb_env_nfs_conf_path);
            g_dfs = leveldb::Nfs::GetInstance();
        }
        else {
            LOG(FATAL) << "init nfs system: no configure file found";
            return -1;
        }
    } else if (FLAGS_tera_leveldb_env_dfs_type == "hdfs2") {
        LOG(INFO) << "hdfs2 system support currently, please use hadoop-client";
        g_dfs = new leveldb::Hdfs2(FLAGS_tera_leveldb_env_hdfs2_nameservice_list);
    } else if (FLAGS_tera_leveldb_env_dfs_type == "hdfs") {
        g_dfs = new leveldb::Hdfs();
    }
    else {
        LOG(INFO) << "init dfs system: " << FLAGS_tera_dfs_so_path << "(" << FLAGS_tera_dfs_conf << ")";
        g_dfs = leveldb::Dfs::NewDfs(FLAGS_tera_dfs_so_path, FLAGS_tera_dfs_conf);
    }
    return 0;
}

int32_t FileSystemOp(Client* client, int32_t argc, std::string* argv, ErrorCode* err) {
    if (argc < 4) {
        PrintCmdHelpInfo(argv[1]);
        return -1;
    }
    if (0 != InitDfsClient()) {
        LOG(FATAL) << "InitDfsClient failed";
        return -1;
    }
    std::string operation = argv[2];
    if (GetFSCommandTable().find(operation) == GetFSCommandTable().end()) {
        std::cerr << "unsupported dfs command: " << operation << std::endl;
        return -1;
    }
    int ret = (GetFSCommandTable().find(operation)->second)(argc, argv, err);
    return ret;
}

int DfsPrintAttr(const char* pathname, struct stat* st, void* arg = NULL) {
    char mode_str[10];
    memset(mode_str, '-', sizeof(mode_str));
    char time_str[64];
    strftime(time_str, sizeof(time_str), "%b %d %H:%M %Y", localtime(&st->st_mtime));
    printf("%c%c%c%c%c%c%c%c%c%c %16lx %16ld %s %s",
            (S_IFDIR & st->st_mode) ? 'd' : '-',
            (S_IRUSR & st->st_mode) ? 'r' : '-',
            (S_IWUSR & st->st_mode) ? 'w' : '-',
            (S_IXUSR & st->st_mode) ? 'x' : '-',
            (S_IRGRP & st->st_mode) ? 'r' : '-',
            (S_IWGRP & st->st_mode) ? 'w' : '-',
            (S_IXGRP & st->st_mode) ? 'x' : '-',
            (S_IROTH & st->st_mode) ? 'r' : '-',
            (S_IWOTH & st->st_mode) ? 'w' : '-',
            (S_IXOTH & st->st_mode) ? 'x' : '-',
            st->st_ino,
            st->st_size, time_str, pathname);
    if (S_IFDIR & st->st_mode) {
        printf("/");
    }
    printf("\n");
    return 0;
}

static std::string FormatPath(const std::string pathname) {
    std::string result;
    bool need_strip = false;
    for (std::string::size_type i = 0; i < pathname.length(); ++i) {
        if (pathname.at(i) == '/') {
            if (need_strip) {
                continue;
            }
            else {
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

int32_t DfsPrintPath(const char* pathname, struct stat* st, void* arg = NULL) {
    printf("%s", FormatPath(pathname).c_str());
    if (S_IFDIR & st->st_mode) {
        printf("/");
    }
    printf("\n");
    return 0;
}

int32_t DfsSizeSum(const char* pathname, struct stat* st, void* arg) {
    uint64_t* sum = reinterpret_cast<uint64_t*>(arg);
    if (!(S_IFDIR & st->st_mode)) {
        *sum += st->st_size;
    }
    return 0;
}

int32_t DfsTryLockParentPath(const std::string path) {
    std::string parent_path = path;
    if (parent_path.at(parent_path.length() - 1) == '/') {
        parent_path.pop_back();
    }
    std::string::size_type pos = parent_path.rfind("/");
    if (pos == std::string::npos) {
        fprintf(stderr, "invalid path: %s\n", path.c_str());
        return -1;
    }
    if (pos == 0) {
        parent_path = "/";
    }
    parent_path = parent_path.substr(0, pos);
    return g_dfs->LockDirectory(parent_path);
}

int32_t DfsRmPath(const char* pathname, struct stat* st, void*) {
    int ret = 0;
    if (S_IFDIR & st->st_mode) {
        ret = g_dfs->DeleteDirectory(pathname);
        if (0 != ret) {
            perror("RmDir fail");
            return ret;
        }
    } else {
        ret = g_dfs->Delete(pathname);
        if (0 != ret) {
            perror("unlink fail");
        }
    }
    return ret;
}

typedef int(*WalkFunc)(const char*, struct stat*, void* arg);
int32_t DfsDirWalk(const char* dir_name, WalkFunc func, bool is_recursive, void* arg = NULL) {
    struct stat st;
    memset(&st, 0, sizeof(struct stat));
    char fullpath[4096] = {0};
    // not a directory, end of recursive call
    if (0 == g_dfs->Stat(dir_name, &st) && !(S_IFDIR & st.st_mode)) {
        return 0;
    }
    std::vector<std::string> sub_paths;
    if (0 != g_dfs->ListDirectory(dir_name, &sub_paths)) {
        return -1;
    }
    if (func == DfsRmPath && FLAGS_asowner) {
        if (0 != g_dfs->LockDirectory(dir_name)) {
            fprintf(stderr, "Lock Directory %s failed", dir_name);
            return -1;
        }
    }
    for (std::size_t i = 0; i < sub_paths.size(); ++i) {
        snprintf(fullpath, sizeof(fullpath), "%s/%s", dir_name, sub_paths[i].c_str());
        memset(&st, 0, sizeof(struct stat));
        if (g_dfs->Stat(fullpath, &st) < 0) {
            perror("Stat failed");
            continue;
        }
        if (is_recursive && (S_IFDIR & st.st_mode)) {
            DfsDirWalk(fullpath, func, true, arg);
        }
        func(fullpath, &st, arg);
    }
    return 0;
}


int32_t DfsGetOp(int32_t argc, std::string* argv, ErrorCode* err) {
    if (argc != 5) {
        fprintf(stderr, "Invalid arguments");
        return -1;
    }
    int ret = 0;
    const std::string& src_path = argv[3];
    const std::string& local_path = argv[4];
    std::string local_file_path = local_path;
    int local_fd = 0;
    if (local_path != "-") {
        struct stat st;
        if (stat(local_path.c_str(), &st) == 0 && (S_IFDIR & st.st_mode)) {
            char* tmp_src_path = strdup(src_path.c_str());
            char* filename = basename(tmp_src_path);
            local_file_path.append("/").append(filename);
            free(tmp_src_path);
        }
        local_fd = open(local_file_path.c_str(), O_CREAT | O_WRONLY | O_TRUNC, 0644);
        if (local_fd < 0) {
            fprintf(stderr, "local file open fail, path=%s, errno=%d", local_file_path.c_str(), errno);
            return errno;
        }
    }
    leveldb::DfsFile* file = g_dfs->OpenFile(src_path, leveldb::RDONLY);
    if (NULL == file) {
        fprintf(stderr, "open dfs file fail, path=%s, errno=%d", src_path.c_str(), errno);
        return errno;
    }
    char buf[128 * 1024];
    ssize_t ret_size = 0;
    while ((ret_size = file->Read(buf, sizeof(buf))) > 0) {
        ssize_t writelen = write(local_fd, buf, ret_size);
        if (writelen < 0) {
            fprintf(stderr, "write local file fail, path=%s, errno=%d", local_file_path.c_str(), errno);
            ret = errno;
            break;
        }
    }
    if (local_fd > 0) {
        close(local_fd);
    }
    file->CloseFile();

    return ret;
}

int32_t DfsPutOp(int32_t argc, std::string* argv, ErrorCode* err) {
    fprintf(stderr, "not implemented");
    return -1;
}


int32_t DfsLsOp(int32_t argc, std::string* argv, ErrorCode* err) {
    const std::string& filename = argv[3];
    struct stat fstat;
    int ret = 0;
    if (0 == g_dfs->Stat(filename.c_str(), &fstat)) {
        if (S_IFDIR & fstat.st_mode) {
            if (FLAGS_attribute) {
                DfsPrintAttr(filename.c_str(), &fstat);
                ret = DfsDirWalk(filename.c_str(), DfsPrintAttr, FLAGS_recursive);
            } else {
                DfsPrintPath(filename.c_str(), &fstat);
                ret = DfsDirWalk(filename.c_str(), DfsPrintPath, FLAGS_recursive);
            }
        }
        else {
            if (FLAGS_attribute) {
                DfsPrintAttr(filename.c_str(), &fstat);
            }
            else {
                DfsPrintPath(filename.c_str(), &fstat);
            }
        }
    }
    return ret;
}
int32_t DfsLsrOp(int32_t argc, std::string* argv, ErrorCode* err) {

    bool old_recursive_flag = FLAGS_recursive;
    FLAGS_recursive = true;
    DfsLsOp(argc, argv, err);
    FLAGS_recursive = old_recursive_flag;
    return errno;
}

int32_t DfsDusOp(int32_t argc, std::string* argv, ErrorCode* err) {
    struct stat st;
    const std::string& path = argv[3];
    uint64_t size = 0;
    if (g_dfs->Stat(path, &st) != 0) {
        perror("Stat failed");
        return errno;
    }
    if (S_IFDIR & st.st_mode) {
        DfsDirWalk(path.c_str(), DfsSizeSum, true, &size);
    } else {
        DfsSizeSum(path.c_str(), &st, &size);
    }
    fprintf(stdout, "%s:\t%lu\n", path.c_str(), size);
    return 0;
}

int32_t DfsTouchzOp(int32_t argc, std::string* argv, ErrorCode* err) {
    const std::string& path = argv[3];
    struct stat st;
    std::string::size_type pos = path.rfind("/");
    if (pos == std::string::npos || pos == path.length() - 1) {
        fprintf(stderr, "invalid filepath: %s", path.c_str());
        return -1;
    }

    int ret = g_dfs->Stat(path, &st);
    if (0 != ret) {
        if (errno != ENOENT) {
            perror("Stat failed");
            return errno;
        }
        std::string parent_path = path.substr(0, pos);
        ret = g_dfs->CreateDirectory(parent_path);
        if (0 != ret) {
            perror("create parent path failed");
            return errno;
        }
        if (FLAGS_asowner) {
            DfsTryLockParentPath(path);
        }
        leveldb::DfsFile* file = g_dfs->OpenFile(path, leveldb::WRONLY);
        if (NULL == file) {
            perror("create or open file fail");
            return errno;
        }
    } else {
        if (S_IFDIR & st.st_mode) {
            fprintf(stderr, "Touchz fail: %s not Regular file", path.c_str());
            ret = EISDIR;
        } else {
            fprintf(stdout, "%s already exists", path.c_str());
            ret = EEXIST;
        }
    }
    return ret;
}

int32_t DfsMkdirOp(int32_t argc, std::string* argv, ErrorCode* err) {
    const std::string& path = argv[3];
    if (FLAGS_asowner) {
        if (0 != DfsTryLockParentPath(path)) {
            fprintf(stderr, "Try lock parent path failed");
            return -1;
        }
    }
    int ret = g_dfs->CreateDirectory(path);
    if (0 != ret) {
        fprintf(stderr, "Create Path: %s failed, errno=%d\n", path.c_str(), errno);
        ret = errno;
    }
    return ret;
}

int32_t DfsRmOp(int32_t argc, std::string* argv, ErrorCode* err) {
    const std::string& path = argv[3];
    struct stat st;
    if (0 != g_dfs->Stat(path.c_str(), &st)) {
        perror("Stat fail: ");
        return -1;
    }
    int ret = 0;
    if (FLAGS_asowner) {
        DfsTryLockParentPath(path);
    }
    if (st.st_mode & S_IFDIR) {
        if (FLAGS_recursive) {
            DfsDirWalk(path.c_str(), DfsRmPath, true, NULL);
            ret = g_dfs->DeleteDirectory(path);
        } else {
            ret = g_dfs->DeleteDirectory(path);
        }
    } else {
        ret = g_dfs->Delete(path);
    }
    if (0 != ret) {
        perror("delete failed: ");
    }

    return errno;
}

int32_t DfsTestOp(int32_t argc, std::string* argv, ErrorCode* err) {
    fprintf(stderr, "not implemented\n");
    return -1;
}

int32_t DfsStatOp(int32_t argc, std::string* argv, ErrorCode* err) {
    struct stat st;
    const std::string& filename = argv[3];
    if (0 != g_dfs->Stat(filename, &st)) {
        return errno;
    }
    const char* file_type;
    if (S_IFREG & st.st_mode) {
        file_type = "Regular";
    } else if (S_IFDIR & st.st_mode) {
        file_type = "Directory";
    } else {
        file_type = "Symlink";
    }
    fprintf(stdout, "File:\t%s\n", filename.c_str());
    fprintf(stdout, "Inode:\t0x%lx\n", st.st_ino);
    fprintf(stdout, "Type:\t%s\n", file_type);
    fprintf(stdout, "Size:\t%lu\n", st.st_size);
    fprintf(stdout, "Mode:\t%o\n", st.st_mode & 0777);
    fprintf(stdout, "Link:\t%lu\n", st.st_nlink);
    fprintf(stdout, "Atime:\t%lu\t%s", st.st_atime, ctime(&st.st_atime));
    fprintf(stdout, "Mtime:\t%lu\t%s", st.st_mtime, ctime(&st.st_mtime));
    fprintf(stdout, "Ctime:\t%lu\t%s", st.st_ctime, ctime(&st.st_ctime));

    return 0;
}

int32_t DfsRenameOp(int32_t argc, std::string* argv, ErrorCode* err) {
    if (argc != 5) {
        fprintf(stderr, "invalid arguments\n");
        return -1;
    }
    std::string& src_path = argv[3];
    std::string& dest_path = argv[4];
    if (FLAGS_asowner) {
        if (0 != DfsTryLockParentPath(dest_path)) {
            fprintf(stderr, "Lock ParentPath failed");
            return -1;
        }
    }

    int ret = g_dfs->Rename(src_path, dest_path);
    if (0 != ret) {
        perror("Rename fail");
        ret = errno;
    }
    return ret;
}

int32_t DfsUnlockDirOp(int32_t argc, std::string* argv, ErrorCode* err) {
    const std::string& path = argv[3];
    return g_dfs->ClearDirOwner(path);
}

int32_t DfsChecksumOp(int32_t argc, std::string* argv, ErrorCode* err) {
    fprintf(stderr, "Not Implemented");
    return -1;
}

int32_t DfsLChecksumOp(int32_t argc, std::string* argv, ErrorCode* err) {
    fprintf(stderr, "Not Implemented");
    return -1;
}

int32_t DfsForceReleaseOp(int32_t argc, std::string* argv, ErrorCode* err) {
    fprintf(stderr, "Not Implemented");
    return -1;
}

static void InitializeFileSystemCommandTable() {
    FSCommandTable& fs_command_table = GetFSCommandTable();
    fs_command_table["get"] = DfsGetOp;
    fs_command_table["put"] = DfsPutOp;
    fs_command_table["lsr"] = DfsLsrOp;
    fs_command_table["ls"] = DfsLsOp;
    fs_command_table["dus"] = DfsDusOp;
    fs_command_table["touchz"] = DfsTouchzOp;
    fs_command_table["mkdir"] = DfsMkdirOp;
    fs_command_table["rm"] = DfsRmOp;
    fs_command_table["test"] = DfsTestOp;
    fs_command_table["stat"] = DfsStatOp;
    fs_command_table["rename"] = DfsRenameOp;
    fs_command_table["unlockdir"] = DfsUnlockDirOp;
    fs_command_table["checksum"] = DfsChecksumOp;
    fs_command_table["lchecksum"] = DfsLChecksumOp;
    fs_command_table["forcerelease"] = DfsForceReleaseOp;
    return;
}

static void InitializeCommandTable(){
    CommandTable& command_table = GetCommandTable();
    command_table["create"] = CreateOp;
    command_table["createbyfile"] = CreateByFileOp;
    command_table["update"] = UpdateOp;
    command_table["update-check"] = UpdateCheckOp;
    command_table["drop"] = DropOp;
    command_table["enable"] = EnableOp;
    command_table["disable"] = DisableOp;
    command_table["show"] = ShowOp;
    command_table["showx"] = ShowOp;
    command_table["showall"] = ShowOp;
    command_table["showschema"] = ShowSchemaOp;
    command_table["showschemax"] = ShowSchemaOp;
    command_table["showts"] = ShowTabletNodesOp;
    command_table["showtsx"] = ShowTabletNodesOp;
    command_table["put"] = PutOp;
    command_table["putint64"] = PutInt64Op;
    command_table["put-ttl"] = PutTTLOp;
    command_table["put_counter"] = PutCounterOp;
    command_table["add"] = AddOp;
    command_table["addint64"] = AddInt64Op;
    command_table["putif"] = PutIfAbsentOp;
    command_table["append"] = AppendOp;
    command_table["get"] = GetOp;
    command_table["getint64"] = GetInt64Op;
    command_table["get_counter"] = GetCounterOp;
    command_table["delete"] = DeleteOp;
    command_table["delete1v"] = DeleteOp;
    command_table["batchput"] = BatchPutOp;
    command_table["batchputint64"] = BatchPutInt64Op;
    command_table["batchget"] = BatchGetOp;
    command_table["batchgetint64"] = BatchGetInt64Op;
    command_table["scan"] = ScanOp;
    command_table["scanallv"] = ScanOp;
    command_table["safemode"] = SafeModeOp;
    command_table["tablet"] = TabletOp;
    command_table["meta"] = MetaOp;
    command_table["compact"] = CompactOp;
    command_table["compactx"] = CompactOp;
    command_table["findmaster"] = FindMasterOp;
    command_table["findts"] = FindTsOp;
    command_table["findtablet"] = FindTabletOp;
    command_table["stat"] = StatOp;
    command_table["meta2"] = Meta2Op;
    command_table["user"] = UserOp;
    command_table["reload"] = ReloadConfigOp;
    command_table["kick"] = KickTabletServerOp;
    command_table["cookie"] = CookieOp;
    command_table["range"] = RangeOp;
    command_table["rangex"] = RangeOp;
    command_table["txn"] = TxnOp;
    command_table["gtxn"] = GTxnOp;
    command_table["help"] = HelpOp;
    command_table["cas"] = CasOp;
    command_table["dfs"] = FileSystemOp;
    InitializeFileSystemCommandTable();
}

int ExecuteCommand(Client* client, int argc, char** arg_list) {
    int ret = 0;
    ErrorCode error_code;

    std::vector<std::string> parsed_arg_list;
    if (!ParseCommand(argc, arg_list, &parsed_arg_list)) {
        return 1;
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
        ret = 1;
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

    Client* client = Client::NewClient(FLAGS_flagfile, NULL);
    if (client == NULL) {
        LOG(ERROR) << "client instance not exist";
        return -1;
    }
    g_printer_opt.print_head = FLAGS_stdout_is_tty;

    InitializeCommandTable();

    int ret  = 0;
    if (argc == 1) {
        char* line = NULL;
        while ((line = readline("tera> ")) != NULL) {
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
    return ret;
}
