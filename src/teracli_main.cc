// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//

#include <stdlib.h>
#include <unistd.h>

#include <fstream>
#include <iostream>
#include <limits>
#include <sstream>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "common/base/string_ext.h"
#include "common/base/string_number.h"
#include "common/file/file_path.h"
#include "io/coding.h"
#include "proto/kv_helper.h"
#include "proto/proto_helper.h"
#include "proto/tabletnode.pb.h"
#include "proto/tabletnode_client.h"
#include "sdk/client_impl.h"
#include "sdk/sdk_utils.h"
#include "sdk/sdk_zk.h"
#include "sdk/table_impl.h"
#include "sdk/tera.h"
#include "utils/string_util.h"
#include "utils/tprinter.h"
#include "utils/utils_cmd.h"
#include "version.h"

DECLARE_string(flagfile);
DECLARE_string(log_dir);
DECLARE_string(tera_master_meta_table_name);
DECLARE_string(tera_zk_addr_list);
DECLARE_string(tera_zk_root_path);

DEFINE_int32(tera_client_batch_put_num, 1000, "num of each batch in batch put mode");
DEFINE_int32(tera_client_scan_package_size, 1024, "the package size (in KB) of each scan request");
DEFINE_bool(tera_client_scan_async_enabled, false, "enable the streaming scan mode");

volatile int32_t g_start_time = 0;
volatile int32_t g_end_time = 0;
volatile int32_t g_used_time = 0;
volatile int32_t g_last_time  = 0;
volatile int64_t g_total_size = 0;
volatile int32_t g_key_num = 0;
Mutex g_stat_lock;

volatile int32_t g_cur_batch_num = 0;

using namespace tera;

void Usage(const std::string& prg_name) {
    std::cout << "\nSYNOPSIS\n";
    std::cout << "       " << prg_name << "  OPERATION  [OPTION...] \n\n";
    std::cout << "DESCRIPTION \n\
       create   <schema> [<delimiter_file>]                                 \n\
              - schema syntax (all properties are optional):                \n\
                    tablename <rawkey=binary,splitsize=1024,...> {          \n\
                        lg1 <storage=flash,...> {                           \n\
                            cf1 <maxversion=3>,                             \n\
                            cf2...},                                        \n\
                        lg2...                                              \n\
                    }                                                       \n\
              - kv mode schema:                                             \n\
                    tablename <splitsize=1024, storage=memory, ...>         \n\
              - simple mode schema:                                         \n\
                    tablename{cf1, cf2, cf3, ...}                           \n\
                                                                            \n\
       createbyfile   <schema_file> [<delimiter_file>]                      \n\
                                                                            \n\
       update <schema>                                                      \n\
              - kv schema:                                                  \n\
                    e.g. tablename<splitsize=512,storage=disk>              \n\
              - table schema:                                               \n\
                - update properties                                         \n\
                    e.g. tablename<splitsize=512>                           \n\
                    e.g. tablename{lg0{cf0<ttl=123>}}                       \n\
                    e.g. tablename<mergesize=233>{lg0<storage=disk>{cf0<ttl=123>}}       \n\
                - add new cf                                                \n\
                    e.g. tablename{lg0{cf0<ttl=250>,new_cf<op=add,ttl=233>}}\n\
                - delete cf                                                 \n\
                    e.g. tablename{lg0{cf0<op=del>}}                        \n\
                                                                            \n\
       enable/disable/drop  <tablename>                                     \n\
                                                                            \n\
       put      <tablename> <rowkey> [<columnfamily:qualifier>] <value>     \n\
                                                                            \n\
       put-ttl  <tablename> <rowkey> [<columnfamily:qualifier>] <value> <ttl(second)>    \n\
                                                                            \n\
       putif    <tablename> <rowkey> [<columnfamily:qualifier>] <value>     \n\
                                                                            \n\
       get      <tablename> <rowkey> [<columnfamily:qualifier>]             \n\
                                                                            \n\
       scan[allv] <tablename> <startkey> <endkey> [<\"cf1|cf2\">]           \n\
                scan table from startkey to endkey.                         \n\
                (return all qulifier version when using suffix \"allv\")    \n\
                                                                            \n\
       delete[1v] <tablename> <rowkey> [<columnfamily:qualifier>]           \n\
                delete row/columnfamily/qualifiers.                         \n\
                (only delete latest version when using suffix \"1v\")       \n\
                                                                            \n\
       put_counter <tablename> <rowkey> [<columnfamily:qualifier>] <integer>\n\
                                                                            \n\
       get_counter <tablename> <rowkey> [<columnfamily:qualifier>]          \n\
                                                                            \n\
       add      <tablename> <rowkey> <columnfamily:qualifier>   delta       \n\
                add 'delta'(int64_t) to specified cell                      \n\
       append   <tablename> <rowkey> [<columnfamily:qualifier>] <value>     \n\
                                                                            \n\
       batchput <tablename> <input file>                                    \n\
                                                                            \n\
       batchget <tablename> <input file>                                    \n\
                                                                            \n\
       show[x]  [<tablename>]                                               \n\
                show table list or tablets info.                            \n\
                (show more detail when using suffix \"x\")                  \n\
                                                                            \n\
       showschema[x] <tablename>                                            \n\
                show table schema (show more detail when using suffix \"x\")\n\
                                                                            \n\
       showts[x] [<tabletnode addr>]                                        \n\
                show all tabletnodes or single tabletnode info.             \n\
                (show more detail when using suffix \"x\")                  \n\
                                                                            \n\
       version\n\n";
}

int32_t CreateOp(Client* client, int32_t argc, char** argv, ErrorCode* err) {
    if (argc < 2) {
        Usage(argv[0]);
        return -1;
    }

    TableDescriptor table_desc;
    std::vector<std::string> delimiters;
    std::string schema = argv[2];
    if (!ParseSchema(schema, &table_desc)) {
        LOG(ERROR) << "fail to parse input table schema.";
        return -1;
    }
    if (argc == 4) {
        // have tablet delimiters
        std::string delim_file = argv[3];
        std::ifstream fin(delim_file.c_str());
        if (fin.fail()) {
            LOG(ERROR) << "fail to read delimiter file: " << delim_file;
            return -1;
        }
        std::string str;
        while (fin >> str) {
            delimiters.push_back(str);
        }
    } else if (argc > 4) {
        LOG(ERROR) << "too many args: " << argc;
        return -1;
    }
    if (!client->CreateTable(table_desc, delimiters, err)) {
        LOG(ERROR) << "fail to create table, "
            << strerr(*err);
        return -1;
    }
    ShowTableDescriptor(table_desc);
    return 0;
}

int32_t CreateByFileOp(Client* client, int32_t argc, char** argv, ErrorCode* err) {
    if (argc < 3) {
        Usage(argv[0]);
        return -1;
    }

    std::ifstream fin(argv[2]);
    std::string schema;
    std::vector<std::string> delimiters;
    if (fin.good()) {
        std::string str;
        while (fin >> str) {
            schema.append(str);
        }
    }

    TableDescriptor table_desc;
    if (!ParseSchema(schema, &table_desc)) {
        LOG(ERROR) << "fail to parse input table schema.";
        return -1;
    }

    if (argc == 4) {
        std::ifstream fin(argv[3]);
        if (fin.fail()) {
            LOG(ERROR) << "fail to read delimiter file.";
            return -1;
        }
        std::string str;
        while (fin >> str) {
            delimiters.push_back(str);
        }
    }
    if (!client->CreateTable(table_desc, delimiters, err)) {
        LOG(ERROR) << "fail to create table, "
            << strerr(*err);
        return -1;
    }
    ShowTableDescriptor(table_desc);
    return 0;
}

int32_t UpdateOp(Client* client, int32_t argc, char** argv, ErrorCode* err) {
    if (argc != 3) {
        Usage(argv[0]);
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

    // if try to update lg or cf, need to disable table
    bool is_update_lg_cf = false;
    if (!UpdateTableDescriptor(schema_tree, table_desc, &is_update_lg_cf )) {
        LOG(ERROR) << "[update] update failed";
        return -1;
    }

    if (is_update_lg_cf && client->IsTableEnabled(table_desc->TableName(), err)) {
        LOG(ERROR) << "[update] table is enabled, disable it first: " << table_desc->TableName();
        return -1;
    }

    if (!client->UpdateTable(*table_desc, err)) {
        LOG(ERROR) << "[update] fail to update table, "
            << strerr(*err);
        return -1;
    }
    ShowTableDescriptor(*table_desc);
    delete table_desc;
    return 0;
}

int32_t DropOp(Client* client, int32_t argc, char** argv, ErrorCode* err) {
    if (argc < 3) {
        Usage(argv[0]);
        return -1;
    }

    std::string tablename = argv[2];
    if (!client->DeleteTable(tablename, err)) {
        LOG(ERROR) << "fail to delete table";
        return -1;
    }
    return 0;
}

int32_t EnableOp(Client* client, int32_t argc, char** argv, ErrorCode* err) {
    if (argc < 3) {
        Usage(argv[0]);
        return -1;
    }

    std::string tablename = argv[2];
    if (!client->EnableTable(tablename, err)) {
        LOG(ERROR) << "fail to enable table";
        return -1;
    }
    return 0;
}

int32_t DisableOp(Client* client, int32_t argc, char** argv, ErrorCode* err) {
    if (argc < 3) {
        Usage(argv[0]);
        return -1;
    }

    std::string tablename = argv[2];
    if (!client->DisableTable(tablename, err)) {
        LOG(ERROR) << "fail to disable table";
        return -1;
    }
    return 0;
}

void ParseCfQualifier(const std::string& input, std::string* columnfamily, std::string* qualifier) {
    std::string::size_type pos = input.find(":", 0);
    if (pos != std::string::npos) {
        *columnfamily = input.substr(0, pos);
        *qualifier = input.substr(pos + 1);
    } else {
        *columnfamily = input;
        *qualifier = "";
    }
}

int32_t PutCounterOp(Client* client, int32_t argc, char** argv, ErrorCode* err) {
    if (argc != 5 && argc != 6) {
        LOG(ERROR) << "args number error: " << argc << ", need 5 | 6.";
        Usage(argv[0]);
        return -1;
    }

    std::string tablename = argv[2];
    Table* table = NULL;
    if ((table = client->OpenTable(tablename, err)) == NULL) {
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
    char counter_buf[sizeof(int64_t)];
    io::EncodeBigEndian(counter_buf,counter);
    std::string s_counter(counter_buf,sizeof(counter_buf));
    if (!table->Put(rowkey, columnfamily, qualifier, s_counter, err)) {
        LOG(ERROR) << "fail to put record to table: " << tablename;
        return -1;
    }
    delete table;
    return 0;
}

int32_t PutOp(Client* client, int32_t argc, char** argv, ErrorCode* err) {
    if (argc != 5 && argc != 6) {
        LOG(ERROR) << "args number error: " << argc << ", need 5 | 6.";
        Usage(argv[0]);
        return -1;
    }

    std::string tablename = argv[2];
    Table* table = NULL;
    if ((table = client->OpenTable(tablename, err)) == NULL) {
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
    if (!table->Put(rowkey, columnfamily, qualifier, value, err)) {
        LOG(ERROR) << "fail to put record to table: " << tablename;
        return -1;
    }
    delete table;
    return 0;
}

int32_t PutTTLOp(Client* client, int32_t argc, char** argv, ErrorCode* err) {
    if (argc != 6 && argc != 7) {
        LOG(ERROR) << "args number error: " << argc << ", need 5 | 6.";
        Usage(argv[0]);
        return -1;
    }

    std::string tablename = argv[2];
    Table* table = NULL;
    if ((table = client->OpenTable(tablename, err)) == NULL) {
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
        ttl = atoi(argv[5]);
    } else if (argc == 7) {
        ParseCfQualifier(argv[4], &columnfamily, &qualifier);
        value = argv[5];
        ttl = atoi(argv[6]);
    }
    if (!table->Put(rowkey, columnfamily, qualifier, value, ttl, err)) {
        LOG(ERROR) << "fail to put record to table: " << tablename;
        return -1;
    }
    return 0;
}

int32_t AppendOp(Client* client, int32_t argc, char** argv, ErrorCode* err) {
    if (argc != 5 && argc != 6) {
        LOG(ERROR) << "args number error: " << argc << ", need 5 | 6.";
        Usage(argv[0]);
        return -1;
    }

    std::string tablename = argv[2];
    Table* table = NULL;
    if ((table = client->OpenTable(tablename, err)) == NULL) {
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
    delete table;
    return 0;
}

int32_t PutIfAbsentOp(Client* client, int32_t argc, char** argv, ErrorCode* err) {
    if (argc != 5 && argc != 6) {
        LOG(ERROR) << "args number error: " << argc << ", need 5 | 6.";
        Usage(argv[0]);
        return -1;
    }

    std::string tablename = argv[2];
    Table* table = NULL;
    if ((table = client->OpenTable(tablename, err)) == NULL) {
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
    delete table;
    return 0;
}

int32_t AddOp(Client* client, int32_t argc, char** argv, ErrorCode* err) {
    if (argc != 5 && argc != 6) {
        LOG(ERROR)<< "args number error: " << argc << ", need 5 | 6.";
        Usage(argv[0]);
        return -1;
    }

    std::string tablename = argv[2];
    Table* table = NULL;
    if ((table = client->OpenTable(tablename, err)) == NULL) {
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
    delete table;
    return 0;
}

int32_t GetOp(Client* client, int32_t argc, char** argv, ErrorCode* err) {
    if (argc != 4 && argc != 5) {
        LOG(ERROR) << "args number error: " << argc << ", need 5 | 6.";
        Usage(argv[0]);
        return -1;
    }

    std::string tablename = argv[2];
    Table* table = NULL;
    if ((table = client->OpenTable(tablename, err)) == NULL) {
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

    std::cout << value;
    delete table;
    return 0;
}

int32_t GetCounterOp(Client* client, int32_t argc, char** argv, ErrorCode* err) {
    if (argc != 4 && argc != 5) {
        LOG(ERROR) << "args number error: " << argc << ", need 5 | 6.";
        Usage(argv[0]);
        return -1;
    }

    std::string tablename = argv[2];
    Table* table = NULL;
    if ((table = client->OpenTable(tablename, err)) == NULL) {
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

    std::cout << io::DecodeBigEndainSign(value.c_str());
    delete table;
    return 0;
}


int32_t DeleteOp(Client* client, int32_t argc, char** argv, ErrorCode* err) {
    if (argc != 4 && argc != 5) {
        LOG(ERROR) << "args number error: " << argc << ", need 4 | 5.";
        Usage(argv[0]);
        return -1;
    }

    std::string tablename = argv[2];
    std::string rowkey = argv[3];
    Table* table = NULL;
    if ((table = client->OpenTable(tablename, err)) == NULL) {
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
            mutation->DeleteFamily(input);
        } else {
            std::string family;
            std::string qualifier;
            ParseCfQualifier(input, &family, &qualifier);
            if (op == "delete") {
                // delete a column (all versions)
                mutation->DeleteColumns(family, qualifier);
            } else if (op == "delete1v") {
                // delete the newest version
                mutation->DeleteColumn(family, qualifier);
            }
        }
    } else {
        LOG(FATAL) << "should not run here.";
    }
    table->ApplyMutation(mutation);

    delete table;
    return 0;
}

int32_t ScanOp(Client* client, int32_t argc, char** argv, ErrorCode* err) {
    if (argc != 5 && argc != 6 && argc != 7) {
        LOG(ERROR) << "args number error: " << argc << ", need 5 | 6 | 7.";
        Usage(argv[0]);
        return -1;
    }

    std::string op = argv[1];
    std::string tablename = argv[2];
    Table* table = NULL;
    if ((table = client->OpenTable(tablename, err)) == NULL) {
        LOG(ERROR) << "fail to open table";
        return -1;
    }

    std::string start_rowkey = argv[3];
    std::string end_rowkey = argv[4];
    ResultStream* result_stream;
    ScanDescriptor desc(start_rowkey);
    desc.SetEnd(end_rowkey);
    desc.SetBufferSize(FLAGS_tera_client_scan_package_size << 10);
    desc.SetAsync(FLAGS_tera_client_scan_async_enabled);

    if (argc == 5) {
        // scan all cells
    } else if (argc == 6) {
        if (!ParseScanSchema(argv[5], &desc)) {
            LOG(ERROR) << "fail to parse scan schema: " << argv[5];
            return -1;
        }
    }
    if (op == "scanallv") {
        desc.SetMaxVersions(std::numeric_limits<int>::max());
    }

    uint64_t snapshot = 0;
    if (argc == 7) {
        std::stringstream is;
        is << std::string(argv[6]);
        is >> snapshot;
    }

    desc.SetSnapshot(snapshot);
    if ((result_stream = table->Scan(desc, err)) == NULL) {
        LOG(ERROR) << "fail to scan records from table: " << tablename;
        return -1;
    }
    g_start_time = time(NULL);
    while (!result_stream->Done(err)) {
        int32_t len = result_stream->RowName().size()
            + result_stream->ColumnName().size()
            + sizeof(result_stream->Timestamp())
            + result_stream->Value().size();
        g_total_size += len;
        g_key_num ++;
        g_cur_batch_num ++;
        std::cout << result_stream->RowName() << ":"
           << result_stream->ColumnName() << ":"
           << result_stream->Timestamp() << ":"
           << result_stream->Value() << std::endl;
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
    if (err->GetType() != ErrorCode::kOK) {
        LOG(ERROR) << "fail to finish scan: " << err->GetReason();
        return -1;
    }
    g_end_time = time(NULL);
    g_used_time = g_end_time - g_start_time;
    LOG(INFO) << "Scan done " << g_key_num << " keys " << g_key_num/(g_used_time?g_used_time:1)
            <<" keys/S " << g_total_size/1024.0/1024/(g_used_time?g_used_time:1) << " MB/S ";
    delete table;
    return 0;
}

int32_t ShowTabletList(const TabletMetaList& tablet_list, bool is_server_addr, bool is_x) {
    TPrinter printer;
    int cols;
    std::vector<string> row;
    if (is_x) {
        if (is_server_addr) {
            cols = 14;
            printer.Reset(cols);
            printer.AddRow(cols,
                           " ", "server_addr", "path", "status", "size",
                           "isbusy", "lread", "read", "rspeed", "write",
                           "wspeed", "scan", "sspeed", "startkey");
        } else {
            cols = 13;
            printer.Reset(cols);
            printer.AddRow(cols,
                           " ", "path", "status", "size", "isbusy",
                           "lread", "read", "rspeed", "write", "wspeed",
                           "scan", "sspeed", "startkey");
        }

        for (int32_t i = 0; i < tablet_list.meta_size(); ++i) {
            const TabletMeta& meta = tablet_list.meta(i);
            row.clear();
            row.push_back(NumberToString(i));
            if (is_server_addr) {
                row.push_back(meta.server_addr());
            }
            row.push_back(meta.path());
            row.push_back(StatusCodeToString(meta.status()));
            row.push_back(utils::ConvertByteToString(meta.table_size()));

            if (tablet_list.counter_size() > 0) {
                const TabletCounter& counter = tablet_list.counter(i);
                if (counter.is_on_busy()) {
                    row.push_back("true");
                } else {
                    row.push_back("false");
                }
                row.push_back(NumberToString(counter.low_read_cell()));
                row.push_back(NumberToString(counter.read_rows()));
                row.push_back(utils::ConvertByteToString(counter.read_size()) + "B/s");
                row.push_back(NumberToString(counter.write_rows()));
                row.push_back(utils::ConvertByteToString(counter.write_size()) + "B/s");
                row.push_back(NumberToString(counter.scan_rows()));
                row.push_back(utils::ConvertByteToString(counter.scan_size()) + "B/s");
            }
            row.push_back(meta.key_range().key_start().substr(0, 20));
            printer.AddRow(row);
        }
    } else {
        cols = 7;
        printer.Reset(cols);
        printer.AddRow(cols,
                       " ", "server_addr", "path", "status",
                       "size", "startkey", "endkey");
        for (int32_t i = 0; i < tablet_list.meta_size(); ++i) {
            const TabletMeta& meta = tablet_list.meta(i);
            row.clear();
            row.push_back(NumberToString(i));
            row.push_back(meta.server_addr());
            row.push_back(meta.path());
            row.push_back(StatusCodeToString(meta.status()));
            row.push_back(utils::ConvertByteToString(meta.table_size()));
            row.push_back(DebugString(meta.key_range().key_start()).substr(0, 20));
            row.push_back(DebugString(meta.key_range().key_end()).substr(0, 20));
            printer.AddRow(row);
        }
    }
    printer.Print();
    return 0;
}

int32_t ShowAllTables(Client* client, bool is_x, bool show_all, ErrorCode* err) {
    TableMetaList table_list;
    TabletMetaList tablet_list;
    tera::ClientImpl* client_impl = static_cast<tera::ClientImpl*>(client);
    if (!client_impl->ShowTablesInfo(&table_list, &tablet_list, err)) {
        LOG(ERROR) << "fail to get meta data from tera.";
        return -1;
    }

    TPrinter printer;
    int cols;
    if (is_x) {
        cols = 17;
        printer.Reset(cols);
        printer.AddRow(cols,
                       " ", "tablename", "status", "size", "tablet",
                       "busy", "notready", "lread", "read", "rmax",
                       "rspeed", "write", "wmax", "wspeed", "scan",
                       "smax", "sspeed");
    } else {
        cols = 6;
        printer.Reset(cols);
        printer.AddRow(cols, " ", "tablename", "status", "size", "tablet", "busy");
    }
    for (int32_t table_no = 0; table_no < table_list.meta_size(); ++table_no) {
        std::string tablename = table_list.meta(table_no).table_name();
        TableStatus status = table_list.meta(table_no).status();
        int64_t size = 0;
        uint32_t tablet = 0;
        uint32_t busy = 0;
        uint32_t notready = 0;
        uint32_t lread = 0;
        uint32_t read = 0;
        uint32_t rmax = 0;
        uint64_t rspeed = 0;
        uint32_t write = 0;
        uint32_t wmax = 0;
        uint64_t wspeed = 0;
        uint32_t scan = 0;
        uint32_t smax = 0;
        uint64_t sspeed = 0;
        for (int32_t i = 0; i < tablet_list.meta_size(); ++i) {
            if (tablet_list.meta(i).table_name() == tablename) {
                size += tablet_list.meta(i).table_size();
                tablet++;
                if (tablet_list.counter(i).is_on_busy()) {
                    busy++;
                }
                if (tablet_list.meta(i).status() != kTableReady) {
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
            }
        }
        if (is_x) {
            printer.AddRow(cols,
                           NumberToString(table_no).data(),
                           tablename.data(),
                           StatusCodeToString(status).data(),
                           utils::ConvertByteToString(size).data(),
                           NumberToString(tablet).data(),
                           NumberToString(busy).data(),
                           NumberToString(notready).data(),
                           utils::ConvertByteToString(lread).data(),
                           utils::ConvertByteToString(read).data(),
                           utils::ConvertByteToString(rmax).data(),
                           (utils::ConvertByteToString(rspeed) + "B/s").data(),
                           utils::ConvertByteToString(write).data(),
                           utils::ConvertByteToString(wmax).data(),
                           (utils::ConvertByteToString(wspeed) + "B/s").data(),
                           utils::ConvertByteToString(scan).data(),
                           utils::ConvertByteToString(smax).data(),
                           (utils::ConvertByteToString(sspeed) + "B/s").data());
        } else {
            printer.AddRow(cols,
                           NumberToString(table_no).data(),
                           tablename.data(),
                           StatusCodeToString(status).data(),
                           utils::ConvertByteToString(size).data(),
                           NumberToString(tablet).data(),
                           NumberToString(busy).data());
        }
    }
    printer.Print();
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

    tera::ClientImpl* client_impl = static_cast<tera::ClientImpl*>(client);
    if (!client_impl->ShowTablesInfo(table_name, &table_meta, &tablet_list, err)) {
        LOG(ERROR) << "table not exist: " << table_name;
        return -1;
    }

    std::cout << std::endl;
    ShowTabletList(tablet_list, true, is_x);
    std::cout << std::endl;
    return 0;
}

int32_t ShowSingleTabletNodeInfo(Client* client, const string& addr,
                                 bool is_x, ErrorCode* err) {
    TabletNodeInfo info;
    TabletMetaList tablet_list;
    tera::ClientImpl* client_impl = static_cast<tera::ClientImpl*>(client);
    if (!client_impl->ShowTabletNodesInfo(addr, &info, &tablet_list, err)) {
        LOG(ERROR) << "fail to show tabletnode: " << addr;
        return -1;
    }

    std::cout << "\nTabletNode Info:\n";
    std::cout << "  address:  " << info.addr() << std::endl;
    std::cout << "  status:   " << info.status_m() << "\n\n";
    int cols = 5;
    TPrinter printer(cols);
    printer.AddRow(cols, "workload", "tablets", "load", "busy", "split");
    std::vector<string> row;
    row.push_back(utils::ConvertByteToString(info.load()));
    row.push_back(NumberToString(info.tablet_total()));
    row.push_back(NumberToString(info.tablet_onload()));
    row.push_back(NumberToString(info.tablet_onbusy()));
    row.push_back(NumberToString(info.tablet_onsplit()));
    printer.AddRow(row);
    printer.Print();

    std::cout << std::endl;
    cols = 7;
    printer.Reset(cols);
    printer.AddRow(cols, "lread", "read", "rspeed", "write", "wspeed", "scan", "sspeed");
    row.clear();
    row.push_back(NumberToString(info.low_read_cell()));
    row.push_back(NumberToString(info.read_rows()));
    row.push_back(utils::ConvertByteToString(info.read_size()) + "B/s");
    row.push_back(NumberToString(info.write_rows()));
    row.push_back(utils::ConvertByteToString(info.write_size()) + "B/s");
    row.push_back(NumberToString(info.scan_rows()));
    row.push_back(utils::ConvertByteToString(info.scan_size()) + "B/s");
    printer.AddRow(row);
    printer.Print();

    std::cout << "\nHardware Info:\n";
    cols = 7;
    printer.Reset(cols);
    row.clear();
    printer.AddRow(cols, "mem_used", "net_tx", "net_rx", "dfs_r", "dfs_w", "local_r", "local_w");
    row.push_back(utils::ConvertByteToString(info.mem_used()));
    row.push_back(utils::ConvertByteToString(info.net_tx()) + "B/s");
    row.push_back(utils::ConvertByteToString(info.net_rx()) + "B/s");
    row.push_back(utils::ConvertByteToString(info.dfs_io_r()) + "B/s");
    row.push_back(utils::ConvertByteToString(info.dfs_io_w()) + "B/s");
    row.push_back(utils::ConvertByteToString(info.local_io_r()) + "B/s");
    row.push_back(utils::ConvertByteToString(info.local_io_w()) + "B/s");
    printer.AddRow(row);
    printer.Print();

    std::cout << "\nOther Infos:\n";
    cols = info.extra_info_size();
    printer.Reset(cols);
    row.clear();
    for (int i = 0; i < cols; ++i) {
        row.push_back(info.extra_info(i).name());
    }
    printer.AddRow(row);
    std::vector<int64_t> row_int;
    for (int i = 0; i < cols; ++i) {
        row_int.push_back(info.extra_info(i).value());
    }
    printer.AddRow(row_int);
    printer.Print();

    std::cout << "\nTablets In this TabletNode:\n";
    ShowTabletList(tablet_list, false, is_x);
    return 0;
}

int32_t ShowTabletNodesInfo(Client* client, bool is_x, ErrorCode* err) {
    std::vector<TabletNodeInfo> infos;
    tera::ClientImpl* client_impl = static_cast<tera::ClientImpl*>(client);
    if (!client_impl->ShowTabletNodesInfo(&infos, err)) {
        LOG(ERROR) << "fail to get meta data from tera.";
        return -1;
    }

    int cols;
    TPrinter printer;
    if (is_x) {
        cols = 20;
        printer.Reset(cols);
        printer.AddRow(cols,
                       " ", "address", "status", "workload", "lread",
                       "read", "rspeed", "write", "wspeed", "scan",
                       "sspeed", "tablet", "load", "busy", "split",
                       "mem", "net_tx", "net_rx", "dfs_r", "dfs_w");
        std::vector<string> row;
        for (size_t i = 0; i < infos.size(); ++i) {
            row.clear();
            row.push_back(NumberToString(i));
            row.push_back(infos[i].addr());
            row.push_back(infos[i].status_m());
            row.push_back(utils::ConvertByteToString(infos[i].load()));
            row.push_back(NumberToString(infos[i].low_read_cell()));
            row.push_back(NumberToString(infos[i].read_rows()));
            row.push_back(utils::ConvertByteToString(infos[i].read_size()) + "B/s");
            row.push_back(NumberToString(infos[i].write_rows()));
            row.push_back(utils::ConvertByteToString(infos[i].write_size()) + "B/s");
            row.push_back(NumberToString(infos[i].scan_rows()));
            row.push_back(utils::ConvertByteToString(infos[i].scan_size()) + "B/s");
            row.push_back(NumberToString(infos[i].tablet_total()));
            row.push_back(NumberToString(infos[i].tablet_onload()));
            row.push_back(NumberToString(infos[i].tablet_onbusy()));
            row.push_back(NumberToString(infos[i].tablet_onsplit()));
            row.push_back(utils::ConvertByteToString(infos[i].mem_used()));
            row.push_back(utils::ConvertByteToString(infos[i].net_tx()));
            row.push_back(utils::ConvertByteToString(infos[i].net_rx()));
            row.push_back(utils::ConvertByteToString(infos[i].dfs_io_r()));
            row.push_back(utils::ConvertByteToString(infos[i].dfs_io_w()));
            printer.AddRow(row);
        }
    } else {
        cols = 7;
        printer.Reset(cols);
        printer.AddRow(cols,
                       " ", "address", "status", "workload",
                       "tablet", "load", "busy");
        std::vector<string> row;
        for (size_t i = 0; i < infos.size(); ++i) {
            row.clear();
            row.push_back(NumberToString(i));
            row.push_back(infos[i].addr());
            row.push_back(infos[i].status_m());
            row.push_back(utils::ConvertByteToString(infos[i].load()));
            row.push_back(NumberToString(infos[i].tablet_total()));
            row.push_back(NumberToString(infos[i].tablet_onload()));
            row.push_back(NumberToString(infos[i].tablet_onbusy()));
            printer.AddRow(row);
        }
    }
    printer.Print();
    std::cout << std::endl;
    return 0;
}

int32_t ShowTabletNodesOp(Client* client, int32_t argc, char** argv, ErrorCode* err) {
    if (argc < 2) {
        LOG(ERROR) << "args number error: " << argc << ", need >2.";
        Usage(argv[0]);
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

int32_t ShowOp(Client* client, int32_t argc, char** argv, ErrorCode* err) {
    if (argc < 2) {
        LOG(ERROR) << "args number error: " << argc << ", need >2.";
        Usage(argv[0]);
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

int32_t ShowSchemaOp(Client* client, int32_t argc, char** argv, ErrorCode* err) {
    if (argc < 3) {
        LOG(ERROR) << "args number error: " << argc << ", need >2.";
        Usage(argv[0]);
        return -1;
    }

    std::string cmd = argv[1];
    std::string table_name = argv[2];
    TableMeta table_meta;
    TabletMetaList tablet_list;

    tera::ClientImpl* client_impl = static_cast<tera::ClientImpl*>(client);
    if (!client_impl->ShowTablesInfo(table_name, &table_meta, &tablet_list, err)) {
        LOG(ERROR) << "table not exist: " << table_name;
        return -1;
    }

    ShowTableSchema(table_meta.schema(), cmd == "showschemax");
    return 0;
}

void BatchPutCallBack(RowMutation* mutation) {
    const ErrorCode& error_code = mutation->GetError();
    if (error_code.GetType() != ErrorCode::kOK) {
        LOG(ERROR) << "exception occured, reason:" << error_code.GetReason();
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

int32_t BatchPutOp(Client* client, int32_t argc, char** argv, ErrorCode* err) {
    if (argc != 4) {
        LOG(ERROR) << "args number error: " << argc << ", need 4.";
        Usage(argv[0]);
        return -1;
    }

    std::string tablename = argv[2];
    std::string record_file = argv[3];
    Table* table = NULL;
    if ((table = client->OpenTable(tablename, err)) == NULL) {
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
        std::string& rowkey = input_v[0];
        std::string family;
        std::string qualifier;
        std::string& value = input_v[input_v.size() - 1];
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
    delete table;
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

int32_t BatchGetOp(Client* client, int32_t argc, char** argv, ErrorCode* err) {
    if (argc != 4 && argc != 5) {
        LOG(ERROR) << "args number error: " << argc << ", need 4 | 5.";
        Usage(argv[0]);
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
    Table* table = NULL;
    if ((table = client->OpenTable(tablename, err)) == NULL) {
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
        std::string& rowkey = input_v[0];
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
                /*
                std::cout << result_stream->RowName() << ":"
                    << result_stream->ColumnName() << ":"
                    << result_stream->Timestamp() << ":"
                    << result_stream->Value() << std::endl;
                */
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
            reader->SetCallBack(BatchGetCallBack);
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
    delete table;
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

int32_t SnapshotOp(Client* client, int32_t argc, char** argv, ErrorCode* err) {
    if (argc < 4) {
      Usage(argv[0]);
      return -1;
    }

    std::string tablename = argv[2];
    uint64_t snapshot = 0;
    if (argc == 5 && strcmp(argv[3], "del") == 0) {
        std::stringstream is;
        is << std::string(argv[4]);
        is >> snapshot;
        if (!client->DelSnapshot(tablename, snapshot, err)) {
            LOG(ERROR) << "fail to del snapshot: " << snapshot << " ," << err->GetReason();
            return -1;
        }
        std::cout << "Del snapshot " << snapshot << std::endl;
    } else if (strcmp(argv[3], "create") == 0 ) {
        if (!client->GetSnapshot(tablename, &snapshot, err)) {
            LOG(ERROR) << "fail to get snapshot: " << err->GetReason();
            return -1;
        }
        std::cout << "new snapshot: " << snapshot << std::endl;
    } else {
        Usage(argv[0]);
        return -1;
    }
    return 0;
}

int32_t SafeModeOp(Client* client, int32_t argc, char** argv, ErrorCode* err) {
    if (argc < 3) {
        Usage(argv[0]);
        return -1;
    }

    std::string op = argv[2];
    if (op != "get" && op != "leave" && op != "enter") {
        Usage(argv[0]);
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

int32_t TabletOp(Client* client, int32_t argc, char** argv, ErrorCode* err) {
    if (argc != 4 && argc != 5) {
        Usage(argv[0]);
        return -1;
    }

    std::string op = argv[2];
    if (op != "move" && op != "split") {
        Usage(argv[0]);
        return -1;
    }

    std::string tablet_id = argv[3];
    std::string server_addr;
    if (argc == 5) {
        server_addr = argv[4];
    }

    std::vector<std::string> arg_list;
    arg_list.push_back(op);
    arg_list.push_back(tablet_id);
    arg_list.push_back(server_addr);
    if (!client->CmdCtrl("tablet", arg_list, NULL, NULL, err)) {
        LOG(ERROR) << "fail to " << op << " tablet " << tablet_id;
        return -1;
    }
    std::cout << op << " tablet " << tablet_id << " success" << std::endl;

    return 0;
}

int32_t MetaOp(Client* client, int32_t argc, char** argv, ErrorCode* err) {
    if (argc != 4 && argc != 5) {
        Usage(argv[0]);
        return -1;
    }

    std::string op = argv[2];
    if (op != "backup") {
        Usage(argv[0]);
        return -1;
    }

    std::string filename = argv[3];

    std::vector<std::string> arg_list;
    arg_list.push_back(op);
    arg_list.push_back(filename);
    if (!client->CmdCtrl("meta", arg_list, NULL, NULL, err)) {
        LOG(ERROR) << "fail to " << op << " meta";
        return -1;
    }
    std::cout << op << " tablet success" << std::endl;

    return 0;
}

int32_t CompactOp(int32_t argc, char** argv) {
    if (argc != 6) {
        Usage(argv[0]);
        return -1;
    }

    CompactTabletRequest request;
    CompactTabletResponse response;
    request.set_sequence_id(0);
    request.set_tablet_name(argv[2]);
    request.mutable_key_range()->set_key_start(argv[3]);
    request.mutable_key_range()->set_key_end(argv[4]);
    tabletnode::TabletNodeClient client(argv[5]); // do not retry

    if (!client.CompactTablet(&request, &response)) {
        std::cerr << "rpc fail to connect [" << client.GetConnectAddr()
            << "] to compact table" << std::endl;
        return -1;
    }

    if (response.status() != kTabletNodeOk) {
        std::cerr << "fail to compact table, status: "
            << StatusCodeToString(response.status()) << std::endl;
        return -1;
    }

    if (response.compact_status() != kTableCompacted) {
        std::cerr << "fail to compact table, status: "
            << StatusCodeToString(response.compact_status()) << std::endl;
        return -1;
    }

    std::cout << "compact tablet success, data size: "
        << response.compact_size() << std::endl;
    return 0;
}

int32_t FindTsOp(Client* client, int32_t argc, char** argv, ErrorCode* err) {
    if (argc != 4) {
        Usage(argv[0]);
        return -1;
    }

    std::string tablename = argv[2];
    TableImpl* table = NULL;
    if ((table = (TableImpl*)client->OpenTable(tablename, err)) == NULL) {
        LOG(ERROR) << "fail to open table";
        return -1;
    }

    std::string rowkey = argv[3];
    table->ScanMetaTable(rowkey, rowkey + '\0');

    TabletMeta meta;
    if (!table->GetTabletMetaForKey(rowkey, &meta)) {
        LOG(ERROR) << "fail to get tablet meta for " << rowkey;
        return -1;
    }
    std::cout << meta.server_addr() << "/" << meta.path() << std::endl;
    delete table;

    return 0;
}

int32_t Meta2Op(Client *client, int32_t argc, char** argv) {
    if (argc < 3) {
        Usage(argv[0]);
        return -1;
    }

    std::string op = argv[2];
    if (op != "check" && op != "show" && op != "bak") {
        Usage(argv[0]);
        return -1;
    }

    // get meta address
    tera::sdk::ClusterFinder finder(FLAGS_tera_zk_root_path, FLAGS_tera_zk_addr_list);
    std::string meta_tablet_addr = finder.RootTableAddr();
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
    tera::tabletnode::TabletNodeClient meta_node_client(meta_tablet_addr);
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
            if (first_key_char == '@') {
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

    // process meta
    int32_t table_num = table_list.meta_size();
    int32_t tablet_num = tablet_list.meta_size();
    if (table_num == 0 && tablet_num == 0) {
        std::cout << "meta table is empty" << std::endl;
        return 0;
    }

    std::ofstream bak;
    if (op == "bak") {
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
        if (op == "bak") {
            std::string buffer;
            if (!meta.SerializeToString(&buffer)) {
                std::cerr << "bak table fail: " << meta.table_name();
                return -1;
            }
            int32_t data_size = buffer.size();
            bak.write((char*)&data_size, sizeof(data_size));
            bak.write(buffer.data(), data_size);
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
                << meta.table_size() << ", "
                << StatusCodeToString(meta.status()) << ", "
                << StatusCodeToString(meta.compact_status()) << std::endl;
        }
        if (op == "bak") {
            std::string buffer;
            if (!meta.SerializeToString(&buffer)) {
                std::cerr << "bak tablet fail: " << meta.table_name() << " ["
                    << meta.key_range().key_start() << ","
                    << meta.key_range().key_end() << "]";
                return -1;
            }
            int32_t data_size = buffer.size();
            bak.write((char*)&data_size, sizeof(data_size));
            bak.write(buffer.data(), data_size);
        }
        // check self range
        if (!meta.key_range().key_end().empty() &&
            meta.key_range().key_start() >= meta.key_range().key_end()) {
            std::cerr << "invalid tablet " << meta.table_name() << " ["
                << meta.key_range().key_start() << ","
                << meta.key_range().key_end() << "], "
                << meta.path() << ", " << meta.server_addr() << ", "
                << meta.table_size() << ", "
                << StatusCodeToString(meta.status()) << ", "
                << StatusCodeToString(meta.compact_status()) << std::endl;
            // ignore invalid tablet
            continue;
        }
        // check miss/cover/overlap with previous tablet
        if (!table_start) {
            bool covered = false;
            if (meta.table_name() != last.table_name()) {
                std::cerr << "miss tablet: " << last.table_name() << ", ["
                    << last.key_range().key_end() << ",-]" << std::endl;
                table_start = true;
            } else if (meta.key_range().key_start() > last.key_range().key_end()) {
                std::cerr << "miss tablet " << last.table_name() << " ["
                    << last.key_range().key_end() << ","
                    << meta.key_range().key_start() << "]" << std::endl;
            } else if (meta.key_range().key_start() == last.key_range().key_end()) {
                // ok
            } else if (meta.key_range().key_end() <= last.key_range().key_end()) {
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
            }
            // ignore covered tablet
            if (!covered) {
                last.CopyFrom(meta);
            }
        } else {
            if (!meta.key_range().key_start().empty()) {
                std::cerr << "miss tablet " << meta.table_name() << " [-,"
                    << meta.key_range().key_start() << "]" << std::endl;
            }
            last.CopyFrom(meta);
        }
        table_start = meta.key_range().key_end().empty();
    }
    if (op == "bak") {
        bak.close();
    }
    return 0;
}

int main(int argc, char* argv[]) {
    if (argc < 2) {
        Usage(argv[0]);
        return -1;
    }

    int ret = 0;
    ErrorCode error_code;
    Client* client = Client::NewClient();

    if (client == NULL) {
        LOG(ERROR) << "client instance not exist";
        return -1;
    }

    std::string cmd = argv[1];
    if (cmd == "create") {
        ret = CreateOp(client, argc, argv, &error_code);
    } else if (cmd == "createbyfile") {
        ret = CreateByFileOp(client, argc, argv, &error_code);
    } else if (cmd == "update") {
        ret = UpdateOp(client, argc, argv, &error_code);
    } else if (cmd == "drop") {
        ret = DropOp(client, argc, argv, &error_code);
    } else if (cmd == "enable") {
        ret = EnableOp(client, argc, argv, &error_code);
    } else if (cmd == "disable") {
        ret = DisableOp(client, argc, argv, &error_code);
    } else if (cmd == "show" || cmd == "showx" || cmd == "showall") {
        ret = ShowOp(client, argc, argv, &error_code);
    } else if (cmd == "showschema" || cmd == "showschemax") {
        ret = ShowSchemaOp(client, argc, argv, &error_code);
    } else if (cmd == "showts" || cmd == "showtsx") {
        ret = ShowTabletNodesOp(client, argc, argv, &error_code);
    } else if (cmd == "put") {
        ret = PutOp(client, argc, argv, &error_code);
    } else if (cmd == "put-ttl") {
        ret = PutTTLOp(client, argc, argv, &error_code);
    } else if (cmd == "put_counter") {
        ret = PutCounterOp(client, argc, argv, &error_code);
    } else if (cmd == "add") {
        ret = AddOp(client, argc, argv, &error_code);
    } else if (cmd == "putif") {
        ret = PutIfAbsentOp(client, argc, argv, &error_code);
    } else if (cmd == "append") {
        ret = AppendOp(client, argc, argv, &error_code);
    } else if (cmd == "get") {
        ret = GetOp(client, argc, argv, &error_code);
    } else if (cmd == "get_counter") {
        ret = GetCounterOp(client, argc, argv, &error_code);
    } else if (cmd == "delete" || cmd == "delete1v") {
        ret = DeleteOp(client, argc, argv, &error_code);
    } else if (cmd == "batchput") {
        ret = BatchPutOp(client, argc, argv, &error_code);
    } else if (cmd == "batchget") {
        ret = BatchGetOp(client, argc, argv, &error_code);
    } else if (cmd == "scan" || cmd == "scanallv") {
        ret = ScanOp(client, argc, argv, &error_code);
    } else if (cmd == "safemode") {
        ret = SafeModeOp(client, argc, argv, &error_code);
    } else if (cmd == "tablet") {
        ret = TabletOp(client, argc, argv, &error_code);
    } else if (cmd == "meta") {
        ret = MetaOp(client, argc, argv, &error_code);
    } else if (cmd == "compact") {
        ret = CompactOp(argc, argv);
    } else if (cmd == "findts") {
        // get tabletnode addr from a key
        ret = FindTsOp(client, argc, argv, &error_code);
    } else if (cmd == "meta2") {
        ret = Meta2Op(client, argc, argv);
    } else if (cmd == "version") {
        PrintSystemVersion();
    } else if (cmd == "snapshot") {
        ret = SnapshotOp(client, argc, argv, &error_code);
    } else {
        Usage(argv[0]);
    }
    if (error_code.GetType() != ErrorCode::kOK) {
        LOG(ERROR) << "fail reason: " << error_code.GetReason();
    }
    delete client;
    return ret;
}
