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

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "ins_sdk.h"

#include "common/base/string_ext.h"
#include "common/base/string_number.h"
#include "common/console/progress_bar.h"
#include "common/file/file_path.h"
#include "io/coding.h"
#include "proto/kv_helper.h"
#include "proto/proto_helper.h"
#include "proto/tabletnode.pb.h"
#include "proto/tabletnode_client.h"
#include "sdk/client_impl.h"
#include "sdk/cookie.h"
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

DEFINE_string(meta_cli_token, "", "Only be executed for the guys who has the token. \
                                   Please figure out what metacli is before use it.");

using namespace tera;
namespace {
    static uint64_t seq_id = 0;
}

const char* metacli_builtin_cmds[] = {
    "show",
    "show                                                                  \n\
        show all meta info in meta table",

    "get",
    "get <table_name> <row_key>                                            \n\
        get the value for table_name+row_key in meta_table                 \n\
        e.g. get \"test_table1\" \"abc\" ",

    "modify",
    "modify <table_name> <row_key>                                         \n\
        modify the value of key_end                                        \n\
        e.g. modify \"test_table1\" \"abc\" ",

    "delete",
    "delete <table_name> <start_key>                                       \n\
        delete the table_name+row_key in meta_table                        \n\
        e.g. delete \"test_table1\" \"abc\" ",

    "help",
    "help [cmd]                                                            \n\
          show manual for a or all cmd(s)",

    "version",
    "version                                                               \n\
             show version info",
};

static void ShowCmdHelpInfo(const char* msg) {
    if (msg == NULL) {
        return;
    }
    int count = sizeof(metacli_builtin_cmds)/sizeof(char*);
    for (int i = 0; i < count; i+=2) {
        if(strncmp(msg, metacli_builtin_cmds[i], 32) == 0) {
            std::cout << metacli_builtin_cmds[i + 1] << std::endl;
            return;
        }
    }
}

static void ShowAllCmd() {
    std::cout << "there is cmd list:" << std::endl;
    int count = sizeof(metacli_builtin_cmds)/sizeof(char*);
    bool newline = false;
    for (int i = 0; i < count; i+=2) {
        std::cout << std::setiosflags(std::ios::left) << std::setw(20) << metacli_builtin_cmds[i];
        if (newline) {
            std::cout << std::endl;
            newline = false;
        } else {
            newline = true;
        }
    }
    std::cout << std::endl << "help [cmd] for details." << std::endl;
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

static void PrintMetaInfo(const TabletMeta* meta) {
    std::cout << "tablet: " << meta->table_name() << " ["
            << meta->key_range().key_start() << ","
            << meta->key_range().key_end() << "], "
            << meta->path() << ", " << meta->server_addr() << ", "
            << meta->size() << ", "
            << StatusCodeToString(meta->status()) << ", "
            << StatusCodeToString(meta->compact_status()) << std::endl;
}

static int GetMetaValue(const std::string& meta_server,
                 common::ThreadPool* thread_pool,
                 const std::string& tablet_name,
                 const std::string& start_key,
                 TableMeta* table_meta,
                 TabletMeta* tablet_meta) {
    tabletnode::TabletNodeClient read_meta_client(thread_pool, meta_server);
    ReadTabletRequest read_request;
    ReadTabletResponse read_response;
    read_request.set_sequence_id(seq_id++);
    read_request.set_tablet_name(FLAGS_tera_master_meta_table_name);
    RowReaderInfo* row_info = read_request.add_row_info_list();
    MakeMetaTableKey(tablet_name, start_key, row_info->mutable_key());
    if (!read_meta_client.ReadTablet(&read_request, &read_response)) {
        std::cout << "read tablet failed" << std::endl;
        return -1;
    }
    StatusCode err = read_response.status();
    if (err != tera::kTabletNodeOk) {
        std::cerr << "Read meta table response not kTabletNodeOk!";
        return -1;
    }
    const KeyValuePair& record = read_response.detail().row_result(0).key_values(0);
    char first_key_char = record.key()[0];
    if (first_key_char == '~') {
        std::cout << "(user: " << record.key().substr(1) << ")" << std::endl;
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
        return -1;
    }
    return 0;
}

static bool Confirm() {
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

int GetMeta(const std::string& meta_server,
            common::ThreadPool* thread_pool,
            const std::string& tablet_name,
            const std::string& start_key) {
    TabletMeta tablet_meta;
    TableMeta table_meta;
    if (-1 == GetMetaValue(meta_server, thread_pool, tablet_name, start_key, &table_meta, &tablet_meta)) {
        std::cout << "wrong tablet input" << std::endl;
        return -1;
    }
    PrintMetaInfo(&tablet_meta);
    return 0;
}

int DeleteMetaTablet(const std::string& meta_server,
                     common::ThreadPool* thread_pool,
                     const std::string& tablet_name,
                     const std::string& start_key) {
    TabletMeta tablet_meta;
    TableMeta table_meta;
    if (-1 == GetMetaValue(meta_server, thread_pool, tablet_name, start_key, &table_meta, &tablet_meta)) {
        std::cout << "wrong tablet input" << std::endl;
        return -1;
    }
    tabletnode::TabletNodeClient write_meta_client(thread_pool, meta_server);
    WriteTabletRequest write_request;
    WriteTabletResponse write_response;
    write_request.set_sequence_id(seq_id++);
    write_request.set_tablet_name(FLAGS_tera_master_meta_table_name);
    RowMutationSequence* mu_seq = write_request.add_row_list();
    
    std::cout << "Are you sure delete the tablet meta info?" << std::endl;
    PrintMetaInfo(&tablet_meta);
    if (!Confirm()) {
        return -1;
    }

    std::string row_key;
    MakeMetaTableKey(tablet_name, start_key, &row_key);
    mu_seq->set_row_key(row_key);
    tera::Mutation* mutation = mu_seq->add_mutation_sequence();
    mutation->set_type(tera::kDeleteRow);
    mutation->set_timestamp(kLatestTimestamp);
    if (!write_meta_client.WriteTablet(&write_request, &write_response)) {
        std::cout << "write tablet failed" << std::endl;
        return -1;
    }
    StatusCode err = write_response.status();
    if (err != tera::kTabletNodeOk) {
        std::cerr << "Write meta table response not kTabletNodeOk!";
        return -1;
    }
    return 0;
}

int ModifyMetaValue(const std::string& meta_server,
                    common::ThreadPool* thread_pool,
                    const std::string& tablet_name,
                    const std::string& start_key) {
    TabletMeta tablet_meta;
    TableMeta table_meta;
    if (-1 == GetMetaValue(meta_server, thread_pool, tablet_name, start_key, &table_meta, &tablet_meta)) {
        std::cout << "wrong tablet input" << std::endl;
        return -1;
    }

    tabletnode::TabletNodeClient write_meta_client(thread_pool, meta_server);
    WriteTabletRequest write_request;
    WriteTabletResponse write_response;
    write_request.set_sequence_id(seq_id++);
    write_request.set_tablet_name(FLAGS_tera_master_meta_table_name);
    RowMutationSequence* mu_seq = write_request.add_row_list();

    std::string end_key;
    std::cout << "Modify key_end as : ";
    if (!std::getline(std::cin, end_key)) {
        std::cout << "Get input error" << std::endl;
        return -1;
    }

    std::cout << "Are you sure modify key_end?" << std::endl;
    std::cout << "[" << tablet_meta.key_range().key_start() << ", "
              << tablet_meta.key_range().key_end() << "] => ";
    tera::KeyRange* key_range = new tera::KeyRange();
    key_range->set_key_start(tablet_meta.key_range().key_start());
    key_range->set_key_end(end_key);

    tablet_meta.clear_key_range();
    tablet_meta.set_allocated_key_range(key_range);
    std::cout << "[" << tablet_meta.key_range().key_start() << ", "
              << tablet_meta.key_range().key_end() << "]" << std::endl;
    if (!Confirm()) {
        return -1;
    }

    std::string row_key;
    MakeMetaTableKey(tablet_name, start_key, &row_key);
    mu_seq->set_row_key(row_key);
    tera::Mutation* mutation = mu_seq->add_mutation_sequence();
    mutation->set_type(tera::kPut);

    std::string modify_value;
    MakeMetaTableValue(tablet_meta, &modify_value);
    mutation->set_value(modify_value);
    mutation->set_timestamp(kLatestTimestamp);

    if (!write_meta_client.WriteTablet(&write_request, &write_response)) {
        std::cout << "write tablet failed" << std::endl;
        return -1;
    }
    StatusCode err = write_response.status();
    if (err != tera::kTabletNodeOk) {
        std::cerr << "Write meta table response not kTabletNodeOk!";
        return -1;
    }
    return 0;
}

int ShowMeta(const std::string& meta_server, common::ThreadPool* thread_pool) {
    tabletnode::TabletNodeClient meta_client(thread_pool, meta_server);
    TableMeta table_meta;
    TableSchema table_schema;
    TableMetaList table_list;
    TabletMetaList tablet_list;
    ScanTabletRequest request;
    ScanTabletResponse response;
    request.set_sequence_id(seq_id++);
    request.set_table_name(FLAGS_tera_master_meta_table_name);
    request.set_start("");
    request.set_end("");
    while (meta_client.ScanTablet(&request, &response)) {
        StatusCode err = response.status();
        if (err != tera::kTabletNodeOk) {
            std::cerr << "Read meta table response not kTabletNodeOk!";
            return -1;
        }

        int32_t record_size = response.results().key_values_size();
        std::cout << "recode size = " << record_size << std::endl;
        if (record_size <= 0) {
            std::cout << "scan meta table success" << std::endl;
            break;
        }
        std::string last_record_key;
        for (int i = 0; i < record_size; ++i) {
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

    int32_t table_num = table_list.meta_size();
    for (int32_t i = 0; i < table_num; ++i) {
        const tera::TableMeta& meta = table_list.meta(i);
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

    int32_t tablet_num = tablet_list.meta_size();
    for (int32_t i = 0; i < tablet_num; ++i) {
        const tera::TabletMeta& meta = tablet_list.meta(i);
        std::cout << "tablet: " << meta.table_name() << " ["
                    << meta.key_range().key_start() << ","
                    << meta.key_range().key_end() << "], "
                    << meta.path() << ", " << meta.server_addr() << ", "
                    << meta.size() << ", "
                    << StatusCodeToString(meta.status()) << ", "
                    << StatusCodeToString(meta.compact_status()) << std::endl;
    }
    return 0;
}

int main(int argc, char* argv[]) {
    ::google::ParseCommandLineFlags(&argc, &argv, true);
    if (FLAGS_flagfile == "") {
        FLAGS_flagfile = "../conf/tera.flag";
        if (access(FLAGS_flagfile.c_str(), R_OK) != 0) {
            FLAGS_flagfile = "./tera.flag";
        }
        utils::LoadFlagFile(FLAGS_flagfile);
    }
    if (FLAGS_meta_cli_token != "2862933555777941757") {
        std::cout << "Please figure out what metacli is before use it." << std::endl;
        return -1;
    }
    scoped_ptr<tera::sdk::ClusterFinder> finder(tera::sdk::NewClusterFinder());
    const std::string meta_server = finder->RootTableAddr();
    if (meta_server.empty()) {
        std::cerr << "read root addr from zk fail";
        return -1;
    }
    if (argc <= 1) {
        HelpOp(argc, argv);
        return -1;
    }
    common::ThreadPool thread_pool(1);
    std::string op(argv[1]);
    if (argc == 2) {
        if (op == "show") {
            return ShowMeta(meta_server, &thread_pool);
        } else if (op == "version") {
            PrintSystemVersion();
        } else {
            HelpOp(argc, argv);
        }
    } else if (argc == 4) {
        const std::string tablet_name(argv[2]);
        const std::string start_key(argv[3]);
        if (op == "get") {
            return GetMeta(meta_server, &thread_pool, tablet_name, start_key);
        } else if (op == "modify") {
            return ModifyMetaValue(meta_server, &thread_pool, tablet_name, start_key);
        } else if (op == "delete") {
            return DeleteMetaTablet(meta_server, &thread_pool, tablet_name, start_key);
        } else {
            HelpOp(argc, argv);
        }
    } else {
        HelpOp(argc, argv);
    }

    return 0;
}