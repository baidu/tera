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

DEFINE_string(dump_tera_src_conf, "../conf/src_tera.flag", "src cluster for tera");
DEFINE_string(dump_tera_dest_conf, "../conf/dest_tera.flag", "dest cluster for tera");
DEFINE_string(dump_tera_src_root_path, "/xxx_", "src tera root path");
DEFINE_string(dump_tera_dest_root_path, "/xxx_", "dest tera root path");
DEFINE_string(ins_cluster_addr, "terautil_ins", "terautil dump ins cluster conf");
DEFINE_string(ins_cluster_root_path, "/terautil/dump/xxxx", "dump meta ins");
DEFINE_string(dump_tera_src_meta_addr, "", "src addr for meta_table");
DEFINE_string(dump_tera_dest_meta_addr, "", "dest addr for meta_table");
DEFINE_int64(dump_manual_split_interval, 1000, "manual split interval in ms");
DEFINE_bool(dump_enable_manual_split, false, "manual split may take a long time, so disable it");

using namespace tera;

const char* terautil_builtin_cmds[] = {
    "dump",
    "dump <operation>                                                  \n\
            prepare_safe                                                    \n\
            prepare                                                    \n\
            run                                                        \n\
            show                                                       \n\
            check",

    "help",
    "help [cmd]                                                           \n\
          show manual for a or all cmd(s)",

    "version",
    "version                                                              \n\
             show version info",
};

static void ShowCmdHelpInfo(const char* msg) {
    if (msg == NULL) {
        return;
    }
    int count = sizeof(terautil_builtin_cmds)/sizeof(char*);
    for (int i = 0; i < count; i+=2) {
        if(strncmp(msg, terautil_builtin_cmds[i], 32) == 0) {
            std::cout << terautil_builtin_cmds[i + 1] << std::endl;
            return;
        }
    }
}

static void ShowAllCmd() {
    std::cout << "there is cmd list:" << std::endl;
    int count = sizeof(terautil_builtin_cmds)/sizeof(char*);
    bool newline = false;
    for (int i = 0; i < count; i+=2) {
        std::cout << std::setiosflags(std::ios::left) << std::setw(20) << terautil_builtin_cmds[i];
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

int DumpRange(const std::string& ins_cluster_addr,
              const std::string& ins_cluster_root_path,
              const tera::TableMetaList& table_list,
              const tera::TabletMetaList& tablet_list) {
    int res = 0;
    galaxy::ins::sdk::SDKError ins_err;
    galaxy::ins::sdk::InsSDK ins_sdk(ins_cluster_addr);
    std::string table_path = ins_cluster_root_path + "/table";
    std::string tablet_path = ins_cluster_root_path + "/tablet";
    //std::string lock_path = ins_cluster_root_path + "/lock";

    for (int32_t i = 0; i < table_list.meta_size(); i++) {
        const tera::TableMeta& meta = table_list.meta(i);
        if (meta.table_name() == FLAGS_tera_master_meta_table_name) {
            continue;
        }
        std::string key = table_path + "/" + meta.table_name();
        if(!ins_sdk.Put(key, meta.table_name(), &ins_err)) {
            LOG(WARNING) << "ins put: " << key << ", error " << ins_err;
            return -1;
        }
    }

    for (int32_t i = 0; i < tablet_list.meta_size(); i++) {
        const tera::TabletMeta& meta = tablet_list.meta(i);
        if (meta.table_name() == FLAGS_tera_master_meta_table_name) {
            continue;
        }
        std::string table_name = meta.table_name();
        std::string key = tablet_path + "/" + meta.table_name() + "/" + meta.key_range().key_start();
        std::string val = "0";
        val.append(meta.key_range().key_end());
        if(!ins_sdk.Put(key, val, &ins_err)) {
            LOG(WARNING) << "ins put: " << key << ", error " << ins_err;
            return -1;
        }
        //std::string lock_key = lock_path + "/" + meta.table_name() + "/" + meta.key_range().key_start();
    }
    return res;
}

int ScanAndDumpMeta(const std::string& src_meta_tablet_addr,
                    const std::string& dest_meta_tablet_addr,
                    tera::TableMetaList* table_list,
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
    tera::tabletnode::TabletNodeClient src_meta_node_client(src_meta_tablet_addr);
    bool success = true;
    while ((success = src_meta_node_client.ScanTablet(&request, &response))) {
        if (response.status() != tera::kTabletNodeOk) {
            LOG(WARNING) << "dump: fail to load meta table: "
                << StatusCodeToString(response.status());
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
            } else if (first_key_char == '@') {
                //ParseMetaTableKeyValue(record.key(), record.value(), table_list->add_meta());
                table_meta.Clear();
                ParseMetaTableKeyValue(record.key(), record.value(), &table_meta);

                std::string key, val;
                //table_meta.set_status(kTableDisable);
                table_meta.mutable_schema()->set_merge_size(0); // never merge during dump
                table_meta.mutable_schema()->set_split_size(10000000); // never split during dump
                MakeMetaTableKeyValue(table_meta, &key, &val);

                RowMutationSequence* mu_seq = write_request.add_row_list();
                mu_seq->set_row_key(record.key());
                Mutation* mutation = mu_seq->add_mutation_sequence();
                mutation->set_type(tera::kPut);
                mutation->set_value(val);
                request_size += mu_seq->ByteSize();
                if (request_size >= kMaxRpcSize) { // write req too large, dump into new tera cluster
                    need_dump = true;
                }

                TableMeta* table_meta2 = table_list->add_meta();
                table_meta2->CopyFrom(table_meta);
            } else if (first_key_char > '@') {
                //ParseMetaTableKeyValue(record.key(), record.value(), tablet_list->add_meta());
                tablet_meta.Clear();
                ParseMetaTableKeyValue(record.key(), record.value(), &tablet_meta);

                std::string key, val;
                tablet_meta.clear_parent_tablets();
                //tablet_meta.set_status(kTabletDisable);
                MakeMetaTableKeyValue(tablet_meta, &key, &val);

                RowMutationSequence* mu_seq = write_request.add_row_list();
                mu_seq->set_row_key(record.key());
                Mutation* mutation = mu_seq->add_mutation_sequence();
                mutation->set_type(tera::kPut);
                mutation->set_value(val);
                request_size += mu_seq->ByteSize();
                if (request_size >= kMaxRpcSize) { // write req too large, dump into new tera cluster
                    need_dump = true;
                }

                TabletMeta* tablet_meta2 = tablet_list->add_meta();
                tablet_meta2->CopyFrom(tablet_meta);
            } else {
                LOG(WARNING) << "dump: invalid meta record: " << record.key();
            }
        }

        if ((need_dump || record_size <= 0) &&
            write_request.row_list_size() > 0) {
            tabletnode::TabletNodeClient dest_meta_node_client(dest_meta_tablet_addr);
            if (!dest_meta_node_client.WriteTablet(&write_request, &write_response)) {
                LOG(WARNING) << "dump: fail to dump meta tablet: "
                    << StatusCodeToString(kRPCError);
                return -1;
            }
            tera::StatusCode status = write_response.status();
            if (status == tera::kTabletNodeOk && write_response.row_status_list_size() > 0) {
                status = write_response.row_status_list(0);
            }
            if (status != kTabletNodeOk) {
                LOG(WARNING) << "dump: fail to dump meta tablet: "
                    << StatusCodeToString(status);
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
    return success? 0: -1;
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
        std::string ins_cluster_addr = FLAGS_ins_cluster_addr;
        std::string ins_cluster_root_path = FLAGS_ins_cluster_root_path;
        res = DumpRange(ins_cluster_addr, ins_cluster_root_path, table_list, tablet_list);
    }
    return res;
}

int GetAndLockDumpRange(const std::string& ins_cluster_root_path,
                        std::string* table_name,
                        std::string* start_key,
                        std::string* end_key,
                        galaxy::ins::sdk::InsSDK* ins_sdk) {
    int res = -1;
    galaxy::ins::sdk::SDKError ins_err;
    //std::string table_path = ins_cluster_root_path + "/table";
    std::string tablet_path = ins_cluster_root_path + "/tablet";
    std::string lock_path = ins_cluster_root_path + "/lock";

    std::string start = tablet_path + "/";
    std::string end = tablet_path + "/";
    if (table_name->size()) {
        start.append(*table_name);
        start.append("/");
        start.append(*start_key);
        if (*start_key == "") {
            start.append(1, '\0');
        }
    }
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
        std::string has_done = val.substr(0, 1);
        if (has_done == "1") { // someone has copy it
            result->Next();
            continue;
        }

        //std::string key = tablet_path + "/" + meta.table_name() + "/" + meta.key_range().key_start();
        std::string str = key.substr(tablet_path.length() + 1);
        std::size_t pos = str.find('/');
        *table_name = str.substr(0, pos);
        *start_key = str.substr(pos + 1);
        *end_key = val.substr(1);

        std::string lock_key = lock_path + "/" + *table_name + "/" + *start_key + "/";
        if (!ins_sdk->TryLock(lock_key, &ins_err)) {
            LOG(INFO) << "ins: TryLock fail: " << lock_key << ", err " << ins_err;
            result->Next();
            continue;
        }

        std::string val1;
        if (ins_sdk->Get(key, &val1, &ins_err)) {
            has_done = val1.substr(0, 1);
        } else {
            LOG(INFO) << "ins: get fail: " << key << ", err " << ins_err;
        }
        if (has_done == "1") { // someone has copy it
            if (!ins_sdk->UnLock(lock_key, &ins_err)) {
                LOG(INFO) << "ins: unlock fail: " << lock_key << ", err " << ins_err;
            }
            result->Next();
            continue;
        }

        res = 0;
        break; // begin to scan
    }
    delete result;
    return res;
}

int ReleaseAndUnlockDumpRange(const std::string& ins_cluster_root_path,
                              const std::string& table_name,
                              const std::string& start_key,
                              const std::string& end_key,
                              galaxy::ins::sdk::InsSDK* ins_sdk) {
    int res = 0;
    galaxy::ins::sdk::SDKError ins_err;
    //std::string table_path = ins_cluster_root_path + "/table";
    std::string tablet_path = ins_cluster_root_path + "/tablet";
    std::string lock_path = ins_cluster_root_path + "/lock";

    std::string key = tablet_path + "/" + table_name + "/" + start_key;
    std::string val = "1";
    val.append(end_key);

    if(!ins_sdk->Put(key, val, &ins_err)) {
        LOG(WARNING) << "ins put: " << key << ", error " << ins_err;
    }

    std::string lock_key = lock_path + "/" + table_name + "/" + start_key + "/";
    if (!ins_sdk->UnLock(lock_key, &ins_err)) {
        LOG(WARNING) << "ins unlock fail: " << lock_key << ", error " << ins_err;
    }
    return res;
}

struct ScanDumpContext {
    Counter counter;
    volatile bool fail;
    std::string reason;
};

void ScanAndDumpCallBack(RowMutation* mu) {
    ScanDumpContext* ctx = (ScanDumpContext*)mu->GetContext();
    if (mu->GetError().GetType() != tera::ErrorCode::kOK) {
        if (ctx->fail == false) {
            ctx->fail = true;
            ctx->reason = mu->GetError().ToString();
        }
    }
    delete mu;

    ctx->counter.Dec();
    return;
}

int ScanAndDumpData(Table* src, Table* dest,
                    const std::string& table_name,
                    const std::string& start_key,
                    const std::string& end_key) {
    int res = 0;
    ErrorCode err;

    ScanDescriptor desc(start_key);
    desc.SetEnd(end_key);
    desc.SetMaxVersions(std::numeric_limits<int>::max());
    ResultStream* result_stream;
    if ((result_stream = src->Scan(desc, &err)) == NULL) {
        LOG(INFO) << "scan dump fail(new scan): " << table_name << ", start " << start_key
            << ", end " << end_key;
        return -1;
    }
    ScanDumpContext* ctx = new ScanDumpContext;
    ctx->counter.Set(1);
    ctx->fail = false;
    while (!result_stream->Done(&err)) {
        RowMutation* mu = dest->NewRowMutation(result_stream->RowName());
        mu->Put(result_stream->Family(), result_stream->Qualifier(),
                result_stream->Value(), result_stream->Timestamp());
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
    if (ctx->fail == true) {
        LOG(INFO) << "scan dump fail: " << table_name << ", start " << start_key
            << ", end " << end_key << ", reason " << ctx->reason;
        res = -1;
    }
    delete ctx;

    if (err.GetType() != tera::ErrorCode::kOK) {
        LOG(INFO) << "scan dump fail: " << table_name << ", start " << start_key
            << ", end " << end_key << ", reason " << err.GetReason();
        res = -1;
    }
    return res;
}

int DumpRunOp() {
    int res = 0;
    std::string ins_cluster_addr = FLAGS_ins_cluster_addr;
    std::string ins_cluster_root_path = FLAGS_ins_cluster_root_path;
    std::string tera_src_conf = FLAGS_dump_tera_src_conf;
    std::string tera_dest_conf = FLAGS_dump_tera_dest_conf;

    // get and lock range
    ErrorCode err;
    Client* src_client = Client::NewClient(tera_src_conf, &err);
    if (src_client == NULL) {
        LOG(INFO) << "open src client fail: " << tera_src_conf << ", err " << err.ToString();
        return -1;
    }
    Client* dest_client = Client::NewClient(tera_dest_conf, &err);
    if (dest_client == NULL) {
        delete src_client;
        src_client = NULL;
        LOG(INFO) << "open dest client fail: " << tera_dest_conf << ", err " << err.ToString();
        return -1;
    }
    Table* src_table = NULL;
    Table* dest_table = NULL;

    galaxy::ins::sdk::InsSDK ins_sdk(ins_cluster_addr);
    std::string table_name, start_key, end_key, last_table_name;
    while (GetAndLockDumpRange(ins_cluster_root_path, &table_name, &start_key, &end_key, &ins_sdk) == 0) {
        if (last_table_name != table_name) { // table change
            delete src_table;
            delete dest_table;
            src_table = NULL;
            dest_table = NULL;
            src_table = src_client->OpenTable(table_name, &err);
            if (src_table == NULL) {
                LOG(INFO) << "open src table fail: " << table_name << ", err " << err.ToString();
                continue;
            }
            dest_table = dest_client->OpenTable(table_name, &err);
            if (dest_table == NULL) {
                delete src_table;
                src_table = NULL;
                LOG(INFO) << "open dest table fail: " << table_name << ", err " << err.ToString();
                continue;
            }
        }
        last_table_name = table_name;
        if ((res = ScanAndDumpData(src_table, dest_table, table_name, start_key, end_key)) < 0) {
            LOG(INFO) << "scan dump data fail: " << table_name << ", start " << start_key
                << ", end " << end_key;
        } else {
            ReleaseAndUnlockDumpRange(ins_cluster_root_path, table_name, start_key, end_key, &ins_sdk);
        }
        start_key = end_key;
    }
    delete src_client;
    delete dest_client;
    return res;
}

void GetTableKeyRange(const std::string& table_name,
                     const TabletMetaList& tablet_list,
                     std::vector<std::string>* delimiters) {
    for (int32_t i = 0; i < tablet_list.meta_size(); i++) {
        const tera::TabletMeta& meta = tablet_list.meta(i);
        if (table_name == meta.table_name() &&
            meta.key_range().key_start().size() > 0) {
            delimiters->push_back(meta.key_range().key_start());
        }
    }
}

int ManualCreateTable(tera::ClientImpl* client,
                      const std::string& table_name,
                      const TableSchema& schema,
                      const std::vector<std::string>& delimiters) {
    ErrorCode err;
    TableDescriptor table_desc;
    table_desc.SetTableName(table_name);
    TableSchemaToDesc(schema, &table_desc);
    table_desc.SetSplitSize(10000000);
    table_desc.SetMergeSize(0);
    if (!client->CreateTable(table_desc, delimiters, &err)) {
        LOG(INFO) << "manual create error: " << table_name << ", err: " << err.ToString();
        return -1;
    }
    return 0;
}

int ManualSplitTable(tera::ClientImpl* client,
                     const std::string& table_name,
                     const std::vector<std::string>& delimiters) {
    ErrorCode err;
    std::vector<std::string> arg_list;
    arg_list.push_back("split");
    arg_list.push_back(table_name);
    for (uint32_t i = 0; i < delimiters.size(); i++) {
        arg_list.push_back(delimiters[i]);
        if (!client->CmdCtrl("table", arg_list, NULL, NULL, &err)) {
            LOG(INFO) << "manual split table fail(ignore old master):  " << table_name
                      << ", delimiters_size: " << delimiters.size()
                      << ", err: " << err.ToString();
        }
        usleep(FLAGS_dump_manual_split_interval);
        arg_list.pop_back();
    }
    return 0;
}

bool SchemaCompare(const TableSchema& src, const TableSchema& dest) {
    return ((src.raw_key() == dest.raw_key()) &&
           (src.kv_only() == dest.kv_only()) &&
           (src.name() == dest.name()) &&
           (!IsSchemaCfDiff(src, dest)) &&
           (!IsSchemaLgDiff(src, dest)));
}

int GetOrSetTabletLocationSafe(Client* src_client,
                               Client* dest_client,
                               TableMetaList* table_list,
                               TabletMetaList* tablet_list) {
    // get src and dest tablet location
    ErrorCode err;
    TableMetaList src_table_list;
    TabletMetaList src_tablet_list;
    tera::ClientImpl* src_client_impl = static_cast<tera::ClientImpl*>(src_client);
    if (!src_client_impl->ShowTablesInfo(&src_table_list, &src_tablet_list, false, &err)) {
        LOG(INFO) << "tera_master show src cluster fail: " << err.ToString();
        return -1;
    }

    TableMetaList dest_table_list;
    TabletMetaList dest_tablet_list;
    tera::ClientImpl* dest_client_impl = static_cast<tera::ClientImpl*>(dest_client);
    if (!dest_client_impl->ShowTablesInfo(&dest_table_list, &dest_tablet_list, false, &err)) {
        LOG(INFO) << "tera_master show dest cluster fail: " << err.ToString();
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
            LOG(INFO) << "table schema not match: " << meta.table_name() << ", src schema: " << meta.schema().ShortDebugString()
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

int DumpPrepareSafeOp() {
    int res = 0;
    std::string ins_cluster_addr = FLAGS_ins_cluster_addr;
    std::string ins_cluster_root_path = FLAGS_ins_cluster_root_path;
    std::string tera_src_conf = FLAGS_dump_tera_src_conf;
    std::string tera_dest_conf = FLAGS_dump_tera_dest_conf;

    ErrorCode err;
    std::unique_ptr<Client> src_client(Client::NewClient(tera_src_conf, &err));
    if (src_client == nullptr) {
        LOG(INFO) << "open src client fail: " << tera_src_conf << ", err " << err.ToString();
        return -1;
    }
    std::unique_ptr<Client> dest_client(Client::NewClient(tera_dest_conf, &err));
    if (dest_client == nullptr) {
        src_client = nullptr;
        LOG(INFO) << "open dest client fail: " << tera_dest_conf << ", err " << err.ToString();
        return -1;
    }

    // dump src cluster range into ins
    TableMetaList table_list;
    TabletMetaList tablet_list;
    if (GetOrSetTabletLocationSafe(src_client.get(), dest_client.get(), &table_list, &tablet_list) < 0) {
        return -1;
    }
    res = DumpRange(ins_cluster_addr, ins_cluster_root_path, table_list, tablet_list);
    return res;
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

    if (argc > 1 && std::string(argv[1]) == "version") {
        PrintSystemVersion();
    } else if (argc > 2 && std::string(argv[1]) == "dump" && std::string(argv[2]) == "prepare") {
        return DumpPrepareOp();
    } else if (argc > 2 && std::string(argv[1]) == "dump" && std::string(argv[2]) == "prepare_safe") {
        return DumpPrepareSafeOp();
    } else if (argc > 2 && std::string(argv[1]) == "dump" && std::string(argv[2]) == "run") {
        return DumpRunOp();
    //} else if (argc > 2 && std::string(argv[1]) == "dump" && std::string(argv[2]) == "show") {
    //    return DumpShowOp();
    //} else if (argc > 2 && std::string(argv[1]) == "dump" && std::string(argv[2]) == "check") {
    //    return DumpCheckOp():
    } else {
        HelpOp(argc, argv);
        return -1;
    }
    return 0;
}

