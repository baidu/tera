// Copyright (c) 2015-2017, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
// 
// Author: baorenyi@baidu.com

#include "sdk/test/global_txn_test_tool.h"

#include <fstream>
#include <iostream>
#include <memory>
#include <unordered_map>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "common/base/string_ext.h"
#include "common/file/file_path.h"
#include "sdk/sdk_utils.h"
#include "sdk/client_impl.h"
#include "utils/config_utils.h"
#include "common/timer.h"
#include "version.h"

DECLARE_string(tera_gtxn_test_flagfile);
DEFINE_string(gtxn_test_conf_dir, "../conf/", "gtxn test conf dir");
DEFINE_string(gtxn_test_case_dir, "../cases/", "gtxn test cases dir");
DEFINE_string(case_number, "", "gtxn test case number");
DEFINE_bool(ignore_bad_case, false, "gtxn test ignore bad case");
DEFINE_bool(gtxn_test_async_mode, false, "gtxn test async mode");
DEFINE_bool(gtxn_test_debug_opened, false, "gtxn test debug opened");
DEFINE_int32(gtxn_test_thread_pool_size, 20, "gtxn test thread pool size");
DEFINE_bool(gtxn_test_drop_table_before, true, "gtxn test set drop tables before test");

namespace tera {
/**
 * cases/ directory format
 *
 * CONF_ROOR/cases/1/schemas/table_1 [table schema file]
 * ....
 * CONF_ROOR/cases/1/schemas/table_x
 *
 * CONF_ROOR/cases/1/T_1/op_list   [operations list]
 *      Format of op_list:
 *          
 *          TABLES:table_1,table_2,table_3
 *          GET table_1 r1 cf1 qu1
 *          PUT table_2 r2 cf2 qu2 valuex
 *          DEL table_3 r3 cf3 qu3
 *
 * CONF_ROOR/cases/1/T_1/gtxn.flag [option]
 * CONF_ROOR/cases/1/T_1/result_list  [set result list]
 *
 * CONF_ROOR/cases/1/T_2/op_list
 * CONF_ROOR/cases/1/T_1/gtxn.flag [option]
 * CONF_ROOR/cases/1/T_2/result_list
 *
 **/
bool GlobalTxnTestTool::LoadTestConf() {
    // list cases
    const std::string case_dir = FLAGS_gtxn_test_case_dir;
    std::vector<std::string> file_list;
    if (IsEmpty(case_dir) || !ListCurrentDir(case_dir, &file_list)) {
        LOG(ERROR) << "list cases failed, dir:" << case_dir;
        return false;
    }
    for (auto it = file_list.begin(); it != file_list.end(); ++it) {
        if (FLAGS_case_number != "" && (*it) != FLAGS_case_number) {
            continue;
        }
        const std::string& dir_name = case_dir + (*it);
        
        if (!IsDir(dir_name)) {
            continue;
        }
        
        int case_num = atoi((*it).c_str());
        if (case_num <= 0) {
            LOG(ERROR) << "load case failed, dir:" << dir_name;
            return false;
        }
        // list cases/x/schemas/
        std::vector<std::string> schema_files;
        const std::string& schema_dir = dir_name + "/schemas/";
        if (IsEmpty(schema_dir) || !ListCurrentDir(schema_dir, &schema_files)) {
            LOG(ERROR) << "list case(" << dir_name << ") schemas failed";
            return false;
        }
        int schema_cnt = 0;
        for (auto sit = schema_files.begin(); sit != schema_files.end(); ++sit) {
            const std::string& schema_file = schema_dir + (*sit);
            if (IsDir(schema_file)) {
                continue;
            }
            // load schemas
            TableDescriptor* desc = new TableDescriptor();
            if (LoadDescriptor(schema_file, desc)) {
                if (case_desc_map_.find(case_num) == case_desc_map_.end()) {
                    case_desc_map_[case_num] = std::vector<TableDescriptor*>();
                }
                case_desc_map_[case_num].push_back(desc);
                ++schema_cnt;
            } else {
                delete desc;
                LOG(ERROR) << "load schema failed, schema_file:" << schema_file;
                break;
            }
        }
        if (schema_cnt == 0) {
            LOG(ERROR) << "schemafile not found";
            return false;
        }

        // mark cases/x/T_xx/
        std::vector<std::string> txn_list;
        if (!ListCurrentDir(dir_name, &txn_list)) {
            LOG(ERROR) << "find txn dir failed, dir:" << dir_name;
            return false;
        }
        int reg_cnt = 0;
        for(auto it = txn_list.begin(); it != txn_list.end(); ++it) {
            if (!IsDir(dir_name + "/" + (*it)) || *it == "schemas") {
                continue;
            }
            if ((*it).find("T_") != std::string::npos) {
                // find transaction 
                int gtxn_id = atoi(((*it).substr(2)).c_str());
                if (gtxn_id <= 0) {
                    LOG(ERROR) << "mark gtxn conf failed, dir:" 
                               << case_dir << "/" << dir_name;
                    return false;
                } else {
                    CaseRegister(case_num, gtxn_id);
                    ++reg_cnt;
                }
            }
        }
        if (reg_cnt == 0) {
            LOG(ERROR) << "transaction not found";
            return false;
        }
    }
    return true;
}

void GlobalTxnTestTool::CaseRegister(const int case_num, const int gtxn_id) {
    CasePair case_pair(case_num, gtxn_id);
    case_list_.push_back(case_pair);
}

bool GlobalTxnTestTool::LoadDescriptor(const std::string& schema_file, 
                                       TableDescriptor* table_desc) {
    ErrorCode err;
    if (!ParseTableSchemaFile(schema_file, table_desc, &err)) {
        LOG(ERROR) << "fail to parse input table schema." << schema_file;
        return false;
    }
    //ShowTableDescriptor(*table_desc, true);
    return true;
}

GlobalTxnTestTool::GlobalTxnTestTool(Client* client):
    thread_pool_(FLAGS_gtxn_test_thread_pool_size),
    client_(client) {
}

void GlobalTxnTestTool::RunTest(tera::Client* client, int case_number) {
    do_cnt_.Set(0);
    done_cnt_.Set(0);
    done_fail_cnt_.Set(0);
    for (auto it = case_list_.begin(); it != case_list_.end(); ++it) {
        CasePair case_pair = *it;
        int case_num = case_pair.first;
        if (case_number != -1 && case_num != case_number) {
            continue;
        }
        int gtxn_id = case_pair.second;

        const std::string case_dir = FLAGS_gtxn_test_case_dir;
        const std::string conf_dir = case_dir + std::to_string(case_num) 
                                   + "/T_" + std::to_string(gtxn_id);
        const std::string& op_list_file = conf_dir + "/op_list";
        std::vector<std::string> op_list;
        std::ifstream ifile(op_list_file);
        std::string line;
        int cnt = 0;
        while (std::getline(ifile, line)) {
            if (cnt == 0) {
                std::size_t found = line.find("TABLES:");
                if (found!=std::string::npos) {
                    std::vector<std::string> tables;
                    SplitString(line.substr(found + 7), ",", &tables); 
                    if (!OpenTestTables(tables)) {
                        return;
                    }
                }
            } else {
                op_list.push_back(line);
            }
            ++cnt;
        }
        ifile.close();
        if (cnt < 1) {
            LOG(ERROR) << "no operations in op_list";
        }
        do_cnt_.Inc();
        ThreadPool::Task task = std::bind(&GlobalTxnTestTool::RunTestInternal, 
                                          this, client, case_num, gtxn_id, op_list);
        thread_pool_.AddTask(task);
    }
}

void GlobalTxnTestTool::RunTestInternal(tera::Client* client, const int case_num, const int gtxn_id, 
                                        const std::vector<std::string>& op_list) {
    const std::string case_dir = FLAGS_gtxn_test_case_dir;
    const std::string conf_dir = case_dir + std::to_string(case_num) 
                               + "/T_" + std::to_string(gtxn_id);
    
    // make sure flagfile only service for this transaction
    tera::Transaction* gtxn = nullptr;
    {
        MutexLock lock(&mu_);
        FLAGS_tera_gtxn_test_flagfile = conf_dir + "/gtxn.flag";
        gtxn = client->NewGlobalTransaction();
    }

    if (!FLAGS_gtxn_test_async_mode) {
        std::vector<std::string> result;
        for (auto it = op_list.begin(); it != op_list.end(); ++it) {
            const std::string& op_str = *it;
            VLOG(12) << "OPERATION:" << op_str;
            OpType op_type;
            std::vector<std::string> op_args;
            if (!ParseOp(op_str, &op_type, &op_args)
                || !DoOp(gtxn, op_type, op_args, &result)) {
                LOG(ERROR) << gtxn->GetError().ToString();
                delete gtxn;
                done_cnt_.Inc();
                return;
            }
        }
        gtxn->Commit();
        result.push_back(std::to_string(gtxn->GetError().GetType()));
        if(!CheckResult(case_num, gtxn_id, result)) {
            done_fail_cnt_.Inc();
        }
        delete gtxn;
        done_cnt_.Inc();
    } else {
        if (op_list.size() > 0) {
            GTxnTestContext* ctx = new GTxnTestContext();
            ctx->tool = this;
            ctx->gtxn = gtxn;
            ctx->op_list = op_list;
            ctx->case_num = case_num;
            ctx->gtxn_id = gtxn_id;
            ctx->it = ctx->op_list.begin();
            const std::string& op_str = *(ctx->it);
            VLOG(12) << "OPERATION:" << op_str;
            OpType op_type;
            std::vector<std::string> op_args;
            if (!ParseOp(op_str, &op_type, &op_args)) {
                LOG(ERROR) << "parse op failed";
                delete ctx->gtxn;
                delete ctx;
                done_cnt_.Inc();
                return;
            }
            DoOpAsync(ctx, op_type, op_args);
        } else {
            LOG(ERROR) << "not set operators";
            delete gtxn;
            done_cnt_.Inc();
        }
    }
}

bool GlobalTxnTestTool::OpenTestTables(const std::vector<std::string>& tables) {
    ErrorCode err;
    MutexLock lock(&mu_);
    for(auto it = tables.begin(); it != tables.end(); ++it) {
        const std::string tablename = *it;
        if (tables_.find(tablename) == tables_.end()) {
            Table* table = client_->OpenTable(tablename, &err);
            if (table == NULL) {
                return false;
            }
            tables_[tablename] = table;
        }
    }
    return true;
}

void GlobalTxnTestTool::DoOpAsync(GTxnTestContext* ctx, 
                                  const OpType& op_type,
                                  const std::vector<std::string>& op_args) {
    if (op_args.size() < 4) {
        return;
    }
    Table* table = nullptr;
    const std::string tablename = op_args[0];
    auto table_it = tables_.find(tablename);
    if (table_it != tables_.end()) {
        table = table_it->second;
    } else {
        return;
    }
    const std::string row = op_args[1];
    const std::string cf = op_args[2];
    const std::string qu = op_args[3];
    if (op_type == OpType::PUT && op_args.size() == 5) {
        const std::string value = op_args[4];
        tera::RowMutation* m = table->NewRowMutation(row);
        m->Put(cf, qu, value);
        ctx->gtxn->ApplyMutation(m);
        ctx->result.push_back("PUT: " + std::to_string(ctx->gtxn->GetError().GetType()));
        delete m;
    } else if (op_type == OpType::GET && op_args.size() == 4) {
        tera::RowReader* r = table->NewRowReader(row);
        r->AddColumn(cf, qu);
        r->SetCallBack([] (RowReader* r) {
            ((GTxnTestContext*)r->GetContext())->tool->DoOpAsyncCallback(r);
        });
        r->SetContext(ctx);
        ctx->gtxn->Get(r);
        return;
    } else if (op_type == OpType::DEL && op_args.size() == 4) {
        tera::RowMutation* m = table->NewRowMutation(row);
        m->DeleteColumns(cf, qu);
        ctx->gtxn->ApplyMutation(m);
        ctx->result.push_back("DEL: " + std::to_string(ctx->gtxn->GetError().GetType()));
        delete m;
    }
    
    // this operation is muation , run next operation
    if (op_type == OpType::PUT || op_type == OpType::DEL) { 
        if (++ctx->it != ctx->op_list.end()) {
            const std::string& op_str = *(ctx->it);
            VLOG(12) << "OPERATION:" << op_str;
            OpType next_op_type;
            std::vector<std::string> next_op_args;
            if (!ParseOp(op_str, &next_op_type, &next_op_args)) {
                LOG(ERROR) << "parse op failed";
                delete ctx->gtxn;
                delete ctx;
                done_cnt_.Inc();
                return;
            }
            DoOpAsync(ctx, next_op_type, next_op_args);
        } else {
            ctx->gtxn->SetCommitCallback([] (Transaction* t) {
                ((GTxnTestContext*)t->GetContext())->tool->DoCommitCallback(t);
            });
            ctx->gtxn->SetContext(ctx);
            ctx->gtxn->Commit();
        }
    }
}

void GlobalTxnTestTool::DoOpAsyncCallback(RowReader* r) {
    GTxnTestContext* ctx = (GTxnTestContext*)r->GetContext();
    if (r->GetError().GetType() == ErrorCode::kOK) {
        while (!r->Done()) {
            const std::string& result_item = "GET: " 
                + std::to_string(r->GetError().GetType()) + " " 
                + std::to_string(r->Timestamp()) + ":" + r->Value();
            ctx->result.push_back(result_item);
            r->Next();
        }
    } else if (r->GetError().GetType() == ErrorCode::kNotFound) {
        ctx->result.push_back("GET: " + std::to_string(r->GetError().GetType()));
    } else {
        ctx->result.push_back("GET: " + std::to_string(r->GetError().GetType()));
    }
    delete r;
    // if not last, call next operation
    if (++ctx->it != ctx->op_list.end()) {
        const std::string& op_str = *(ctx->it);
        VLOG(12) << "OPERATION:" << op_str;
        OpType next_op_type;
        std::vector<std::string> next_op_args;
        if (!ParseOp(op_str, &next_op_type, &next_op_args)) {
            LOG(ERROR) << "parse op failed";
            delete ctx->gtxn;
            delete ctx;
            done_cnt_.Inc();
            return;
        }
        DoOpAsync(ctx, next_op_type, next_op_args);
    } else {
        ctx->gtxn->SetCommitCallback([] (Transaction* t) {
            ((GTxnTestContext*)t->GetContext())->tool->DoCommitCallback(t);
        });
        ctx->gtxn->SetContext(ctx);
        ctx->gtxn->Commit();
    }
}

void GlobalTxnTestTool::DoCommitCallback(Transaction* t) {
    GTxnTestContext* ctx = (GTxnTestContext*)t->GetContext();
    
    ctx->result.push_back(std::to_string(t->GetError().GetType()));
    if (!CheckResult(ctx->case_num, ctx->gtxn_id, ctx->result)) {
        done_fail_cnt_.Inc();
    } 
    delete ctx;
    delete t;
    done_cnt_.Inc();
}

bool GlobalTxnTestTool::DoOp(tera::Transaction* gtxn, 
                             const OpType& op_type,
                             const std::vector<std::string>& op_args,
                             std::vector<std::string>* result) {
    if (op_args.size() < 4) {
        return false;
    }
    Table* table = nullptr;
    const std::string tablename = op_args[0];
    auto table_it = tables_.find(tablename);
    if (table_it != tables_.end()) {
        table = table_it->second;
    } else {
        return false;
    }
    const std::string row = op_args[1];
    const std::string cf = op_args[2];
    const std::string qu = op_args[3];
    if (op_type == OpType::PUT && op_args.size() == 5) {
        const std::string value = op_args[4];
        std::unique_ptr<tera::RowMutation> m(table->NewRowMutation(row));
        m->Put(cf, qu, value);
        gtxn->ApplyMutation(m.get());
        result->push_back("PUT: " + std::to_string(gtxn->GetError().GetType()));
        return true;
    } else if (op_type == OpType::GET && op_args.size() == 4) {
        std::unique_ptr<tera::RowReader> r(table->NewRowReader(row));
        r->AddColumn(cf, qu);
        gtxn->Get(r.get());
        if (r->GetError().GetType() == ErrorCode::kOK) {
            while (!r->Done()) {
                const std::string& result_item = "GET: " 
                    + std::to_string(r->GetError().GetType()) + " " 
                    + std::to_string(r->Timestamp()) + ":" + r->Value();
                result->push_back(result_item);
                r->Next();
            }
            return true;
        } else if (r->GetError().GetType() == ErrorCode::kNotFound) {
            result->push_back("GET: " + std::to_string(r->GetError().GetType()));
            return true;
        } else {
            result->push_back("GET: " + std::to_string(r->GetError().GetType()));
        }
    } else if (op_type == OpType::DEL && op_args.size() == 4) {
        std::unique_ptr<tera::RowMutation> m(table->NewRowMutation(row));
        m->DeleteColumns(cf, qu);
        gtxn->ApplyMutation(m.get());
        result->push_back("DEL: " + std::to_string(gtxn->GetError().GetType()));
        return true;
    }
    return false;
}

bool GlobalTxnTestTool::ParseOp(const std::string& op_str, 
             OpType* op_type, std::vector<std::string>* op_args) {
    std::vector<std::string> args;
    SplitString(op_str, " ", &args);
    if (TrimString(args[0]) == "PUT") {
        *op_type = OpType::PUT;
    } else if (TrimString(args[0]) == "GET") {
        *op_type = OpType::GET;
    } else if (TrimString(args[0]) == "DEL") {
        *op_type = OpType::DEL;
    } else {
        LOG(ERROR) << "operation type not support :[" << TrimString(args[0]) << "]";
        return false;
    }
    for (size_t i = 1; i < args.size(); ++i) {
        op_args->push_back(TrimString(args[i]));
    }
    return true;
}

void GlobalTxnTestTool::DebugOpList(const std::string& op_list_file) {
    std::vector<std::string> op_list;
    std::ifstream ofile(op_list_file);
    std::string line;
    int cnt = 0;
    while (std::getline(ofile, line)) {
        op_list.push_back(line);
        ++cnt;
    }
    ofile.close();
    if (cnt < 1) {
        LOG(ERROR) << "no operators in op_list";
    }
    std::cout  << "OpList:" << std::endl;
    for (auto l : op_list) {
        std::cout << l <<std::endl;
    } 
    std::cout  << "-------------------------------------------" << std::endl;
}

void GlobalTxnTestTool::DebugFlagFile(const std::string& flag_file) {
    std::vector<std::string> flag_list;
    std::ifstream ofile(flag_file);
    std::string line;
    int cnt = 0;
    while (std::getline(ofile, line)) {
        flag_list.push_back(line);
        ++cnt;
    }
    ofile.close();
    if (cnt < 1) {
        LOG(ERROR) << "no flags in gtxn.flag";
    }
    std::cout  << "FLAGS:" << std::endl;
    for (auto f : flag_list) {
        std::string flag = TrimString(f);
        if (flag.length() > 0 && flag[0] == '#') {
            continue;
        }
        std::cout << flag <<std::endl;
    } 
    std::cout  << "-------------------------------------------" << std::endl;
} 

bool GlobalTxnTestTool::CheckResult(const int case_num, const int gtxn_id, 
                                    const std::vector<std::string>& result) {
    MutexLock lock(&mu_);
    const std::string case_dir = FLAGS_gtxn_test_case_dir;
    const std::string conf_dir = case_dir + std::to_string(case_num) 
                               + "/T_" + std::to_string(gtxn_id);
    std::cout  << "===========================================" << std::endl;
    std::cout  << "CASE:" << case_num << " GTXN_ID:" << gtxn_id << std::endl;
    if (FLAGS_gtxn_test_debug_opened) {
        const std::string& op_list_file = conf_dir + "/op_list";
        const std::string& flag_file = conf_dir + "/gtxn.flag";
        DebugOpList(op_list_file);
        DebugFlagFile(flag_file);
        std::cout  << "Result Printing:" << std::endl;
        for (auto it = result.begin(); it != result.end(); ++it) {
            std::cout << "RESULT:" << *it << std::endl;
        }
        std::cout  << "-------------------------------------------" << std::endl;
    }

    VLOG(12)  << "case:" << case_num 
              << " gtxn_id:" << gtxn_id << " Printing";
    for (auto it = result.begin(); it != result.end(); ++it) {
        VLOG(12) << "RESULT:" << *it;
    }

    const std::string& result_list_file = conf_dir + "/result_list";
    std::vector<std::string> result_list;
    std::ifstream ofile(result_list_file);
    std::string line;
    int cnt = 0;
    while (std::getline(ofile, line)) {
        result_list.push_back(line);
        ++cnt;
    }
    ofile.close();
    if (cnt < 1) {
        LOG(ERROR) << "no results in result_list";
        return false;
    }

    if (result_list.size() != result.size()) {
        std::cout << "\tERROR[expect_line_count: " << result_list.size() << " actual_line_count: " << result.size() << "]\n";
        return false;
    } else {
        int have_diff = 0;
        for (size_t i = 0; i < result.size(); ++i) {
            const std::string& ret = result[i];
            const std::string& default_ret = result_list[i];
            if (TrimString(ret) != TrimString(default_ret)) {
                std::cout << "\tERROR[expect: (" << default_ret << ") actual: (" << ret << ")]\n";
                ++have_diff;
            }
        }
        if (have_diff > 0) {
            std::cout << "FAILED :" << have_diff << std::endl;
            return false;
        }
    }
    std::cout << "SUCCEED" << std::endl;
    return true;
}

bool GlobalTxnTestTool::InitTestTables(int case_num) {
    ErrorCode err;
    std::unordered_map<std::string, TableDescriptor*> table_map;
    for (auto it = case_desc_map_.begin(); it != case_desc_map_.end(); ++it) {
        if (case_num != -1 && case_num != it->first) {
            continue;
        }
        std::vector<TableDescriptor*>& desc_list = it->second;
        for (auto dit = desc_list.begin(); dit != desc_list.end(); ++dit) {
            TableDescriptor* desc = (*dit);
            const std::string& tablename = desc->TableName();
            if (table_map.find(tablename) == table_map.end()) {
                table_map[tablename] = desc;
            }
        }
    }

    for (auto& table : table_map) {
        if (client_->CreateTable(*(table.second), &err) && err.GetType() == ErrorCode::kOK) {
            VLOG(12) << "create table " << table.first << " ok";
        } else {
            LOG(ERROR) << "create table " << table.first << " failed";
            return false;
        }
    }
    return true;
}

bool GlobalTxnTestTool::DropTestTables(int case_num) {
    ErrorCode err;
    std::unordered_map<std::string, TableDescriptor*> table_map;
    for (auto it = case_desc_map_.begin(); it != case_desc_map_.end(); ++it) {
        if (case_num != -1 && case_num != it->first) {
            continue;
        }
        std::vector<TableDescriptor*>& desc_list = it->second;
        for (auto dit = desc_list.begin(); dit != desc_list.end(); ++dit) {
            TableDescriptor* desc = (*dit);
            const std::string& tablename = desc->TableName();
            if (table_map.find(tablename) == table_map.end()) {
                table_map[tablename] = desc;
            }
        }
    }

    for (auto& table : table_map) {
        const std::string& tablename = table.first;
        if (!client_->DisableTable(tablename, &err)) {
            LOG(ERROR) << "disable table failed, table: " << tablename;
            return false;
        }
        TableMeta table_meta;
        TabletMetaList tablet_list;
        tera::ClientImpl* client_impl = static_cast<tera::ClientImpl*>(client_);
        if (!client_impl->ShowTablesInfo(tablename, &table_meta, &tablet_list, &err)) {
            LOG(ERROR) << "table not exist: " << tablename;
            return false;
        }

        uint64_t tablet_num = tablet_list.meta_size();
        while (true) {
            if (!client_impl->ShowTablesInfo(tablename, &table_meta, &tablet_list, &err)) {
                LOG(ERROR) << "table not exist: " << tablename;
                return false;
            }
            uint64_t tablet_cnt = 0;
            for (int32_t i = 0; i < tablet_list.meta_size(); ++i) {
                const TabletMeta& tablet = tablet_list.meta(i);
                if (tablet.status() == TabletMeta::kTabletDisable || 
                    tablet.status() == TabletMeta::kTabletOffline) {
                    tablet_cnt++;
                }
            }
            if (tablet_cnt == tablet_num) {
                // disable finish
                break;
            }
            sleep(1);
        }

        if (!client_->DropTable(tablename, &err)) {
            LOG(ERROR) << "drop table " << tablename << " failed";
            return false;
        }
    }
    return true;
}

void GlobalTxnTestTool::Wait() {
    while(do_cnt_.Get() > done_cnt_.Get()) {
        sleep(1);
    }
}

void GlobalTxnTestTool::RunCaseOneByOne() {
    std::set<int> cases;
    for (auto it = case_list_.begin(); it != case_list_.end(); ++it) {
        CasePair case_pair = *it;
        int case_num = case_pair.first;
        cases.insert(case_num);
    }
    for (auto& case_num : cases) {
        LOG(INFO) << "GlobalTxnTest Case " << case_num << " Begin";
        // drop table
        if (FLAGS_gtxn_test_drop_table_before) {
            DropTestTables(case_num);
        }

        if (!InitTestTables(case_num)) {
            LOG(ERROR) << "GlobalTxnTest Case " << case_num 
                       << " InitTestTables Failed";
            if (FLAGS_ignore_bad_case == true) {
                continue;
            } else {
                break;
            }
        }
        RunTest(client_, case_num);
        Wait();
        LOG(INFO) << "GlobalTxnTest Case " << case_num << " Finish";
        if (done_fail_cnt_.Get() > 0) {
            if (FLAGS_ignore_bad_case == true) {
                continue;
            } else {
                break;
            }
        }
    }
}

} // namespace tera


int main(int argc, char *argv[]){
    ::google::ParseCommandLineFlags(&argc, &argv, true);
    
    if (argc > 1 && std::string(argv[1]) == "version") {
        PrintSystemVersion();
        return 0;
    } 
    if (FLAGS_gtxn_test_conf_dir == "") {
        LOG(ERROR) << "not set \"--gtxn_test_conf_dir\"";
        return -1;
    }
    if (FLAGS_gtxn_test_case_dir == "") {
        LOG(ERROR) << "not set \"--gtxn_test_case_dir\"";
        return -1;
    }
    
    tera::ErrorCode error_code;
    tera::Client* client = tera::Client::NewClient(FLAGS_gtxn_test_conf_dir + "/tera.flag", 
                                                   &error_code);
    if (client == NULL) {
        return -1;
    }

    tera::GlobalTxnTestTool gtxn_test_tool(client);
    // init table
    if (!gtxn_test_tool.LoadTestConf()) {
        return -1;
    } 
    gtxn_test_tool.RunCaseOneByOne();
    return 0;
}
