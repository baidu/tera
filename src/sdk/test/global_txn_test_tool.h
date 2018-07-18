// Copyright (c) 2015-2017, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef  TERA_SDK_TEST_GLOBAL_TXN_TEST_TOOL_H_
#define  TERA_SDK_TEST_GLOBAL_TXN_TEST_TOOL_H_

#include <string>

#include "common/thread_pool.h"
#include "common/counter.h"
#include "tera.h"

namespace tera {

enum OpType { GET, PUT, DEL };

class GlobalTxnTestTool;


struct GTxnTestContext {
    GlobalTxnTestTool* tool;
    tera::Transaction* gtxn;
    std::vector<std::string> op_list;
    std::vector<std::string> result;
    std::vector<std::string>::iterator it;
    int case_num;
    int gtxn_id;
};

class GlobalTxnTestTool {
public:
    GlobalTxnTestTool(Client* client);
    ~GlobalTxnTestTool(){}

    bool LoadTestConf();

    bool InitTestTables(int case_num = -1);

    bool DropTestTables(int case_num = -1);

    void RunTest(tera::Client* client, int case_num = -1);

    void Wait();

    void RunCaseOneByOne();
private:
    void RunTestInternal(tera::Client* client, const int case_num, const int gtxn_id, 
                         const std::vector<std::string>& op_list);

    void CaseRegister(const int case_num, const int gtxn_id);

    bool LoadDescriptor(const std::string& schema_file, TableDescriptor* schema);

    void DebugOpList(const std::string& op_list_file);

    void DebugFlagFile(const std::string& flag_file);
    
    bool CheckResult(const int case_num, const int gtxn_id, 
                     const std::vector<std::string>& result);

    bool ParseOp(const std::string& op_str, 
                 OpType* op_type, std::vector<std::string>* op_args);

    bool DoOp(tera::Transaction* gtxn, 
              const OpType& op_type, 
              const std::vector<std::string>& op_args,
              std::vector<std::string>* result); 

    void DoOpAsync(GTxnTestContext* ctx, const OpType& op_type, 
                   const std::vector<std::string>& op_args);

    void DoOpAsyncCallback(tera::RowReader* r);

    void DoCommitCallback(tera::Transaction* t);

    bool OpenTestTables(const std::vector<std::string>& tables);

private:
    typedef std::pair<int, int> CasePair;
    std::vector<CasePair> case_list_;
    typedef std::map<int, std::vector<TableDescriptor*>> CaseDescMap;
    CaseDescMap case_desc_map_;
    std::map<std::string, Table*> tables_;
    mutable Mutex mu_;
    common::ThreadPool thread_pool_;
    Client* client_;
    Counter do_cnt_;
    Counter done_cnt_;
    Counter done_fail_cnt_;
};

} // namespace tera

#endif  // TERA_SDK_TEST_GLOBAL_TXN_TEST_TOOL_H_
