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
#include <iostream>
#include <limits>
#include <sstream>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "common/counter.h"
#include "common/thread_pool.h"
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
#include "sdk/sdk_utils.h"
#include "sdk/sdk_zk.h"
#include "sdk/table_impl.h"
#include "sdk/tera.h"
#include "utils/crypt.h"
#include "utils/string_util.h"
#include "utils/tprinter.h"
#include "utils/utils_cmd.h"
#include "version.h"

DECLARE_string(flagfile);

DEFINE_string(table, "", "table name");
DEFINE_string(cf, "cf0", "table name");
DEFINE_int64(row_num, 10000, "test row num");
DEFINE_int64(value_size, 1000, "value size");

using namespace tera;

void Usage(const std::string& prg_name) {
    std::cout << "DESCRIPTION \n\
       rw-consistency-test  \n";
}

static common::Counter w_pending;
static common::Counter w_total;
static common::Counter r_pending;
static common::Counter r_total;
static common::Counter last_print_time;
static common::Counter last_read_timestamp;

void ReaderCallBack(RowReader* reader) {
    const ErrorCode& error_code = reader->GetError();
    if (error_code.GetType() == ErrorCode::kOK) {
        while (!reader->Done()) {
            //std::cout << reader->RowName() << "\t"
            //    << reader->Timestamp() << std::endl;
            int64_t diff = last_read_timestamp.Get() - reader->Timestamp();
            if (last_read_timestamp.Get() > reader->Timestamp()) {
                LOG(ERROR) << "ERROR: " << last_read_timestamp.Get()
                    << " vs " << reader->Timestamp()
                    << ", diff " << diff << "us.";
                if (diff > 1000) { // 1ms
                    exit(-10);
                }
            }

            last_read_timestamp.Set(reader->Timestamp());
            reader->Next();
        }
    } else {
        LOG(ERROR) << "exception occured, reason:" << error_code.GetReason()
            << ", key: " << reader->RowName();
    }
    r_total.Inc();
    r_pending.Dec();

    delete reader;
}

void RWConsistencyTestCallBack(RowMutation* mutation) {
    const ErrorCode& error_code = mutation->GetError();
    if (error_code.GetType() != ErrorCode::kOK) {
        LOG(ERROR) << "exception occured, reason:" << error_code.GetReason()
            << ", key: " << mutation->RowKey();
    }
    w_total.Inc();
    w_pending.Dec();

    int32_t time_cur = time(NULL);
    if (time_cur > last_print_time.Get()) {
        last_print_time.Set(time_cur);
        LOG(ERROR) << "Write "<< w_total.Get() <<" keys total, "
            << "wait r_pending: " << r_pending.Get()
            << ", w_pending: " << w_pending.Get();
    }
    delete mutation;
}

std::string RandomNumString(int32_t size){
    std::stringstream ss;
    for(int i = 0; i != size; ++i) {
        ss << rand() % 10;
    }
    return ss.str();
}
int32_t RWConsistencyTest(int32_t argc, char** argv, ErrorCode* err) {
    // rowkey set: kRowSetSize
    if (FLAGS_table.empty()) {
        Usage(argv[0]);
        return -1;
    }

    const uint32_t kRowSetSize = 1000;
    std::vector<uint64_t> row_set;
    for (uint32_t i = 0; i < kRowSetSize; ++i) {
        int64_t key = rand() * rand();
        row_set.push_back(key);
        CHECK(row_set.size() == i + 1);
    }

    Client* client = Client::NewClient(FLAGS_flagfile, NULL);
    if (client == NULL) {
        LOG(ERROR) << "client instance not exist";
        return -2;
    }

    std::string tablename = FLAGS_table;
    Table* table = NULL;
    if ((table = client->OpenTable(tablename, err)) == NULL) {
        LOG(ERROR) << "fail to open table: " << tablename;
        return -3;
    }

    std::stringstream ss;
    ss << row_set[rand() % kRowSetSize] << "abcdefghijklmnopqrstuvwxyz";
    string read_key = ss.str();
    LOG(ERROR) << "Write " << kRowSetSize << " keys to " << FLAGS_table
        << " total, Read key: " << read_key;

    for (int64_t cnt = 0; cnt < FLAGS_row_num; ++cnt) {
        std::stringstream ss;
        ss << row_set[rand() % kRowSetSize] << "abcdefghijklmnopqrstuvwxyz";
        std::string rowkey = ss.str();
        std::string columnfamily = FLAGS_cf;
        std::string qualifier = "";
        std::string value = RandomNumString(FLAGS_value_size);

        // write
        RowMutation* mutation = table->NewRowMutation(rowkey);
        mutation->Put(columnfamily, qualifier, value);
        mutation->SetCallBack(RWConsistencyTestCallBack);
        table->ApplyMutation(mutation);
        w_pending.Inc();
        while (w_pending.Get() > 100000) {
            usleep(100000);
        }

        // read
        if (r_pending.Get() == 0) {
            RowReader* reader = table->NewRowReader(read_key);
            reader->SetCallBack(ReaderCallBack);
            table->Get(reader);
            r_pending.Inc();
            // while (r_pending.Get() > 100000) {
            //     usleep(100000);
            // }
        }
    }

    while (w_pending.Get() > 0 || r_pending.Get() > 0) {
        usleep(1000000);
        LOG(ERROR) << "wait r_pending: " << r_pending.Get()
            << ", w_pending: " << w_pending.Get();
    }

    delete table;
    delete client;
    return 0;
}

int ExecuteCommand(int argc, char* argv[]) {
    int ret = 0;
    ErrorCode error_code;
    if (argc <= 1) {
        Usage(argv[0]);
        return 0;
    }
    std::string cmd = argv[1];
    if (cmd == "rw-consistency-test") {
        ret = RWConsistencyTest(argc, argv, &error_code);
    } else {
        Usage(argv[0]);
        return -1;
    }
    return ret;
}

int main(int argc, char* argv[]) {
    ::google::ParseCommandLineFlags(&argc, &argv, true);
    int ret = ExecuteCommand(argc, argv);
    return ret;
}
