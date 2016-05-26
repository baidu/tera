// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#include <fstream>
#include <iostream>
#include <limits>
#include <sstream>

#include <boost/bind.hpp>
#include <gflags/gflags.h>
#include <glog/logging.h>

#include "common/base/string_ext.h"
#include "common/counter.h"
#include "common/mutex.h"
#include "common/timer.h"
#include "common/thread_pool.h"
#include "sdk/tera.h"
#include "version.h"

DEFINE_string(table, "", "table name");
DEFINE_string(column_families, "cf0,cf1,cf2", "column family set");
DEFINE_int64(row_num, 10000, "test row num");
DEFINE_int64(key_set_size, 10000, "key set size");
DEFINE_int64(value_size, 1000, "value size");
DEFINE_int64(pending_sleep_interval, 1, "ms");
DEFINE_int64(pending_num, 100000, "");

DECLARE_string(flagfile);

using namespace tera;
using namespace common::timer;

void Usage(const std::string& prg_name) {
    std::cout << "DESCRIPTION \n\
       rw-consistency-test  \n\
       shared-tableimpl-test\n\
       version \n";
}

static common::Counter w_pending;
static common::Counter w_succ;
static common::Counter w_total;
static common::Counter r_pending;
static common::Counter r_succ;
static common::Counter r_total;
static common::Counter launch_time;

void PrintStat() {
    LOG(INFO) << "Write total " << w_total.Get()
        << " succ " << w_succ.Get() << " pending " << w_pending.Get()
        << ", Read total " << r_total.Get()
        << " succ " << r_succ.Get() << " pending: " << r_pending.Get();
}

class KeySet {
public:
    void Init(const uint32_t key_num) {
        key_num_ = key_num;

        // gen row keys
        while (keys_.size() < key_num) {
            std::stringstream ss;
            ss << (uint64_t)(rand() * rand()) << "abcdefghijklmnopqrstuvwxyz";
            std::string key = ss.str();
            keys_[key] = 0;
            keys_stat_[key] = false;
        }
        CHECK(keys_.size() == key_num);
        srand(get_micros() % 1000000);

        // fill key index_
        std::map<std::string, uint64_t>::iterator it = keys_.begin();
        index_.clear();
        for (; it != keys_.end(); ++it) {
            index_.push_back(&(it->first));
        }

        // fill column families
        SplitString(FLAGS_column_families, ",", &cfs_);
        CHECK(cfs_.size() > 0);
    }

    std::string RandKey() {
        MutexLock l(&mu_);
        return *(index_[rand() % key_num_]);
    }

    std::string RandCF() {
        return cfs_[rand() % cfs_.size()];
    }

    std::string RandIdleKey() {
        MutexLock l(&mu_);
        std::string key = *(index_[rand() % key_num_]);
        if (keys_stat_[key] == false) {
            return key;
        } else {
            // key is busy
            return "";
        }
    }

    void SetKeyStatus(const std::string& key, bool busy_or_not) {
        MutexLock l(&mu_);
        if (busy_or_not) {
            CHECK(!keys_stat_[key]);
            keys_stat_[key] = true;
        } else {
            keys_stat_[key] = false;
        }
    }

    void UpdateTime(const std::string& key, uint64_t ts) {
        MutexLock l(&mu_);
        uint64_t ts_t = keys_[key];
        if (ts_t <= ts) {
            keys_[key] = ts;
        } else {
            uint64_t diff = ts_t - ts;
            LOG(ERROR) << "CONSISTENCY ERROR: " << key << " " << ts_t << " > " << ts
                << ", diff " << diff << "us " << diff / 1000000 << "s.";
            PrintStat();
            _Exit(-10);
        }
        CHECK(keys_.size() == key_num_);
        keys_stat_[key] = false;
    }

private:
    Mutex mu_;
    uint32_t key_num_;
    std::map<std::string, uint64_t> keys_; // key, update time
    std::map<std::string, bool> keys_stat_; // key, reading or not
    std::vector<const std::string*> index_;
    std::vector<std::string> cfs_;
};

KeySet g_key_set;

void ReaderCallBack(RowReader* reader) {
    const ErrorCode& error_code = reader->GetError();
    if (error_code.GetType() == ErrorCode::kOK) {
        //std::cout << reader->RowName() << "\t"
        //    << reader->Timestamp() << std::endl;
        if (reader->Qualifier() != "" && reader->RowName() != reader->Qualifier()) {
            LOG(ERROR) << "CONSISTENCY ERROR: rowkey[" << reader->RowName()
                << "] vs qualifier[" << reader->Qualifier() << "]";
            _Exit(-11);
        }
        g_key_set.UpdateTime(reader->RowName(), reader->Timestamp());
        r_succ.Inc();
    } else if (error_code.GetType() != ErrorCode::kNotFound) {
        //LOG(ERROR) << "exception occured, reason:" << error_code.GetReason()
        //    << ", key: " << reader->RowName();
    } else {
        r_succ.Inc();
    }
    g_key_set.SetKeyStatus(reader->RowName(), false);
    r_total.Inc();
    r_pending.Dec();
    delete reader;
}

void MutationCallBack(RowMutation* mutation) {
    const ErrorCode& error_code = mutation->GetError();
    if (error_code.GetType() != ErrorCode::kOK) {
        //LOG(ERROR) << "exception occured, reason:" << error_code.GetReason()
        //    << ", key: " << mutation->RowKey();
    } else {
        w_succ.Inc();
    }
    w_total.Inc();
    w_pending.Dec();
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
    if (FLAGS_table.empty()) {
        Usage(argv[0]);
        return -1;
    }

    Client* client = Client::NewClient(FLAGS_flagfile, NULL);
    if (client == NULL) {
        LOG(ERROR) << "client instance not exist";
        return -2;
    }

    std::string tablename = FLAGS_table;
    Table* table = client->OpenTable(tablename, err);
    if (table == NULL) {
        LOG(ERROR) << "fail to open table: " << tablename;
        return -3;
    }

    LOG(INFO) << "Write " << FLAGS_key_set_size << " keys to " << FLAGS_table
        << " total.";

    g_key_set.Init(FLAGS_key_set_size);

    uint64_t last_print_time = 0;
    launch_time.Set(get_micros());
    for (int64_t cnt = 0; cnt < FLAGS_row_num; ++cnt) {
        uint64_t cur_ts = get_micros();
        std::string rowkey;
        std::string cf;
        std::string qu;
        std::string value;

        // write
        if (w_pending.Get() < FLAGS_pending_num) {
            rowkey = g_key_set.RandKey();
            cf = g_key_set.RandCF();
            qu = rowkey;
            value = RandomNumString(FLAGS_value_size);
            RowMutation* mutation = table->NewRowMutation(rowkey);
            mutation->Put(cf, qu, value);
            mutation->SetCallBack(MutationCallBack);
            table->ApplyMutation(mutation);
            w_pending.Inc();
        } else {
            usleep(FLAGS_pending_sleep_interval * 1000);
        }

        // read
        rowkey = g_key_set.RandIdleKey();
        if (!rowkey.empty() && r_pending.Get() < FLAGS_pending_num) {
            RowReader* reader = table->NewRowReader(rowkey);
            reader->SetCallBack(ReaderCallBack);
            table->Get(reader);
            g_key_set.SetKeyStatus(rowkey, true);
            r_pending.Inc();
        }
        // while (r_pending.Get() > 10000) {
        //     usleep(100000);
        // }

        // print
        if (cur_ts > last_print_time + 1000000) {
            PrintStat();
            last_print_time = cur_ts;
        }
    }

    while (w_pending.Get() > 0 || r_pending.Get() > 0) {
        usleep(1000000);
        LOG(INFO) << "wait r_pending: " << r_pending.Get()
            << ", w_pending: " << w_pending.Get();
    }

    delete table;
    delete client;
    return 0;
}

int32_t SharedTableImplTask(Client* client, ErrorCode* err) {
    std::string tablename = FLAGS_table;
    Table* table = client->OpenTable(tablename, err);
    if (table == NULL) {
        LOG(ERROR) << "fail to open table: " << tablename;
        return -1;
    }
    delete table;
    return 0;
}

int32_t SharedTableImplTest(int32_t argc, char** argv, ErrorCode* err) {
    if (FLAGS_table.empty()) {
        Usage(argv[0]);
        return -1;
    }

    Client* client = Client::NewClient(FLAGS_flagfile, NULL);
    if (client == NULL) {
        LOG(ERROR) << "client instance not exist";
        return -2;
    }

    ThreadPool thread_pool(100);
    for (int i = 0; i < 1000000; ++i) {
        ThreadPool::Task task =
                boost::bind(&SharedTableImplTask, client, err);
        thread_pool.AddTask(task);
    }
    while (thread_pool.PendingNum() > 0) {
        std::cerr << common::timer::get_time_str(time(NULL)) << " "
            << "waiting for test finish, pending " << thread_pool.PendingNum()
            << " tasks ..." << std::endl;
        sleep(1);
    }
    thread_pool.Stop(true);
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
    } else if (cmd == "shared-tableimpl-test") {
        ret = SharedTableImplTest(argc, argv, &error_code);
    } else if (cmd == "version") {
        PrintSystemVersion();
        ret = 0;
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
