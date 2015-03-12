// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "tera/io/tablet_writer.h"

#include <vector>

#include "common/base/scoped_ptr.h"
#include "common/base/string_format.h"
#include "common/base/string_number.h"
#include "common/event.h"
#include "common/this_thread.h"
#include "gflags/gflags.h"
#include "glog/logging.h"
#include "gtest/gtest.h"

#include "tera/proto/proto_helper.h"
#include "tera/proto/status_code.pb.h"
#include "tera/proto/tabletnode_rpc.pb.h"
#include "tera/io/tablet_io.h"

DECLARE_string(tera_tabletnode_path_prefix);
DECLARE_int32(tera_asyncwriter_sync_task_threshold);
DECLARE_int32(tera_asyncwriter_sync_record_threshold);
DECLARE_int64(tera_tablet_write_buffer_size);

DECLARE_string(tera_leveldb_env_type);
DECLARE_int32(v);

namespace tera {
namespace io {

const std::string working_dir = "async_writer_testdata/";

class AsyncWriterTest : public ::testing::Test {
public:
    AsyncWriterTest() {
        std::string cmd = std::string("mkdir -p ") + working_dir;
        FLAGS_tera_tabletnode_path_prefix = "./";
        system(cmd.c_str());
    }
    ~AsyncWriterTest() {
        std::string cmd = std::string("rm -rf ") + working_dir;
        system(cmd.c_str());
    }

    void Done() {
        m_callback_count++;
    }

    void CreateTestData(const std::string& table_name,
                        int32_t key_start, int32_t key_end,
                        bool is_sync, bool is_instant,
                        std::vector<TabletWriter::WriteTask>* task_list) {
        for (int32_t i = key_start; i < key_end; ++i) {
            TabletWriter::WriteTask task;
            WriteTabletRequest* request = new WriteTabletRequest();
            WriteTabletResponse* response = new WriteTabletResponse();;
            google::protobuf::Closure* done =
                google::protobuf::NewCallback(this, &AsyncWriterTest::Done);
            request->set_sequence_id(i);
            request->set_tablet_name(table_name);
            request->set_is_sync(is_sync);
            request->set_is_instant(is_instant);
            std::string str = StringFormat("%08llu", i);
            RowMutationSequence* mu_seq = request->add_row_list();
            mu_seq->set_row_key(str);
            Mutation* mutation = mu_seq->add_mutation_sequence();
            mutation->set_type(kPut);
            mutation->set_value(str);

            response->set_status(kTabletNodeOk);
            response->mutable_row_status_list()->Reserve(1);
            response->mutable_row_status_list()->AddAlreadyReserved();
            std::vector<int32_t>* index_list = new std::vector<int32_t>;
            index_list->push_back(0);
            Counter* done_counter = new Counter;
            WriteRpcTimer* write_rpc = new WriteRpcTimer(request, response, done, 1);

            task.request = request;
            task.response = response;
            task.done = done;
            task.index_list = index_list;
            task.done_counter = done_counter;
            task.timer = write_rpc;
            task_list->push_back(task);
        }
    }
    void CreateSingleTestData(const std::string& table_name,
                             int32_t key, int32_t value,
                             bool is_sync, bool is_instant,
                             std::vector<TabletWriter::WriteTask>* task_list) {
        TabletWriter::WriteTask task;
        WriteTabletRequest* request = new WriteTabletRequest();
        WriteTabletResponse* response = new WriteTabletResponse();;
        google::protobuf::Closure* done =
            google::protobuf::NewCallback(this, &AsyncWriterTest::Done);
        request->set_sequence_id(key);
        request->set_tablet_name(table_name);
        request->set_is_sync(is_sync);
        request->set_is_instant(is_instant);
        std::string key_str = StringFormat("%08llu", key);
        std::string value_str = StringFormat("%08llu", value);
        RowMutationSequence* mu_seq = request->add_row_list();
        mu_seq->set_row_key(key_str);
        Mutation* mutation = mu_seq->add_mutation_sequence();
        mutation->set_type(kPut);
        mutation->set_value(value_str);

        response->set_status(kTabletNodeOk);
        response->mutable_row_status_list()->Reserve(1);
        response->mutable_row_status_list()->AddAlreadyReserved();
        std::vector<int32_t>* index_list = new std::vector<int32_t>;
        index_list->push_back(0);
        Counter* done_counter = new Counter;
        WriteRpcTimer* write_rpc = new WriteRpcTimer(request, response, done, 1);

        task.request = request;
        task.response = response;
        task.done = done;
        task.index_list = index_list;
        task.done_counter = done_counter;
        task.timer = write_rpc;
        task_list->push_back(task);
    }

    void CleanTestData(std::vector<TabletWriter::WriteTask> task_list) {
        for (uint32_t i = 0; i < task_list.size(); ++i) {
            delete task_list[i].request;
            delete task_list[i].response;
        }
    }

    void CreateTestTable(const std::string& table_name,
                         const std::vector<TabletWriter::WriteTask>& task_list) {
        std::string tablet_path = working_dir + table_name;
        io::TabletIO tablet;
        EXPECT_TRUE(tablet.Load(TableSchema(), "", "", tablet_path, std::vector<uint64_t>(), std::map<uint64_t, uint64_t>()));
        for (uint32_t i = 0; i < task_list.size(); ++i) {
            EXPECT_TRUE(tablet.Write(task_list[i].request,
                                     task_list[i].response,
                                     task_list[i].done,
                                     task_list[i].index_list,
                                     task_list[i].done_counter,
                                     task_list[i].timer));
        }
        EXPECT_TRUE(tablet.Unload());
    }

    void VerifyOperation(const std::string& table_name,
                         int32_t key_start, int32_t key_end) {
        std::string tablet_path = working_dir + table_name;
        io::TabletIO tablet;
        EXPECT_TRUE(tablet.Load(TableSchema(), "", "", tablet_path, std::vector<uint64_t>(), std::map<uint64_t, uint64_t>()));
        for (int32_t i = key_start; i < key_end; ++i) {
            std::string key = StringFormat("%08llu", i);
            std::string value;
            EXPECT_TRUE(tablet.Read(key, &value));
            EXPECT_EQ(key, value);
        }
        EXPECT_TRUE(tablet.Unload());
    }

    void VerifySingleOperation(const std::string& table_name,
                               int32_t key, int32_t value) {
        std::string tablet_path = working_dir + table_name;
        io::TabletIO tablet;
        EXPECT_TRUE(tablet.Load(TableSchema(), "", "", tablet_path, std::vector<uint64_t>(), std::map<uint64_t, uint64_t>()));
        std::string key_str = StringFormat("%08llu", key);
        std::string value_str = StringFormat("%08llu", value);
        std::string value;
        EXPECT_TRUE(tablet.Read(key_str, &value));
        EXPECT_EQ(value_str, value);
        EXPECT_TRUE(tablet.Unload());
    }
protected:
    uint32_t m_callback_count;
};

TEST_F(AsyncWriterTest, Instant) {
    int32_t start = 0;
    int32_t end = 1000;
    bool is_sync = false;
    bool is_instant = true;
    std::string table_name = "instant";
    std::vector<TabletWriter::WriteTask> task_list;

    CreateTestData(table_name, start, end, is_sync, is_instant,
                   &task_list);
    EXPECT_TRUE(task_list.size() > 0);
    CreateTestTable(table_name, task_list);
    VerifyOperation(table_name, start, end);
    CleanTestData(task_list);
}

TEST_F(AsyncWriterTest, NotInstant) {
    int32_t start = 0;
    int32_t end = 10;
    bool is_sync = false;
    bool is_instant = false;
    std::string table_name = "no_instant";
    std::vector<TabletWriter::WriteTask> task_list;

    CreateTestData(table_name, start, end, is_sync, is_instant,
                   &task_list);
    EXPECT_TRUE(task_list.size() > 0);
    CreateTestTable(table_name, task_list);
    VerifyOperation(table_name, start, end);
    CleanTestData(task_list);
}

TEST_F(AsyncWriterTest, InstantToNotInstant) {
    LOG(INFO) << "InstantToNot";
    std::string table_name = "from_instant_to_not_instant";
    std::vector<TabletWriter::WriteTask> task_list;

    // create test data for instantly return query
    CreateTestData(table_name, 0, 10, false, true, &task_list);
    // create test data for un-instantly return query
    CreateTestData(table_name, 10, 20, false, false, &task_list);
    EXPECT_TRUE(task_list.size() > 0);

    CreateTestTable(table_name, task_list);
    VerifyOperation(table_name, 0, 20);
    CleanTestData(task_list);
}

TEST_F(AsyncWriterTest, NotInstantToInstant) {
    LOG(INFO) << "NotInstantToInstant";
    std::string table_name = "from_not_instant_to_instant";
    std::vector<TabletWriter::WriteTask> task_list;

    // create test data for un-instantly return query
    CreateTestData(table_name, 10, 24, false, false, &task_list);
    // create test data for instantly return query
    CreateTestData(table_name, 0, 10, false, true, &task_list);
    EXPECT_TRUE(task_list.size() > 0);

    CreateTestTable(table_name, task_list);
    VerifyOperation(table_name, 0, 20);
    CleanTestData(task_list);

    LOG(INFO) << "m_callback_count = " << m_callback_count;
}

TEST_F(AsyncWriterTest, KeepOrderForInstant) {
    LOG(INFO) << "KeepOrder";
    std::string table_name = "keep_order";
    std::vector<TabletWriter::WriteTask> task_list;

    CreateTestData(table_name, 0, 2, false, false, &task_list);
    CreateSingleTestData(table_name, 1, 10, false, true, &task_list);
    EXPECT_TRUE(task_list.size() > 0);

    CreateTestTable(table_name, task_list);
//     VerifySingleOperation(table_name, 1, 10);
    CleanTestData(task_list);

    LOG(INFO) << "m_callback_count = " << m_callback_count;
}

TEST_F(AsyncWriterTest, KeepOrderForNotInstant) {
    LOG(INFO) << "KeepOrder";
    std::string table_name = "keep_order";
    std::vector<TabletWriter::WriteTask> task_list;

    CreateTestData(table_name, 0, 2, false, true, &task_list);
    CreateSingleTestData(table_name, 1, 10, false, false, &task_list);
    EXPECT_TRUE(task_list.size() > 0);

    CreateTestTable(table_name, task_list);
    VerifySingleOperation(table_name, 1, 10);
    CleanTestData(task_list);
}

} // namespace io
} // namespace tera

int main(int argc, char** argv) {
    FLAGS_v = 6;
    FLAGS_tera_tablet_write_buffer_size = 2;
    FLAGS_tera_leveldb_env_type = "local";
    ::google::InitGoogleLogging(argv[0]);
    ::google::ParseCommandLineFlags(&argc, &argv, true);
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

