// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "io/tablet_io.h"

#include <stdlib.h>

#include "gflags/gflags.h"
#include "glog/logging.h"
#include "gtest/gtest.h"

#include "common/base/scoped_ptr.h"
#include "common/base/string_format.h"
#include "common/base/string_number.h"
#include "db/filename.h"
#include "leveldb/raw_key_operator.h"
#include "leveldb/table_utils.h"
#include "proto/proto_helper.h"
#include "proto/status_code.pb.h"
#include "utils/timer.h"
#include "utils/utils_cmd.h"
#include "io/tablet_scanner.h"

DECLARE_string(tera_tabletnode_path_prefix);
DECLARE_int32(tera_io_retry_max_times);
DECLARE_int64(tera_tablet_living_period);
DECLARE_string(tera_leveldb_env_type);

DECLARE_int64(tera_tablet_max_write_buffer_size);
DECLARE_string(log_dir);

namespace tera {
namespace io {

const std::string working_dir = "testdata/";
class TabletScannerTest : public ::testing::Test {
public:
    TabletScannerTest() {
        std::string cmd = std::string("mkdir -p ") + working_dir;
        FLAGS_tera_tabletnode_path_prefix = "./";
        system(cmd.c_str());
        InitSchema();
    }

    ~TabletScannerTest() {
         std::string cmd = std::string("rm -rf ") + working_dir;
         system(cmd.c_str());
    }

    const TableSchema& GetTableSchema() {
        return schema_;
    }

    void InitSchema() {
        schema_.set_name("tera");
        schema_.set_raw_key(Binary);

        LocalityGroupSchema* lg = schema_.add_locality_groups();
        lg->set_name("lg0");

        ColumnFamilySchema* cf = schema_.add_column_families();
        cf->set_name("column");
        cf->set_locality_group("lg0");
        cf->set_max_versions(3);
    }

    void NewRpcRequestDone(ScanTabletRequest* request, ScanTabletResponse* response) {
        uint32_t size = response->results().key_values_size();
        for (uint32_t i = 0; i < size; i++) {
            const tera::KeyValuePair& row = response->results().key_values(i);
            //LOG(INFO) << row.key() << ":" << row.column_family() << ":" << row.qualifier() << ":" << row.value();
            std::string last_key = StringFormat("%011llu", last_key_); // NumberToString(500);
            EXPECT_TRUE(last_key == row.key());
            last_key_++;
        }
        if (size == 0) {
            LOG(INFO) << "req[" << done_cnt_ << "] scan done";
        }
        done_cnt_++;
        if (req_vec.size() == done_cnt_) {
            for (uint32_t j = 0; j < done_cnt_; j++) {
                delete req_vec[j];
                delete resp_vec[j];
            }
            req_vec.clear();
            resp_vec.clear();
            done_vec.clear();
        }
    }

    void NewRpcRequest(uint64_t nr_req, uint64_t s, uint64_t e) {
        std::string start_key = StringFormat("%011llu", s); // NumberToString(500);
        std::string end_key = StringFormat("%011llu", e); // NumberToString(500);
        session_id_ = get_micros();
        uint64_t ts = get_micros();

        last_key_ = s;
        for (uint32_t i = 0; i < nr_req; i++) {
            ScanTabletRequest* request = new ScanTabletRequest;
            ScanTabletResponse* response = new ScanTabletResponse;
            google::protobuf::Closure* done =
                google::protobuf::NewCallback(this, &TabletScannerTest::NewRpcRequestDone, request, response);

            request->set_part_of_session(true);
            if (i == 0) {
                request->set_part_of_session(false);
            }
            request->set_session_id(session_id_);
            request->set_sequence_id(100);
            request->set_table_name(schema_.name());
            request->set_start(start_key);
            request->set_end(end_key);
            request->set_snapshot_id(0);
            request->set_timeout(5000);
            request->set_buffer_limit(65536);
            request->set_snapshot_id(0);
            request->set_max_version(1);
            TimeRange* time_range = request->mutable_timerange();
            time_range->set_ts_start(0);
            time_range->set_ts_end(ts);
            request->set_timestamp(ts);

            req_vec.push_back(request);
            resp_vec.push_back(response);
            done_vec.push_back(done);
        }
    }

    // prepare test data
    void PrepareData(TabletIO* tablet, uint64_t e, uint64_t s = 0) {
        leveldb::WriteBatch batch;
        for (uint64_t i = s; i < e; ++i) {
            std::string str = StringFormat("%011llu", i); // NumberToString(i);

            std::string key;
            tablet->GetRawKeyOperator()->EncodeTeraKey(str, "column", "qualifer", get_micros(), leveldb::TKT_VALUE, &key);
            batch.Put(key, str);
        }
        EXPECT_TRUE(tablet->WriteBatch(&batch));
        return;
    }

public:
    uint64_t session_id_;

    std::vector<ScanTabletRequest*> req_vec;
    std::vector<ScanTabletResponse*> resp_vec;
    std::vector<google::protobuf::Closure*> done_vec;
    uint64_t done_cnt_;
    uint64_t last_key_;

    std::map<uint64_t, uint64_t> empty_snaphsots_;
    std::map<uint64_t, uint64_t> empty_rollback_;
    TableSchema schema_;
};

TEST_F(TabletScannerTest, General) {
    std::string tablet_path = working_dir + "general";
    std::string key_start = "";
    std::string key_end = "";
    StatusCode status;

    TabletIO tablet(key_start, key_end);
    EXPECT_TRUE(tablet.Load(GetTableSchema(), tablet_path, std::vector<uint64_t>(),
                            empty_snaphsots_, empty_rollback_, NULL, NULL, NULL, &status));

    PrepareData(&tablet, 1000000);
    uint64_t nr = 400;
    NewRpcRequest(nr, 5, 500000);

    for (uint32_t i = 0; i < nr; i++) {
        tablet.ScanRows(req_vec[i], resp_vec[i], done_vec[i]);
    }

    EXPECT_TRUE(tablet.Unload());
}

} // namespace io
} // namespace tera

int main(int argc, char** argv) {
    FLAGS_tera_io_retry_max_times = 1;
    FLAGS_tera_tablet_living_period = 0;
    FLAGS_tera_tablet_max_write_buffer_size = 1;
    FLAGS_tera_leveldb_env_type = "local";
    ::google::InitGoogleLogging(argv[0]);
    FLAGS_log_dir = "./log";
    if (access(FLAGS_log_dir.c_str(), F_OK)) {
        mkdir(FLAGS_log_dir.c_str(), 0777);
    }
    std::string pragram_name("tera");
    tera::utils::SetupLog(pragram_name);
    ::google::ParseCommandLineFlags(&argc, &argv, true);
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

