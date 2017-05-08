// Copyright (c) 2016, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "io/tablet_io.h"

#include <fcntl.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <sys/types.h>

#include "gflags/gflags.h"
#include "glog/logging.h"
#include "gtest/gtest.h"

#include "common/base/scoped_ptr.h"
#include "common/base/string_format.h"
#include "common/base/string_number.h"
#include "db/filename.h"
#include "io/tablet_scanner.h"
#include "io/utils_leveldb.h"
#include "leveldb/env_mock.h"
#include "leveldb/raw_key_operator.h"
#include "leveldb/table_utils.h"
#include "proto/proto_helper.h"
#include "proto/status_code.pb.h"
#include "utils/timer.h"
#include "utils/utils_cmd.h"

DECLARE_int32(tera_io_retry_max_times);
DECLARE_int64(tera_tablet_living_period);
DECLARE_int64(tera_tablet_max_write_buffer_size);
DECLARE_string(log_dir);
DECLARE_string(tera_leveldb_env_type);
DECLARE_string(tera_tabletnode_path_prefix);

namespace tera {
namespace io {

static const std::string mock_env_prefix = "mock-env/";
static const std::string working_dir = "testdata/";
static const uint32_t N = 50000;

class TabletIOTest : public ::testing::Test {
public:
    TabletIOTest() {
        std::string cmd = std::string("mkdir -p ") + working_dir;
        FLAGS_tera_tabletnode_path_prefix = "./";
        system(cmd.c_str());

        InitSchema();
    }

    ~TabletIOTest() {
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

    std::map<uint64_t, uint64_t> empty_snaphsots_;
    std::map<uint64_t, uint64_t> empty_rollback_;
    TableSchema schema_;
};

// prepare test data
bool PrepareTestData(TabletIO* tablet, uint64_t e, uint64_t s = 0) {
    leveldb::WriteBatch batch;
    for (uint64_t i = s; i < e; ++i) {
        std::string str = StringFormat("%011llu", i); // NumberToString(i);
        batch.Put(str, str);
    }
    return tablet->WriteBatch(&batch);
}

TEST_F(TabletIOTest, General) {
    std::string tablet_path = working_dir + "general";
    std::string key_start = "";
    std::string key_end = "";
    StatusCode status;

    TabletIO tablet(key_start, key_end, tablet_path);
    leveldb::MockEnv* env = (leveldb::MockEnv*)LeveldbMockEnv();
    env->SetPrefix(mock_env_prefix);
    tablet.SetMockEnv(env);

    leveldb::Logger* ldb_logger;
    leveldb::Status s = leveldb::Env::Default()->NewLogger("./log/leveldblog", &ldb_logger);
    assert(s.ok());
    EXPECT_TRUE(tablet.Load(TableSchema(), tablet_path, std::vector<uint64_t>(),
                            empty_snaphsots_, empty_rollback_, ldb_logger, NULL, NULL, &status));

    std::string key = "555";
    std::string value = "value of 555";

    EXPECT_TRUE(tablet.WriteOne(key, value));

    std::string read_value;

    EXPECT_TRUE(tablet.Read(key, &read_value));

    EXPECT_EQ(value, read_value);

    EXPECT_TRUE(tablet.Unload());

    env->ResetMock();
}

static bool DropCurrent(int32_t t, const std::string& fname) {
    // std::cout << "[DropCurrent]" << t << " " << fname << std::endl;
    if ((t == 1) && (fname == "CURRENT")) {
        return true;
    }
    return false;
}

TEST_F(TabletIOTest, CurrentLost) {
    std::string tablet_path = working_dir + "general";
    std::string key_start = "";
    std::string key_end = "";
    StatusCode status;

    TabletIO tablet(key_start, key_end, tablet_path);
    leveldb::MockEnv* env = (leveldb::MockEnv*)LeveldbMockEnv();
    env->SetPrefix(mock_env_prefix);
    env->SetGetChildrenCallback(DropCurrent);
    tablet.SetMockEnv(env);

    leveldb::Logger* ldb_logger;
    leveldb::Status s = leveldb::Env::Default()->NewLogger("./log/leveldblog", &ldb_logger);
    assert(s.ok());

    ASSERT_FALSE(tablet.Load(TableSchema(), tablet_path, std::vector<uint64_t>(),
                            empty_snaphsots_, empty_rollback_, ldb_logger, NULL, NULL, &status));

    env->ResetMock();
}

//#if 0
static bool CannotReadCurrent(int32_t t, const std::string& fname) {
    // std::cout << "[CannotReadCurrent]" << t << " " << fname << std::endl;
    if ((t == 1) && (fname.find("CURRENT") != std::string::npos)) {
        return true;
    }
    return false;
}

TEST_F(TabletIOTest, CurrentReadFailed) {
    std::string tablet_path = working_dir + "general";
    std::string key_start = "";
    std::string key_end = "";
    StatusCode status;

    TabletIO tablet(key_start, key_end, tablet_path);
    leveldb::MockEnv* env = (leveldb::MockEnv*)LeveldbMockEnv();
    env->SetPrefix(mock_env_prefix);
    env->SetNewSequentialFileFailedCallback(CannotReadCurrent);
    tablet.SetMockEnv(env);

    leveldb::Logger* ldb_logger;
    leveldb::Status s = leveldb::Env::Default()->NewLogger("./log/leveldblog", &ldb_logger);
    assert(s.ok());

    ASSERT_FALSE(tablet.Load(TableSchema(), tablet_path, std::vector<uint64_t>(),
                            empty_snaphsots_, empty_rollback_, ldb_logger, NULL, NULL, &status));

    env->ResetMock();
}

bool ReadCorrputedCurrent(int32_t t, char* scratch, size_t* mock_size) {
    // std::cout << "[ReadCorrputedCurrent]" << t << std::endl;
    if (t != 1) {
        // no  mock
        return false;
    }
    // NOTE: don't fill too many bytes into scratch, otherwise overflow ocurred
    // users need only N bytes in Read
    //       the `N' is not passed to this callback, carefully!
    const char* c = "oops";
    memcpy(scratch, c, strlen(c));
    *mock_size = strlen(c);
    return true;
}

TEST_F(TabletIOTest, CurrentCorrupted) {
    std::string tablet_path = working_dir + "general";
    std::string key_start = "";
    std::string key_end = "";
    StatusCode status;

    TabletIO tablet(key_start, key_end, tablet_path);
    leveldb::MockEnv* env = (leveldb::MockEnv*)LeveldbMockEnv();
    env->SetPrefix(mock_env_prefix);

    env->SetSequentialFileReadCallback(ReadCorrputedCurrent);
    tablet.SetMockEnv(env);

    leveldb::Logger* ldb_logger;
    leveldb::Status s = leveldb::Env::Default()->NewLogger("./log/leveldblog", &ldb_logger);
    assert(s.ok());

    ASSERT_FALSE(tablet.Load(TableSchema(), tablet_path, std::vector<uint64_t>(),
                            empty_snaphsots_, empty_rollback_, ldb_logger, NULL, NULL, &status));

    env->ResetMock();
}

bool ReadCurrentGetNotExistsManifest(int32_t t, char* scratch, size_t* mock_size) {
    // std::cout << "[ReadCurrentGetMockManifest]" << t << std::endl;
    if (t != 1) {
        // no  mock
        return false;
    }
    // NOTE: don't fill too many bytes into scratch, otherwise overflow ocurred
    // users need only N bytes in Read
    //       the `N' is not passed to this callback, carefully!
    const char* c = "MANIFEST-999997\n"; // manifest not exists
    memcpy(scratch, c, strlen(c));
    *mock_size = strlen(c);
    return true;
}

TEST_F(TabletIOTest, ManifestLost) {
    std::string tablet_path = working_dir + "general";
    std::string key_start = "";
    std::string key_end = "";
    StatusCode status;

    TabletIO tablet(key_start, key_end, tablet_path);
    leveldb::MockEnv* env = (leveldb::MockEnv*)LeveldbMockEnv();
    env->SetPrefix(mock_env_prefix);

    env->SetSequentialFileReadCallback(ReadCurrentGetNotExistsManifest);
    tablet.SetMockEnv(env);

    leveldb::Logger* ldb_logger;
    leveldb::Status s = leveldb::Env::Default()->NewLogger("./log/leveldblog", &ldb_logger);
    assert(s.ok());

    ASSERT_FALSE(tablet.Load(TableSchema(), tablet_path, std::vector<uint64_t>(),
                            empty_snaphsots_, empty_rollback_, ldb_logger, NULL, NULL, &status));

    env->ResetMock();
}

static bool CannotReadManifest(int32_t t, const std::string& fname) {
    // std::cout << "[CannotReadCurrent]" << t << " " << fname << std::endl;
    if ((t == 2) && (fname.find("MANIFEST") != std::string::npos)) {
        return true;
    }
    return false;
}

TEST_F(TabletIOTest, ManifestReadFailed) {
    std::string tablet_path = working_dir + "general";
    std::string key_start = "";
    std::string key_end = "";
    StatusCode status;

    TabletIO tablet(key_start, key_end, tablet_path);
    leveldb::MockEnv* env = (leveldb::MockEnv*)LeveldbMockEnv();
    env->SetPrefix(mock_env_prefix);
    env->SetNewSequentialFileFailedCallback(CannotReadManifest);
    tablet.SetMockEnv(env);

    leveldb::Logger* ldb_logger;
    leveldb::Status s = leveldb::Env::Default()->NewLogger("./log/leveldblog", &ldb_logger);
    assert(s.ok());

    ASSERT_FALSE(tablet.Load(TableSchema(), tablet_path, std::vector<uint64_t>(),
                            empty_snaphsots_, empty_rollback_, ldb_logger, NULL, NULL, &status));

    env->ResetMock();
}

bool ReadCorrputedManifest(int32_t t, char* scratch, size_t* mock_size) {
    // std::cout << "[ReadCorrputedManifest]" << t << std::endl;
    if (t != 3) {
        // no  mock
        return false;
    }
    // NOTE: don't fill too many bytes into scratch, otherwise overflow ocurred
    // users need only N bytes in Read
    //       the `N' is not passed to this callback, carefully!
    const char* c = "oops2";
    memcpy(scratch, c, strlen(c));
    *mock_size = strlen(c);
    return true;
}

TEST_F(TabletIOTest, ManifestCorrupted) {
    std::string tablet_path = working_dir + "general";
    std::string key_start = "";
    std::string key_end = "";
    StatusCode status;

    TabletIO tablet(key_start, key_end, tablet_path);
    leveldb::MockEnv* env = (leveldb::MockEnv*)LeveldbMockEnv();
    env->SetPrefix(mock_env_prefix);

    env->SetSequentialFileReadCallback(ReadCorrputedManifest);
    tablet.SetMockEnv(env);

    leveldb::Logger* ldb_logger;
    leveldb::Status s = leveldb::Env::Default()->NewLogger("./log/leveldblog", &ldb_logger);
    assert(s.ok());

    ASSERT_FALSE(tablet.Load(TableSchema(), tablet_path, std::vector<uint64_t>(),
                            empty_snaphsots_, empty_rollback_, ldb_logger, NULL, NULL, &status));

    env->ResetMock();
}

static bool DropSst(int32_t t, const std::string& fname) {
    // std::cout << "[DropSst]" << t << " " << fname << std::endl;
    if ((fname.find(".sst") != std::string::npos)) {
        return true;
    }
    return false;
}

TEST_F(TabletIOTest, SstLost) {
    std::string tablet_path = working_dir + "general";
    std::string key_start = "";
    std::string key_end = "";
    StatusCode status;

    TabletIO tablet(key_start, key_end, tablet_path);
    leveldb::MockEnv* env = (leveldb::MockEnv*)LeveldbMockEnv();
    env->SetPrefix(mock_env_prefix);

    env->SetGetChildrenCallback(DropSst);
    tablet.SetMockEnv(env);

    leveldb::Logger* ldb_logger;
    leveldb::Status s = leveldb::Env::Default()->NewLogger("./log/leveldblog", &ldb_logger);
    assert(s.ok());

    ASSERT_FALSE(tablet.Load(TableSchema(), tablet_path, std::vector<uint64_t>(),
                            empty_snaphsots_, empty_rollback_, ldb_logger, NULL, NULL, &status));

    env->ResetMock();
}

TEST_F(TabletIOTest, SstLostButIgnore) {
    std::string tablet_path = working_dir + "general";
    std::string key_start = "";
    std::string key_end = "";
    StatusCode status;

    TabletIO tablet(key_start, key_end, tablet_path);
    leveldb::MockEnv* env = (leveldb::MockEnv*)LeveldbMockEnv();

    std::string fname = mock_env_prefix + tablet_path + "/0/__oops";
    int fd = open(fname.c_str(), O_RDWR | O_CREAT);
    if (fd == -1) {
        std::cout << strerror(errno) << fname << std::endl;
        abort();
    }
    env->SetPrefix(mock_env_prefix);

    env->SetGetChildrenCallback(DropSst);
    tablet.SetMockEnv(env);

    leveldb::Logger* ldb_logger;
    leveldb::Status s = leveldb::Env::Default()->NewLogger("./log/leveldblog", &ldb_logger);
    assert(s.ok());

    ASSERT_TRUE(tablet.Load(TableSchema(), tablet_path, std::vector<uint64_t>(),
                            empty_snaphsots_, empty_rollback_, ldb_logger, NULL, NULL, &status));

    env->ResetMock();
    close(fd);
}
//#endif

} // namespace io
} // namespace tera

int main(int argc, char** argv) {
    FLAGS_tera_io_retry_max_times = 1;
    FLAGS_tera_tablet_living_period = 0;
    FLAGS_tera_tablet_max_write_buffer_size = 1;
    // FLAGS_tera_leveldb_env_type = "local";
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
