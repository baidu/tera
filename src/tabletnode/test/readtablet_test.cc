// Copyright (c) 2018, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
#include <memory>
#include <atomic>
#include "gmock/gmock.h"

#include "io/mock_tablet_io.h"
#include "mock_tablet_manager.h"
#include "tabletnode/tabletnode_impl.h"
#include "common/thread_pool.h"

using ::testing::Invoke;
using ::testing::Return;
using ::testing::_;

namespace tera {
namespace tabletnode {
class ReadTabletTest : public ::testing::Test {
 public:
  ReadTabletTest()
      : mock_tablet_manager_(std::make_shared<MockTabletManager>()), thread_pool_(new ThreadPool) {}

 private:
  std::shared_ptr<MockTabletManager> mock_tablet_manager_;
  io::MockTabletIO mock_io_;
  ThreadPool* thread_pool_;
  ReadTabletResponse res_;
  ReadTabletRequest req_;
  AutoResetEvent done_event_;
  std::atomic<int64_t> done_time{0};

 public:
  io::TabletIO* GetTablet(const std::string& table_name, const std::string& key,
                          StatusCode* status) {
    *status = StatusCode::kTabletNodeOk;
    mock_io_.AddRef();
    return &mock_io_;
  }

  bool ReadCells(const RowReaderInfo& row_reader, RowResult* value_list, uint64_t snapshot_id,
                 StatusCode* status, int64_t timeout_ms) {
    return true;
  }

  bool ReadCellsInc(const RowReaderInfo& row_reader, RowResult* value_list, uint64_t snapshot_id,
                    StatusCode* status, int64_t timeout_ms) {
    auto kv = value_list->add_key_values();
    kv->set_key(row_reader.key());
    kv->set_value(std::to_string(std::stoi(row_reader.key()) + 1));
    return true;
  }

  bool ReadCellsWithNull(const RowReaderInfo& row_reader, RowResult* value_list,
                         uint64_t snapshot_id, StatusCode* status, int64_t timeout_ms) {
    int key = std::stoi(row_reader.key());
    if (key % 3 == 0) {
      *status = kKeyNotExist;
      return false;
    } else {
      auto kv = value_list->add_key_values();
      kv->set_key(row_reader.key());
      kv->set_value(row_reader.key());
      return true;
    }
  }

  bool ReadCellsTimeout(const RowReaderInfo& row_reader, RowResult* value_list,
                        uint64_t snapshot_id, StatusCode* status, int64_t timeout_ms) {
    SetStatusCode(kRPCTimeout, status);
  }

  void BaseDone() {
    EXPECT_EQ(res_.sequence_id(), req_.sequence_id());
    EXPECT_EQ(res_.status(), kTabletNodeOk);
    EXPECT_EQ(res_.detail().status_size(), req_.row_info_list_size());
    EXPECT_EQ(done_time.fetch_add(1), 0);
    done_event_.Set();
  }

  void CheckIncDone() {
    EXPECT_EQ(res_.sequence_id(), req_.sequence_id());
    EXPECT_EQ(res_.status(), kTabletNodeOk);
    EXPECT_EQ(res_.detail().status_size(), req_.row_info_list_size());
    EXPECT_EQ(done_time.fetch_add(1), 0);
    EXPECT_GT(res_.detail().row_result_size(), 0);
    int base_key = std::stoi(res_.detail().row_result(0).key_values(0).key());
    int base_value = std::stoi(res_.detail().row_result(0).key_values(0).value());
    EXPECT_EQ(base_key + 1, base_value);
    for (int i = 1; i < res_.detail().row_result_size(); ++i) {
      int key = std::stoi(res_.detail().row_result(i).key_values(0).key());
      int value = std::stoi(res_.detail().row_result(i).key_values(0).value());
      EXPECT_EQ(key, base_key + 1);
      EXPECT_EQ(value, base_value + 1);
      base_key = key;
      base_value = value;
    }
    done_event_.Set();
  }

  void CheckNullDone() {
    EXPECT_EQ(res_.sequence_id(), req_.sequence_id());
    EXPECT_EQ(res_.status(), kTabletNodeOk);
    EXPECT_EQ(res_.detail().status_size(), req_.row_info_list_size());
    EXPECT_EQ(done_time.fetch_add(1), 0);
    EXPECT_GT(res_.detail().row_result_size(), 0);
    int row_result_index = 0;
    for (int i = 0; i < res_.detail().status_size(); ++i) {
      if (i % 3 == 0) {
        EXPECT_EQ(res_.detail().status(i), kKeyNotExist);
      } else {
        EXPECT_EQ(res_.detail().status(i), kTabletNodeOk);
        EXPECT_EQ(res_.detail().row_result(row_result_index).key_values(0).key(),
                  res_.detail().row_result(row_result_index).key_values(0).value());
        ++row_result_index;
      }
    }
    done_event_.Set();
  }

  void CheckTimeoutDone() {
    EXPECT_EQ(res_.sequence_id(), req_.sequence_id());
    EXPECT_EQ(res_.status(), kRPCTimeout);
    EXPECT_EQ(res_.detail().status_size(), req_.row_info_list_size());
    EXPECT_EQ(done_time.fetch_add(1), 0);
    EXPECT_EQ(res_.detail().row_result_size(), 0);
    done_event_.Set();
  }

  google::protobuf::Closure* CreateDone(void (ReadTabletTest::*p)()) {
    return google::protobuf::NewCallback(this, p);
  }
};

TEST_F(ReadTabletTest, ParallelReadTablet) {
  EXPECT_CALL(*mock_tablet_manager_, GetTablet(_, _, _))
      .WillRepeatedly(Invoke(this, &ReadTabletTest::GetTablet));
  EXPECT_CALL(mock_io_, ReadCells(_, _, _, _, _))
      .WillRepeatedly(Invoke(this, &ReadTabletTest::ReadCellsInc));

  for (int i = 0; i != 280; ++i) {
    auto row_info = req_.add_row_info_list();
    row_info->set_key(std::to_string(i));
  }
  req_.set_sequence_id(2);
  req_.set_tablet_name("read_table");

  std::shared_ptr<ReadTabletTask> task =
      std::make_shared<ReadTabletTask>(0, mock_tablet_manager_, &req_, &res_,
                                       CreateDone(&ReadTabletTest::CheckIncDone), thread_pool_);
  task->StartRead();
  done_event_.Wait();
  EXPECT_EQ(task->row_results_list_.size(), 10);
}

TEST_F(ReadTabletTest, ParallelReadTabletWithNullValue) {
  EXPECT_CALL(*mock_tablet_manager_, GetTablet(_, _, _))
      .WillRepeatedly(Invoke(this, &ReadTabletTest::GetTablet));
  EXPECT_CALL(mock_io_, ReadCells(_, _, _, _, _))
      .WillRepeatedly(Invoke(this, &ReadTabletTest::ReadCellsWithNull));

  for (int i = 0; i != 180; ++i) {
    auto row_info = req_.add_row_info_list();
    row_info->set_key(std::to_string(i));
  }
  req_.set_sequence_id(2);
  req_.set_tablet_name("read_table");

  std::shared_ptr<ReadTabletTask> task =
      std::make_shared<ReadTabletTask>(0, mock_tablet_manager_, &req_, &res_,
                                       CreateDone(&ReadTabletTest::CheckNullDone), thread_pool_);
  task->StartRead();
  done_event_.Wait();
  EXPECT_EQ(task->row_results_list_.size(), 6);
}

TEST_F(ReadTabletTest, ReadTabletInOneThread) {
  EXPECT_CALL(*mock_tablet_manager_, GetTablet(_, _, _))
      .WillRepeatedly(Invoke(this, &ReadTabletTest::GetTablet));
  EXPECT_CALL(mock_io_, ReadCells(_, _, _, _, _))
      .WillRepeatedly(Invoke(this, &ReadTabletTest::ReadCells));

  req_.set_sequence_id(1);
  req_.set_tablet_name("read_table");
  RowReaderInfo* row_info = req_.add_row_info_list();
  row_info->set_key("key");

  std::shared_ptr<ReadTabletTask> task = std::make_shared<ReadTabletTask>(
      0, mock_tablet_manager_, &req_, &res_, CreateDone(&ReadTabletTest::BaseDone), thread_pool_);
  task->StartRead();
  done_event_.Wait();
  EXPECT_EQ(task->row_results_list_.size(), 1);
}

TEST_F(ReadTabletTest, ParallelReadTimeout) {
  EXPECT_CALL(*mock_tablet_manager_, GetTablet(_, _, _))
      .WillRepeatedly(Invoke(this, &ReadTabletTest::GetTablet));
  EXPECT_CALL(mock_io_, ReadCells(_, _, _, _, _))
      .WillRepeatedly(Invoke(this, &ReadTabletTest::ReadCellsTimeout));

  req_.set_sequence_id(1);
  req_.set_tablet_name("read_table");
  for (int i = 0; i != 360; ++i) {
    auto row_info = req_.add_row_info_list();
    row_info->set_key(std::to_string(i));
  }

  std::shared_ptr<ReadTabletTask> task =
      std::make_shared<ReadTabletTask>(0, mock_tablet_manager_, &req_, &res_,
                                       CreateDone(&ReadTabletTest::CheckTimeoutDone), thread_pool_);
  task->StartRead();
  done_event_.Wait();
  EXPECT_EQ(task->row_results_list_.size(), 10);
}

}  // namespace tabletnode
}  // namespace tera

int main(int argc, char** argv) {
  ::google::InitGoogleLogging(argv[0]);
  ::google::ParseCommandLineFlags(&argc, &argv, true);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
