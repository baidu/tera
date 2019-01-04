// Copyright (c) 2015-2018, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include <chrono>
#include <functional>
#include <gtest/gtest.h>
#include <gflags/gflags.h>
#include <thread>
#include "quota/master_quota_entry.h"
#include "master/master_env.h"

DECLARE_string(tera_quota_limiter_type);

namespace tera {
namespace quota {
namespace test {

static const std::string test_table("test");
static const int64_t quota_limit = 3000;
static const int64_t consume_amount = 2000;
static const int64_t quota_period = 2;
static const int64_t default_quota_period = 1;
static const std::string server_addr("abc.baidu.com:9001");

class QuotaBuilder {
 public:
  QuotaBuilder() {}

  virtual ~QuotaBuilder() {}

  static TableQuota BuildTableQuota() {
    // build table_quota
    TableQuota table_quota;
    table_quota.set_table_name(test_table);
    table_quota.set_type(TableQuota::kSetQuota);

    // add write req limit 1s
    QuotaInfo* quota_info = table_quota.add_quota_infos();
    quota_info->set_type(kQuotaWriteReqs);
    quota_info->set_limit(quota_limit);
    quota_info->set_period(default_quota_period);

    // add write bytes 2s
    quota_info = table_quota.add_quota_infos();
    quota_info->set_type(kQuotaWriteBytes);
    quota_info->set_limit(quota_limit);
    quota_info->set_period(quota_period);

    // add read req limit 2s
    quota_info = table_quota.add_quota_infos();
    quota_info->set_type(kQuotaReadReqs);
    quota_info->set_limit(quota_limit);
    quota_info->set_period(quota_period);

    // add read req limit 1s
    quota_info = table_quota.add_quota_infos();
    quota_info->set_type(kQuotaReadBytes);
    quota_info->set_limit(quota_limit);
    quota_info->set_period(default_quota_period);

    // add scan req limit 1s
    quota_info = table_quota.add_quota_infos();
    quota_info->set_type(kQuotaScanReqs);
    quota_info->set_limit(quota_limit);
    quota_info->set_period(default_quota_period);

    // add scan bytes limit 2s
    quota_info = table_quota.add_quota_infos();
    quota_info->set_type(kQuotaScanBytes);
    quota_info->set_limit(quota_limit);
    quota_info->set_period(quota_period);

    return std::move(table_quota);
  }
};

class MasterQuotaEntryTest : public ::testing::Test {
 public:
  MasterQuotaEntryTest()
      : tablet_manager_(new master::TabletManager(nullptr, nullptr, nullptr)),
        tabletnode_manager_(new master::TabletNodeManager(nullptr)) {
    InitMasterEnv();

    TableSchema schema;
    master::TablePtr table_ptr(
        master::TabletManager::CreateTable(test_table, schema, kTableEnable));

    TabletMeta tablet_meta;
    KeyRange* key_range = tablet_meta.mutable_key_range();
    key_range->set_key_start("aaa");
    key_range->set_key_end("zzz");
    tablet_meta.set_path("tablet0001");
    tablet_meta.set_server_addr(server_addr);
    tablet_meta.set_status(TabletMeta::kTabletOffline);
    master::TabletPtr tablet_ptr(new master::Tablet(tablet_meta, table_ptr));
    table_ptr->AddTablet(tablet_meta, nullptr);
    tablet_manager_->AddTable(table_ptr, nullptr);
    tabletnode_manager_->AddTabletNode(server_addr, "123");
  }

  virtual ~MasterQuotaEntryTest() {}

  MasterQuotaEntry* NewMasterQuotaEntry() {
    FLAGS_tera_quota_limiter_type = "general_quota_limiter";
    quota_entry_.reset(new MasterQuotaEntry);
    return quota_entry_.get();
  }

  MasterQuotaEntry* NewNotGeneralMasterQuotaEntry() {
    FLAGS_tera_quota_limiter_type = "not_general_quota_limiter";
    quota_entry_.reset(new MasterQuotaEntry);
    return quota_entry_.get();
  }

  std::shared_ptr<master::TabletManager> GetTabletManager() { return tablet_manager_; }
  std::shared_ptr<master::TabletNodeManager> GetTabletNodeManager() { return tabletnode_manager_; }

 private:
  void InitMasterEnv() {
    master::MasterEnv().Init(nullptr, tabletnode_manager_, tablet_manager_, nullptr, nullptr,
                             nullptr, nullptr, nullptr, nullptr,
                             std::shared_ptr<master::TabletAvailability>(
                                 new master::TabletAvailability(tablet_manager_)),
                             nullptr);
    // push one element to the queue, avoiding call TryMoveTablet while call
    // SuspendMetaOperation
    master::MasterEnv().meta_task_queue_.push(nullptr);
  }

 private:
  std::unique_ptr<MasterQuotaEntry> quota_entry_;
  std::shared_ptr<master::TabletManager> tablet_manager_;
  std::shared_ptr<master::TabletNodeManager> tabletnode_manager_;
};

TEST_F(MasterQuotaEntryTest, AddRecord) {
  MasterQuotaEntry* quota_entry = NewMasterQuotaEntry();
  EXPECT_FALSE(quota_entry->AddRecord("", ""));
  EXPECT_FALSE(quota_entry->AddRecord(test_table, ""));

  TableQuota nullptr_table_quota;
  EXPECT_FALSE(MasterQuotaHelper::NewMetaRecordFromQuota(nullptr_table_quota));

  TableQuota table_quota = QuotaBuilder::BuildTableQuota();
  EXPECT_TRUE(table_quota.type() == TableQuota::kSetQuota);
  std::unique_ptr<master::MetaWriteRecord> meta_write_record(
      MasterQuotaHelper::NewMetaRecordFromQuota(table_quota));
  EXPECT_TRUE(nullptr != meta_write_record.get());
  EXPECT_TRUE("" == MasterQuotaHelper::GetTableNameFromMetaKey(""));
  EXPECT_TRUE(test_table == MasterQuotaHelper::GetTableNameFromMetaKey(meta_write_record->key));

  EXPECT_TRUE(nullptr == MasterQuotaHelper::NewTableQuotaFromMetaValue(""));
  EXPECT_TRUE(nullptr != MasterQuotaHelper::NewTableQuotaFromMetaValue(meta_write_record->value));

  EXPECT_FALSE(quota_entry->AddRecord(meta_write_record->key, meta_write_record->value));

  quota_entry->SetTabletManager(GetTabletManager());
  quota_entry->SetTabletNodeManager(GetTabletNodeManager());
  EXPECT_TRUE(quota_entry->AddRecord(meta_write_record->key, meta_write_record->value));

  EXPECT_TRUE(quota_entry->quota_update_status_ == QuotaUpdateStatus::WaitUpdate);
  EXPECT_TRUE(quota_entry->version_recorder_.NeedUpdate());

  // build request
  QueryRequest req;
  quota_entry->BuildReq(&req, server_addr);
  EXPECT_TRUE(quota_entry->quota_update_status_ == QuotaUpdateStatus::Updating);
  EXPECT_TRUE(req.table_quotas_size() == 1);
  EXPECT_TRUE(quota_entry->delta_ts_table_quotas_list_.size() == 1);
  quota_entry->ClearDeltaQuota();
  EXPECT_TRUE(quota_entry->delta_ts_table_quotas_list_.size() == 0);
  EXPECT_TRUE(quota_entry->quota_update_status_ == QuotaUpdateStatus::FinishUpdated);
}

TEST_F(MasterQuotaEntryTest, CaculateDeltaQuota) {
  MasterQuotaEntry* quota_entry = NewMasterQuotaEntry();
  quota_entry->SetTabletManager(GetTabletManager());
  quota_entry->SetTabletNodeManager(GetTabletNodeManager());
  TableQuota table_quota = QuotaBuilder::BuildTableQuota();
  std::unique_ptr<master::MetaWriteRecord> meta_write_record(
      MasterQuotaHelper::NewMetaRecordFromQuota(table_quota));
  EXPECT_TRUE(quota_entry->AddRecord(meta_write_record->key, meta_write_record->value));
  quota_entry->ClearDeltaQuota();

  quota_entry->CaculateDeltaQuota(test_table);
  EXPECT_TRUE(quota_entry->quota_update_status_ == QuotaUpdateStatus::WaitUpdate);
  EXPECT_TRUE(quota_entry->version_recorder_.NeedUpdate());

  // build request
  QueryRequest req;
  quota_entry->BuildReq(&req, server_addr);
  EXPECT_TRUE(quota_entry->quota_update_status_ == QuotaUpdateStatus::Updating);
  EXPECT_TRUE(req.table_quotas_size() == 1);
  EXPECT_TRUE(quota_entry->delta_ts_table_quotas_list_.size() == 1);
  quota_entry->ClearDeltaQuota();
  EXPECT_TRUE(quota_entry->delta_ts_table_quotas_list_.size() == 0);
  EXPECT_TRUE(quota_entry->quota_update_status_ == QuotaUpdateStatus::FinishUpdated);
}

TEST_F(MasterQuotaEntryTest, DelRecord) {
  MasterQuotaEntry* quota_entry = NewMasterQuotaEntry();
  quota_entry->SetTabletManager(GetTabletManager());
  quota_entry->SetTabletNodeManager(GetTabletNodeManager());
  TableQuota table_quota = QuotaBuilder::BuildTableQuota();
  std::unique_ptr<master::MetaWriteRecord> meta_write_record(
      MasterQuotaHelper::NewMetaRecordFromQuota(table_quota));
  EXPECT_TRUE(quota_entry->AddRecord(meta_write_record->key, meta_write_record->value));
  EXPECT_TRUE(quota_entry->DelRecord(test_table));
}
}
}
}
