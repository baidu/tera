// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include <signal.h>

#include "gflags/gflags.h"
#include "glog/logging.h"
#include "gtest/gtest.h"

#include "common/base/scoped_ptr.h"
#include "master/master_impl.h"
#include "master/tablet_manager.h"
#include "utils/utils_cmd.h"
#include "version.h"

DECLARE_string(tera_master_port);
DECLARE_string(log_dir);
DECLARE_string(tera_coord_type);
DECLARE_string(tera_leveldb_env_type);

namespace tera {
namespace master {

class MasterImplTest : public ::testing::Test, public MasterImpl {
public:
    MasterImplTest() : merge_enter_phase2(false) {
        FLAGS_tera_coord_type = "fake_zk";
        FLAGS_tera_leveldb_env_type = "local";
    }

    bool merge_enter_phase2;

    virtual void MergeTabletAsyncPhase2(TabletPtr tablet_p1, TabletPtr tablet_p2) {
        merge_enter_phase2 = true;
    }

    void MergeTabletTest() {
        TabletMeta meta;
        TablePtr table(new Table("mergetest"));
        TabletPtr tablet_p1(new Tablet(meta, table));
        TabletPtr tablet_p2(new Tablet(meta, table));
        tablet_p1->SetStatus(kTableReady);
        tablet_p2->SetStatus(kTableReady);
        tablet_p1->SetStatus(kTableUnLoading);
        tablet_p2->SetStatus(kTableUnLoading);
        tablet_p1->SetAddr("ts1");
        tablet_p2->SetAddr("ts2");

        MutexPtr mu(new Mutex());
        MergeParam* param1 = new MergeParam(mu, tablet_p2);
        MergeParam* param2 = new MergeParam(mu, tablet_p1);
        tablet_p1->SetMergeParam(param1);
        tablet_p2->SetMergeParam(param2);

        UnloadTabletRequest* request = new UnloadTabletRequest;
        UnloadTabletResponse* response = new UnloadTabletResponse;
        int32_t retry = 0;
        bool failed = false;
        int error_code = 0;
        response->set_status(kTabletNodeOk);

        // ts1 unload success, ts2 server down
        tabletnode_manager_->AddTabletNode("ts1", "");
        UnloadTabletCallback(tablet_p1, retry, request, response, failed, error_code);

        request = new UnloadTabletRequest;
        response = new UnloadTabletResponse;
        UnloadTabletCallback(tablet_p2, retry, request, response, failed, error_code);
        EXPECT_TRUE(merge_enter_phase2);
    }

    TabletPtr MakeTabletPtr(const std::string& start, const std::string& end, TablePtr table) {
        TabletMeta meta;
        meta.mutable_key_range()->set_key_start(start);
        meta.mutable_key_range()->set_key_end(end);
        TabletPtr tablet(new Tablet(meta, table));
        return tablet;
    }

    void DeleteTabletNodeTest() {
        // add server
        std::string addr1 = "127.0.0.1:22000";
        std::string addr2 = "127.0.0.2:22000";
        tabletnode_manager_->AddTabletNode(addr1, addr1);
        tabletnode_manager_->AddTabletNode(addr2, addr2);

        // add tabelt
        StatusCode s;
        TabletMeta meta;
        TablePtr table(new Table("table001"));
        TabletPtr tablet = MakeTabletPtr("a", "z", table);
        tablet->SetStatus(kTableReady);
        tablet->SetAddr(addr1);
        tablet->ToMeta(&meta);
        tablet_manager_->AddTablet(meta, tablet->GetSchema(), &tablet, &s);
        tablet->SetServerId(addr1);

        // thread1: get tablet from addr1
        std::vector<TabletPtr> tablet_list;
        std::vector<TabletPtr>::iterator it;
        tablet_manager_->FindTablet(addr1, &tablet_list, true);
        EXPECT_TRUE(it != tablet_list.end());
        EXPECT_TRUE(tablet_list.size() == 1);

        // thread2: load tablet into addr2
        LoadTabletRequest* request = new LoadTabletRequest;
        LoadTabletResponse* response = new LoadTabletResponse;
        tablet->SetAddr(addr2);
        tablet->SetServerId(addr2);

        TabletNodePtr node;
        tabletnode_manager_->FindTabletNode(addr2, &node);
        node->TryLoad(tablet);
        tablet->SetStatus(kTableOffLine);
        tablet->SetStatus(kTableOnLoad);
        response->set_status(kTabletNodeOk);
        LoadTabletCallback(tablet, 10, request, response, 0, 0);
        EXPECT_TRUE(tablet->GetStatus() == kTableReady);

        // thread1: check addr1 and set status
        for (it = tablet_list.begin(); it != tablet_list.end(); ++it) {
            TabletPtr t = *it;
            t->SetStatusIf(kTabletPending, kTableReady, addr1);
        }
        EXPECT_TRUE(tablet->GetStatus() == kTableReady);
        EXPECT_STREQ(tablet->GetServerAddr().c_str(), addr2.c_str());
    }

    // This unload function will not send unload request
    // Tablet will stay in kTableUnLoading status forever
    // It can be used to simulate a slow unload
    virtual void UnloadTabletAsync(TabletPtr tablet, UnloadClosure done) {
        LOG(ERROR) << "dummy UnloadTabletAsync...";
    }

    void MergeTabletBrokenTest() {
        TablePtr table(new Table("mergetest"));
        TabletPtr t1 = MakeTabletPtr("", "a", table);
        t1->SetStatus(kTableReady);

        TabletPtr t2 = MakeTabletPtr("a", "z", table);
        t2->SetStatus(kTableReady);

        TabletPtr t3 = MakeTabletPtr("z", "", table);
        t3->SetStatus(kTableReady);

        LOG(ERROR) << t1->GetStatus() << ";" << t2->GetStatus() << ";" << t3->GetStatus();

        MergeTabletAsync(t1, t2);
        LOG(ERROR) << t1->GetStatus() << ";" << t2->GetStatus() << ";" << t3->GetStatus();
        EXPECT_TRUE((t1->GetStatus() == kTableUnLoading)
                    && (t2->GetStatus() == kTableUnLoading)
                    && (t3->GetStatus() == kTableReady)); // t2 & t3's merge should fail since t1 & t2 is merging
        MergeTabletAsync(t2, t3);
        LOG(ERROR) << t1->GetStatus() << ";" << t2->GetStatus() << ";" << t3->GetStatus();
        EXPECT_TRUE((t1->GetStatus() == kTableUnLoading)
                    && (t2->GetStatus() == kTableUnLoading)
                    && (t3->GetStatus() == kTableReady));

        // t3 & t2's merge should fail since t1 & t2 is merging
        MergeTabletAsync(t3, t2);
        LOG(ERROR) << t1->GetStatus() << ";" << t2->GetStatus() << ";" << t3->GetStatus();
        EXPECT_TRUE((t1->GetStatus() == kTableUnLoading)
                    && (t2->GetStatus() == kTableUnLoading)
                    && (t3->GetStatus() == kTableReady));
    }

    virtual void ScanMetaTableAsync(const std::string& table_name,
            const std::string& tablet_key_start,
            const std::string& tablet_end_key,
            ScanClosure done);

    virtual void SplitTabletWriteMetaAsync(TabletPtr tablet, const std::string& split_key);
};

void MasterImplTest::ScanMetaTableAsync(const std::string& table_name,
        const std::string& tablet_key_start,
        const std::string& tablet_end_key,
        ScanClosure done) {

    const ::testing::TestInfo* test_case = ::testing::UnitTest::GetInstance()->current_test_info();
    std::string case_name(test_case->test_case_name());
    if (case_name == "InteractWithOldTS") {
        EXPECT_TRUE(true);
    }
    if (case_name.find("InteractWithNewTS") != std::string::npos) {
        EXPECT_TRUE(false);
    }
}

void MasterImplTest::SplitTabletWriteMetaAsync(TabletPtr tablet, const std::string& split_key) {
    const ::testing::TestInfo* test_case = ::testing::UnitTest::GetInstance()->current_test_info();
    std::string case_name(test_case->test_case_name());
    if (case_name.find("InteractWithOldTS") != std::string::npos) {
        EXPECT_TRUE(false);
    }
    if (case_name.find("InteractWithNewTS") != std::string::npos) {
        EXPECT_TRUE(true);
    }
    EXPECT_EQ(tablet->GetStatus(), kTableOnSplit);
    EXPECT_FALSE(split_key.empty());
    EXPECT_GT(split_key, tablet->GetKeyStart());
    if (!tablet->GetKeyEnd().empty()) {
        EXPECT_GT(tablet->GetKeyEnd(), split_key);
    }
}

TEST_F(MasterImplTest, DeleteTabletNodeTest) {
    DeleteTabletNodeTest();
}

TEST_F(MasterImplTest, MergeTest) {
    MergeTabletTest();
}

TEST_F(MasterImplTest, MergeTabletBrokenTest) {
    MergeTabletBrokenTest();
}

TEST_F(MasterImplTest, SplitNotSupport) {
    SplitTabletRequest* request = NULL;
    SplitTabletResponse* response = NULL;
    bool failed;
    int error_code;
    TablePtr table;
    TabletPtr tablet;
    TabletMeta meta;

    table.reset(new Table("splittest"));
    tablet.reset(new Tablet(meta, table));
    request = new SplitTabletRequest;
    response = new SplitTabletResponse;

    tablet->SetStatus(kTableReady);
    tablet->SetStatus(kTableOnSplit);
    response->set_status(kTableNotSupport);
    failed = false;
    error_code = 0;

    MasterImpl::SplitTabletCallback(tablet, request, response, failed, error_code);
    EXPECT_TRUE(tablet->GetStatus() == kTableOffLine);
}

TEST_F(MasterImplTest, InteractWithOldTS) {
    SplitTabletRequest* request = NULL;
    SplitTabletResponse* response = NULL;
    TablePtr table;
    TabletPtr tablet;
    TabletMeta meta;

    table.reset(new Table("splittest"));
    tablet.reset(new Tablet(meta, table));
    request = new SplitTabletRequest;
    response = new SplitTabletResponse;

    tablet->SetStatus(kTableReady);
    tablet->SetStatus(kTableOnSplit);
    response->set_status(kTabletNodeOk);

    bool failed = false;
    int error_code = 0;
    MasterImpl::SplitTabletCallback(tablet, request, response, failed, error_code);
}

TEST_F(MasterImplTest, InteractWithNewTSOK){
    TablePtr table;
    TabletPtr tablet;
    TabletMeta meta;

    table.reset(new Table("splittest"));
    tablet.reset(new Tablet(meta, table));
    SplitTabletRequest* request = new SplitTabletRequest;
    SplitTabletResponse* response = new SplitTabletResponse;
    tablet->SetStatus(kTableReady);
    tablet->SetStatus(kTableOnSplit);
    response->set_status(kTabletNodeOk);
    response->add_split_keys("abc");
    bool failed = false;
    int error_code = 0;
    MasterImpl::SplitTabletCallback(tablet, request, response, failed, error_code);

    meta.mutable_key_range()->set_key_start("ab");
    meta.mutable_key_range()->set_key_end("bc");
    tablet.reset(new Tablet(meta, table));
    tablet->SetStatus(kTableReady);
    tablet->SetStatus(kTableOnSplit);
    request = new SplitTabletRequest;
    response = new SplitTabletResponse;
    response->add_split_keys("b");
    MasterImpl::SplitTabletCallback(tablet, request, response, failed, error_code);
    EXPECT_EQ(tablet->GetStatus(), kTableOnSplit);
}

TEST_F(MasterImplTest, NewTSReturnInvalidSplitKey){
    TablePtr table;
    TabletPtr tablet;
    TabletMeta meta;

    meta.mutable_key_range()->set_key_start("aa");
    meta.mutable_key_range()->set_key_end("cc");
    table.reset(new Table("splittest"));
    tablet.reset(new Tablet(meta, table));
    tablet->SetStatus(kTableReady);
    tablet->SetStatus(kTableOnSplit);
    MasterImpl::SplitTabletWriteMetaAsync(tablet, "");
    EXPECT_EQ(tablet->GetStatus(), kTableOffLine);

    tablet->SetStatus(kTableReady);
    tablet->SetStatus(kTableOnSplit);
    MasterImpl::SplitTabletWriteMetaAsync(tablet, "aa");
    EXPECT_EQ(tablet->GetStatus(), kTableOffLine);

    tablet->SetStatus(kTableReady);
    tablet->SetStatus(kTableOnSplit);
    MasterImpl::SplitTabletWriteMetaAsync(tablet, "cc");
    EXPECT_EQ(tablet->GetStatus(), kTableOffLine);

    tablet->SetStatus(kTableReady);
    tablet->SetStatus(kTableOnSplit);
    MasterImpl::SplitTabletWriteMetaAsync(tablet, "d");
    EXPECT_EQ(tablet->GetStatus(), kTableOffLine);

    meta.mutable_key_range()->set_key_end("");
    tablet.reset(new Tablet(meta, table));
    tablet->SetStatus(kTableReady);
    tablet->SetStatus(kTableOnSplit);
    MasterImpl::SplitTabletWriteMetaAsync(tablet, "");
    EXPECT_EQ(tablet->GetStatus(), kTableOffLine);

    meta.Clear();
    tablet.reset(new Tablet(meta, table));
    tablet->SetStatus(kTableReady);
    tablet->SetStatus(kTableOnSplit);
    MasterImpl::SplitTabletWriteMetaAsync(tablet, "");
    EXPECT_EQ(tablet->GetStatus(), kTableOffLine);

}

TEST_F(MasterImplTest, SplitTabletWriteMetaCallback) {
    TablePtr table;
    TabletPtr tablet;
    TabletMeta meta;

    meta.mutable_key_range()->set_key_start("a");
    meta.mutable_key_range()->set_key_end("c");
    table.reset(new Table("splittest"));
    tablet.reset(new Tablet(meta, table));
    tablet->SetStatus(kTableReady);
    tablet->SetStatus(kTableOnSplit);
    std::vector<TabletPtr> child_tablets;
    meta.mutable_key_range()->set_key_end("b");
    child_tablets.emplace_back(new Tablet(meta));
    meta.mutable_key_range()->set_key_start("b");
    meta.mutable_key_range()->set_key_end("c");
    child_tablets.emplace_back(new Tablet(meta));
    bool failed = false;
    int error_code = 0;

    WriteTabletRequest* request = new WriteTabletRequest;
    WriteTabletResponse* response = new WriteTabletResponse;

    response->set_status(kTabletNodeOk);
    response->add_row_status_list(kTabletNodeOk);
    response->add_row_status_list(kTabletNodeOk);

    MasterImpl::SplitTabletWriteMetaCallback(tablet,
            child_tablets,  1, request, response, failed, error_code);
    EXPECT_EQ(table->tablets_list_.size(), 2);
    TabletPtr t1, t2;
    table->FindTablet("a", &t1);
    table->FindTablet("b", &t2);
    EXPECT_EQ(t1->GetStatus(), kTableOffLine);
    EXPECT_EQ(t2->GetStatus(), kTableOffLine);
    EXPECT_STREQ(t1->GetKeyEnd().c_str(), t2->GetKeyStart().c_str());
}

} // master
} // tera

