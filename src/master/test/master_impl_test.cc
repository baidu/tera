// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include <signal.h>

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "common/base/scoped_ptr.h"
#include "master/master_impl.h"
#include "master/tablet_manager.h"
#include "utils/utils_cmd.h"
#include "version.h"

DECLARE_string(tera_master_port);
DECLARE_string(log_dir);
DECLARE_bool(tera_zk_enabled);
DECLARE_string(tera_leveldb_env_type);
DECLARE_string(tera_fake_zk_path_prefix);

namespace tera {
namespace master {

class MasterImplTest : public ::testing::Test, public MasterImpl {
public:
    MasterImplTest() : merge_enter_phase2(false) {}

    void SplitTabletTest() {
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

    // This unload function will not send unload request
    // Tablet will stay in kTableUnLoading status forever
    // It can be used to simulate a slow unload
    virtual void UnloadTabletAsync(TabletPtr tablet, UnloadClosure done) {
        LOG(ERROR) << "dummy UnloadTabletAsync...";
    }

    void MergeTabletBorkenTest() {
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
                    && (t3->GetStatus() == kTableReady));

        // t2 & t3's merge should fail since t1 & t2 is merging
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
};

TEST_F(MasterImplTest, SplitTest) {
    SplitTabletTest();
}

TEST_F(MasterImplTest, MergeTest) {
    MergeTabletTest();
}

TEST_F(MasterImplTest, MergeTabletBorkenTest) {
    MergeTabletBorkenTest();
}

} // master
} // tera

int main(int argc, char** argv) {
    ::google::ParseCommandLineFlags(&argc, &argv, true);
    ::google::InitGoogleLogging(argv[0]);
    FLAGS_tera_zk_enabled = false;
    FLAGS_tera_leveldb_env_type = "local";

    tera::utils::SetupLog("master_test");
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

