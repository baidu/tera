// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "gflags/gflags.h"
#include "glog/logging.h"
#include "gtest/gtest.h"

#include "db/filename.h"
#include "io/utils_leveldb.h"
#include "master/tablet_manager.h"
#include "utils/utils_cmd.h"

DECLARE_string(tera_coord_type);
DECLARE_string(tera_leveldb_env_type);
DECLARE_string(tera_master_gc_strategy);
DECLARE_string(tera_tabletnode_path_prefix);

namespace tera {
namespace master {

class TrackableGcTest : public ::testing::Test {
public:
    TrackableGcTest() : mgr_(nullptr, nullptr, nullptr) {
        std::cout << "TrackableGcTest()" << std::endl;
    }
    virtual ~TrackableGcTest() {
        std::cout << "~TrackableGcTest()" << std::endl;
    }

    TablePtr CreateTable(const std::string& name) {
        TablePtr table(new Table(name));
        TableMeta meta;
        EXPECT_TRUE(mgr_.AddTable(name, meta, &table, nullptr));
        std::cout << "create table " << name << " success" << std::endl;

        return table;
    }

    TabletMeta CreateTabletMeta(const std::string& table_name, const std::string& start, const std::string& end) {
        TabletMeta meta;
        meta.set_table_name(table_name);
        meta.mutable_key_range()->set_key_start(start);
        meta.mutable_key_range()->set_key_end(end);

        return meta;
    }

    TabletPtr CreateTablet(const std::string& start, const std::string& end, TablePtr table) {
        TabletMeta meta = CreateTabletMeta(table->GetTableName(), start, end);
        TabletPtr tablet(new Tablet(meta, table));
        TableSchema schema;
        EXPECT_TRUE(mgr_.AddTablet(meta, schema, &tablet));
        std::cout << "create tablet [" << start << ", " << end << "]" << " success" << std::endl;

        return tablet;
    }

    TabletFile CreateTabletFile(uint64_t tablet_id, uint32_t lg_id, uint64_t file_id, bool create_local_file = false) {
        TabletFile tablet_file;
        tablet_file.tablet_id = tablet_id;
        tablet_file.lg_id = lg_id;
        tablet_file.file_id = file_id;

        if (create_local_file) {
            leveldb::Env* env = io::LeveldbBaseEnv();
            std::string table_path = FLAGS_tera_tabletnode_path_prefix + kTableName_;
            std::string path;

            if (lg_id == 0 && file_id == 0) {
                path = leveldb::BuildTabletPath(table_path, tablet_id);
                EXPECT_TRUE(env->CreateDir(path).ok());
            } else {
                path = leveldb::BuildTableFilePath(table_path, tablet_id, lg_id, file_id);
                size_t dir_pos = path.rfind("/");
                EXPECT_TRUE(dir_pos != std::string::npos);
                EXPECT_TRUE(env->CreateDir(path.substr(0, dir_pos)).ok());
                leveldb::WritableFile* writable_file;
                EXPECT_TRUE(env->NewWritableFile(path, &writable_file).ok());
                delete writable_file;
            }
        }

        return tablet_file;
    }

    TabletInheritedFileInfo CreateTabletInheritedFileInfo(const TabletPtr& tablet, const TabletFile& tablet_file) {
        TabletInheritedFileInfo inh_file_info;
        inh_file_info.set_table_name(tablet->GetTableName());
        inh_file_info.set_key_start(tablet->GetKeyStart());
        inh_file_info.set_key_end(tablet->GetKeyEnd());

        LgInheritedLiveFiles* lg_files = inh_file_info.add_lg_inh_files();
        lg_files->set_lg_no(tablet_file.lg_id);
        lg_files->add_file_number(1UL << 63 | tablet_file.tablet_id << 32 | tablet_file.file_id);

        return inh_file_info;
    }

    TabletInheritedFileInfo CreateEmptyTabletInheritedFileInfo(const TabletPtr& tablet) {
        TabletInheritedFileInfo inh_file_info;
        inh_file_info.set_table_name(tablet->GetTableName());
        inh_file_info.set_key_start(tablet->GetKeyStart());
        inh_file_info.set_key_end(tablet->GetKeyEnd());

        return inh_file_info;
    }

    void TestAddInheritedFile() {
        TablePtr table = CreateTable(kTableName_);
        ASSERT_EQ(0, table->gc_disabled_dead_tablets_.size());
        ASSERT_EQ(0, table->useful_inh_files_.size());

        TabletFile file1 = CreateTabletFile(1, 0, 1);
        MutexLock l(&table->mutex_);

        // first add, ref inc to 1
        // this step simulates collecting inherited file from filesystem
        table->AddInheritedFile(file1, false);
        ASSERT_EQ(1, table->gc_disabled_dead_tablets_.size());
        ASSERT_EQ(1, table->useful_inh_files_.size());
        ASSERT_EQ(1, table->useful_inh_files_[1].size());
        ASSERT_EQ(1, table->useful_inh_files_[1][file1].ref);
        ASSERT_EQ(0, table->obsolete_inh_files_.size());

        // add the same file again, ref will not inc
        // this step simulates collecting inherited file from filesystem again
        table->AddInheritedFile(file1, false);
        ASSERT_EQ(1, table->gc_disabled_dead_tablets_.size());
        ASSERT_EQ(1, table->useful_inh_files_.size());
        ASSERT_EQ(1, table->useful_inh_files_[1].size());
        ASSERT_EQ(1, table->useful_inh_files_[1][file1].ref);
        ASSERT_EQ(0, table->obsolete_inh_files_.size());

        // add the same file again with ref, ref will inc to 2
        // this step simulates ts reporting using the TabletFile
        table->AddInheritedFile(file1, true);
        ASSERT_EQ(1, table->gc_disabled_dead_tablets_.size());
        ASSERT_EQ(1, table->useful_inh_files_.size());
        ASSERT_EQ(1, table->useful_inh_files_[1].size());
        ASSERT_EQ(2, table->useful_inh_files_[1][file1].ref);
        ASSERT_EQ(0, table->obsolete_inh_files_.size());

        // add a new file from the same tablet, ref will inc to 1
        TabletFile file2 = CreateTabletFile(1, 0, 2);
        table->AddInheritedFile(file2, false);
        ASSERT_EQ(1, table->gc_disabled_dead_tablets_.size());
        ASSERT_EQ(1, table->useful_inh_files_.size());
        ASSERT_EQ(2, table->useful_inh_files_[1].size());
        ASSERT_EQ(1, table->useful_inh_files_[1][file2].ref);
        ASSERT_EQ(0, table->obsolete_inh_files_.size());

        // add a new file from a different tablet, ref will inc to 1
        TabletFile file3 = CreateTabletFile(2, 0, 1);
        table->AddInheritedFile(file3, false);
        ASSERT_EQ(2, table->gc_disabled_dead_tablets_.size());
        ASSERT_EQ(2, table->useful_inh_files_.size());
        ASSERT_EQ(1, table->useful_inh_files_[2].size());
        ASSERT_EQ(1, table->useful_inh_files_[2][file3].ref);
        ASSERT_EQ(0, table->obsolete_inh_files_.size());
    }

    void TestReleaseInheritedFile() {
        TablePtr table = CreateTable(kTableName_);
        ASSERT_EQ(0, table->gc_disabled_dead_tablets_.size());
        ASSERT_EQ(0, table->useful_inh_files_.size());

        TabletFile file1 = CreateTabletFile(1, 0, 1);
        MutexLock l(&table->mutex_);

        // first add, ref inc to 1
        // this step simulates collecting inherited file from filesystem
        table->AddInheritedFile(file1, false);
        ASSERT_EQ(1, table->gc_disabled_dead_tablets_.size());
        ASSERT_EQ(1, table->useful_inh_files_.size());
        ASSERT_EQ(1, table->useful_inh_files_[1].size());
        ASSERT_EQ(1, table->useful_inh_files_[1][file1].ref);
        ASSERT_EQ(0, table->obsolete_inh_files_.size());

        // add the same file again with ref, ref will inc to 2
        // this step simulates ts reporting using the TabletFile
        table->AddInheritedFile(file1, true);
        ASSERT_EQ(1, table->gc_disabled_dead_tablets_.size());
        ASSERT_EQ(1, table->useful_inh_files_.size());
        ASSERT_EQ(1, table->useful_inh_files_[1].size());
        ASSERT_EQ(2, table->useful_inh_files_[1][file1].ref);
        ASSERT_EQ(0, table->obsolete_inh_files_.size());

        // reease the file, ref will dec to 1
        // this step simulates ts reporting unusing the TabletFile
        table->ReleaseInheritedFile(file1);
        ASSERT_EQ(1, table->gc_disabled_dead_tablets_.size());
        ASSERT_EQ(1, table->useful_inh_files_.size());
        ASSERT_EQ(1, table->useful_inh_files_[1].size());
        ASSERT_EQ(1, table->useful_inh_files_[1][file1].ref);
        ASSERT_EQ(0, table->obsolete_inh_files_.size());

        // this step simulates all live tablets fo the table have reported
        table->EnableDeadTabletGarbageCollect(1);
        ASSERT_EQ(0, table->useful_inh_files_.size());
        ASSERT_EQ(2, table->obsolete_inh_files_.size()); // tablet dir and file 1-0-1
    }

    void TestSplit() {
        TablePtr table = CreateTable(kTableName_);
        TabletPtr tablet_1 = CreateTablet("a", "z", table);

        TabletFile file1 = CreateTabletFile(1, 0, 1);
        {
            MutexLock l(&table->mutex_);
            table->AddInheritedFile(file1, false);
        }
        table->reported_live_tablets_num_ = 1;
        tablet_1->inh_files_.insert(file1);
        tablet_1->gc_reported_ = true;

        ASSERT_EQ(1, table->gc_disabled_dead_tablets_.size());
        ASSERT_EQ(1, table->useful_inh_files_.size());
        ASSERT_EQ(1, table->useful_inh_files_[1].size());
        ASSERT_EQ(1, table->useful_inh_files_[1][file1].ref);
        ASSERT_EQ(0, table->obsolete_inh_files_.size());

        TabletPtr tablet_2, tablet_3;
        TabletMeta meta_2 = CreateTabletMeta(table->GetTableName(), "a", "k");
        TabletMeta meta_3 = CreateTabletMeta(table->GetTableName(), "k", "z");

        table->SplitTablet(tablet_1, meta_2, meta_3, &tablet_2, &tablet_3);

        // afer split:
        //     1. each sub tablet shoud ref the inh file from the parent tablet
        //     2. ref to the file shoud inc to 2
        //     3. reported_live_tablets_num_ shoud dec 1 if the parent tablet has reported

        // 1
        ASSERT_EQ(1, tablet_1->inh_files_.size());
        ASSERT_EQ(1, tablet_2->inh_files_.size());
        ASSERT_EQ(1, tablet_3->inh_files_.size());

        // 2
        ASSERT_EQ(1, table->gc_disabled_dead_tablets_.size());
        ASSERT_EQ(1, table->useful_inh_files_.size());
        ASSERT_EQ(1, table->useful_inh_files_[1].size());
        ASSERT_EQ(2, table->useful_inh_files_[1][file1].ref);
        ASSERT_EQ(0, table->obsolete_inh_files_.size());

        // 3
        ASSERT_EQ(0, table->reported_live_tablets_num_);
    }

    void TestMerge() {
        TablePtr table = CreateTable(kTableName_);
        TabletPtr tablet_1 = CreateTablet("a", "k", table);
        TabletPtr tablet_2 = CreateTablet("k", "z", table);

        TabletFile file1 = CreateTabletFile(1, 0, 1);
        {
            MutexLock l(&table->mutex_);
            table->AddInheritedFile(file1, false);
        }
        TabletFile file2 = CreateTabletFile(2, 0, 1);
        {
            MutexLock l(&table->mutex_);
            table->AddInheritedFile(file2, false);
        }

        table->reported_live_tablets_num_ = 2;
        tablet_1->inh_files_.insert(file1);
        tablet_1->gc_reported_ = true;
        tablet_2->inh_files_.insert(file2);
        tablet_2->gc_reported_ = true;

        ASSERT_EQ(2, table->gc_disabled_dead_tablets_.size());
        ASSERT_EQ(2, table->useful_inh_files_.size());
        ASSERT_EQ(1, table->useful_inh_files_[1].size());
        ASSERT_EQ(1, table->useful_inh_files_[1][file1].ref);
        ASSERT_EQ(1, table->useful_inh_files_[2].size());
        ASSERT_EQ(1, table->useful_inh_files_[2][file2].ref);
        ASSERT_EQ(0, table->obsolete_inh_files_.size());

        TabletPtr tablet_3;
        TabletMeta meta_3 = CreateTabletMeta(table->GetTableName(), "a", "z");
        table->MergeTablets(tablet_1, tablet_2, meta_3, &tablet_3);

        // afer merge:
        //     1. the merged tablet shoud ref all the inh file from the two parent tablet
        //     2. ref to the file shoud not change
        //     3. reported_live_tablets_num_ shoud dec by the reported num of parent tablets

        // 1
        ASSERT_EQ(1, tablet_1->inh_files_.size());
        ASSERT_EQ(1, tablet_2->inh_files_.size());
        ASSERT_EQ(2, tablet_3->inh_files_.size());

        // 2
        ASSERT_EQ(2, table->gc_disabled_dead_tablets_.size());
        ASSERT_EQ(2, table->useful_inh_files_.size());
        ASSERT_EQ(1, table->useful_inh_files_[1].size());
        ASSERT_EQ(1, table->useful_inh_files_[1][file1].ref);
        ASSERT_EQ(1, table->useful_inh_files_[2].size());
        ASSERT_EQ(1, table->useful_inh_files_[2][file2].ref);

        // 3
        ASSERT_EQ(0, table->reported_live_tablets_num_);
    }

    // Case1: ts report using file1 when master restart
    void TestGarbageCollect1() {
        TablePtr table = CreateTable(kTableName_);
        table->reported_live_tablets_num_ = 0;
        TabletPtr tablet_1 = CreateTablet("a", "z", table);
        TabletFile file1 = CreateTabletFile(2, 0, 1);

        ASSERT_EQ(0, table->gc_disabled_dead_tablets_.size());
        ASSERT_EQ(0, table->useful_inh_files_.size());
        ASSERT_EQ(0, tablet_1->inh_files_.size());
        ASSERT_EQ(0, table->obsolete_inh_files_.size());
        // no tablets have reported till now
        ASSERT_EQ(0, table->reported_live_tablets_num_);

        // this step simulates tablet_1 reporting using file1
        TabletInheritedFileInfo inh_file_1 = CreateTabletInheritedFileInfo(tablet_1, file1);
        table->GarbageCollect(inh_file_1);
        ASSERT_EQ(1, tablet_1->inh_files_.size());

        ASSERT_EQ(0, table->gc_disabled_dead_tablets_.size());
        ASSERT_EQ(1, table->useful_inh_files_.size());
        ASSERT_EQ(1, table->useful_inh_files_[2].size());
        // ref inc to 1:
        //      first, ref inc to 2 since tablet_1 report using file1 with need_ref=TRUE
        //      then, ref des to 1 since EnableDeadTabletGarbageCollect()
        ASSERT_EQ(1, table->useful_inh_files_[2][file1].ref);

        // all live tablets have reported, but file1 hasn't been released, gc won't work
        ASSERT_EQ(1, table->reported_live_tablets_num_);
        ASSERT_EQ(0, table->obsolete_inh_files_.size());
    }

    // Case2: ts report using file1, then report releasing file1
    void TestGarbageCollect2() {
        TablePtr table = CreateTable(kTableName_);
        table->reported_live_tablets_num_ = 0;
        TabletPtr tablet_1 = CreateTablet("a", "z", table);
        TabletFile file1 = CreateTabletFile(2, 0, 1);

        ASSERT_EQ(0, table->gc_disabled_dead_tablets_.size());
        ASSERT_EQ(0, table->useful_inh_files_.size());
        ASSERT_EQ(0, tablet_1->inh_files_.size());
        ASSERT_EQ(0, table->obsolete_inh_files_.size());
        // no tablets have reported till now
        ASSERT_EQ(0, table->reported_live_tablets_num_);

        // this step simulates tablet_1 reporting using file1
        TabletInheritedFileInfo inh_file_1 = CreateTabletInheritedFileInfo(tablet_1, file1);
        table->GarbageCollect(inh_file_1);
        ASSERT_EQ(1, tablet_1->inh_files_.size());

        ASSERT_EQ(0, table->gc_disabled_dead_tablets_.size());
        ASSERT_EQ(1, table->useful_inh_files_.size());
        ASSERT_EQ(1, table->useful_inh_files_[2].size());
        // ref inc to 1:
        //      first, ref inc to 2 since tablet_1 report using file1 with need_ref=TRUE
        //      then, ref des to 1 since EnableDeadTabletGarbageCollect()
        ASSERT_EQ(1, table->useful_inh_files_[2][file1].ref);

        // all live tablets have reported, but file1 hasn't been released, gc won't work
        ASSERT_EQ(1, table->reported_live_tablets_num_);
        ASSERT_EQ(0, table->gc_disabled_dead_tablets_.size());
        ASSERT_EQ(0, table->obsolete_inh_files_.size());

        // this step simulates tablet_1 reporting releasing file1
        TabletInheritedFileInfo inh_file_2 = CreateEmptyTabletInheritedFileInfo(tablet_1);
        table->GarbageCollect(inh_file_2);
        ASSERT_EQ(0, tablet_1->inh_files_.size());

        // all live tablets have reported, and ref files have been released, gc worked
        ASSERT_EQ(0, table->gc_disabled_dead_tablets_.size());
        ASSERT_EQ(0, table->useful_inh_files_.size());
        ASSERT_EQ(2, table->obsolete_inh_files_.size()); // tablet dir and file 2-0-1
    }

    // Case3: tablet_1 split into tablet_2 and tablet_3,
    // then tablet_2 and tablet_3 will report
    void TestGarbageCollect3() {
        TablePtr table = CreateTable(kTableName_);
        table->reported_live_tablets_num_ = 0;
        TabletPtr tablet_1 = CreateTablet("a", "z", table);
        TabletFile file1 = CreateTabletFile(1, 0, 1);
        {
            MutexLock l(&table->mutex_);
            table->AddInheritedFile(file1, false);
        }

        ASSERT_EQ(1, table->gc_disabled_dead_tablets_.size());
        ASSERT_EQ(1, table->useful_inh_files_.size());
        ASSERT_EQ(1, table->useful_inh_files_[1].size());
        ASSERT_EQ(1, table->useful_inh_files_[1][file1].ref);
        ASSERT_EQ(0, table->obsolete_inh_files_.size());

        // no tablets have reported till now
        ASSERT_EQ(0, table->reported_live_tablets_num_);

        TabletPtr tablet_2, tablet_3;
        TabletMeta meta_2 = CreateTabletMeta(table->GetTableName(), "a", "k");
        TabletMeta meta_3 = CreateTabletMeta(table->GetTableName(), "k", "z");

        table->SplitTablet(tablet_1, meta_2, meta_3, &tablet_2, &tablet_3);

        // suppose after split, tablet_2 will ref file1 and talbet_3 has no ref

        // this step simulates tablet_2 reporting using file1
        TabletInheritedFileInfo inh_file_1 = CreateTabletInheritedFileInfo(tablet_2, file1);
        table->GarbageCollect(inh_file_1);
        ASSERT_EQ(1, tablet_2->inh_files_.size());

        ASSERT_EQ(1, table->gc_disabled_dead_tablets_.size());
        ASSERT_EQ(1, table->useful_inh_files_.size());
        ASSERT_EQ(1, table->useful_inh_files_[1].size());
        // ref inc to 2 since tablet_2 report using file1
        ASSERT_EQ(2, table->useful_inh_files_[1][file1].ref);

        // only tablet_2 has reported, gc won't work
        ASSERT_EQ(1, table->reported_live_tablets_num_);
        ASSERT_EQ(0, table->obsolete_inh_files_.size());

        // this step simulates tablet_3 reporting using no file
        TabletInheritedFileInfo inh_file_2 = CreateEmptyTabletInheritedFileInfo(tablet_3);
        table->GarbageCollect(inh_file_2);
        ASSERT_EQ(0, tablet_3->inh_files_.size());

        // all live tablets have reported, but file1 hasn't been released, gc won't work
        ASSERT_EQ(2, table->reported_live_tablets_num_);
        ASSERT_EQ(0, table->gc_disabled_dead_tablets_.size());
        ASSERT_EQ(0, table->obsolete_inh_files_.size());

        // this step simulates tablet_2 reporting releasing file1
        TabletInheritedFileInfo inh_file_3 = CreateEmptyTabletInheritedFileInfo(tablet_2);
        table->GarbageCollect(inh_file_3);
        ASSERT_EQ(0, tablet_2->inh_files_.size());

        // all live tablets have reported, and ref files have been released, gc worked
        ASSERT_EQ(0, table->gc_disabled_dead_tablets_.size());
        ASSERT_EQ(0, table->useful_inh_files_.size());
        ASSERT_EQ(2, table->obsolete_inh_files_.size()); // tablet dir and file 1-0-1
    }

    void TestCleanObsoleteFile() {
        TablePtr table = CreateTable(kTableName_);

        // empty
        ASSERT_EQ(0, table->CleanObsoleteFile());

        // add a TabletFile and tablet dir
        TabletFile file1 = CreateTabletFile(1, 0, 1, true);
        TabletFile tablet_dir = CreateTabletFile(1, 0, 0, true);
        table->obsolete_inh_files_.push(file1);
        table->obsolete_inh_files_.push(tablet_dir);

        // obsolete_inh_files_ is has tablet_dir and file1
        ASSERT_EQ(2, table->obsolete_inh_files_.size());

        // successfully delete 2 files: tablet_dir and file1
        ASSERT_EQ(2, table->CleanObsoleteFile());

        // obsolete_inh_files_ is empty now
        ASSERT_EQ(0, table->obsolete_inh_files_.size());
    }

    void TestTryCollectInheritedFile() {
        TablePtr table = CreateTable(kTableName_);

        // no dead tablet
        ASSERT_FALSE(table->TryCollectInheritedFile());

        // dead tablet1 with file1
        TabletFile file1 = CreateTabletFile(1, 0, 1, true);

        ASSERT_EQ(0, table->gc_disabled_dead_tablets_.size());
        ASSERT_EQ(0, table->useful_inh_files_.size());
        ASSERT_EQ(0, table->obsolete_inh_files_.size());

        // collect dead tablet from filesystem
        ASSERT_TRUE(table->TryCollectInheritedFile());

        ASSERT_EQ(1, table->gc_disabled_dead_tablets_.size());
        ASSERT_EQ(1, table->useful_inh_files_.size());
        ASSERT_EQ(1, table->useful_inh_files_[1].size());
        ASSERT_EQ(1, table->useful_inh_files_[1][file1].ref);
        ASSERT_EQ(0, table->obsolete_inh_files_.size());
    }

protected:
    virtual void SetUp() {
        std::cout << "SetUp" << std::endl;
    }

    virtual void TearDown() {
        mgr_.ClearTableList();
        std::cout << "TearDown" << std::endl;
    }

    static void SetUpTestCase() {
        std::cout << "SetUpTestCase" << std::endl;
        FLAGS_tera_coord_type = "fake_zk";
        FLAGS_tera_leveldb_env_type = "local";
        FLAGS_tera_master_gc_strategy = "trackable";
        FLAGS_tera_tabletnode_path_prefix = "./";
    }

    static void TearDownTestCase() {
        std::cout << "TearDownTestCase" << std::endl;
        std::string table_path = FLAGS_tera_tabletnode_path_prefix + kTableName_;
        EXPECT_TRUE(io::DeleteEnvDir(table_path).ok());
    }

private:
    TabletManager mgr_;
    const static std::string kTableName_;
};

const std::string TrackableGcTest::kTableName_ = "MasterTestTable";

TEST_F(TrackableGcTest, AddInheritedFile) {
    TestAddInheritedFile();
}

TEST_F(TrackableGcTest, ReleaseInheritedFile) {
    TestReleaseInheritedFile();
}

TEST_F(TrackableGcTest, Split) {
    TestSplit();
}

TEST_F(TrackableGcTest, Merge) {
    TestMerge();
}

TEST_F(TrackableGcTest, GarbageCollect1) {
    TestGarbageCollect1();
}

TEST_F(TrackableGcTest, GarbageCollect2) {
    TestGarbageCollect2();
}

TEST_F(TrackableGcTest, GarbageCollect3) {
    TestGarbageCollect3();
}

TEST_F(TrackableGcTest, CleanObsoleteFile) {
    TestCleanObsoleteFile();
}

TEST_F(TrackableGcTest, TryCollectInheritedFile) {
    TestTryCollectInheritedFile();
}

} // master
} // tera

