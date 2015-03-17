// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "tabletnode/tablet_manager.h"

#include "gtest/gtest.h"

#include "io/tablet_io.h"
#include "proto/status_code.pb.h"
#include "proto/table_schema.pb.h"

DECLARE_string(tera_leveldb_env_type);

namespace tera {
namespace tabletnode {

class TabletManagerTest : public ::testing::Test {
public:
    TabletManagerTest() {}
    ~TabletManagerTest() {}

protected:
    TabletManager m_tablet_manager;
};

TEST_F(TabletManagerTest, TabletRange_General) {
    TabletRange tr1("t1", "start1", "end1");
    TabletRange tr2("t2", "start2", "end2");
    TabletRange tr3("t1", "start2", "end2");
    TabletRange tr4("t1", "start2", "end2");

    EXPECT_TRUE(tr1 < tr2);
    EXPECT_TRUE(tr1 < tr3);
    EXPECT_TRUE(tr3 == tr4);
}

TEST_F(TabletManagerTest, AddTabletSuccess) {
    std::string table_name = "add_tablet";
    std::string table_path = "add_tablet";
    std::string start_key = "start_key";
    std::string end_key = "end_key";
    io::TabletIO* tablet_io = NULL;
    StatusCode err_code = kTabletNodeOk;

    EXPECT_TRUE(m_tablet_manager.AddTablet(
            table_name, table_path, start_key, end_key,
            &tablet_io, &err_code));
    EXPECT_TRUE(tablet_io != NULL);
    EXPECT_EQ(err_code, kTabletNodeOk);

    tablet_io->DecRef();
    EXPECT_TRUE(m_tablet_manager.RemoveTablet(
            table_name, start_key, end_key));
}

TEST_F(TabletManagerTest, AddTabletFailureForExist) {
    std::string table_name = "add_tablet";
    std::string table_path = "add_tablet";
    std::string start_key = "start_key";
    std::string end_key = "end_key";
    io::TabletIO* tablet_io1 = NULL;
    io::TabletIO* tablet_io2 = NULL;
    StatusCode err_code = kTabletNodeOk;

    EXPECT_TRUE(m_tablet_manager.AddTablet(
            table_name, table_path, start_key, end_key,
            &tablet_io1, &err_code));
    EXPECT_TRUE(tablet_io1 != NULL);
    EXPECT_EQ(err_code, kTabletNodeOk);

    EXPECT_FALSE(m_tablet_manager.AddTablet(
            table_name, table_path, start_key, end_key,
            &tablet_io2, &err_code));
    EXPECT_TRUE(tablet_io2 != NULL);
    EXPECT_TRUE(tablet_io2 == tablet_io1);
    EXPECT_EQ(err_code, kTableExist);

    tablet_io1->DecRef();
    tablet_io2->DecRef();
    EXPECT_TRUE(m_tablet_manager.RemoveTablet(
            table_name, start_key, end_key));
}

TEST_F(TabletManagerTest, RemoveTabletFailureNotExist) {
    std::string table_name = "add_tablet";
    std::string table_path = "add_tablet";
    std::string start_key = "start_key";
    std::string end_key = "end_key";
    io::TabletIO* tablet_io = NULL;
    StatusCode err_code = kTabletNodeOk;

    EXPECT_TRUE(m_tablet_manager.AddTablet(
            table_name, table_path, start_key, end_key,
            &tablet_io, &err_code));
    EXPECT_TRUE(tablet_io != NULL);
    EXPECT_EQ(err_code, kTabletNodeOk);

    EXPECT_FALSE(m_tablet_manager.RemoveTablet(
            "not_exist_table", start_key, end_key));
    EXPECT_FALSE(m_tablet_manager.RemoveTablet(
            table_name, "incorrect_start_key", end_key));
    EXPECT_FALSE(m_tablet_manager.RemoveTablet(
            table_name, start_key, "incorrect_end_key"));

    tablet_io->DecRef();
    EXPECT_TRUE(m_tablet_manager.RemoveTablet(
            table_name, start_key, end_key));
}

TEST_F(TabletManagerTest, GetTabletSuccess) {
    std::string table_name = "get_tablet";
    std::string table_path = "get_tablet";
    std::string start_key = "start_key";
    std::string end_key = "end_key";
    io::TabletIO* tablet_io = NULL;
    StatusCode err_code = kTabletNodeOk;

    EXPECT_TRUE(m_tablet_manager.AddTablet(
            table_name, table_path, start_key, end_key,
            &tablet_io, &err_code));
    EXPECT_TRUE(tablet_io != NULL);
    EXPECT_EQ(err_code, kTabletNodeOk);

    io::TabletIO* get_tablet_io = m_tablet_manager.GetTablet(
        table_name, start_key, end_key, &err_code);
    EXPECT_TRUE(get_tablet_io != NULL);
    EXPECT_EQ(err_code, kTabletNodeOk);
    EXPECT_EQ(get_tablet_io, tablet_io);

    tablet_io->DecRef();
    get_tablet_io->DecRef();
    EXPECT_TRUE(m_tablet_manager.RemoveTablet(
            table_name, start_key, end_key));
}

TEST_F(TabletManagerTest, GetTabletSuccessForNoEndKey) {
    std::string table_name = "get_tablet";
    std::string table_path = "get_tablet";
    std::string start_key = "start_key";
    std::string end_key = "";
    io::TabletIO* tablet_io = NULL;
    StatusCode err_code = kTabletNodeOk;

    EXPECT_TRUE(m_tablet_manager.AddTablet(
            table_name, table_path, start_key, end_key,
            &tablet_io, &err_code));
    EXPECT_TRUE(tablet_io != NULL);
    EXPECT_EQ(err_code, kTabletNodeOk);

    io::TabletIO* get_tablet_io = m_tablet_manager.GetTablet(
        table_name, start_key, &err_code);
    EXPECT_TRUE(get_tablet_io != NULL);
    EXPECT_EQ(err_code, kTabletNodeOk);
    EXPECT_EQ(get_tablet_io, tablet_io);

    tablet_io->DecRef();
    get_tablet_io->DecRef();
    EXPECT_TRUE(m_tablet_manager.RemoveTablet(
            table_name, start_key, end_key));
}

TEST_F(TabletManagerTest, GetTabletFailure) {
    std::string table_name = "get_tablet";
    std::string table_path = "get_tablet";
    std::string start_key = "start_key";
    std::string end_key = "end_key";
    io::TabletIO* tablet_io = NULL;
    StatusCode err_code = kTabletNodeOk;

    EXPECT_TRUE(m_tablet_manager.AddTablet(
            table_name, table_path, start_key, end_key,
            &tablet_io, &err_code));
    EXPECT_TRUE(tablet_io != NULL);
    EXPECT_EQ(err_code, kTabletNodeOk);

    io::TabletIO* get_tablet_io = NULL;
    get_tablet_io = m_tablet_manager.GetTablet(
        "not_exist_tablet", start_key, end_key, &err_code);
    EXPECT_TRUE(get_tablet_io == NULL);
    EXPECT_EQ(err_code, kKeyNotInRange);

    get_tablet_io = m_tablet_manager.GetTablet(
        table_name, "incorrect_start_key", end_key, &err_code);
    EXPECT_TRUE(get_tablet_io == NULL);
    EXPECT_EQ(err_code, kKeyNotInRange);

    get_tablet_io = m_tablet_manager.GetTablet(
        table_name, start_key, "incorrect_end_key", &err_code);
    EXPECT_TRUE(get_tablet_io == NULL);
    EXPECT_EQ(err_code, kKeyNotInRange);

    get_tablet_io = m_tablet_manager.GetTablet(
        "not_exist_tablet", start_key, &err_code);
    EXPECT_TRUE(get_tablet_io == NULL);
    EXPECT_EQ(err_code, kKeyNotInRange);

    get_tablet_io = m_tablet_manager.GetTablet(
        table_name, "incorrect_start_key", &err_code);
    EXPECT_TRUE(get_tablet_io == NULL);
    EXPECT_EQ(err_code, kKeyNotInRange);

    tablet_io->DecRef();
    EXPECT_TRUE(m_tablet_manager.RemoveTablet(
            table_name, start_key, end_key));
}

TEST_F(TabletManagerTest, GetAllTabletSuccess) {
    std::vector<TabletMeta*> meta_list;
    m_tablet_manager.GetAllTabletMeta(&meta_list);
    EXPECT_TRUE(meta_list.size() == 0U);

    std::string table_name = "get_all_tablet";
    std::string table_path = "get_all_tablet";
    std::string start_key = "start_key";
    std::string end_key = "end_key";
    io::TabletIO* tablet_io = NULL;
    StatusCode err_code = kTabletNodeOk;

    EXPECT_TRUE(m_tablet_manager.AddTablet(
            table_name, table_path, start_key, end_key,
            &tablet_io, &err_code));
    EXPECT_TRUE(tablet_io != NULL);
    EXPECT_EQ(err_code, kTabletNodeOk);

    tablet_io->Load(TableSchema(), start_key, end_key, table_path, std::vector<uint64_t>(), std::map<uint64_t, uint64_t>());
    m_tablet_manager.GetAllTabletMeta(&meta_list);
    EXPECT_TRUE(meta_list.size() == 1U);

    tablet_io->DecRef();
    EXPECT_TRUE(m_tablet_manager.RemoveTablet(
            table_name, start_key, end_key));
}

TEST_F(TabletManagerTest, GetAllTabletIOSuccess) {
    std::vector<io::TabletIO*> io_list;
    m_tablet_manager.GetAllTablets(&io_list);
    EXPECT_TRUE(io_list.size() == 0U);

    std::string table_name = "get_all_tablet";
    std::string table_path = "get_all_tablet";
    std::string start_key = "start_key";
    std::string end_key = "end_key";
    io::TabletIO* tablet_io = NULL;
    StatusCode err_code = kTabletNodeOk;

    EXPECT_TRUE(m_tablet_manager.AddTablet(
            table_name, table_path, start_key, end_key,
            &tablet_io, &err_code));
    EXPECT_TRUE(tablet_io != NULL);
    EXPECT_EQ(err_code, kTabletNodeOk);

    tablet_io->Load(TableSchema(), start_key, end_key, table_path, std::vector<uint64_t>(), std::map<uint64_t, uint64_t>());
    m_tablet_manager.GetAllTablets(&io_list);
    EXPECT_TRUE(io_list.size() == 1U);

    tablet_io->DecRef();
    io_list[0]->DecRef();
    EXPECT_TRUE(m_tablet_manager.RemoveTablet(
            table_name, start_key, end_key));
}

TEST_F(TabletManagerTest, RemoveAllTabletSuccess) {
    EXPECT_TRUE(m_tablet_manager.RemoveAllTablets());

    std::string table_name = "remove_all_tablet";
    std::string table_path = "remove_all_tablet";
    std::string start_key = "start_key";
    std::string end_key = "end_key";
    io::TabletIO* tablet_io = NULL;
    StatusCode err_code = kTabletNodeOk;

    EXPECT_TRUE(m_tablet_manager.AddTablet(
            table_name, table_path, start_key, end_key,
            &tablet_io, &err_code));
    EXPECT_TRUE(tablet_io != NULL);
    EXPECT_EQ(err_code, kTabletNodeOk);

    tablet_io->Load(TableSchema(), start_key, end_key, table_path, std::vector<uint64_t>(), std::map<uint64_t, uint64_t>());
    tablet_io->DecRef();
    EXPECT_TRUE(m_tablet_manager.RemoveAllTablets());
}

TEST_F(TabletManagerTest, ForceRemoveAllTabletSuccess) {
    EXPECT_TRUE(m_tablet_manager.RemoveAllTablets());

    std::string table_name = "remove_all_tablet";
    std::string table_path = "remove_all_tablet";
    std::string start_key = "start_key";
    std::string end_key = "end_key";
    io::TabletIO* tablet_io = NULL;
    StatusCode err_code = kTabletNodeOk;

    EXPECT_TRUE(m_tablet_manager.AddTablet(
            table_name, table_path, start_key, end_key,
            &tablet_io, &err_code));
    EXPECT_TRUE(tablet_io != NULL);
    EXPECT_EQ(err_code, kTabletNodeOk);

    tablet_io->DecRef();
    EXPECT_TRUE(m_tablet_manager.RemoveAllTablets(true));
}

TEST_F(TabletManagerTest, Size) {
    EXPECT_EQ(m_tablet_manager.Size(), 0U);

    std::string table_name = "get_all_tablet";
    std::string table_path = "get_all_tablet";
    std::string start_key = "start_key";
    std::string end_key = "end_key";
    io::TabletIO* tablet_io = NULL;
    StatusCode err_code = kTabletNodeOk;

    EXPECT_TRUE(m_tablet_manager.AddTablet(
            table_name, table_path, start_key, end_key,
            &tablet_io, &err_code));
    EXPECT_TRUE(tablet_io != NULL);
    EXPECT_EQ(err_code, kTabletNodeOk);
    EXPECT_EQ(m_tablet_manager.Size(), 1U);

    tablet_io->DecRef();
    EXPECT_TRUE(m_tablet_manager.RemoveTablet(
            table_name, start_key, end_key));
}

} // namespace tabletnode
} // namespace tera

int main(int argc, char** argv) {
    FLAGS_tera_leveldb_env_type = "local";
    ::google::InitGoogleLogging(argv[0]);
    ::google::ParseCommandLineFlags(&argc, &argv, true);
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
