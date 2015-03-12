// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "tera/tabletnode/tabletnode_impl.h"

#include "gflags/gflags.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "google/protobuf/stubs/common.h"

#include "tera/master/mock_master_client.h"
#include "tera/proto/master_rpc.pb.h"
#include "tera/proto/test_helper.h"
#include "tera/tabletnode/mock_tablet_manager.h"
#include "tera/proto/proto_helper.h"
#include "tera/io/mock_tablet_io.h"

DECLARE_bool(tera_zk_enabled);
DECLARE_int32(tera_tabletnode_retry_period);
DECLARE_string(tera_leveldb_env_type);

using ::testing::Invoke;
using ::testing::Return;
using ::testing::_;

namespace tera {
namespace tabletnode {

class TabletNodeImplTest : public ::testing::Test {
public:
    TabletNodeImplTest()
        : m_tablet_manager(new MockTabletManager()),
          m_tabletnode_impl(m_tabletnode_info, &m_master_client,
                            m_tablet_manager),
          m_ret_status(kTabletNodeOk), m_ret_tm_add(false),
          m_ret_tm_remove(false), m_ret_io_load(false), m_ret_io_unload(false),
          m_ret_io_compact(false), m_ret_io_readcell(false),
          m_ret_io_write(false), m_ret_io_scan(false), m_ret_io_scanrow(false),
          m_ret_io_split(false),
          m_start_key("start_key"), m_end_key("end_key"),
          m_schema(DefaultTableSchema()) {
        FLAGS_tera_zk_enabled = false;

        m_tablet_meta.set_table_name("name");
        m_tablet_meta.set_path("path");
        CreateKeyRange("", "", m_tablet_meta.mutable_key_range());
    }
    ~TabletNodeImplTest() {}

    void Done() {}

    void CreateCallback() {
        m_done = google::protobuf::NewCallback(this, &TabletNodeImplTest::Done);
    }

    void CreateKeyRange(const std::string& start_key,
                        const std::string& end_key,
                        KeyRange* key_range) {
        key_range->set_key_start("");
        key_range->set_key_end("");
    }

    // mock tablet manager

    bool AddTablet(const std::string& table_name, const std::string& table_path,
                   const std::string& key_start, const std::string& key_end,
                   io::TabletIO** tablet_io, StatusCode* status) {
        m_tablet_io.AddRef();
        *tablet_io = &m_tablet_io;
        *status = m_ret_status;
        return m_ret_tm_add;
    }
    bool RemoveTablet(const std::string& table_name,
                      const std::string& key_start,
                      const std::string& key_end,
                      StatusCode* status) {
        return m_ret_tm_remove;
    }
    io::TabletIO* GetTablet(const std::string& table_name,
                            const std::string& key_start,
                            const std::string& key_end,
                            StatusCode* status) {
        m_tablet_io.AddRef();
        return &m_tablet_io;
    }
    io::TabletIO* GetTablet2(const std::string& table_name,
                             const std::string& key,
                             StatusCode* status) {
        m_tablet_io.AddRef();
        return &m_tablet_io;
    }
    void GetAllTabletMeta(std::vector<TabletMeta*>* tablet_meta_list) {
        TabletMeta* meta = new TabletMeta(m_tablet_meta);
        tablet_meta_list->push_back(meta);
    }

    // mock tablet io

    bool IO_Load(const TableSchema& schema,
                 const std::string& key_start, const std::string& key_end,
                 const std::string& path,
                 const std::vector<uint64_t>& parent_tablets,
                 std::map<uint64_t, uint64_t> snapshots,
                 leveldb::Logger* logger,
                 leveldb::Cache* block_cache,
                 leveldb::TableCache* table_cache,
                 StatusCode* status) {
        return m_ret_io_load;
    }
    bool IO_Unload(StatusCode* status) {
        return m_ret_io_unload;
    }
    bool IO_Compact(StatusCode* status) {
        return m_ret_io_compact;
    }
    CompactStatus IO_GetCompactStatus() {
        return kTableCompacted;
    }
    int64_t IO_GetDataSize(StatusCode* status) {
        return 0;
    }
    bool IO_Read(const KeyList& key_list, BytesList* value_list,
                 uint32_t* success_num, uint64_t snapshot_id,
                 StatusCode* status) {
        return true;
    }
    bool IO_ReadCells(const RowReaderInfo& row_reader, RowResult* value_list,
                      StatusCode* status) {
        return m_ret_io_readcell;
    }
    bool IO_Write(const WriteTabletRequest* request,
                  WriteTabletResponse* response,
                  google::protobuf::Closure* done,
                  const std::vector<int32_t>* index_list,
                  Counter* done_counter, WriteRpcTimer* timer = NULL,
                  StatusCode* status = NULL) {
        return m_ret_io_write;
    }
    bool IO_Scan(const ScanOption& option, KeyValueList* kv_list,
                 bool* complete, StatusCode* status) {
        return m_ret_io_scan;
    }
    bool IO_ScanRows(const ScanTabletRequest* request,
                     ScanTabletResponse* response,
                     google::protobuf::Closure* done) {
        response->set_status(kTabletNodeOk);
        return m_ret_io_scanrow;
    }
    bool IO_Split(std::string* split_key, StatusCode* status) {
        return m_ret_io_split;
    }

protected:
    TabletNodeInfo m_tabletnode_info;
    master::MockMasterClient m_master_client;
    MockTabletManager* m_tablet_manager;
    google::protobuf::Closure* m_done;
    TabletNodeImpl m_tabletnode_impl;

    io::MockTabletIO m_tablet_io;
    StatusCode m_ret_status;
    bool m_ret_tm_add;
    bool m_ret_tm_remove;
    bool m_ret_io_load;
    bool m_ret_io_unload;
    bool m_ret_io_compact;
    bool m_ret_io_readcell;
    bool m_ret_io_write;
    bool m_ret_io_scan;
    bool m_ret_io_scanrow;
    bool m_ret_io_split;

    std::string m_table_name;
    std::string m_start_key;
    std::string m_end_key;
    TableSchema m_schema;
    TabletMeta m_tablet_meta;
};

TEST_F(TabletNodeImplTest, Init_Exit) {
    EXPECT_TRUE(m_tabletnode_impl.Init());
    EXPECT_TRUE(m_tabletnode_impl.Exit());
}

TEST_F(TabletNodeImplTest, LoadTabletSuccess) {
    EXPECT_CALL(*m_tablet_manager, AddTablet(_, _, _, _, _, _))
        .WillRepeatedly(Invoke(this, &TabletNodeImplTest::AddTablet));
    EXPECT_CALL(m_tablet_io, Load(_, _, _, _, _, _, _, _, _, _))
        .WillRepeatedly(Invoke(this, &TabletNodeImplTest::IO_Load));

    LoadTabletRequest request;
    LoadTabletResponse response;
    request.set_sequence_id(1);
    request.set_session_id("1");
    request.mutable_schema()->CopyFrom(m_schema);

    m_tabletnode_impl.SetSessionId("1");
    m_ret_tm_add = true;
    m_ret_io_load = true;
    CreateCallback();
    m_tabletnode_impl.LoadTablet(&request, &response, m_done);
    EXPECT_EQ(response.status(), kTabletNodeOk)
        << ": " << StatusCodeToString(response.status())
        << " vs. " << StatusCodeToString(kTabletNodeOk);
}

TEST_F(TabletNodeImplTest, LoadTabletFailureForSessionId) {
    EXPECT_CALL(*m_tablet_manager, AddTablet(_, _, _, _, _, _))
        .WillRepeatedly(Invoke(this, &TabletNodeImplTest::AddTablet));

    LoadTabletRequest request;
    LoadTabletResponse response;
    request.set_sequence_id(1);
    request.set_session_id("1");
    request.mutable_schema()->CopyFrom(m_schema);

    m_tabletnode_impl.SetSessionId("2");
    m_ret_tm_add = true;
    CreateCallback();
    m_tabletnode_impl.LoadTablet(&request, &response, m_done);
    EXPECT_EQ(response.status(), kIllegalAccess);
}

TEST_F(TabletNodeImplTest, LoadTabletFailureForInvalidSchema) {
    EXPECT_CALL(*m_tablet_manager, AddTablet(_, _, _, _, _, _))
        .WillRepeatedly(Invoke(this, &TabletNodeImplTest::AddTablet));

    LoadTabletRequest request;
    LoadTabletResponse response;
    request.set_sequence_id(1);
    request.set_session_id("1");

    m_tabletnode_impl.SetSessionId("1");
    m_ret_tm_add = true;
    CreateCallback();
    m_tabletnode_impl.LoadTablet(&request, &response, m_done);
    EXPECT_EQ(response.status(), kIllegalAccess);
}

TEST_F(TabletNodeImplTest, LoadTabletFailureForAddTablet) {
    EXPECT_CALL(*m_tablet_manager, AddTablet(_, _, _, _, _, _))
        .WillRepeatedly(Invoke(this, &TabletNodeImplTest::AddTablet));

    LoadTabletRequest request;
    LoadTabletResponse response;
    request.set_sequence_id(1);
    request.set_session_id("1");
    request.mutable_schema()->CopyFrom(m_schema);

    m_tabletnode_impl.SetSessionId("1");
    m_ret_tm_add = false;
    CreateCallback();
    m_tabletnode_impl.LoadTablet(&request, &response, m_done);
    EXPECT_NE(response.status(), kTabletNodeOk);
}

TEST_F(TabletNodeImplTest, LoadTabletFailureForIOLoad) {
    EXPECT_CALL(*m_tablet_manager, AddTablet(_, _, _, _, _, _))
        .WillRepeatedly(Invoke(this, &TabletNodeImplTest::AddTablet));
    EXPECT_CALL(*m_tablet_manager, RemoveTablet(_, _, _, _))
        .WillRepeatedly(Invoke(this, &TabletNodeImplTest::RemoveTablet));
    EXPECT_CALL(m_tablet_io, Load(_, _, _, _, _, _, _, _, _, _))
        .WillRepeatedly(Invoke(this, &TabletNodeImplTest::IO_Load));

    LoadTabletRequest request;
    LoadTabletResponse response;
    request.set_sequence_id(1);
    request.set_session_id("1");
    request.mutable_schema()->CopyFrom(m_schema);

    m_tabletnode_impl.SetSessionId("1");
    m_ret_tm_add = true;
    m_ret_tm_remove = true;
    m_ret_io_load = false;
    CreateCallback();
    m_tabletnode_impl.LoadTablet(&request, &response, m_done);
    EXPECT_NE(response.status(), kTabletNodeOk);
}

TEST_F(TabletNodeImplTest, UnloadTabletSuccess) {
    EXPECT_CALL(*m_tablet_manager, GetTablet(_, _, _, _))
        .WillRepeatedly(Invoke(this, &TabletNodeImplTest::GetTablet));
    EXPECT_CALL(*m_tablet_manager, RemoveTablet(_, _, _, _))
        .WillRepeatedly(Invoke(this, &TabletNodeImplTest::RemoveTablet));
    EXPECT_CALL(m_tablet_io, Unload(_))
        .WillRepeatedly(Invoke(this, &TabletNodeImplTest::IO_Unload));

    UnloadTabletRequest request;
    UnloadTabletResponse response;
    request.set_sequence_id(2);
    request.set_tablet_name("unload_table");
    KeyRange* key_range = request.mutable_key_range();
    key_range->set_key_start("");
    key_range->set_key_end("");

    m_ret_tm_remove = true;
    m_ret_io_unload = true;
    CreateCallback();
    m_tabletnode_impl.UnloadTablet(&request, &response, m_done);
    EXPECT_EQ(response.status(), kTabletNodeOk);
}

TEST_F(TabletNodeImplTest, UnloadTabletFailureForIOError) {
    FLAGS_tera_tabletnode_retry_period = 0;
    EXPECT_CALL(*m_tablet_manager, GetTablet(_, _, _, _))
        .WillRepeatedly(Invoke(this, &TabletNodeImplTest::GetTablet));
    EXPECT_CALL(*m_tablet_manager, RemoveTablet(_, _, _, _))
        .WillRepeatedly(Invoke(this, &TabletNodeImplTest::RemoveTablet));
    EXPECT_CALL(m_tablet_io, Unload(_))
        .WillRepeatedly(Invoke(this, &TabletNodeImplTest::IO_Unload));

    UnloadTabletRequest request;
    UnloadTabletResponse response;
    request.set_sequence_id(2);
    request.set_tablet_name("unload_table");
    KeyRange* key_range = request.mutable_key_range();
    key_range->set_key_start("");
    key_range->set_key_end("");

    m_ret_tm_remove = true;
    m_ret_io_unload = false;
    CreateCallback();
    m_tabletnode_impl.UnloadTablet(&request, &response, m_done);
    EXPECT_NE(response.status(), kTabletNodeOk);
}

TEST_F(TabletNodeImplTest, UnloadTabletButRemoveFailure) {
    FLAGS_tera_tabletnode_retry_period = 0;
    EXPECT_CALL(*m_tablet_manager, GetTablet(_, _, _, _))
        .WillRepeatedly(Invoke(this, &TabletNodeImplTest::GetTablet));
    EXPECT_CALL(*m_tablet_manager, RemoveTablet(_, _, _, _))
        .WillRepeatedly(Invoke(this, &TabletNodeImplTest::RemoveTablet));
    EXPECT_CALL(m_tablet_io, Unload(_))
        .WillRepeatedly(Invoke(this, &TabletNodeImplTest::IO_Unload));

    UnloadTabletRequest request;
    UnloadTabletResponse response;
    request.set_sequence_id(2);
    request.set_tablet_name("unload_table");
    CreateKeyRange("", "", request.mutable_key_range());

    m_ret_tm_remove = false;
    m_ret_io_unload = true;
    CreateCallback();
    m_tabletnode_impl.UnloadTablet(&request, &response, m_done);
    EXPECT_EQ(response.status(), kTabletNodeOk);
}

TEST_F(TabletNodeImplTest, CompactTabletSuccess) {
    EXPECT_CALL(*m_tablet_manager, GetTablet(_, _, _, _))
        .WillRepeatedly(Invoke(this, &TabletNodeImplTest::GetTablet));
    EXPECT_CALL(m_tablet_io, Compact(_))
        .WillRepeatedly(Invoke(this, &TabletNodeImplTest::IO_Compact));
    EXPECT_CALL(m_tablet_io, GetCompactStatus())
        .WillRepeatedly(Invoke(this, &TabletNodeImplTest::IO_GetCompactStatus));
    EXPECT_CALL(m_tablet_io, GetDataSize(_))
        .WillRepeatedly(Invoke(this, &TabletNodeImplTest::IO_GetDataSize));

    CompactTabletRequest request;
    CompactTabletResponse response;
    request.set_sequence_id(1);
    request.set_tablet_name("compact_table");
    CreateKeyRange("", "", request.mutable_key_range());

    m_ret_io_compact = true;
    CreateCallback();
    m_tabletnode_impl.CompactTablet(&request, &response, m_done);
    EXPECT_EQ(response.status(), kTabletNodeOk);
}

TEST_F(TabletNodeImplTest, ReadTabletSuccessOfKeyList) {
    EXPECT_CALL(*m_tablet_manager, GetTablet(_, _, _))
        .WillRepeatedly(Invoke(this, &TabletNodeImplTest::GetTablet2));
    EXPECT_CALL(m_tablet_io, Read(_, _, _, _, _))
        .WillRepeatedly(Invoke(this, &TabletNodeImplTest::IO_Read));

    ReadTabletRequest request;
    ReadTabletResponse response;
    request.set_sequence_id(1);
    request.set_tablet_name("read_table");
    RowReaderInfo* row = request.add_row_info_list();
    row->set_key("key");
    CreateCallback();
    m_tabletnode_impl.ReadTablet(1111, &request, &response, m_done);
    EXPECT_EQ(response.status(), kTabletNodeOk);
}

TEST_F(TabletNodeImplTest, ReadTabletSuccessOfRowList) {
    EXPECT_CALL(*m_tablet_manager, GetTablet(_, _, _))
        .WillRepeatedly(Invoke(this, &TabletNodeImplTest::GetTablet2));
    EXPECT_CALL(m_tablet_io, ReadCells(_, _, _))
        .WillRepeatedly(Invoke(this, &TabletNodeImplTest::IO_ReadCells));

    ReadTabletRequest request;
    ReadTabletResponse response;
    request.set_sequence_id(1);
    request.set_tablet_name("read_table");
    RowReaderInfo* row_info = request.add_row_info_list();
    row_info->set_key("key");


    m_ret_io_readcell = true;
    CreateCallback();
    m_tabletnode_impl.ReadTablet(1111, &request, &response, m_done);
    EXPECT_EQ(response.status(), kTabletNodeOk);
}

TEST_F(TabletNodeImplTest, ReadTabletSuccessOfNullData) {
    EXPECT_CALL(*m_tablet_manager, GetTablet(_, _, _))
        .WillRepeatedly(Invoke(this, &TabletNodeImplTest::GetTablet2));

    ReadTabletRequest request;
    ReadTabletResponse response;
    request.set_sequence_id(1);
    request.set_tablet_name("read_table");

    CreateCallback();
    m_tabletnode_impl.ReadTablet(1111, &request, &response, m_done);
    EXPECT_EQ(response.status(), kTabletNodeOk);
}

TEST_F(TabletNodeImplTest, WriteTabletSuccessOfKeyValue) {
    EXPECT_CALL(*m_tablet_manager, GetTablet(_, _, _))
        .WillRepeatedly(Invoke(this, &TabletNodeImplTest::GetTablet2));
    EXPECT_CALL(m_tablet_io, Write(_, _, _, _, _, _, _))
        .WillRepeatedly(Invoke(this, &TabletNodeImplTest::IO_Write));

    WriteTabletRequest request;
    WriteTabletResponse response;
    request.set_sequence_id(1);
    request.set_tablet_name("write_table");
    RowMutationSequence* mu_seq = request.add_row_list();
    mu_seq->set_row_key("key");
    Mutation* mutation = mu_seq->add_mutation_sequence();
    mutation->set_type(kPut);
    mutation->set_value("value");

    m_ret_io_write = true;
    CreateCallback();
    m_tabletnode_impl.WriteTablet(&request, &response, m_done);
    EXPECT_EQ(response.status(), kTabletNodeOk);
}

TEST_F(TabletNodeImplTest, WriteTabletSuccessOfTable) {
    EXPECT_CALL(*m_tablet_manager, GetTablet(_, _, _))
        .WillRepeatedly(Invoke(this, &TabletNodeImplTest::GetTablet2));
    EXPECT_CALL(m_tablet_io, Write(_, _, _, _, _, _, _))
        .WillRepeatedly(Invoke(this, &TabletNodeImplTest::IO_Write));

    WriteTabletRequest request;
    WriteTabletResponse response;
    request.set_sequence_id(1);
    request.set_tablet_name("write_table");
    RowMutationSequence* row_list = request.add_row_list();
    row_list->set_row_key("row_key");
    Mutation* mutation = row_list->add_mutation_sequence();
    mutation->set_type(kDeleteRow);
    mutation->set_ts_start(1111);
    mutation->set_ts_end(2222);

    m_ret_io_write = true;
    CreateCallback();
    m_tabletnode_impl.WriteTablet(&request, &response, m_done);
    EXPECT_EQ(response.status(), kTabletNodeOk);
}

TEST_F(TabletNodeImplTest, QuerySuccess) {
    QueryRequest request;
    QueryResponse response;
    request.set_sequence_id(1);

    CreateCallback();
    m_tabletnode_impl.Query(&request, &response, m_done);
    EXPECT_EQ(response.status(), kTabletNodeOk);
}

TEST_F(TabletNodeImplTest, ScanTabletSuccessOfTable) {
    EXPECT_CALL(*m_tablet_manager, GetTablet(_, _, _))
        .WillRepeatedly(Invoke(this, &TabletNodeImplTest::GetTablet2));
    EXPECT_CALL(m_tablet_io, ScanRows(_, _, _))
        .WillRepeatedly(Invoke(this, &TabletNodeImplTest::IO_ScanRows));

    ScanTabletRequest request;
    ScanTabletResponse response;
    request.set_sequence_id(1);
    request.set_table_name("scan_table");

    m_ret_io_scanrow = true;
    CreateCallback();
    m_tabletnode_impl.ScanTablet(&request, &response, m_done);
    EXPECT_EQ(response.status(), kTabletNodeOk);
}


TEST_F(TabletNodeImplTest, SplitTabletSuccess) {
    FLAGS_tera_tabletnode_retry_period = 0;
    EXPECT_CALL(*m_tablet_manager, GetTablet(_, _, _, _))
        .WillRepeatedly(Invoke(this, &TabletNodeImplTest::GetTablet));
    EXPECT_CALL(m_tablet_io, Split(_, _))
        .WillRepeatedly(Invoke(this, &TabletNodeImplTest::IO_Split));

    // not finished yet
}

} // namespace tabletnode
} // namespace tera

int main(int argc, char** argv) {
    ::google::InitGoogleLogging(argv[0]);
    ::google::ParseCommandLineFlags(&argc, &argv, true);
    ::testing::InitGoogleTest(&argc, argv);
    FLAGS_tera_leveldb_env_type = "local";
    return RUN_ALL_TESTS();
}

