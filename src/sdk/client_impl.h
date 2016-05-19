// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef TERA_SDK_CLIENT_IMPL_
#define TERA_SDK_CLIENT_IMPL_

#include "common/thread_pool.h"
#include "proto/master_rpc.pb.h"
#include "proto/tabletnode_client.h"
#include "sdk/sdk_zk.h"
#include "sdk/tera.h"
#include "utils/timer.h"

using std::string;

namespace tera {

struct TSInfo {
    std::string addr;
    std::string query_status;
    std::string status;
    uint64_t data_size;
    uint64_t update_time;
    uint32_t onload_count;
    uint32_t onsplit_count;
};

class ClientImpl : public Client {
public:
    ClientImpl(const std::string& user_identity,
               const std::string& user_passcode,
               const std::string& zk_addr_list,
               const std::string& zk_root_path);

    virtual ~ClientImpl();

    virtual bool CreateTable(const TableDescriptor& desc, ErrorCode* err);

    virtual bool CreateTable(const TableDescriptor& desc,
                             const std::vector<string>& tablet_delim,
                             ErrorCode* err);

    virtual bool UpdateTable(const TableDescriptor& desc, ErrorCode* err);
    virtual bool UpdateCheck(const std::string& table_name, bool* done, ErrorCode* err);

    virtual bool DeleteTable(const std::string& name, ErrorCode* err);

    virtual bool DisableTable(const std::string& name, ErrorCode* err);

    virtual bool EnableTable(const std::string& name, ErrorCode* err);

    virtual bool CreateUser(const std::string& user,
                            const std::string& password, ErrorCode* err);
    virtual bool DeleteUser(const std::string& user, ErrorCode* err);
    virtual bool ChangePwd(const std::string& user,
                           const std::string& password, ErrorCode* err);
    virtual bool ShowUser(const std::string& user, std::vector<std::string>& user_groups,
                          ErrorCode* err);
    virtual bool AddUserToGroup(const std::string& user,
                                const std::string& group, ErrorCode* err);
    virtual bool DeleteUserFromGroup(const std::string& user,
                                     const std::string& group, ErrorCode* err);
    bool OperateUser(UserInfo& operated_user, UserOperateType type,
                     std::vector<std::string>& user_groups, ErrorCode* err);

    virtual Table* OpenTable(const string& table_name, ErrorCode* err);

    virtual bool GetTabletLocation(const string& table_name,
                                   std::vector<TabletInfo>* tablets,
                                   ErrorCode* err);

    virtual TableDescriptor* GetTableDescriptor(const string& table_name, ErrorCode* err);

    virtual bool List(std::vector<TableInfo>* table_list, ErrorCode* err);

    virtual bool List(const string& table_name, TableInfo* table_info,
                      std::vector<TabletInfo>* tablet_list, ErrorCode* err);

    virtual bool IsTableExist(const string& table_name, ErrorCode* err);

    virtual bool IsTableEnabled(const string& table_name, ErrorCode* err);

    virtual bool IsTableEmpty(const string& table_name, ErrorCode* err);

    virtual bool GetSnapshot(const string& name, uint64_t* snapshot, ErrorCode* err);

    virtual bool DelSnapshot(const string& name, uint64_t snapshot, ErrorCode* err);

    virtual bool Rollback(const string& name, uint64_t snapshot,
                          const std::string& rollback_name, ErrorCode* err);

    virtual bool CmdCtrl(const string& command,
                         const std::vector<string>& arg_list,
                         bool* bool_result,
                         string* str_result,
                         ErrorCode* err);

    bool ShowTablesInfo(const string& name,
                        TableMeta* meta,
                        TabletMetaList* tablet_list,
                        ErrorCode* err);

    bool ShowTablesInfo(TableMetaList* table_list,
                        TabletMetaList* tablet_list,
                        bool is_brief,
                        ErrorCode* err);

    bool ShowTabletNodesInfo(const string& addr,
                             TabletNodeInfo* info,
                             TabletMetaList* tablet_list,
                             ErrorCode* err);

    bool ShowTabletNodesInfo(std::vector<TabletNodeInfo>* infos,
                             ErrorCode* err);

    bool Rename(const std::string& old_table_name,
                const std::string& new_table_name,
                ErrorCode* err);

    std::string GetZkAddrList() { return _zk_addr_list; }
    std::string GetZkRootPath() { return _zk_root_path; }

    void CloseTable(const string& table_name);

private:
    bool ListInternal(std::vector<TableInfo>* table_list,
                      std::vector<TabletInfo>* tablet_list,
                      const string& start_table_name,
                      const string& start_tablet_key,
                      uint32_t max_table_found,
                      uint32_t max_tablet_found,
                      ErrorCode* err);

    bool ParseTableEntry(const TableMeta meta,
                         std::vector<TableInfo>* table_list);

    bool ParseTabletEntry(const TabletMeta& meta,
                          std::vector<TabletInfo>* tablet_list);

    std::string GetUserToken(const std::string& user, const std::string& password);
    void DoShowUser(OperateUserResponse& response,
                    std::vector<std::string>& user_groups);
    bool CheckReturnValue(StatusCode status, std::string& reason, ErrorCode* err);
    bool GetInternalTableName(const std::string& table_name, ErrorCode* err,
                              std::string* internal_table_name);

    /// show all tables info: `table_name' should be an empty string
    /// show a single table info: `table_name' should be the table name
    bool DoShowTablesInfo(TableMetaList* table_list,
                          TabletMetaList* tablet_list,
                          const string& table_name,
                          bool is_brief,
                          ErrorCode* err);

    Table* OpenTableInternal(const string& table_name, ErrorCode* err);
private:
    ClientImpl(const ClientImpl&);
    void operator=(const ClientImpl&);
    ThreadPool _thread_pool;

    std::string _user_identity;
    std::string _user_passcode;
    std::string _zk_addr_list;
    std::string _zk_root_path;

    /// _cluster could cache the master_addr & root_table_addr.
    /// if there is no _cluster,
    ///    we have to access zookeeper whenever we need master_addr or root_table_addr.
    /// if there is _cluster,
    ///    we save master_addr & root_table_addr in _cluster, access zookeeper only once.
    sdk::ClusterFinder* _cluster;

    Mutex _open_table_mutex;
    struct TableHandle {
        Table* handle;
        Mutex* mu;
        int ref;
        ErrorCode err;
        TableHandle() : handle(NULL), mu(NULL), ref(0) {}
    };
    std::map<std::string, TableHandle> _open_table_map;
};

} // namespace tera
#endif // TERA_SDK_CLIENT_IMPL_
