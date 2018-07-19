// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef TERA_SDK_CLIENT_IMPL_
#define TERA_SDK_CLIENT_IMPL_

#include "common/thread_pool.h"
#include "proto/master_rpc.pb.h"
#include "proto/tabletnode_client.h"
#include "sdk/sdk_perf.h"
#include "sdk/sdk_zk.h"
#include "sdk/timeoracle_client_impl.h"
#include "tera.h"
#include "common/timer.h"
#include <memory>

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

struct ClientOptions {
    std::string confpath;
    std::string flagfile;

    std::string user_identity;
    std::string user_passcode;

    ClientOptions() {}
    ~ClientOptions() {}
};

class TableImpl;
class ClientImpl : public Client, public std::enable_shared_from_this<ClientImpl> {
public:
    explicit ClientImpl(const ClientOptions& client_option,
                        ThreadPool* sdk_thread_pool,
                        ThreadPool* sdk_gtxn_thread_pool);

    virtual ~ClientImpl();

    virtual bool CreateTable(const TableDescriptor& desc, ErrorCode* err);

    virtual bool CreateTable(const TableDescriptor& desc,
                             const std::vector<string>& tablet_delim,
                             ErrorCode* err);

    virtual bool UpdateTable(const TableDescriptor& desc, ErrorCode* err);
    virtual bool UpdateTableSchema(const TableDescriptor& desc, ErrorCode* err);
    virtual bool UpdateCheck(const std::string& table_name, bool* done, ErrorCode* err);

    virtual bool DeleteTable(const std::string& name, ErrorCode* err);
    virtual bool DropTable(const std::string& name, ErrorCode* err);

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

    virtual bool CmdCtrl(const string& command,
                         const std::vector<string>& arg_list,
                         bool* bool_result,
                         string* str_result,
                         ErrorCode* err);

    virtual Transaction* NewGlobalTransaction();

    bool ShowTableSchema(const string& name, TableSchema* meta, ErrorCode* err);

    bool ShowTablesInfo(const string& name, TableMeta* meta,
                        TabletMetaList* tablet_list, ErrorCode* err);

    bool ShowTablesInfo(TableMetaList* table_list, TabletMetaList* tablet_list,
                        bool is_brief, ErrorCode* err);

    bool ShowTabletNodesInfo(const string& addr, TabletNodeInfo* info,
                             TabletMetaList* tablet_list, ErrorCode* err);

    bool ShowTabletNodesInfo(std::vector<TabletNodeInfo>* infos, ErrorCode* err);

    void CloseTable(const string& table_name) {}

    bool IsClientAlive(const string& path);

    string ClientSession();

    sdk::ClusterFinder* GetClusterFinder();

private:
    std::shared_ptr<TableImpl> OpenTableInternal(const string& table_name, ErrorCode* err);

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

    /// show all tables info: `table_name' should be an empty string
    /// show a single table info: `table_name' should be the table name
    bool DoShowTablesInfo(TableMetaList* table_list,
                          TabletMetaList* tablet_list,
                          const string& table_name,
                          bool is_brief,
                          ErrorCode* err);

    bool RegisterSelf();

private:
    ClientImpl(const ClientImpl&);
    void operator=(const ClientImpl&);
    ThreadPool* thread_pool_;
    ThreadPool* gtxn_thread_pool_;

    ClientOptions client_options_;

    /// cluster_ could cache the master_addr & root_table_addr.
    /// if there is no cluster_,
    ///    we have to access zookeeper whenever we need master_addr or root_table_addr.
    /// if there is cluster_,
    ///    we save master_addr & root_table_addr in cluster_, access zookeeper only once.
    sdk::ClientZkAdapterBase* client_zk_adapter_;
    sdk::ClusterFinder* cluster_;
    sdk::ClusterFinder* tso_cluster_;
    sdk::PerfCollecter* collecter_;
    std::string session_str_;

    Mutex open_table_mutex_;
    struct TableHandle {
        std::weak_ptr<TableImpl> handle;
        Mutex mu;
        ErrorCode err;
        TableHandle() {}
    };
    std::map<std::string, TableHandle> open_table_map_;
};

// Compatibility with old interface (delete *client)
class ClientWrapper : public Client {
public:
    explicit ClientWrapper(std::shared_ptr<ClientImpl> client_impl) : client_impl_(client_impl) {}
    virtual ~ClientWrapper() {}

    Table* OpenTable(const std::string& table_name, ErrorCode* err) {
        return client_impl_->OpenTable(table_name, err);
    }
    bool CreateTable(const TableDescriptor& desc, ErrorCode* err) {
        return client_impl_->CreateTable(desc, err);
    }
    bool CreateTable(const TableDescriptor& desc,
                             const std::vector<std::string>& tablet_delim,
                             ErrorCode* err) {
        return client_impl_->CreateTable(desc, tablet_delim, err);
    }
    bool UpdateTableSchema(const TableDescriptor& desc, ErrorCode* err) {
        return client_impl_->UpdateTableSchema(desc, err);
    }
    bool UpdateCheck(const std::string& table_name, bool* done, ErrorCode* err) {
        return client_impl_->UpdateCheck(table_name, done, err);
    }
    bool DisableTable(const std::string& name, ErrorCode* err) {
        return client_impl_->DisableTable(name, err);
    }
    bool DropTable(const std::string& name, ErrorCode* err) {
        return client_impl_->DropTable(name, err);
    }
    bool EnableTable(const std::string& name, ErrorCode* err) {
        return client_impl_->EnableTable(name, err);
    }
    TableDescriptor* GetTableDescriptor(const std::string& table_name, ErrorCode* err) {
        return client_impl_->GetTableDescriptor(table_name, err);
    }
    bool List(std::vector<TableInfo>* table_list, ErrorCode* err) {
        return client_impl_->List(table_list, err);
    }
    bool List(const std::string& table_name, TableInfo* table_info,
                      std::vector<TabletInfo>* tablet_list, ErrorCode* err) {
        return client_impl_->List(table_name, table_info, tablet_list, err);
    }
    bool IsTableExist(const std::string& table_name, ErrorCode* err) {
        return client_impl_->IsTableExist(table_name, err);
    }
    bool IsTableEnabled(const std::string& table_name, ErrorCode* err) {
        return client_impl_->IsTableEnabled(table_name, err);
    }
    bool IsTableEmpty(const std::string& table_name, ErrorCode* err) {
        return client_impl_->IsTableEmpty(table_name, err);
    }
    bool CmdCtrl(const std::string& command, const std::vector<std::string>& arg_list,
                         bool* bool_result, std::string* str_result, ErrorCode* err) {
        return client_impl_->CmdCtrl(command, arg_list, bool_result, str_result, err);
    }
    bool CreateUser(const std::string& user, const std::string& password, ErrorCode* err) {
        return client_impl_->CreateUser(user, password, err);
    }
    bool DeleteUser(const std::string& user, ErrorCode* err) {
        return client_impl_->DeleteUser(user, err);
    }
    bool ChangePwd(const std::string& user, const std::string& password, ErrorCode* err) {
        return client_impl_->ChangePwd(user, password, err);
    }
    bool ShowUser(const std::string& user, std::vector<std::string>& user_groups,
                          ErrorCode* err) {
        return client_impl_->ShowUser(user, user_groups, err);
    }
    bool AddUserToGroup(const std::string& user, const std::string& group, ErrorCode* err) {
        return client_impl_->AddUserToGroup(user, group, err);
    }
    bool DeleteUserFromGroup(const std::string& user,
                                     const std::string& group, ErrorCode* err) {
        return client_impl_->DeleteUserFromGroup(user, group, err);
    }
    Transaction* NewGlobalTransaction() {
        return client_impl_->NewGlobalTransaction();
    }

    /* DEPRECATED functions */
    bool DeleteTable(const std::string& name, ErrorCode* err) {
        return client_impl_->DeleteTable(name, err);
    }
    bool UpdateTable(const TableDescriptor& desc, ErrorCode* err) {
        return client_impl_->UpdateTable(desc, err);
    }
    bool GetTabletLocation(const std::string& table_name, std::vector<TabletInfo>* tablets,
                                   ErrorCode* err) {
        return client_impl_->GetTabletLocation(table_name, tablets, err);
    }

    std::shared_ptr<ClientImpl> GetClientImpl() {
        return client_impl_;
    }
   
private:
    std::shared_ptr<ClientImpl> client_impl_;
};


} // namespace tera
#endif // TERA_SDK_CLIENT_IMPL_
