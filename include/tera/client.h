// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef  TERA_CLIENT_H_
#define  TERA_CLIENT_H_

#include <stdint.h>
#include <string>
#include <vector>

#include "error_code.h"
#include "table.h"
#include "table_descriptor.h"
#include "transaction.h"

#pragma GCC visibility push(default)
namespace tera {

class Client {
public:
    // Create a new client
    // User should delete Client* if it is no longer needed.
    // A Client can only be deleted if ALL the tables it opened have been deleted.
    static Client* NewClient(const std::string& confpath,
                             const std::string& log_prefix,
                             ErrorCode* err = NULL);
    static Client* NewClient(const std::string& confpath,
                             ErrorCode* err = NULL);
    static Client* NewClient();

    // Open a table by name.
    // This operation could fail due to zookeeper down, meta not avaliable, table not exists, etc.
    virtual Table* OpenTable(const std::string& table_name, ErrorCode* err) = 0;

    // Create a new table with specified descriptor.
    virtual bool CreateTable(const TableDescriptor& desc, ErrorCode* err) = 0;
    // Create a new table with multiple tablets pre-assigned by tablet_delim.
    virtual bool CreateTable(const TableDescriptor& desc,
                             const std::vector<std::string>& tablet_delim,
                             ErrorCode* err) = 0;

    // Update table schema. User should call UpdateCheck to check if the update operation is complete.
    virtual bool UpdateTableSchema(const TableDescriptor& desc, ErrorCode* err) = 0;
    virtual bool UpdateCheck(const std::string& table_name, bool* done, ErrorCode* err) = 0;
    // Disable a table by name. A disabled table will not provide any service.
    virtual bool DisableTable(const std::string& name, ErrorCode* err) = 0;
    // Drop a table by name. Only a disabled table can be dropped.
    virtual bool DropTable(const std::string& name, ErrorCode* err) = 0;
    // Revive a disabled table.
    virtual bool EnableTable(const std::string& name, ErrorCode* err) = 0;

    // Get the descriptor of the table
    virtual TableDescriptor* GetTableDescriptor(const std::string& table_name, ErrorCode* err) = 0;
    // List all tables.
    virtual bool List(std::vector<TableInfo>* table_list, ErrorCode* err) = 0;
    // Get table & tablet(s) info for a specified table.
    virtual bool List(const std::string& table_name, TableInfo* table_info,
                      std::vector<TabletInfo>* tablet_list, ErrorCode* err) = 0;

    // Check the table status by name.
    virtual bool IsTableExist(const std::string& table_name, ErrorCode* err) = 0;
    virtual bool IsTableEnabled(const std::string& table_name, ErrorCode* err) = 0;
    virtual bool IsTableEmpty(const std::string& table_name, ErrorCode* err) = 0;

    // Send command to to server, like meta, tablet, etc.
    virtual bool CmdCtrl(const std::string& command, const std::vector<std::string>& arg_list,
                         bool* bool_result, std::string* str_result, ErrorCode* err) = 0;
    // User who use glog besides tera should call this method to prevent conflict.
    static void SetGlogIsInitialized();

    // User management.
    virtual bool CreateUser(const std::string& user, const std::string& password, ErrorCode* err) = 0;
    virtual bool DeleteUser(const std::string& user, ErrorCode* err) = 0;
    virtual bool ChangePwd(const std::string& user, const std::string& password, ErrorCode* err) = 0;
    virtual bool ShowUser(const std::string& user, std::vector<std::string>& user_groups,
                          ErrorCode* err) = 0;
    virtual bool AddUserToGroup(const std::string& user, const std::string& group, ErrorCode* err) = 0;
    virtual bool DeleteUserFromGroup(const std::string& user,
                                     const std::string& group, ErrorCode* err) = 0;

    // EXPERIMENTAL

    // Create a snapshot for the table.
    virtual bool GetSnapshot(const std::string& name, uint64_t* snapshot, ErrorCode* err) = 0;
    // Delete a specified snapshot.
    virtual bool DelSnapshot(const std::string& name, uint64_t snapshot, ErrorCode* err) = 0;
    // Perform a rollback operation to a specified snapshot
    virtual bool Rollback(const std::string& name, uint64_t snapshot,
                          const std::string& rollback_name, ErrorCode* err) = 0;

    // DEPRECATED

    // Use DropTable instead.
    virtual bool DeleteTable(const std::string& name, ErrorCode* err) = 0;
    // Use UpdateTableSchema instead.
    virtual bool UpdateTable(const TableDescriptor& desc, ErrorCode* err) = 0;
    // Use List instead.
    virtual bool GetTabletLocation(const std::string& table_name, std::vector<TabletInfo>* tablets,
                                   ErrorCode* err) = 0;

    // Rename a table.
    virtual bool Rename(const std::string& old_table_name, const std::string& new_table_name,
                        ErrorCode* err) = 0 ;

    /// New a global transaction
    virtual Transaction* NewGlobalTransaction() = 0;

    Client() {}
    virtual ~Client() {}

private:
    Client(const Client&);
    void operator=(const Client&);
};

} // namespace tera
#pragma GCC visibility pop

#endif  // TERA_CLIENT_H_
