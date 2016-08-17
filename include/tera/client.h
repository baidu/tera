// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef  TERA_CLIENT_H_
#define  TERA_CLIENT_H_

#include <stdint.h>
#include <list>
#include <map>
#include <set>
#include <string>
#include <vector>

#include "error_code.h"
#include "table.h"
#include "table_descriptor.h"

#pragma GCC visibility push(default)
namespace tera {

class Client {
public:
    /// 使用glog的用户必须调用此接口，避免glog被重复初始化
    static void SetGlogIsInitialized();

    static Client* NewClient(const std::string& confpath,
                             const std::string& log_prefix,
                             ErrorCode* err = NULL);

    static Client* NewClient(const std::string& confpath,
                             ErrorCode* err = NULL);

    static Client* NewClient();

    /// 创建表格
    virtual bool CreateTable(const TableDescriptor& desc, ErrorCode* err) = 0;
    virtual bool CreateTable(const TableDescriptor& desc,
                             const std::vector<std::string>& tablet_delim,
                             ErrorCode* err) = 0;
    /// 更新表格Schema
    virtual bool UpdateTable(const TableDescriptor& desc, ErrorCode* err) = 0;
    virtual bool UpdateCheck(const std::string& table_name, bool* done, ErrorCode* err) = 0;
    /// 删除表格
    virtual bool DeleteTable(const std::string& name, ErrorCode* err) = 0;
    /// 停止表格服务
    virtual bool DisableTable(const std::string& name, ErrorCode* err) = 0;
    /// 恢复表格服务
    virtual bool EnableTable(const std::string& name, ErrorCode* err) = 0;

    /// acl
    virtual bool CreateUser(const std::string& user,
                            const std::string& password, ErrorCode* err) = 0;
    virtual bool DeleteUser(const std::string& user, ErrorCode* err) = 0;
    virtual bool ChangePwd(const std::string& user,
                           const std::string& password, ErrorCode* err) = 0;
    virtual bool ShowUser(const std::string& user, std::vector<std::string>& user_groups,
                          ErrorCode* err) = 0;
    virtual bool AddUserToGroup(const std::string& user,
                                const std::string& group, ErrorCode* err) = 0;
    virtual bool DeleteUserFromGroup(const std::string& user,
                                     const std::string& group, ErrorCode* err) = 0;
    /// 打开表格, 失败返回NULL
    virtual Table* OpenTable(const std::string& table_name, ErrorCode* err) = 0;
    /// 获取表格分布信息
    virtual bool GetTabletLocation(const std::string& table_name,
                                   std::vector<TabletInfo>* tablets,
                                   ErrorCode* err) = 0;
    /// 获取表格Schema
    virtual TableDescriptor* GetTableDescriptor(const std::string& table_name,
                                                ErrorCode* err) = 0;

    virtual bool List(std::vector<TableInfo>* table_list,
                      ErrorCode* err) = 0;

    virtual bool List(const std::string& table_name,
                      TableInfo* table_info,
                      std::vector<TabletInfo>* tablet_list,
                      ErrorCode* err) = 0;

    virtual bool IsTableExist(const std::string& table_name, ErrorCode* err) = 0;

    virtual bool IsTableEnabled(const std::string& table_name, ErrorCode* err) = 0;

    virtual bool IsTableEmpty(const std::string& table_name, ErrorCode* err) = 0;

    virtual bool GetSnapshot(const std::string& name, uint64_t* snapshot, ErrorCode* err) = 0;
    virtual bool DelSnapshot(const std::string& name, uint64_t snapshot,ErrorCode* err) = 0;
    virtual bool Rollback(const std::string& name, uint64_t snapshot,
                          const std::string& rollback_name, ErrorCode* err) = 0;

    virtual bool CmdCtrl(const std::string& command,
                         const std::vector<std::string>& arg_list,
                         bool* bool_result,
                         std::string* str_result,
                         ErrorCode* err) = 0;
    virtual bool Rename(const std::string& old_table_name,
                        const std::string& new_table_name,
                        ErrorCode* err) = 0 ;
    Client() {}
    virtual ~Client() {}

private:
    Client(const Client&);
    void operator=(const Client&);
};

} // namespace tera
#pragma GCC visibility pop

#endif  // TERA_CLIENT_H_
