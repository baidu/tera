
# Client接口说明

## 主要功能
 
#### 1. 表格管理
##### (1) 新建client  Client::NewClient
```
1.1) static Client* NewClient(const std::string& confpath, const std::string& log_prefix, ErrorCode* err = NULL)
1.2) static Client* NewClient(const std::string& confpath, ErrorCode* err = NULL)
1.3) static Client* NewClient()
```
 
##### (2) 打开表格 Client::OpenTable
```
Table* OpenTable(const std::string& table_name, ErrorCode* err) = 0
```
##### (3) 建表 Client::CreateTable
```
1） bool CreateTable(const TableDescriptor& desc, ErrorCode* err) = 0  //新建带有具体描述符的表格
2） bool CreateTable(const TableDescriptor& desc, const std::vector<std::string>& tablet_delim, ErrorCode* err) = 0 //新建多个前缀为tablet_delim的tablets
```
 
##### (4) 更新schema Client::UpdateTableSchema
 
```
bool ClientImpl::UpdateTableSchema(const TableDescriptor& desc, ErrorCode* err) = 0
```
调用UpdateTable(desc, err)，分两种情况：
* 更新lg属性。需要先disable表格
* 更新cf属性。直接更新
##### (5) 检查更新状态 Client::UpdateCheck
 
```
bool UpdateCheck(const std::string& table_name, bool* done, ErrorCode* err) = 0
```
 
##### (6) disable表 Client::DisableTable
暂停表，表格不再提供读、写服务。某些属性的更新需要先disable表；使用drop删除表时，需要先执行disable操作，此操作不可回滚。
 
```
bool DisableTable(const std::string& name, ErrorCode* err) = 0
```
 
##### (7) drop表 Client::DropTable
删除处于disable状态的表格，此操作不可回滚。
 
```
bool DropTable(const std::string& name, ErrorCode* err) = 0
```
 
##### (8) enable表 Client::EnableTable
 
将处于disable状态的表格重新enable，恢复读、写服务。
 
```
bool EnableTable(const std::string& name, ErrorCode* err) = 0
```
 
##### (9) 获取表的描述符 Client::GetTableDescriptor
```
TableDescriptor* GetTableDescriptor(const std::string& table_name, ErrorCode* err) = 0
```
 
##### (10) 列出所有的表 Client::List
```
bool List(std::vector<TableInfo>* table_list, ErrorCode* err) = 0;//列出所有的表
bool List(const std::string& table_name, TableInfo* table_info, std::vector<TabletInfo>* tablet_list, ErrorCode* err) = 0;//获取指定的表
```
##### (11) 检查表是否存在 Client::IsTableExist
```
bool IsTableExist(const std::string& table_name, ErrorCode* err) = 0
``` 
 
##### (12) 检查表是否为enable状态 Client::IsTableEnabled
```
bool IsTableEnabled(const std::string& table_name, ErrorCode* err) = 0
```
 
##### (13) 检查表是否为空 Client::IsTableEmpty
```
bool IsTableEmpty(const std::string& table_name, ErrorCode* err) = 0
```
 
##### (14) 发送请求给服务器 Client::CmdCtrl
```
bool CmdCtrl(const std::string& command, const std::vector<std::string>& arg_list, bool* bool_result, std::string* str_result, ErrorCode* err) = 0
```
 
##### (15) 使用glog的用户防止冲突 Client::SetGlogIsInitialized
```
void SetGlogIsInitialized()
```
 
##### (16) 删除表格 Client::DeleteTable
```
bool DeleteTable(const std::string& name, ErrorCode* err) = 0
```
 
##### (17) 更新表格 Client::UpdateTable
```
bool UpdateTable(const TableDescriptor& desc, ErrorCode* err) = 0
```
 
##### (18) 获得表格的位置 Client::GetTabletLocation
```
bool GetTabletLocation(const std::string& table_name, std::vector<TabletInfo>* tablets, ErrorCode* err) = 0
```
 
##### (19) 重命名表格 Client::Rename
```
bool Rename(const std::string& old_table_name, const std::string& new_table_name, ErrorCode* err) = 0
```
#### 2. 用户管理
 
##### (1) 创建用户 Client::CreateUser
 
```
bool ClientImpl::CreateUser(const std::string& user,
                            const std::string& password, ErrorCode* err) = 0
```
##### (2) 删除用户 Client::DeleteUser
 
```
bool ClientImpl::DeleteUser(const std::string& user, ErrorCode* err) = 0
```
 
##### (3) 修改用户密码 Client::ChangePwd
 
```
bool ClientImpl::ChangePwd(const std::string& user, const std::string& password, ErrorCode* err) = 0
```
 
##### (4) 显示指定用户信息 Client::ShowUser
 
```
bool ClientImpl::ShowUser(const std::string& user, std::vector<std::string>& user_groups, ErrorCode* err) = 0
```
 
##### (5) 添加用户到用户群 Client::AddUserToGroup
 
```
bool ClientImpl::AddUserToGroup(const std::string& user_name, const std::string& group_name, ErrorCode* err)= 0
```
 
##### (6) 从用户群中删除用户 Client::DeleteUserFromGroup
 
```
bool ClientImpl::DeleteUserFromGroup(const std::string& user_name, const std::string& group_name, ErrorCode* err) = 0
```   
<!-- 
#### 3. 快照管理
##### (1) 为表格创建快照 Client::GetSnapshot
```
bool GetSnapshot(const std::string& name, uint64_t* snapshot, ErrorCode* err) = 0
```
 
##### (2) 删除快照 Client::DelSnapshot
```
bool DelSnapshot(const std::string& name, uint64_t snapshot, ErrorCode* err) = 0
```
 
##### (3) 为特定的快照执行回滚操作 Client::Rollback
```
bool Rollback(const std::string& name, uint64_t snapshot, const std::string& rollback_name, ErrorCode* err) = 0
```
-->    
 

