# Tera SDK主要api接口说明


### 主要数据结构
 
* tera::[client](../sdk_reference/client.md)
* tera::[table](../sdk_reference/table.md)
* tera::[mutation](../sdk_reference/mutation.md)
* tera::[reader](../sdk_reference/reader.md)
* tera::[table_descriptor](../sdk_reference/table_descriptor.md)
* tera::[transaction](../sdk_reference/transaction.md)
* tera::[scan](../sdk_reference/scan.md)
* tera::[utils](../sdk_reference/utils.md)

<a name="main-data-structure"></a> 
### 介绍
#### (1) tera::client  访问tera服务主结构，所有对tera的访问或操作全部由此发起。
一个集群对应一个client即可，如需访问多个client，需要创建多个
##### 主要功能包括：
* 表格操作：建、删、加载、卸载、打开、关闭、更新表结构、获取表格信息、快照等
* 用户管理：建、删、修改密码、组管理等
* 集群信息获取：获取全部表格列表、状态等
 
#### (2) tera::table  表格主结构，对表格的所有增删查改操作由此发起。
由tera::Client::OpenTable产生，tera::Client::CloseTable关闭，不可析构。
 
#### (3) tera::error_code 错误码，很多操作会返回，注意检查。

#### (4) tera::mutation
 
#### (5) tera::scan 扫描操作，并获取返回数据。
 
#### (6) tera::reader 读取操作，并获取返回数据。
 
#### (7) tera::table_descriptor 表格描述符主体
 
#### (8) tera::transaction 单行事务

 
#### (9) tera::scan 扫描
 
#### (10) tera::utils 编码解码
