#事务用户手册
Copyright 2016, Baidu, Inc.

##单行事务

###功能
* 提供单行的read-modify-write原子语义  
  下面的例子中，Session B在事务进行过程中，value被Session A所修改，因此B的提交将失败，否则将出现A的更新被覆盖的问题：
  ```
             -------------------
             |  Wang  |  100   |
             -------------------
               Session A              Session B
  time
  |                                   Begin
  |          Begin
  |          value=Get(Wang);
  |          100
  |                                   value=Get(Wang);
  |                                   100
  |
  v          value+=20;
             120
             Put(Wang, value);
                                      value-=20;
                                      80
                                      Put(Wang, value);
             Commit
             success
                                      Commit
                                      fail
  ```

* 两个事务修改同一行，但读操作所涉及到的CF或者Colum无任何交集时，两个事务不会冲突  
  下面的例子中，Session A和Session B分别对同一行的Cf1和Cf2进行read-modify-write操作，后提交的事务不会因先提交的事务而失败：
  ```
             ---------------------------
             |  ROW   |  CF1   |  CF2  |
             ---------------------------
             |  Wang  |  100   |  100  |
             ---------------------------
               Session A              Session B
  time
  |                                   Begin
  |          Begin
  |          value=Get(Wang, CF1);
  |          100
  |                                   value=Get(Wang, CF2);
  |                                   100
  |
  v          value+=20;
             120
             Put(Wang, CF1, value);
                                      value-=20;
                                      80
                                      Put(Wang, CF2, value);
             Commit
             success
                                      Commit
                                      success
  ```
* 能够避免“幻影读”现象  
  下面的例子中，Session A在事务中读取了CF1的[C1 ~ C5)区间，区间内只有C2和C4两个列；Session B插入了一个新的列C4，导致Session A的事务提交失败：
  ```
             ----------------------------------
             |  ROW   |      CF1      |  CF2  |
             |        |  C2   |  C4   |   C1  |
             |  Wang  |  100  |  200  |  300  |
             ----------------------------------
               Session A                                  Session B
  time
  |                                                       Begin;
  |          Begin;
  |          column_list=[C1, C5);
  |          value_list=Get(Wang, CF1, column_list);
  |          100, 200
  |                                                       value=150;
  |                                                       Put(Wang, CF1, C3, value);
  |          value_list+=20;
  v          120, 220
             Put(Wang, CF1, column_list, value_list);
                                                          Commit;
                                                          success
             Commit;
             fail
  ```

###约束
* 不支持多版本语义
  * 写操作的时间戳不能由用户指定，而是由Tera分配，Tera保证一行内的修改时间戳单调递增，也就是说，只支持以下四种操作：  
    * 增加最新版本
    * 删除整行
    * 删除整个CF
    * 删除整个Column
  * 读操作的时间戳不能由用户指定，只能读到第一次Get时快照视图上每个Column的最新版本
  * 数据的历史版本只能由非事务操作修改，历史版本不能参与到事务过程中

###API
```
class Table {
    /// 创建事务
    virtual Transaction* StartRowTransaction(const std::string& row_key) = 0;
    /// 提交事务
    virtual void CommitRowTransaction(Transaction* transaction) = 0;
    /// 回滚事务
    virtual void RollbackRowTransaction(Transaction* transaction) = 0;
};

class Transaction {
    /// 提交一个修改操作
    virtual void ApplyMutation(RowMutation* row_mu) = 0;
    /// 读取操作
    virtual void Get(RowReader* row_reader) = 0;

    /// 回调函数原型
    typedef void (*Callback)(Transaction* transaction);
    /// 设置提交回调, 提交操作会异步返回
    virtual void SetCommitCallback(Callback callback) = 0;
    /// 获取提交回调
    virtual Callback GetCommitCallback() = 0;
    /// 设置回滚回调, 回滚操作会异步返回
    virtual void SetRollbackCallback(Callback callback) = 0;
    /// 获取回滚回调
    virtual Callback GetRollbackCallback() = 0;

    /// 设置用户上下文，可在回调函数中获取
    virtual void SetContext(void* context) = 0;
    /// 获取用户上下文
    virtual void* GetContext() = 0;

    /// 获得结果错误码
    virtual const ErrorCode& GetError() = 0;
  }
```

###使用示例
```
#include "tera.h"

int main() {
    tera::ErrorCode error_code;
    
    // Get a client instance
    tera::Client* client = tera::Client::NewClient("./tera.flag", "txn_sample", &error_code);
    assert(client);
    
    // Create table
    tera::TableDescriptor schema("employee");
    schema.AddColumnFamily("title");
    schema.AddColumnFamily("salary");
    client->CreateTable(schema, &error_code);
    assert(error_code.GetType() == tera::ErrorCode::kOK);
    
    // Open table
    tera::Table* table = client->OpenTable("employee", &error_code);
    assert(table);
    
    // init a row
    tera::RowMutation* init = table->NewRowMutation("Amy");
    init->Put("title", "", "junior");
    init->Put("salary", "", "100");
    table->ApplyMutation(init);
    assert(init->GetError().GetType() == tera::ErrorCode::kOK);
    delete init;
    
    // txn read the row
    tera::Transaction* txn = table->StartRowTransaction();
    tera::RowReader* reader = table->NewRowReader("Amy");
    read->AddColumnFamily("title");
    txn->Get(reader);
    assert(reader->GetError().GetType() == tera::ErrorCode::kOK);
    
    // get title
    std::string title;
    while (!reader->Done()) {
        if (reader->Family() == "title") {
            title = read->Value();
            break;
        }
        reader->Next();
    }
    delete reader;
    
    // txn write the row
    tera::RowMutation* mutation = table->NewRowMutation("Amy");
    if (title == "junior") {
        mutation->Put("title", "", "senior");
        mutation->Put("salary", "", "200");
    } else if (title == "senior") {
        mutation->Put("title", "", "director");
        mutation->Put("salary", "", "300");
    }
    txn->ApplyMutation(mutation);
    assert(read->GetError().GetType() == tera::ErrorCode::kOK);
    delete mutation;
    
    // txn commit
    table->Commit(txn);
    printf("Transaction commit result %s\n", txn->GetError().ToString().c_str());
    delete txn;
    
    // Close
    delete table;
    delete client;
    return 0;
}
```
