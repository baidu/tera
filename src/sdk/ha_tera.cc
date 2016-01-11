#include "mutate_impl.h"
#include "read_impl.h"
#include "ha_tera.h"

namespace tera {

void HATable::Add(Table *t) {
    _tables.push_back(t);
}

RowMutation* HATable::NewRowMutation(const std::string& row_key) {
    return new RowMutationImpl(NULL, row_key);
}

RowReader* HATable::NewRowReader(const std::string& row_key) {
    return new RowReaderImpl(NULL, row_key);
}

void HATable::ApplyMutation(RowMutation* row_mu) {
    size_t failed_count = 0;
    for (size_t i = 0; i < _tables.size(); i++) {
        _tables[i]->ApplyMutation(row_mu);
        if (row_mu->GetError().GetType() != ErrorCode::kOK) {
            failed_count++;
            LOG(WARNING) << "ApplyMutation failed! " << row_mu->GetError().GetType() << " at tera:" << i;
        }
        // 如果所有集群都失败了，则认为失败
        if (failed_count < _tables.size()) {
            // 重置除用户数据外的数据，以用于后面的写
            row_mu->Reset();
        }
    }
}

void HATable::ApplyMutation(const std::vector<RowMutation*>& row_mu_list) {
    std::vector<size_t> failed_count_list;
    failed_count_list.resize(row_mu_list.size());

    for (size_t i = 0; i < _tables.size(); i++) {
        _tables[i]->ApplyMutation(row_mu_list);
        for (size_t j = 0; j < row_mu_list.size(); j++) {
            RowMutation* row_mu = row_mu_list[j];
            if (row_mu->GetError().GetType() != ErrorCode::kOK) {
                LOG(WARNING) << j << " ApplyMutation failed! "
                             << row_mu->GetError().GetType()
                             << " at tera:" << i;
                failed_count_list[i]++;
            }
            // 如果所有集群都失败了，则认为失败
            if (failed_count_list[i] < _tables.size()) {
                // 重置除用户数据外的数据，以用于后面的写
                row_mu->Reset();
            }
        }
    }
}

bool HATable::Put(const std::string& row_key, const std::string& family,
                   const std::string& qualifier, const std::string& value,
                   ErrorCode* err) {
    size_t failed_count = 0;
    for (size_t i = 0; i < _tables.size(); i++) {
        bool ok = _tables[i]->Put(row_key, family, qualifier, value, err);
        if (!ok) {
            LOG(WARNING) << "Put failed! " << err->GetReason() << " at tera:" << i;
            failed_count++;
        }
    }

    if (failed_count >= _tables.size()) {
        return false;
    } else {
        err->SetFailed(ErrorCode::kOK, "success");
        return true;
    }
}

bool HATable::Put(const std::string& row_key, const std::string& family,
                   const std::string& qualifier, const int64_t value,
                   ErrorCode* err) {
    size_t failed_count = 0;
    for (size_t i = 0; i < _tables.size(); i++) {
        bool ok = _tables[i]->Put(row_key, family, qualifier, value, err);
        if (!ok) {
            LOG(WARNING) << "Put failed! " << err->GetReason() << " at tera:" << i;
            failed_count++;
        }
    }

    if (failed_count >= _tables.size()) {
        return false;
    } else {
        err->SetFailed(ErrorCode::kOK, "success");
        return true;
    }
}

bool HATable::Put(const std::string& row_key, const std::string& family,
                   const std::string& qualifier, const std::string& value,
                   int32_t ttl, ErrorCode* err) {
    size_t failed_count = 0;
    for (size_t i = 0; i < _tables.size(); i++) {
        bool ok = _tables[i]->Put(row_key, family, qualifier, value, ttl, err);
        if (!ok) {
            LOG(WARNING) << "Put failed! " << err->GetReason() << " at tera:" << i;
            failed_count++;
        }
    }

    if (failed_count >= _tables.size()) {
        return false;
    } else {
        err->SetFailed(ErrorCode::kOK, "success");
        return true;
    }
}

bool HATable::Put(const std::string& row_key, const std::string& family,
                   const std::string& qualifier, const std::string& value,
                   int64_t timestamp, int32_t ttl, ErrorCode* err) {
    size_t failed_count = 0;
    for (size_t i = 0; i < _tables.size(); i++) {
        bool ok = _tables[i]->Put(row_key, family, qualifier, value, timestamp, ttl, err);
        if (!ok) {
            LOG(WARNING) << "Put failed! " << err->GetReason() << " at tera:" << i;
            failed_count++;
        }
    }

    if (failed_count >= _tables.size()) {
        return false;
    } else {
        err->SetFailed(ErrorCode::kOK, "success");
        return true;
    }
}

bool HATable::Add(const std::string& row_key, const std::string& family,
                   const std::string& qualifier, int64_t delta,
                   ErrorCode* err) {
    size_t failed_count = 0;
    for (size_t i = 0; i < _tables.size(); i++) {
        bool ok = _tables[i]->Add(row_key, family, qualifier, delta, err);
        if (!ok) {
            LOG(WARNING) << "Add failed! " << err->GetReason() << " at tera:" << i;
            failed_count++;
        }
    }

    if (failed_count >= _tables.size()) {
        return false;
    } else {
        err->SetFailed(ErrorCode::kOK, "success");
        return true;
    }
}

bool HATable::AddInt64(const std::string& row_key, const std::string& family,
                        const std::string& qualifier, int64_t delta,
                        ErrorCode* err) {
    size_t failed_count = 0;
    for (size_t i = 0; i < _tables.size(); i++) {
        bool ok = _tables[i]->AddInt64(row_key, family, qualifier, delta, err);
        if (!ok) {
            LOG(WARNING) << "AddInt64 failed! " << err->GetReason() << " at tera:" << i;
            failed_count++;
        }
    }

    if (failed_count >= _tables.size()) {
        return false;
    } else {
        err->SetFailed(ErrorCode::kOK, "success");
        return true;
    }
}

bool HATable::PutIfAbsent(const std::string& row_key,
                          const std::string& family,
                          const std::string& qualifier,
                          const std::string& value,
                          ErrorCode* err) {
    size_t failed_count = 0;
    for (size_t i = 0; i < _tables.size(); i++) {
        bool ok = _tables[i]->PutIfAbsent(row_key, family, qualifier, value, err);
        if (!ok) {
            LOG(WARNING) << "PutIfAbsent failed! " << err->GetReason() << " at tera:" << i;
            failed_count++;
        }
    }

    if (failed_count >= _tables.size()) {
        return false;
    } else {
        err->SetFailed(ErrorCode::kOK, "success");
        return true;
    }
}

bool HATable::Append(const std::string& row_key, const std::string& family,
                     const std::string& qualifier, const std::string& value,
                     ErrorCode* err) {
    size_t failed_count = 0;
    for (size_t i = 0; i < _tables.size(); i++) {
        bool ok = _tables[i]->Append(row_key, family, qualifier, value, err);
        if (!ok) {
            LOG(WARNING) << "Append failed! " << err->GetReason() << " at tera:" << i;
            failed_count++;
        }
    }

    if (failed_count >= _tables.size()) {
        return false;
    } else {
        err->SetFailed(ErrorCode::kOK, "success");
        return true;
    }
}

void HATable::Get(RowReader* row_reader) {
    size_t failed_count = 0;
    for (size_t i = 0; i < _tables.size(); i++) {
        _tables[i]->Get(row_reader);
        if (row_reader->GetError().GetType() != ErrorCode::kOK) {
            LOG(WARNING) << "Get failed! " << row_reader->GetError().GetReason() << " at tera:" << i;
            failed_count++;
        } else {
            break;
        }
        if (failed_count < _tables.size()) {
            row_reader->Reset();
        }
    }
}

// 可能有一批数据来自集群1，另一批数据来自集群2
void HATable::Get(const std::vector<RowReader*>& row_readers) {
    std::vector<size_t> failed_count_list;
    failed_count_list.resize(row_readers.size());
    std::vector<RowReader*> tmp_read = row_readers;

    for (size_t i = 0; i < _tables.size(); i++) {
        if (tmp_read.size() <= 0) {
            continue;
        }
        std::vector<RowReader*> need_read = tmp_read;
        _tables[i]->Get(need_read);
        tmp_read.clear();
        for (size_t j = 0; j < need_read.size(); j++) {
            RowReader* row_reader = need_read[j];
            if (row_reader->GetError().GetType() != ErrorCode::kOK) {
                LOG(WARNING) << j << " Get failed! error: "
                             << row_reader->GetError().GetType()
                             << ", " << row_reader->GetError().GetReason()
                             << " at tera:" << i;
                failed_count_list[j] += 1;

                // 如果所有集群都失败了，则认为失败
                if (failed_count_list[j] < _tables.size()) {
                    // 重置除用户数据外的数据，以用于后面的写
                    row_reader->Reset();
                    tmp_read.push_back(row_reader);
                }
            } //fail
        }
    }
}

bool HATable::Get(const std::string& row_key, const std::string& family,
                  const std::string& qualifier, std::string* value,
                  ErrorCode* err, uint64_t snapshot_id) {
    size_t failed_count = 0;
    for (size_t i = 0; i < _tables.size(); i++) {
        bool ok = _tables[i]->Get(row_key, family, qualifier, value, err, snapshot_id);
        if (!ok) {
            LOG(WARNING) << "Get failed! " << err->GetReason() << " at tera:" << i;
            failed_count++;
        } else {
            break;
        }
    }
    return (failed_count>=_tables.size()) ? false : true;
}

bool HATable::Get(const std::string& row_key, const std::string& family,
                  const std::string& qualifier, int64_t* value,
                  ErrorCode* err, uint64_t snapshot_id) {
    size_t failed_count = 0;
    for (size_t i = 0; i < _tables.size(); i++) {
        bool ok = _tables[i]->Get(row_key, family, qualifier, value, err, snapshot_id);
        if (!ok) {
            LOG(WARNING) << "Get failed! " << err->GetReason() << " at tera:" << i;
            failed_count++;
        } else {
            break;
        }
    }
    return (failed_count>=_tables.size()) ? false : true;
}

bool HATable::IsPutFinished() {
    for (size_t i = 0; i < _tables.size(); i++) {
        if (!_tables[i]->IsPutFinished()) {
            return false;
        }
    }
    return true;
}

bool HATable::IsGetFinished() {
    for (size_t i = 0; i < _tables.size(); i++) {
        if (!_tables[i]->IsGetFinished()) {
            return false;
        }
    }
    return true;
}

ResultStream* HATable::Scan(const ScanDescriptor& desc, ErrorCode* err) {
    for (size_t i = 0; i < _tables.size(); i++) {
        ResultStream* rs = _tables[i]->Scan(desc, err);
        if (rs == NULL) {
            LOG(WARNING) << "Scan failed! " << err->GetReason() << " at tera:" << i;
        } else {
            return rs;
        }
    }
    return NULL;
}

const std::string HATable::GetName() {
    for (size_t i = 0; i < _tables.size(); i++) {
        return _tables[i]->GetName();
    }
    return "";
}

bool HATable::Flush() {
    return false;
}

bool HATable::CheckAndApply(const std::string& rowkey, const std::string& cf_c,
                            const std::string& value, const RowMutation& row_mu,
                            ErrorCode* err) {
    err->SetFailed(ErrorCode::kNotImpl);
    return false;
}

int64_t HATable::IncrementColumnValue(const std::string& row, const std::string& family,
                                      const std::string& qualifier, int64_t amount,
                                      ErrorCode* err) {
    err->SetFailed(ErrorCode::kNotImpl);
    return 0L;
}

void HATable::SetWriteTimeout(int64_t timeout_ms) {
    for (size_t i = 0; i < _tables.size(); i++) {
        _tables[i]->SetWriteTimeout(timeout_ms);
    }
}

void HATable::SetReadTimeout(int64_t timeout_ms) {
    for (size_t i = 0; i < _tables.size(); i++) {
        _tables[i]->SetReadTimeout(timeout_ms);
    }
}

bool HATable::LockRow(const std::string& rowkey, RowLock* lock, ErrorCode* err) {
    err->SetFailed(ErrorCode::kNotImpl);
    return false;
}

bool HATable::GetStartEndKeys(std::string* start_key, std::string* end_key,
                              ErrorCode* err) {
    err->SetFailed(ErrorCode::kNotImpl);
    return false;
}

bool HATable::GetTabletLocation(std::vector<TabletInfo>* tablets,
                                ErrorCode* err) {
    err->SetFailed(ErrorCode::kNotImpl);
    return false;
}

bool HATable::GetDescriptor(TableDescriptor* desc, ErrorCode* err) {
    err->SetFailed(ErrorCode::kNotImpl);
    return false;
}

void HATable::SetMaxMutationPendingNum(uint64_t max_pending_num) {
    for (size_t i = 0; i < _tables.size(); i++) {
        _tables[i]->SetMaxMutationPendingNum(max_pending_num);
    }
}

void HATable::SetMaxReaderPendingNum(uint64_t max_pending_num) {
    for (size_t i = 0; i < _tables.size(); i++) {
        _tables[i]->SetMaxReaderPendingNum(max_pending_num);
    }
}

Table* HATable::GetClusterHandle(size_t i) {
    return (i < _tables.size()) ? _tables[i] : NULL;
}

void HAClient::SetGlogIsInitialized() {
    Client::SetGlogIsInitialized();
}

HAClient* HAClient::NewClient(const std::vector<std::string>& confpaths,
                              const std::string& log_prefix,
                              ErrorCode* err) {
    std::vector<std::string> t_confpaths = confpaths;

    // 空则设置一个默认
    if (t_confpaths.size() <= 0) {
        t_confpaths.push_back("");
    }

    size_t failed_count = 0;
    std::vector<Client*> cs;
    for (size_t i = 0; i < t_confpaths.size(); i++) {
        Client* c = Client::NewClient(t_confpaths[i], log_prefix, err);
        if (c == NULL) {
            failed_count++;
        } else {
            cs.push_back(c);
        }
    }

    if (failed_count >= cs.size()) {
        return NULL;
    } else {
        err->SetFailed(ErrorCode::kOK);
        return new HAClient(cs);
    }
}

bool HAClient::CreateTable(const TableDescriptor& desc, ErrorCode* err) {
    bool ok = false;
    size_t failed_count = 0;
    for (size_t i = 0; i < _clients.size(); i++) {
        ok = _clients[i]->CreateTable(desc, err);
        if (!ok) {
            LOG(WARNING) << "Create table failed! " << err->GetReason() << " at tera:" << i;
            failed_count++;
        }
    }

    if (failed_count >= _clients.size()) {
        return false;
    } else {
        err->SetFailed(ErrorCode::kOK, "success");
        return true;
    }
}

bool HAClient::CreateTable(const TableDescriptor& desc,
                           const std::vector<std::string>& tablet_delim,
                           ErrorCode* err) {
    bool ok = false;
    size_t failed_count = 0;
    for (size_t i = 0; i < _clients.size(); i++) {
        ok = _clients[i]->CreateTable(desc, tablet_delim, err);
        if (!ok) {
            LOG(WARNING) << "CreateTable failed! " << err->GetReason() << " at tera:" << i;
            failed_count++;
        }
    }

    if (failed_count >= _clients.size()) {
        return false;
    } else {
        err->SetFailed(ErrorCode::kOK, "success");
        return true;
    }
}

bool HAClient::UpdateTable(const TableDescriptor& desc, ErrorCode* err) {
    bool ok = false;
    size_t failed_count = 0;
    for (size_t i = 0; i < _clients.size(); i++) {
        ok = _clients[i]->UpdateTable(desc, err);
        if (!ok) {
            LOG(WARNING) << "UpdateTable failed! " << err->GetReason() << " at tera:" << i;
            failed_count++;
        }
    }

    if (failed_count >= _clients.size()) {
        return false;
    } else {
        err->SetFailed(ErrorCode::kOK, "success");
        return true;
    }
}

bool HAClient::DeleteTable(std::string name, ErrorCode* err) {
    bool ok = false;
    size_t failed_count = 0;
    for (size_t i = 0; i < _clients.size(); i++) {
        ok = _clients[i]->DeleteTable(name, err);
        if (!ok) {
            LOG(WARNING) << "DeleteTable failed! " << err->GetReason() << " at tera:" << i;
            failed_count++;
        }
    }

    if (failed_count >= _clients.size()) {
        return false;
    } else {
        err->SetFailed(ErrorCode::kOK, "success");
        return true;
    }
}

bool HAClient::DisableTable(std::string name, ErrorCode* err) {
    bool ok = false;
    size_t failed_count = 0;
    for (size_t i = 0; i < _clients.size(); i++) {
        ok = _clients[i]->DisableTable(name, err);
        if (!ok) {
            LOG(WARNING) << "DisableTable failed! " << err->GetReason() << " at tera:" << i;
            failed_count++;
        }
    }

    if (failed_count >= _clients.size()) {
        return false;
    } else {
        err->SetFailed(ErrorCode::kOK, "success");
        return true;
    }
}

bool HAClient::EnableTable(std::string name, ErrorCode* err) {
    bool ok = false;
    size_t failed_count = 0;
    for (size_t i = 0; i < _clients.size(); i++) {
        ok = _clients[i]->EnableTable(name, err);
        if (!ok) {
            LOG(WARNING) << "EnableTable failed! " << err->GetReason() << " at tera:" << i;
            failed_count++;
        }
    }

    if (failed_count >= _clients.size()) {
        return false;
    } else {
        err->SetFailed(ErrorCode::kOK, "success");
        return true;
    }
}

bool HAClient::CreateUser(const std::string& user,
                          const std::string& password, ErrorCode* err) {
    bool ok = false;
    size_t failed_count = 0;
    for (size_t i = 0; i < _clients.size(); i++) {
        ok = _clients[i]->CreateUser(user, password, err);
        if (!ok) {
            LOG(WARNING) << "CreateUser failed! " << err->GetReason() << " at tera:" << i;
            failed_count++;
        }
    }

    if (failed_count >= _clients.size()) {
        return false;
    } else {
        err->SetFailed(ErrorCode::kOK, "success");
        return true;
    }
}


bool HAClient::DeleteUser(const std::string& user, ErrorCode* err) {
    bool ok = false;
    size_t failed_count = 0;
    for (size_t i = 0; i < _clients.size(); i++) {
        ok = _clients[i]->DeleteUser(user, err);
        if (!ok) {
            LOG(WARNING) << "DeleteUser failed! " << err->GetReason() << " at tera:" << i;
            failed_count++;
        }
    }

    if (failed_count >= _clients.size()) {
        return false;
    } else {
        err->SetFailed(ErrorCode::kOK, "success");
        return true;
    }
}

bool HAClient::ChangePwd(const std::string& user,
                         const std::string& password, ErrorCode* err) {
    bool ok = false;
    size_t failed_count = 0;
    for (size_t i = 0; i < _clients.size(); i++) {
        ok = _clients[i]->ChangePwd(user, password, err);
        if (!ok) {
            LOG(WARNING) << "ChangePwd failed! " << err->GetReason() << " at tera:" << i;
            failed_count++;
        }
    }

    if (failed_count >= _clients.size()) {
        return false;
    } else {
        err->SetFailed(ErrorCode::kOK, "success");
        return true;
    }
}

bool HAClient::ShowUser(const std::string& user, std::vector<std::string>& user_groups,
                        ErrorCode* err) {
    bool ok = false;
    size_t failed_count = 0;
    for (size_t i = 0; i < _clients.size(); i++) {
        ok = _clients[i]->ShowUser(user, user_groups, err);
        if (!ok) {
            LOG(WARNING) << "ChangePwd failed! " << err->GetReason() << " at tera:" << i;
            failed_count++;
        } else {
            // 对于读操作，只要一个成功就行
            break;
        }
    }

    return (failed_count >= _clients.size()) ? false : true;
}

bool HAClient::AddUserToGroup(const std::string& user,
                              const std::string& group, ErrorCode* err) {
    bool ok = false;
    size_t failed_count = 0;
    for (size_t i = 0; i < _clients.size(); i++) {
        ok = _clients[i]->AddUserToGroup(user, group, err);
        if (!ok) {
            LOG(WARNING) << "AddUserToGroup failed! " << err->GetReason() << " at tera:" << i;
            failed_count++;
        }
    }

    if (failed_count >= _clients.size()) {
        return false;
    } else {
        err->SetFailed(ErrorCode::kOK, "success");
        return true;
    }
}

bool HAClient::DeleteUserFromGroup(const std::string& user,
                                   const std::string& group, ErrorCode* err) {
    bool ok = false;
    size_t failed_count = 0;
    for (size_t i = 0; i < _clients.size(); i++) {
        ok = _clients[i]->DeleteUserFromGroup(user, group, err);
        if (!ok) {
            LOG(WARNING) << "DeleteUserFromGroup failed! " << err->GetReason() << " at tera:" << i;
            failed_count++;
        }
    }

    if (failed_count >= _clients.size()) {
        return false;
    } else {
        err->SetFailed(ErrorCode::kOK, "success");
        return true;
    }
}

HATable* HAClient::OpenTable(const std::string& table_name, ErrorCode* err) {
    size_t failed_count = 0;
    HATable* ha_table = NULL;
    for (size_t i = 0; i < _clients.size(); i++) {
        Table *t = _clients[i]->OpenTable(table_name, err);
        if (t == NULL) {
            LOG(WARNING) << "OpenTable failed! " << err->GetReason() << " at tera:" << i;
            failed_count++;
        } else {
            if (ha_table == NULL) {
                ha_table = new HATable();
            }
            ha_table->Add(t);
        }
    }

    if (failed_count >= _clients.size()) {
        return NULL;
    } else {
        err->SetFailed(ErrorCode::kOK, "success");
        return ha_table;
    }
}

bool HAClient::GetTabletLocation(const std::string& table_name,
                                 std::vector<TabletInfo>* tablets,
                                 ErrorCode* err) {
    bool ok = false;
    size_t failed_count = 0;
    for (size_t i = 0; i < _clients.size(); i++) {
        ok = _clients[i]->GetTabletLocation(table_name, tablets, err);
        if (!ok) {
            LOG(WARNING) << "ChangePwd failed! " << err->GetReason() << " at tera:" << i;
            failed_count++;
        } else {
            // 对于读操作，只要一个成功就行
            break;
        }
    }

    return (failed_count >= _clients.size()) ? false : true;
}

TableDescriptor* HAClient::GetTableDescriptor(const std::string& table_name,
                                              ErrorCode* err) {
    TableDescriptor* td = NULL;
    size_t failed_count = 0;
    for (size_t i = 0; i < _clients.size(); i++) {
        td = _clients[i]->GetTableDescriptor(table_name, err);
        if (td == NULL) {
            LOG(WARNING) << "ChangePwd failed! " << err->GetReason() << " at tera:" << i;
            failed_count++;
        } else {
            break;
        }
    }

    return (failed_count >= _clients.size()) ? NULL : td;
}

bool HAClient::List(std::vector<TableInfo>* table_list,
                    ErrorCode* err) {
    bool ok = false;
    size_t failed_count = 0;
    for (size_t i = 0; i < _clients.size(); i++) {
        ok = _clients[i]->List(table_list, err);
        if (!ok) {
            LOG(WARNING) << "List failed! " << err->GetReason() << " at tera:" << i;
            failed_count++;
        } else {
            // 对于读操作，只要一个成功就行
            break;
        }
    }

    return (failed_count >= _clients.size()) ? false : true;
}


bool HAClient::List(const std::string& table_name,
                    TableInfo* table_info,
                    std::vector<TabletInfo>* tablet_list,
                    ErrorCode* err) {
    bool ok = false;
    size_t failed_count = 0;
    for (size_t i = 0; i < _clients.size(); i++) {
        ok = _clients[i]->List(table_name, table_info, tablet_list, err);
        if (!ok) {
            LOG(WARNING) << "List failed! " << err->GetReason() << " at tera:" << i;
            failed_count++;
        } else {
            // 对于读操作，只要一个成功就行
            break;
        }
    }

    return (failed_count >= _clients.size()) ? false : true;
}

bool HAClient::IsTableExist(const std::string& table_name, ErrorCode* err) {
    bool ok = false;
    size_t failed_count = 0;
    for (size_t i = 0; i < _clients.size(); i++) {
        ok = _clients[i]->IsTableExist(table_name, err);
        if (!ok) {
            LOG(WARNING) << "IsTableExist failed! " << err->GetReason() << " at tera:" << i;
            failed_count++;
        } else {
            // 对于读操作，只要一个成功就行
            break;
        }
    }

    return (failed_count >= _clients.size()) ? false : true;
}

bool HAClient::IsTableEnabled(const std::string& table_name, ErrorCode* err) {
    bool ok = false;
    size_t failed_count = 0;
    for (size_t i = 0; i < _clients.size(); i++) {
        ok = _clients[i]->IsTableEnabled(table_name, err);
        if (!ok) {
            LOG(WARNING) << "IsTableEnabled failed! " << err->GetReason() << " at tera:" << i;
            failed_count++;
        } else {
            // 对于读操作，只要一个成功就行
            break;
        }
    }

    return (failed_count >= _clients.size()) ? false : true;
}

bool HAClient::IsTableEmpty(const std::string& table_name, ErrorCode* err) {
    bool ok = false;
    size_t failed_count = 0;
    for (size_t i = 0; i < _clients.size(); i++) {
        ok = _clients[i]->IsTableEmpty(table_name, err);
        if (!ok) {
            LOG(WARNING) << "IsTableEmpty failed! " << err->GetReason() << " at tera:" << i;
            failed_count++;
        } else {
            // 对于读操作，只要一个成功就行
            break;
        }
    }

    return (failed_count >= _clients.size()) ? false : true;
}

bool HAClient::GetSnapshot(const std::string& name, uint64_t* snapshot, ErrorCode* err) {
    bool ok = false;
    size_t failed_count = 0;
    for (size_t i = 0; i < _clients.size(); i++) {
        ok = _clients[i]->GetSnapshot(name, snapshot, err);
        if (!ok) {
            LOG(WARNING) << "GetSnapshot failed! " << err->GetReason() << " at tera:" << i;
            failed_count++;
        } else {
            // 对于读操作，只要一个成功就行
            break;
        }
    }

    return (failed_count >= _clients.size()) ? false : true;
}

bool HAClient::DelSnapshot(const std::string& name, uint64_t snapshot,ErrorCode* err) {
    bool ok = false;
    size_t failed_count = 0;
    for (size_t i = 0; i < _clients.size(); i++) {
        ok = _clients[i]->DelSnapshot(name, snapshot, err);
        if (!ok) {
            LOG(WARNING) << "DelSnapshot failed! " << err->GetReason() << " at tera:" << i;
            failed_count++;
        }
    }

    if (failed_count >= _clients.size()) {
        return false;
    } else {
        err->SetFailed(ErrorCode::kOK, "success");
        return true;
    }
}

bool HAClient::Rollback(const std::string& name, uint64_t snapshot,
                        const std::string& rollback_name, ErrorCode* err) {
    bool ok = false;
    size_t failed_count = 0;
    for (size_t i = 0; i < _clients.size(); i++) {
        ok = _clients[i]->Rollback(name, snapshot, rollback_name, err);
        if (!ok) {
            LOG(WARNING) << "Rollback failed! " << err->GetReason() << " at tera:" << i;
            failed_count++;
        }
    }

    if (failed_count >= _clients.size()) {
        return false;
    } else {
        err->SetFailed(ErrorCode::kOK, "success");
        return true;
    }
}

bool HAClient::CmdCtrl(const std::string& command,
                       const std::vector<std::string>& arg_list,
                       bool* bool_result,
                       std::string* str_result,
                       ErrorCode* err) {
    bool ok = false;
    size_t failed_count = 0;
    for (size_t i = 0; i < _clients.size(); i++) {
        ok = _clients[i]->CmdCtrl(command, arg_list, bool_result, str_result, err);
        if (!ok) {
            LOG(WARNING) << "CmdCtrl failed! " << err->GetReason() << " at tera:" << i;
            failed_count++;
        } else {
            // 对于读操作，只要一个成功就行
            break;
        }
    }

    return (failed_count >= _clients.size()) ? false : true;
}

bool HAClient::Rename(const std::string& old_table_name,
                      const std::string& new_table_name,
                      ErrorCode* err) {
    bool ok = false;
    size_t failed_count = 0;
    for (size_t i = 0; i < _clients.size(); i++) {
        ok = _clients[i]->Rename(old_table_name, new_table_name, err);
        if (!ok) {
            LOG(WARNING) << "Rename failed! " << err->GetReason() << " at tera:" << i;
            failed_count++;
        }
    }

    if (failed_count >= _clients.size()) {
        return false;
    } else {
        err->SetFailed(ErrorCode::kOK, "success");
        return true;
    }
}
}

