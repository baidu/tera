#include "mutate_impl.h"
#include "read_impl.h"
#include "ha_tera.h"
#include "utils/timer.h"

DECLARE_bool(tera_sdk_ha_ddl_enable);
DECLARE_int32(tera_sdk_ha_timestamp_diff);
DECLARE_bool(tera_sdk_ha_get_random_mode);

namespace tera {

class PutCallbackChecker : public CallChecker {
public:
    explicit PutCallbackChecker(int size)
        : _has_call(false),
          _total_clusters(size),
          _check_cout(1) {}

    virtual ~PutCallbackChecker() {}

    // 对于异步写，只要有一个调用callback就行了
    bool NeedCall(ErrorCode::ErrorCodeType code) {
        if (_has_call) {
            return false;
        } else {
            if (_check_cout < _total_clusters) {
                if (code == ErrorCode::kOK) {
                    MutexLock lock(&_mutex);
                    if (_has_call) {
                        return false;
                    } else {
                        _has_call = true;
                        return true;
                    }
                } else {
                    MutexLock lock(&_mutex);
                    _check_cout++;
                    return false;
                }
            } else {
                MutexLock lock(&_mutex);
                if (_has_call) {
                    return false;
                } else {
                    _has_call = true;
                    return true;
                }
            }
        }
    }

private:
    mutable Mutex _mutex;
    bool _has_call;
    int _total_clusters;
    int _check_cout;
};

class GetCallbackChecker : public CallChecker {
public:
    GetCallbackChecker(const std::vector<Table*> &clusters, RowReader* row_reader)
        : _has_call(false),
          _cluster_index(0),
          _clusters(clusters),
          _row_reader(row_reader) {}

    virtual ~GetCallbackChecker() {}

    // 对于异步读，如果读失败，则尝试从下一个集群读；
    // 否则，返回true
    bool NeedCall(ErrorCode::ErrorCodeType code) {
        if (_has_call) {
            return false;
        } else if (code == ErrorCode::kOK) {
            _has_call = true;
            return true;
        } else {
            if (++_cluster_index >= _clusters.size()) {
                _has_call = true;
                return true;
            } else {
                _row_reader->Reset();
                _clusters[_cluster_index]->Get(_row_reader);
                return false;
            }
        }
    }

private:
    bool _has_call;
    size_t _cluster_index;
    std::vector<Table*> _clusters;
    RowReader* _row_reader;
};

class LGetCallbackChecker : public CallChecker {
public:
    LGetCallbackChecker(const std::vector<Table*> &clusters, RowReaderImpl* row_reader)
        : _has_call(false),
          _cluster_index(0),
          _clusters(clusters),
          _row_reader(row_reader) {}

    virtual ~LGetCallbackChecker() {}

    /// 比较两个集群，选择时间戳比较大的结果
    bool NeedCall(ErrorCode::ErrorCodeType code) {
        if (_has_call) {
            return false;
        } else if (code == ErrorCode::kOK) {
            _results.push_back(_row_reader->GetResult());
        }
        if (++_cluster_index >= _clusters.size()) {
            // 合并结果
            if (_results.size() > 0) {
                RowResult final_result;
                HATable::MergeResult(_results, final_result, _row_reader->GetMaxVersions());
                _row_reader->SetResult(final_result);
            }
            _has_call = true;
            return true;
        } else {
            _row_reader->Reset();
            _clusters[_cluster_index]->Get(_row_reader);
            return false;
        }
    }

private:
    bool _has_call;
    size_t _cluster_index;
    std::vector<Table*> _clusters;
    RowReaderImpl* _row_reader;
    std::vector<RowResult> _results;
};

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

    // 如果是异步操作，则设置回调的检查器
    if (row_mu->IsAsync()) {
        row_mu->SetCallChecker(new PutCallbackChecker(_tables.size()));
    }

    for (size_t i = 0; i < _tables.size(); i++) {
        _tables[i]->ApplyMutation(row_mu);
        if (row_mu->GetError().GetType() != ErrorCode::kOK) {
            failed_count++;
            LOG(WARNING) << "ApplyMutation failed! "
                         << row_mu->GetError().GetType()
                         << " at tera:" << i;
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

    for (size_t i = 0; i < row_mu_list.size(); i++) {
        RowMutation* row_mu = row_mu_list[i];
        if (row_mu->IsAsync()) {
            row_mu->SetCallChecker(new PutCallbackChecker(_tables.size()));
        }
    }

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

// 从两个集群里获取时间戳比较新的数据, latest-get
void HATable::LGet(RowReader* row_reader) {
    size_t failed_count = 0;

    RowReaderImpl* row_reader_impl = dynamic_cast<RowReaderImpl*>(row_reader);
    // 如果是异步操作，则设置回调的检查器
    if (row_reader_impl->IsAsync()) {
        row_reader_impl->SetCallChecker(new LGetCallbackChecker(_tables, row_reader_impl));
        if (_tables.size() > 0) {
            _tables[0]->Get(row_reader);
        }
    } else {
        // 同步Get
        std::vector<RowResult> results;
        for (size_t i = 0; i < _tables.size(); i++) {
            _tables[i]->Get(row_reader_impl);
            if (row_reader_impl->GetError().GetType() != ErrorCode::kOK) {
                LOG(WARNING) << "Get failed! " << row_reader_impl->GetError().GetReason()
                             << " at tera:" << i;
                failed_count++;
                if (failed_count < _tables.size()) {
                    row_reader_impl->Reset();
                }
            } else {
                results.push_back(row_reader_impl->GetResult());
                row_reader_impl->Reset();
            }
        }
        if (results.size() > 0) {
            RowResult final_result;
            HATable::MergeResult(results, final_result, row_reader_impl->GetMaxVersions());
            row_reader_impl->SetResult(final_result);
        }
    }
}

void HATable::LGet(const std::vector<RowReader*>& row_readers) {
    for (size_t i = 0; i < row_readers.size(); i++) {
        LGet(row_readers[i]);
    }
}

void HATable::Get(RowReader* row_reader) {
    size_t failed_count = 0;

    std::vector<Table*> table_set = _tables;
    // 如果是随机Get，则每次对tables进行排序
    if (FLAGS_tera_sdk_ha_get_random_mode) {
        HATable::ShuffleArray(table_set);
    }

    // 如果是异步操作，则设置回调的检查器
    if (row_reader->IsAsync()) {
        row_reader->SetCallChecker(new GetCallbackChecker(table_set, row_reader));
        if (table_set.size() > 0) {
            table_set[0]->Get(row_reader);
        }
    } else {
        // 同步Get
        for (size_t i = 0; i < table_set.size(); i++) {
            table_set[i]->Get(row_reader);
            if (row_reader->GetError().GetType() != ErrorCode::kOK) {
                LOG(WARNING) << "Get failed! " << row_reader->GetError().GetReason()
                             << " at tera:" << i;
                failed_count++;
            } else {
                break;
            }
            if (failed_count < table_set.size()) {
                row_reader->Reset();
            }
        }
    }
}

// 可能有一批数据来自集群1，另一批数据来自集群2
void HATable::Get(const std::vector<RowReader*>& row_readers) {

    std::vector<Table*> table_set = _tables;
    // 如果是随机Get，则每次对tables进行排序
    if (FLAGS_tera_sdk_ha_get_random_mode) {
        HATable::ShuffleArray(table_set);
    }

    std::vector<RowReader*> async_readers;
    std::vector<RowReader*> sync_readers;
    for (size_t i = 0; i < row_readers.size(); i++) {
        if (row_readers[i]->IsAsync()) {
            async_readers.push_back(row_readers[i]);
        } else {
            sync_readers.push_back(row_readers[i]);
        }
    }

    // 处理异步读
    for (size_t i = 0; i < async_readers.size(); i++) {
        Get(async_readers[i]);
    }

    // 处理同步读
    std::vector<size_t> failed_count_list;
    failed_count_list.resize(row_readers.size());

    for (size_t i = 0; i < table_set.size(); i++) {
        if (sync_readers.size() <= 0) {
            continue;
        }
        std::vector<RowReader*> need_read = sync_readers;
        table_set[i]->Get(need_read);
        sync_readers.clear();
        for (size_t j = 0; j < need_read.size(); j++) {
            RowReader* row_reader = need_read[j];
            if (row_reader->GetError().GetType() != ErrorCode::kOK) {
                LOG(WARNING) << j << " Get failed! error: "
                             << row_reader->GetError().GetType()
                             << ", " << row_reader->GetError().GetReason()
                             << " at tera:" << i;
                failed_count_list[j] += 1;

                // 如果所有集群都失败了，则认为失败
                if (failed_count_list[j] < table_set.size()) {
                    // 重置除用户数据外的数据，以用于后面的写
                    row_reader->Reset();
                    sync_readers.push_back(row_reader);
                }
            } //fail
        }
    }
}

bool HATable::Get(const std::string& row_key, const std::string& family,
                  const std::string& qualifier, std::string* value,
                  ErrorCode* err, uint64_t snapshot_id) {

    std::vector<Table*> table_set = _tables;
    // 如果是随机Get，则每次对tables进行排序
    if (FLAGS_tera_sdk_ha_get_random_mode) {
        HATable::ShuffleArray(table_set);
    }

    size_t failed_count = 0;
    for (size_t i = 0; i < table_set.size(); i++) {
        bool ok = table_set[i]->Get(row_key, family, qualifier, value, err, snapshot_id);
        if (!ok) {
            LOG(WARNING) << "Get failed! " << err->GetReason() << " at tera:" << i;
            failed_count++;
        } else {
            break;
        }
    }
    return (failed_count>=table_set.size()) ? false : true;
}

bool HATable::Get(const std::string& row_key, const std::string& family,
                  const std::string& qualifier, int64_t* value,
                  ErrorCode* err, uint64_t snapshot_id) {

    std::vector<Table*> table_set = _tables;
    // 如果是随机Get，则每次对tables进行排序
    if (FLAGS_tera_sdk_ha_get_random_mode) {
        HATable::ShuffleArray(table_set);
    }

    size_t failed_count = 0;
    for (size_t i = 0; i < table_set.size(); i++) {
        bool ok = table_set[i]->Get(row_key, family, qualifier, value, err, snapshot_id);
        if (!ok) {
            LOG(WARNING) << "Get failed! " << err->GetReason() << " at tera:" << i;
            failed_count++;
        } else {
            break;
        }
    }
    return (failed_count>=table_set.size()) ? false : true;
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

void HATable::MergeResult(const std::vector<RowResult>& results, RowResult& res, uint32_t max_size) {
    std::vector<int> results_pos;
    results_pos.resize(results.size());
    for (uint32_t i = 0; i < results.size(); i++) {
        results_pos[i] = 0;
    }
    for (uint32_t i = 0; i < max_size; i++) {
        // 获取时间戳最大的结果
        bool found = false;
        uint32_t candidate_index;
        int64_t timestamp;
        for (uint32_t j = 0; j < results.size(); j++) {
            if (results_pos[j] < results[j].key_values_size()) {
                int64_t tmp_ts = results[j].key_values(results_pos[j]).timestamp();
                if (!found || tmp_ts > timestamp) {
                    timestamp = tmp_ts;
                    candidate_index = j;
                    found = true;
                }
            }
        }
        if (!found) {
            break;
        }
        // 移动数组下标
        for (uint32_t j = 0; j < results.size() && j != candidate_index; j++) {
            if (results_pos[j] < results[j].key_values_size()) {
                int64_t tmp_ts = results[j].key_values(results_pos[j]).timestamp();
                // 时间戳相近，说明是同一批次
                if (abs(timestamp-tmp_ts) < FLAGS_tera_sdk_ha_timestamp_diff) {
                    results_pos[j] += 1;
                }
            }
        }
        // 保存结果
        KeyValuePair* kv = res.add_key_values();
        *kv = results[candidate_index].key_values(results_pos[candidate_index]);
        results_pos[candidate_index] += 1;
    }
}
void HATable::ShuffleArray(std::vector<Table*>& table_set) {
    int64_t seed = get_micros();
    srandom(seed);
    for (size_t i = table_set.size()-1; i > 0; i--) {
        int rnd = random()%(i+1);

        // swap
        Table* t = table_set[rnd];
        table_set[rnd] = table_set[i];
        table_set[i] = t;
    }
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
    bool fail_fast = false;
    for (size_t i = 0; i < _clients.size(); i++) {
        ok = _clients[i]->CreateTable(desc, err);
        if (!ok) {
            if (FLAGS_tera_sdk_ha_ddl_enable) {
                LOG(ERROR) << "CreateTable failed! " << err->GetReason()
                           << " at tera:" << i << ", STOP try other cluster!";
                fail_fast = true;
                break;
            } else {
                LOG(WARNING) << "CreateTable failed! " << err->GetReason() << " at tera:" << i;
                failed_count++;
            }
        }
    }

    if (fail_fast) {
        return false;
    } else if (failed_count >= _clients.size()) {
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
    bool fail_fast = false;
    for (size_t i = 0; i < _clients.size(); i++) {
        ok = _clients[i]->CreateTable(desc, tablet_delim, err);
        if (!ok) {
            if (FLAGS_tera_sdk_ha_ddl_enable) {
                LOG(ERROR) << "CreateTable failed! " << err->GetReason()
                           << " at tera:" << i << ", STOP try other cluster!";
                fail_fast = true;
                break;
            } else {
                LOG(WARNING) << "CreateTable failed! " << err->GetReason() << " at tera:" << i;
                failed_count++;
            }
        }
    }

    if (fail_fast) {
        return false;
    } else if (failed_count >= _clients.size()) {
        return false;
    } else {
        err->SetFailed(ErrorCode::kOK, "success");
        return true;
    }
}

bool HAClient::UpdateTable(const TableDescriptor& desc, ErrorCode* err) {
    bool ok = false;
    size_t failed_count = 0;
    bool fail_fast = false;
    for (size_t i = 0; i < _clients.size(); i++) {
        ok = _clients[i]->UpdateTable(desc, err);
        if (!ok) {
            if (FLAGS_tera_sdk_ha_ddl_enable) {
                LOG(ERROR) << "UpdateTable failed! " << err->GetReason()
                           << " at tera:" << i << ", STOP try other cluster!";
                fail_fast = true;
                break;
            } else {
                LOG(WARNING) << "UpdateTable failed! " << err->GetReason() << " at tera:" << i;
                failed_count++;
            }
        }
    }

    if (fail_fast) {
        return false;
    } else if (failed_count >= _clients.size()) {
        return false;
    } else {
        err->SetFailed(ErrorCode::kOK, "success");
        return true;
    }
}

bool HAClient::DeleteTable(std::string name, ErrorCode* err) {
    bool ok = false;
    size_t failed_count = 0;
    bool fail_fast = false;
    for (size_t i = 0; i < _clients.size(); i++) {
        ok = _clients[i]->DeleteTable(name, err);
        if (!ok) {
            if (FLAGS_tera_sdk_ha_ddl_enable) {
                LOG(ERROR) << "DeleteTable failed! " << err->GetReason()
                           << " at tera:" << i << ", STOP try other cluster!";
                fail_fast = true;
                break;
            } else {
                LOG(WARNING) << "DeleteTable failed! " << err->GetReason() << " at tera:" << i;
                failed_count++;
            }
        }
    }

    if (fail_fast) {
        return false;
    } else if (failed_count >= _clients.size()) {
        return false;
    } else {
        err->SetFailed(ErrorCode::kOK, "success");
        return true;
    }
}

bool HAClient::DisableTable(std::string name, ErrorCode* err) {
    bool ok = false;
    size_t failed_count = 0;
    bool fail_fast = false;
    for (size_t i = 0; i < _clients.size(); i++) {
        ok = _clients[i]->DisableTable(name, err);
        if (!ok) {
            if (FLAGS_tera_sdk_ha_ddl_enable) {
                LOG(ERROR) << "DisableTable failed! " << err->GetReason()
                           << " at tera:" << i << ", STOP try other cluster!";
                fail_fast = true;
                break;
            } else {
                LOG(WARNING) << "DisableTable failed! " << err->GetReason() << " at tera:" << i;
                failed_count++;
            }
        }
    }

    if (fail_fast) {
        return false;
    } else if (failed_count >= _clients.size()) {
        return false;
    } else {
        err->SetFailed(ErrorCode::kOK, "success");
        return true;
    }
}

bool HAClient::EnableTable(std::string name, ErrorCode* err) {
    bool ok = false;
    size_t failed_count = 0;
    bool fail_fast = false;
    for (size_t i = 0; i < _clients.size(); i++) {
        ok = _clients[i]->EnableTable(name, err);
        if (!ok) {
            if (FLAGS_tera_sdk_ha_ddl_enable) {
                LOG(ERROR) << "EnableTable failed! " << err->GetReason()
                           << " at tera:" << i << ", STOP try other cluster!";
                fail_fast = true;
                break;
            } else {
                LOG(WARNING) << "EnableTable failed! " << err->GetReason() << " at tera:" << i;
                failed_count++;
            }
        }
    }

    if (fail_fast) {
        return false;
    } else if (failed_count >= _clients.size()) {
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
    bool fail_fast = false;
    for (size_t i = 0; i < _clients.size(); i++) {
        ok = _clients[i]->CreateUser(user, password, err);
        if (!ok) {
            if (FLAGS_tera_sdk_ha_ddl_enable) {
                LOG(ERROR) << "CreateUser failed! " << err->GetReason()
                           << " at tera:" << i << ", STOP try other cluster!";
                fail_fast = true;
                break;
            } else {
                LOG(WARNING) << "CreateUser failed! " << err->GetReason() << " at tera:" << i;
                failed_count++;
            }
        }
    }

    if (fail_fast) {
        return false;
    } else if (failed_count >= _clients.size()) {
        return false;
    } else {
        err->SetFailed(ErrorCode::kOK, "success");
        return true;
    }
}


bool HAClient::DeleteUser(const std::string& user, ErrorCode* err) {
    bool ok = false;
    size_t failed_count = 0;
    bool fail_fast = false;
    for (size_t i = 0; i < _clients.size(); i++) {
        ok = _clients[i]->DeleteUser(user, err);
        if (!ok) {
            if (FLAGS_tera_sdk_ha_ddl_enable) {
                LOG(ERROR) << "DeleteUser failed! " << err->GetReason()
                           << " at tera:" << i << ", STOP try other cluster!";
                fail_fast = true;
                break;
            } else {
                LOG(WARNING) << "DeleteUser failed! " << err->GetReason() << " at tera:" << i;
                failed_count++;
            }
        }
    }

    if (fail_fast) {
        return false;
    } else if (failed_count >= _clients.size()) {
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
    bool fail_fast = false;
    for (size_t i = 0; i < _clients.size(); i++) {
        ok = _clients[i]->ChangePwd(user, password, err);
        if (!ok) {
            if (FLAGS_tera_sdk_ha_ddl_enable) {
                LOG(ERROR) << "ChangePwd failed! " << err->GetReason()
                           << " at tera:" << i << ", STOP try other cluster!";
                fail_fast = true;
                break;
            } else {
                LOG(WARNING) << "ChangePwd failed! " << err->GetReason() << " at tera:" << i;
                failed_count++;
            }
        }
    }

    if (fail_fast) {
        return false;
    } else if (failed_count >= _clients.size()) {
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
            LOG(WARNING) << "ShowUser failed! " << err->GetReason() << " at tera:" << i;
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
    bool fail_fast = false;
    for (size_t i = 0; i < _clients.size(); i++) {
        ok = _clients[i]->AddUserToGroup(user, group, err);
        if (!ok) {
            if (FLAGS_tera_sdk_ha_ddl_enable) {
                LOG(ERROR) << "AddUserToGroup failed! " << err->GetReason()
                           << " at tera:" << i << ", STOP try other cluster!";
                fail_fast = true;
                break;
            } else {
                LOG(WARNING) << "AddUserToGroup failed! " << err->GetReason() << " at tera:" << i;
                failed_count++;
            }
        }
    }

    if (fail_fast) {
        return false;
    } else if (failed_count >= _clients.size()) {
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
    bool fail_fast = false;
    for (size_t i = 0; i < _clients.size(); i++) {
        ok = _clients[i]->DeleteUserFromGroup(user, group, err);
        if (!ok) {
            if (FLAGS_tera_sdk_ha_ddl_enable) {
                LOG(ERROR) << "DeleteUserFromGroup failed! " << err->GetReason()
                           << " at tera:" << i << ", STOP try other cluster!";
                fail_fast = true;
                break;
            } else {
                LOG(WARNING) << "DeleteUserFromGroup failed! " << err->GetReason() << " at tera:" << i;
                failed_count++;
            }
        }
    }

    if (fail_fast) {
        return false;
    } else if (failed_count >= _clients.size()) {
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
            LOG(WARNING) << "GetTabletLocation failed! " << err->GetReason() << " at tera:" << i;
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
            LOG(WARNING) << "GetTableDescriptor failed! " << err->GetReason() << " at tera:" << i;
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
    bool fail_fast = false;
    for (size_t i = 0; i < _clients.size(); i++) {
        ok = _clients[i]->DelSnapshot(name, snapshot, err);
        if (!ok) {
            if (FLAGS_tera_sdk_ha_ddl_enable) {
                LOG(ERROR) << "DelSnapshot failed! " << err->GetReason()
                           << " at tera:" << i << ", STOP try other cluster!";
                fail_fast = true;
                break;
            } else {
                LOG(WARNING) << "DelSnapshot failed! " << err->GetReason() << " at tera:" << i;
                failed_count++;
            }
        }
    }

    if (fail_fast) {
        return false;
    } else if (failed_count >= _clients.size()) {
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
    bool  fail_fast = false;
    for (size_t i = 0; i < _clients.size(); i++) {
        ok = _clients[i]->Rollback(name, snapshot, rollback_name, err);
        if (!ok) {
            if (FLAGS_tera_sdk_ha_ddl_enable) {
                LOG(ERROR) << "AddUserToGroup failed! " << err->GetReason()
                           << " at tera:" << i << ", STOP try other cluster!";
                fail_fast = true;
                break;
            } else {
                LOG(WARNING) << "Rollback failed! " << err->GetReason() << " at tera:" << i;
                failed_count++;
            }
        }
    }

    if (fail_fast) {
        return false;
    } else if (failed_count >= _clients.size()) {
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
    bool fail_fast = false;
    for (size_t i = 0; i < _clients.size(); i++) {
        ok = _clients[i]->Rename(old_table_name, new_table_name, err);
        if (!ok) {
            if (FLAGS_tera_sdk_ha_ddl_enable) {
                LOG(ERROR) << "AddUserToGroup failed! " << err->GetReason()
                           << " at tera:" << i << ", STOP try other cluster!";
                fail_fast = true;
                break;
            } else {
                LOG(WARNING) << "Rename failed! " << err->GetReason() << " at tera:" << i;
                failed_count++;
            }
        }
    }

    if (fail_fast) {
        return false;
    } else if (failed_count >= _clients.size()) {
        return false;
    } else {
        err->SetFailed(ErrorCode::kOK, "success");
        return true;
    }
}
}

