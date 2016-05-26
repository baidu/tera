// Copyright (c) 2016, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "sdk/tera_replication.h"

#include "common/mutex.h"
#include "types.h"
#include "utils/config_utils.h"

DEFINE_bool(tera_replication_read_try_all, false, "try to read all replicas instread of randomly choose one");
DEFINE_bool(tera_replication_write_need_all_success, false, "return OK only if all replicas write success");
DEFINE_string(tera_replication_conf_paths, "../conf/tera.flag", "paths for flag files. use \';\' to split");

namespace tera {

class RowMutationReplicateImpl : public RowMutationReplicate {
public:
    RowMutationReplicateImpl(const std::vector<RowMutation*>& row_mutations,
                             const std::vector<Table*>& tables)
        : _row_mutations(row_mutations),
          _tables(tables),
          _user_callback(NULL),
          _user_context(NULL),
          _finish_cond(&_mutex),
          _finish_count(0),
          _success_row_mutation(NULL),
          _fail_row_mutation(NULL) {
        CHECK_GT(_row_mutations.size(), 0u);
        for (size_t i = 0; i < _row_mutations.size(); i++) {
            _row_mutations[i]->SetCallBack(RowMutationCallback);
            _row_mutations[i]->SetContext(this);
        }
    }

    virtual ~RowMutationReplicateImpl() {
        for (size_t i = 0; i < _row_mutations.size(); i++) {
            delete _row_mutations[i];
        }
    }

    virtual const std::string& RowKey() {
        return _row_mutations[0]->RowKey();
    }

    virtual void Put(const std::string& value) {
        for (size_t i = 0; i < _row_mutations.size(); i++) {
            _row_mutations[i]->Put(value);
        }
    }

    virtual void Put(const std::string& value, int32_t ttl) {
        for (size_t i = 0; i < _row_mutations.size(); i++) {
            _row_mutations[i]->Put(value, ttl);
        }
    }

    virtual void DeleteRow() {
        for (size_t i = 0; i < _row_mutations.size(); i++) {
            _row_mutations[i]->DeleteRow();
        }
    }

    virtual void SetCallBack(Callback callback) {
        _user_callback = callback;
    }

    virtual Callback GetCallBack() {
        return _user_callback;
    }

    virtual void SetContext(void* context) {
        _user_context = context;
    }

    virtual void* GetContext() {
        return _user_context;
    }

    virtual const ErrorCode& GetError() {
        if (_fail_row_mutation == NULL) {
            CHECK_NOTNULL(_success_row_mutation);
            return _success_row_mutation->GetError();
        }
        if (_success_row_mutation == NULL) {
            CHECK_NOTNULL(_fail_row_mutation);
            return _fail_row_mutation->GetError();
        }
        if (FLAGS_tera_replication_write_need_all_success) {
            return _fail_row_mutation->GetError();
        } else {
            return _success_row_mutation->GetError();
        }
    }

public:
    const std::vector<RowMutation*>& GetRowMutationList() {
        return _row_mutations;
    }

    const std::vector<Table*>& GetTableList() {
        return _tables;
    }

    bool IsAsync() {
        return (_user_callback != NULL);
    }

    void Wait() {
        CHECK(_user_callback == NULL);
        MutexLock l(&_mutex);
        while (_finish_count < _row_mutations.size()) {
            _finish_cond.Wait();
        }
    }

private:
    RowMutationReplicateImpl(const RowMutationReplicateImpl&);
    void operator=(const RowMutationReplicateImpl&);

    static void RowMutationCallback(RowMutation* mutation) {
        RowMutationReplicateImpl* mutation_rep = (RowMutationReplicateImpl*)mutation->GetContext();
        mutation_rep->ProcessCallback(mutation);
    }

    void ProcessCallback(RowMutation* mutation) {
        _mutex.Lock();
        if (mutation->GetError().GetType() == tera::ErrorCode::kOK) {
            if (_success_row_mutation == NULL) {
                _success_row_mutation = mutation;
            }
        } else {
            if (_fail_row_mutation == NULL) {
                _fail_row_mutation = mutation;
            }
        }
        if (++_finish_count == _row_mutations.size()) {
            if (_user_callback != NULL) {
                _mutex.Unlock(); // remember to unlock
                _user_callback(this);
                return; // remember to return
            } else {
                _finish_cond.Signal();
            }
        }
        _mutex.Unlock();
    }

    std::vector<RowMutation*> _row_mutations;
    std::vector<Table*> _tables;
    RowMutationReplicate::Callback _user_callback;
    void* _user_context;

    Mutex _mutex;
    CondVar _finish_cond;
    uint32_t _finish_count;
    RowMutation* _success_row_mutation;
    RowMutation* _fail_row_mutation;
};

class RowReaderReplicateImpl : public RowReaderReplicate {
public:
    RowReaderReplicateImpl(const std::vector<RowReader*>& row_readers,
                           const std::vector<Table*>& tables)
        : _row_readers(row_readers),
          _tables(tables),
          _user_callback(NULL),
          _user_context(NULL),
          _finish_cond(&_mutex),
          _finish_count(0),
          _valid_row_reader(NULL) {
        CHECK_GT(_row_readers.size(), 0u);
        for (size_t i = 0; i < _row_readers.size(); i++) {
            _row_readers[i]->SetCallBack(RowReaderCallback);
            _row_readers[i]->SetContext(this);
        }
    }

    virtual ~RowReaderReplicateImpl() {
        for (size_t i = 0; i < _row_readers.size(); i++) {
            delete _row_readers[i];
        }
    }

    virtual const std::string& RowName() {
        return _row_readers[0]->RowName();
    }

    virtual void SetCallBack(Callback callback) {
        _user_callback = callback;
        for (size_t i = 0; i < _row_readers.size(); i++) {
            _row_readers[i]->SetCallBack(RowReaderCallback);
            _row_readers[i]->SetContext(this);
        }
    }

    virtual void SetContext(void* context) {
        _user_context = context;
    }

    virtual void* GetContext() {
        return _user_context;
    }

    virtual ErrorCode GetError() {
        CHECK_NOTNULL(_valid_row_reader);
        return _valid_row_reader->GetError();
    }

    virtual std::string Value() {
        CHECK_NOTNULL(_valid_row_reader);
        return _valid_row_reader->Value();
    }

public:
    const std::vector<RowReader*>& GetRowReaderList() {
        return _row_readers;
    }

    const std::vector<Table*>& GetTableList() {
        return _tables;
    }

    bool IsAsync() {
        return (_user_callback != NULL);
    }

    void Wait() {
        CHECK(_user_callback == NULL);
        MutexLock l(&_mutex);
        while (_finish_count < _row_readers.size()) {
            _finish_cond.Wait();
        }
    }

private:
    RowReaderReplicateImpl(const RowReaderReplicateImpl&);
    void operator=(const RowReaderReplicateImpl&);

    static void RowReaderCallback(RowReader* reader) {
        RowReaderReplicateImpl* reader_rep = (RowReaderReplicateImpl*)reader->GetContext();
        reader_rep->ProcessCallback(reader);
    }

    void ProcessCallback(RowReader* reader) {
        _mutex.Lock();
        if (_valid_row_reader == NULL && reader->GetError().GetType() == tera::ErrorCode::kOK) {
            _valid_row_reader = reader;
        }
        if (++_finish_count == _row_readers.size()) {
            // if all readers fail, use readers[0]
            if (_valid_row_reader == NULL) {
                _valid_row_reader = _row_readers[0];
            }
            if (_user_callback != NULL) {
                _mutex.Unlock(); // remember to unlock
                _user_callback(this);
                return; // remember to return
            } else {
                _finish_cond.Signal();
            }
        }
        _mutex.Unlock();
    }

    std::vector<RowReader*> _row_readers;
    std::vector<Table*> _tables;
    RowReaderReplicate::Callback _user_callback;
    void* _user_context;

    Mutex _mutex;
    CondVar _finish_cond;
    uint32_t _finish_count;
    RowReader* _valid_row_reader;
};


/// 表接口
class TableReplicateImpl : public TableReplicate {
public:
    TableReplicateImpl(std::vector<Table*> tables) : _tables(tables) {}
    virtual ~TableReplicateImpl() {
        for (size_t i = 0; i < _tables.size(); i++) {
            delete _tables[i];
        }
    }

    virtual RowMutationReplicate* NewRowMutation(const std::string& row_key) {
        std::vector<RowMutation*> row_mutations;
        for (size_t i = 0; i < _tables.size(); i++) {
            row_mutations.push_back(_tables[i]->NewRowMutation(row_key));
        }
        return new RowMutationReplicateImpl(row_mutations, _tables);
    }

    virtual void ApplyMutation(RowMutationReplicate* mutation_rep) {
        RowMutationReplicateImpl* mutation_rep_impl = (RowMutationReplicateImpl*)mutation_rep;
        bool is_async = mutation_rep_impl->IsAsync();
        const std::vector<RowMutation*>& mutation_list = mutation_rep_impl->GetRowMutationList();
        const std::vector<Table*>& table_list = mutation_rep_impl->GetTableList();
        for (size_t i = 0; i < mutation_list.size(); i++) {
            table_list[i]->ApplyMutation(mutation_list[i]);
        }
        if (!is_async) {
            mutation_rep_impl->Wait();
        }
    }

    virtual RowReaderReplicate* NewRowReader(const std::string& row_key) {
        std::vector<RowReader*> row_readers;
        std::vector<Table*> tables;
        if (FLAGS_tera_replication_read_try_all) {
            for (size_t i = 0; i < _tables.size(); i++) {
                row_readers.push_back(_tables[i]->NewRowReader(row_key));
                tables.push_back(_tables[i]);
            }
        } else {
            size_t i = random() % _tables.size();
            row_readers.push_back(_tables[i]->NewRowReader(row_key));
            tables.push_back(_tables[i]);
        }
        return new RowReaderReplicateImpl(row_readers, tables);
    }

    virtual void Get(RowReaderReplicate* reader_rep) {
        RowReaderReplicateImpl* reader_rep_impl = (RowReaderReplicateImpl*)reader_rep;
        bool is_async = reader_rep_impl->IsAsync();
        const std::vector<RowReader*>& reader_list = reader_rep_impl->GetRowReaderList();
        const std::vector<Table*>& table_list = reader_rep_impl->GetTableList();
        for (size_t i = 0; i < reader_list.size(); i++) {
            table_list[i]->Get(reader_list[i]);
        }
        if (!is_async) {
            reader_rep_impl->Wait();
        }
    }

private:
    TableReplicateImpl(const TableReplicateImpl&);
    void operator=(const TableReplicateImpl&);

    std::vector<Table*> _tables;
};

class ClientReplicateImpl : public ClientReplicate {
public:
    /// 打开表格, 失败返回NULL
    virtual TableReplicate* OpenTable(const std::string& table_name, ErrorCode* err) {
        std::vector<Table*> tables;
        for (size_t i = 0; i < _clients.size(); i++) {
            Table* table = _clients[i]->OpenTable(table_name, err);
            if (table == NULL) {
                for (size_t j = 0; j < tables.size(); j++) {
                    delete tables[j];
                }
                return NULL;
            }
            tables.push_back(table);
        }
        return new TableReplicateImpl(tables);
    }

    ClientReplicateImpl(const std::vector<Client*>& clients) : _clients(clients) {}
    virtual ~ClientReplicateImpl() {
        for (size_t i = 0; i < _clients.size(); i++) {
            delete _clients[i];
        }
    }

private:
    ClientReplicateImpl(const ClientReplicateImpl&);
    void operator=(const ClientReplicateImpl&);

    std::vector<Client*> _clients;
};

void ClientReplicate::SetGlogIsInitialized() {
    Client::SetGlogIsInitialized();
}

ClientReplicate* ClientReplicate::NewClient(const std::string& confpath,
                                            const std::string& log_prefix,
                                            ErrorCode* err) {
    utils::LoadFlagFile(confpath);
    std::string conf_paths = FLAGS_tera_replication_conf_paths;
    std::vector<std::string> confs;
    size_t token_pos = 0;
    while (token_pos < conf_paths.size()) {
        size_t delim_pos = conf_paths.find(';', token_pos);
        std::string token(conf_paths, token_pos, delim_pos - token_pos);
        if (!token.empty()) {
            confs.push_back(token);
        }
        if (delim_pos == std::string::npos) {
            break;
        }
        token_pos = delim_pos + 1;
    }

    std::vector<Client*> clients;
    for (size_t i = 0; i < confs.size(); i++) {
        Client* client = Client::NewClient(confs[i], log_prefix, err);
        if (client == NULL) {
            for (size_t j = 0; j < clients.size(); j++) {
                delete clients[j];
            }
            return NULL;
        }
        clients.push_back(client);
    }
    return new ClientReplicateImpl(clients);
}

ClientReplicate* ClientReplicate::NewClient(const std::string& confpath, ErrorCode* err) {
    return NewClient(confpath, "tera", err);
}

ClientReplicate* ClientReplicate::NewClient() {
    return NewClient("", NULL);
}

} // namespace tera

