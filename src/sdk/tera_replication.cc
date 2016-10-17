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
        : row_mutations_(row_mutations),
          tables_(tables),
          user_callback_(NULL),
          user_context_(NULL),
          finish_cond_(&mutex_),
          finish_count_(0),
          success_row_mutation_(NULL),
          fail_row_mutation_(NULL) {
        CHECK_GT(row_mutations_.size(), 0u);
        for (size_t i = 0; i < row_mutations_.size(); i++) {
            row_mutations_[i]->SetCallBack(RowMutationCallback);
            row_mutations_[i]->SetContext(this);
        }
    }

    virtual ~RowMutationReplicateImpl() {
        for (size_t i = 0; i < row_mutations_.size(); i++) {
            delete row_mutations_[i];
        }
    }

    virtual const std::string& RowKey() {
        return row_mutations_[0]->RowKey();
    }

    virtual void Put(const std::string& value) {
        for (size_t i = 0; i < row_mutations_.size(); i++) {
            row_mutations_[i]->Put(value);
        }
    }

    virtual void Put(const std::string& value, int32_t ttl) {
        for (size_t i = 0; i < row_mutations_.size(); i++) {
            row_mutations_[i]->Put(value, ttl);
        }
    }

    virtual void DeleteRow() {
        for (size_t i = 0; i < row_mutations_.size(); i++) {
            row_mutations_[i]->DeleteRow();
        }
    }

    virtual void SetCallBack(Callback callback) {
        user_callback_ = callback;
    }

    virtual Callback GetCallBack() {
        return user_callback_;
    }

    virtual void SetContext(void* context) {
        user_context_ = context;
    }

    virtual void* GetContext() {
        return user_context_;
    }

    virtual const ErrorCode& GetError() {
        if (fail_row_mutation_ == NULL) {
            CHECK_NOTNULL(success_row_mutation_);
            return success_row_mutation_->GetError();
        }
        if (success_row_mutation_ == NULL) {
            CHECK_NOTNULL(fail_row_mutation_);
            return fail_row_mutation_->GetError();
        }
        if (FLAGS_tera_replication_write_need_all_success) {
            return fail_row_mutation_->GetError();
        } else {
            return success_row_mutation_->GetError();
        }
    }

public:
    const std::vector<RowMutation*>& GetRowMutationList() {
        return row_mutations_;
    }

    const std::vector<Table*>& GetTableList() {
        return tables_;
    }

    bool IsAsync() {
        return (user_callback_ != NULL);
    }

    void Wait() {
        CHECK(user_callback_ == NULL);
        MutexLock l(&mutex_);
        while (finish_count_ < row_mutations_.size()) {
            finish_cond_.Wait();
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
        mutex_.Lock();
        if (mutation->GetError().GetType() == tera::ErrorCode::kOK) {
            if (success_row_mutation_ == NULL) {
                success_row_mutation_ = mutation;
            }
        } else {
            if (fail_row_mutation_ == NULL) {
                fail_row_mutation_ = mutation;
            }
        }
        if (++finish_count_ == row_mutations_.size()) {
            if (user_callback_ != NULL) {
                mutex_.Unlock(); // remember to unlock
                user_callback_(this);
                return; // remember to return
            } else {
                finish_cond_.Signal();
            }
        }
        mutex_.Unlock();
    }

    std::vector<RowMutation*> row_mutations_;
    std::vector<Table*> tables_;
    RowMutationReplicate::Callback user_callback_;
    void* user_context_;

    Mutex mutex_;
    CondVar finish_cond_;
    uint32_t finish_count_;
    RowMutation* success_row_mutation_;
    RowMutation* fail_row_mutation_;
};

class RowReaderReplicateImpl : public RowReaderReplicate {
public:
    RowReaderReplicateImpl(const std::vector<RowReader*>& row_readers,
                           const std::vector<Table*>& tables)
        : row_readers_(row_readers),
          tables_(tables),
          user_callback_(NULL),
          user_context_(NULL),
          finish_cond_(&mutex_),
          finish_count_(0),
          valid_row_reader_(NULL) {
        CHECK_GT(row_readers_.size(), 0u);
        for (size_t i = 0; i < row_readers_.size(); i++) {
            row_readers_[i]->SetCallBack(RowReaderCallback);
            row_readers_[i]->SetContext(this);
        }
    }

    virtual ~RowReaderReplicateImpl() {
        for (size_t i = 0; i < row_readers_.size(); i++) {
            delete row_readers_[i];
        }
    }

    virtual const std::string& RowName() {
        return row_readers_[0]->RowName();
    }

    virtual void SetCallBack(Callback callback) {
        user_callback_ = callback;
    }

    virtual void SetContext(void* context) {
        user_context_ = context;
    }

    virtual void* GetContext() {
        return user_context_;
    }

    virtual ErrorCode GetError() {
        CHECK_NOTNULL(valid_row_reader_);
        return valid_row_reader_->GetError();
    }

    virtual std::string Value() {
        CHECK_NOTNULL(valid_row_reader_);
        return valid_row_reader_->Value();
    }

public:
    const std::vector<RowReader*>& GetRowReaderList() {
        return row_readers_;
    }

    const std::vector<Table*>& GetTableList() {
        return tables_;
    }

    bool IsAsync() {
        return (user_callback_ != NULL);
    }

    void Wait() {
        CHECK(user_callback_ == NULL);
        MutexLock l(&mutex_);
        while (finish_count_ < row_readers_.size()) {
            finish_cond_.Wait();
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
        mutex_.Lock();
        if (valid_row_reader_ == NULL && reader->GetError().GetType() == tera::ErrorCode::kOK) {
            valid_row_reader_ = reader;
        }
        if (++finish_count_ == row_readers_.size()) {
            // if all readers fail, use readers[0]
            if (valid_row_reader_ == NULL) {
                valid_row_reader_ = row_readers_[0];
            }
            if (user_callback_ != NULL) {
                mutex_.Unlock(); // remember to unlock
                user_callback_(this);
                return; // remember to return
            } else {
                finish_cond_.Signal();
            }
        }
        mutex_.Unlock();
    }

    std::vector<RowReader*> row_readers_;
    std::vector<Table*> tables_;
    RowReaderReplicate::Callback user_callback_;
    void* user_context_;

    Mutex mutex_;
    CondVar finish_cond_;
    uint32_t finish_count_;
    RowReader* valid_row_reader_;
};


/// 表接口
class TableReplicateImpl : public TableReplicate {
public:
    TableReplicateImpl(std::vector<Table*> tables) : tables_(tables) {}
    virtual ~TableReplicateImpl() {
        for (size_t i = 0; i < tables_.size(); i++) {
            delete tables_[i];
        }
    }

    virtual RowMutationReplicate* NewRowMutation(const std::string& row_key) {
        std::vector<RowMutation*> row_mutations;
        for (size_t i = 0; i < tables_.size(); i++) {
            row_mutations.push_back(tables_[i]->NewRowMutation(row_key));
        }
        return new RowMutationReplicateImpl(row_mutations, tables_);
    }

    virtual void ApplyMutation(RowMutationReplicate* mutation_rep) {
        RowMutationReplicateImpl* mutation_rep_impl = (RowMutationReplicateImpl*)mutation_rep;
        bool is_async = mutation_rep_impl->IsAsync();
        const std::vector<RowMutation*>& mutation_list = mutation_rep_impl->GetRowMutationList();
        const std::vector<Table*>& table_list = mutation_rep_impl->GetTableList();
        // in async mode, after the last call of ApplyMutation, we should not access
        // 'mutation_rep_impl' anymore, that's why we assign the value of 'mutation_list.size()'
        // to a local variable 'mutation_num'
        size_t mutation_num = mutation_list.size();
        for (size_t i = 0; i < mutation_num; i++) {
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
            for (size_t i = 0; i < tables_.size(); i++) {
                row_readers.push_back(tables_[i]->NewRowReader(row_key));
                tables.push_back(tables_[i]);
            }
        } else {
            size_t i = random() % tables_.size();
            row_readers.push_back(tables_[i]->NewRowReader(row_key));
            tables.push_back(tables_[i]);
        }
        return new RowReaderReplicateImpl(row_readers, tables);
    }

    virtual void Get(RowReaderReplicate* reader_rep) {
        RowReaderReplicateImpl* reader_rep_impl = (RowReaderReplicateImpl*)reader_rep;
        bool is_async = reader_rep_impl->IsAsync();
        const std::vector<RowReader*>& reader_list = reader_rep_impl->GetRowReaderList();
        const std::vector<Table*>& table_list = reader_rep_impl->GetTableList();
        size_t reader_num = reader_list.size();
        for (size_t i = 0; i < reader_num; i++) {
            table_list[i]->Get(reader_list[i]);
        }
        if (!is_async) {
            reader_rep_impl->Wait();
        }
    }

private:
    TableReplicateImpl(const TableReplicateImpl&);
    void operator=(const TableReplicateImpl&);

    std::vector<Table*> tables_;
};

class ClientReplicateImpl : public ClientReplicate {
public:
    /// 打开表格, 失败返回NULL
    virtual TableReplicate* OpenTable(const std::string& table_name, ErrorCode* err) {
        std::vector<Table*> tables;
        for (size_t i = 0; i < clients_.size(); i++) {
            Table* table = clients_[i]->OpenTable(table_name, err);
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

    ClientReplicateImpl(const std::vector<Client*>& clients) : clients_(clients) {}
    virtual ~ClientReplicateImpl() {
        for (size_t i = 0; i < clients_.size(); i++) {
            delete clients_[i];
        }
    }

private:
    ClientReplicateImpl(const ClientReplicateImpl&);
    void operator=(const ClientReplicateImpl&);

    std::vector<Client*> clients_;
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

