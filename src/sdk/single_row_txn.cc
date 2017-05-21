// Copyright (c) 2016, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include <functional>

#include "common/thread_pool.h"
#include "common/base/string_format.h"

#include "io/coding.h"
#include "sdk/read_impl.h"
#include "sdk/single_row_txn.h"
#include "sdk/table_impl.h"
#include "types.h"
#include "utils/timer.h"

namespace tera {

SingleRowTxn::SingleRowTxn(Table* table, const std::string& row_key,
                           common::ThreadPool* thread_pool)
    : table_(table),
      row_key_(row_key),
      thread_pool_(thread_pool),
      has_read_(false),
      user_reader_callback_(NULL),
      user_reader_context_(NULL),
      reader_max_versions_(1),
      reader_start_timestamp_(kOldestTs),
      reader_end_timestamp_(kLatestTs),
      mutation_buffer_(table, row_key),
      user_commit_callback_(NULL),
      user_commit_context_(NULL) {
}

SingleRowTxn::~SingleRowTxn() {
}

bool SingleRowTxn::MarkHasRead() {
    MutexLock l(&mu_);
    if (has_read_) {
        return false;
    } else {
        has_read_ = true;
        return true;
    }
}

void SingleRowTxn::MarkNoRead() {
    MutexLock l(&mu_);
    assert(has_read_ == true);
    has_read_ = false;
}

/// 提交一个修改操作
void SingleRowTxn::ApplyMutation(RowMutation* row_mu) {
    RowMutationImpl* row_mu_impl = static_cast<RowMutationImpl*>(row_mu);
    row_mu_impl->SetTransaction(this);

    if (row_mu->RowKey() == row_key_) {
        mutation_buffer_.Concatenate(*row_mu_impl);
        row_mu_impl->SetError(ErrorCode::kOK);
    } else {
        row_mu_impl->SetError(ErrorCode::kBadParam, "not same row");
    }

    if (row_mu->IsAsync()) {
        ThreadPool::Task task = std::bind(&RowMutationImpl::RunCallback, row_mu_impl);
        thread_pool_->AddTask(task);
    }
}

void ReadCallbackWrapper(RowReader* row_reader) {
    RowReaderImpl* reader_impl = static_cast<RowReaderImpl*>(row_reader);
    SingleRowTxn* txn_impl = static_cast<SingleRowTxn*>(reader_impl->GetContext());
    txn_impl->ReadCallback(reader_impl);
}

/// 读取操作
void SingleRowTxn::Get(RowReader* row_reader) {
    RowReaderImpl* reader_impl = static_cast<RowReaderImpl*>(row_reader);
    reader_impl->SetTransaction(this);
    bool is_async = reader_impl->IsAsync();


    // safe check
    if (reader_impl->RowName() != row_key_) {
        reader_impl->SetError(ErrorCode::kBadParam, "not same row");
    } else if (!MarkHasRead()) {
        reader_impl->SetError(ErrorCode::kBadParam, "not support read more than once in txn");
    } else if (reader_impl->GetSnapshot() != 0) {
        reader_impl->SetError(ErrorCode::kBadParam, "not support read a snapshot in txn");
    }
    if (reader_impl->GetError().GetType() != ErrorCode::kOK) {
        if (is_async) {
            ThreadPool::Task task = std::bind(&RowReaderImpl::RunCallback, reader_impl);
            thread_pool_->AddTask(task);
        }
        return;
    }

    int64_t ts_start = 0, ts_end = 0;
    reader_impl->GetTimeRange(&ts_start, &ts_end);
    reader_start_timestamp_ = ts_start;
    reader_end_timestamp_ = ts_end;
    reader_max_versions_ = reader_impl->GetMaxVersions();

    // save user's callback & context
    user_reader_callback_ = reader_impl->GetCallBack();
    user_reader_context_ = reader_impl->GetContext();

    // use our callback wrapper
    reader_impl->SetCallBack(ReadCallbackWrapper);
    reader_impl->SetContext(this);

    table_->Get(reader_impl);
    if (!is_async) {
        reader_impl->Wait();
    }
}

/// 设置提交回调, 提交操作会异步返回
void SingleRowTxn::SetCommitCallback(Callback callback) {
    user_commit_callback_ = callback;
}

/// 获取提交回调
Transaction::Callback SingleRowTxn::GetCommitCallback() {
    return user_commit_callback_;
}

/// 设置用户上下文，可在回调函数中获取
void SingleRowTxn::SetContext(void* context) {
    user_commit_context_ = context;
}

/// 获取用户上下文
void* SingleRowTxn::GetContext() {
    return user_commit_context_;
}

/// 获得结果错误码
const ErrorCode& SingleRowTxn::GetError() {
    return mutation_buffer_.GetError();
}

/// 内部读操作回调
void SingleRowTxn::ReadCallback(RowReaderImpl* reader_impl) {
    // restore user's callback & context
    reader_impl->SetCallBack(user_reader_callback_);
    reader_impl->SetContext(user_reader_context_);

    // save results for commit check
    ErrorCode::ErrorCodeType code = reader_impl->GetError().GetType();
    if (code == ErrorCode::kOK || code == ErrorCode::kNotFound) {
        // copy read_column_list
        read_column_list_ = reader_impl->GetReadColumnList();

        // copy read result (not including value)
        while (!reader_impl->Done()) {
            const std::string& family = reader_impl->Family();
            const std::string& qualifier = reader_impl->Qualifier();
            int64_t timestamp = reader_impl->Timestamp();
            read_result_[family][qualifier][timestamp] = reader_impl->Value();
            reader_impl->Next();
        }
        reader_impl->ResetResultPos();
    } else {
        MarkNoRead();
    }

    // run user's callback
    reader_impl->RunCallback();
}

void CommitCallbackWrapper(RowMutation* row_mu) {
    RowMutationImpl* mu_impl = static_cast<RowMutationImpl*>(row_mu);
    SingleRowTxn* txn_impl = static_cast<SingleRowTxn*>(row_mu->GetContext());
    txn_impl->CommitCallback(mu_impl);
}

/// 提交事务
void SingleRowTxn::Commit() {
    if (mutation_buffer_.MutationNum() > 0) {
        if (user_commit_callback_ != NULL) {
            // use our callback wrapper
            mutation_buffer_.SetCallBack(CommitCallbackWrapper);
            mutation_buffer_.SetContext(this);
        }
        mutation_buffer_.SetTransaction(this);
        table_->ApplyMutation(&mutation_buffer_);
    } else {
        if (user_commit_callback_ != NULL) {
            ThreadPool::Task task = std::bind(user_commit_callback_, this);
            thread_pool_->AddTask(task);
        }
    }
}

/// 内部提交回调
void SingleRowTxn::CommitCallback(RowMutationImpl* mu_impl) {
    CHECK_EQ(&mutation_buffer_, mu_impl);
    CHECK_NOTNULL(user_commit_callback_);
    // run user's commit callback
    user_commit_callback_(this);
}

/// 序列化
void SingleRowTxn::Serialize(RowMutationSequence* mu_seq) {
    SingleRowTxnReadInfo* pb_read_info = mu_seq->mutable_txn_read_info();
    pb_read_info->set_has_read(has_read_);
    assert(reader_max_versions_ >= 1);
    pb_read_info->set_max_versions(reader_max_versions_);
    if (reader_start_timestamp_ != kOldestTs) {
        pb_read_info->set_start_timestamp(reader_start_timestamp_);
    }
    if (reader_end_timestamp_ != kLatestTs) {
        pb_read_info->set_end_timestamp(reader_end_timestamp_);
    }

    // serialize read_clumn_list
    RowReader::ReadColumnList::iterator column_it = read_column_list_.begin();
    for (; column_it != read_column_list_.end(); ++column_it) {
        const std::string& family = column_it->first;
        std::set<std::string>& qualifier_set = column_it->second;

        ColumnFamily* pb_column_info = pb_read_info->add_read_column_list();
        pb_column_info->set_family_name(family);

        std::set<std::string>::iterator cq_it = qualifier_set.begin();
        for (; cq_it != qualifier_set.end(); ++cq_it) {
            pb_column_info->add_qualifier_list(*cq_it);
        }
    }

    // serialize read_result (family & qualifier & timestamp & value)
    ReadResult::iterator cf_it = read_result_.begin();
    for (; cf_it != read_result_.end(); ++cf_it) {
        const std::string& family = cf_it->first;
        auto& qualifier_map = cf_it->second;

        auto cq_it = qualifier_map.begin();
        for (; cq_it != qualifier_map.end(); ++cq_it) {
            const std::string& qualifier = cq_it->first;
            auto& cell_map = cq_it->second;

            auto it = cell_map.rbegin();
            for (; it != cell_map.rend(); ++it) {
                KeyValuePair* kv = pb_read_info->mutable_read_result()->add_key_values();
                kv->set_column_family(family);
                kv->set_qualifier(qualifier);
                kv->set_timestamp(it->first);
                kv->set_value(it->second);
            }
        }
    }
}

} // namespace tera

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
