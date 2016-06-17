// Copyright (c) 2016, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include <boost/bind.hpp>

#include "common/thread_pool.h"
#include "common/base/string_format.h"

#include "io/coding.h"
#include "sdk/read_impl.h"
#include "sdk/single_row_txn.h"
#include "sdk/table_impl.h"
#include "types.h"
#include "utils/timer.h"

namespace tera {

SingleRowTxn::SingleRowTxn(TableImpl* table, const std::string& row_key,
                           common::ThreadPool* thread_pool)
    : _table(table),
      _row_key(row_key),
      _thread_pool(thread_pool),
      _has_read(false),
      _user_reader_callback(NULL),
      _user_reader_context(NULL),
      _mutation_buffer(table, row_key),
      _user_commit_callback(NULL),
      _user_commit_context(NULL) {
}

SingleRowTxn::~SingleRowTxn() {
}

/// 提交一个修改操作
void SingleRowTxn::ApplyMutation(RowMutation* row_mu) {
    RowMutationImpl* row_mu_impl = static_cast<RowMutationImpl*>(row_mu);
    row_mu_impl->SetTransaction(this);

    if (row_mu->RowKey() == _row_key) {
        _mutation_buffer.Concatenate(*row_mu_impl);
        row_mu_impl->SetError(ErrorCode::kOK);
    } else {
        row_mu_impl->SetError(ErrorCode::kBadParam, "not same row");
    }

    if (row_mu->IsAsync()) {
        ThreadPool::Task task = boost::bind(&RowMutationImpl::RunCallback, row_mu_impl);
        _thread_pool->AddTask(task);
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

    int64_t ts_start = 0, ts_end = 0;
    reader_impl->GetTimeRange(&ts_start, &ts_end);

    // safe check
    if (reader_impl->RowName() != _row_key) {
        reader_impl->SetError(ErrorCode::kBadParam, "not same row");
    } else if (_has_read) {
        reader_impl->SetError(ErrorCode::kBadParam, "not support read more than once in txn");
    } else if (ts_start > kOldestTs || ts_end < kLatestTs) {
        reader_impl->SetError(ErrorCode::kBadParam, "not support read a time range in txn");
    } else if (reader_impl->GetSnapshot() != 0) {
        reader_impl->SetError(ErrorCode::kBadParam, "not support read a snapshot in txn");
    } else if (reader_impl->GetMaxVersions() != 1) {
        reader_impl->SetError(ErrorCode::kBadParam, "not support read old versions in txn");
    }
    if (reader_impl->GetError().GetType() != ErrorCode::kOK) {
        if (is_async) {
            ThreadPool::Task task = boost::bind(&RowReaderImpl::RunCallback, reader_impl);
            _thread_pool->AddTask(task);
        }
        return;
    }

    // save user's callback & context
    _user_reader_callback = reader_impl->GetCallBack();
    _user_reader_context = reader_impl->GetContext();

    // use our callback wrapper
    reader_impl->SetCallBack(ReadCallbackWrapper);
    reader_impl->SetContext(this);

    _table->Get(reader_impl);
    if (!is_async) {
        reader_impl->Wait();
    }
}

/// 设置提交回调, 提交操作会异步返回
void SingleRowTxn::SetCommitCallback(Callback callback) {
    _user_commit_callback = callback;
}

/// 获取提交回调
Transaction::Callback SingleRowTxn::GetCommitCallback() {
    return _user_commit_callback;
}

/// 设置用户上下文，可在回调函数中获取
void SingleRowTxn::SetContext(void* context) {
    _user_commit_context = context;
}

/// 获取用户上下文
void* SingleRowTxn::GetContext() {
    return _user_commit_context;
}

/// 获得结果错误码
const ErrorCode& SingleRowTxn::GetError() {
    return _mutation_buffer.GetError();
}

/// 内部读操作回调
void SingleRowTxn::ReadCallback(RowReaderImpl* reader_impl) {
    // restore user's callback & context
    reader_impl->SetCallBack(_user_reader_callback);
    reader_impl->SetContext(_user_reader_context);

    // save results for commit check
    ErrorCode::ErrorCodeType code = reader_impl->GetError().GetType();
    if (code == ErrorCode::kOK || code == ErrorCode::kNotFound) {
        _has_read = true;

        // copy read_column_list
        _read_column_list = reader_impl->GetReadColumnList();

        // copy read result (not including value)
        while (!reader_impl->Done()) {
            const std::string& family = reader_impl->Family();
            const std::string& qualifier = reader_impl->Qualifier();
            int64_t timestamp = reader_impl->Timestamp();
            _read_result[family][qualifier] = timestamp;
            reader_impl->Next();
        }
        reader_impl->ResetResultPos();
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
    if (_mutation_buffer.MutationNum() > 0) {
        if (_user_commit_callback != NULL) {
            // use our callback wrapper
            _mutation_buffer.SetCallBack(CommitCallbackWrapper);
            _mutation_buffer.SetContext(this);
        }
        _mutation_buffer.SetTransaction(this);
        _table->ApplyMutation(&_mutation_buffer);
    } else {
        if (_user_commit_callback != NULL) {
            ThreadPool::Task task = boost::bind(_user_commit_callback, this);
            _thread_pool->AddTask(task);
        }
    }
}

/// 内部提交回调
void SingleRowTxn::CommitCallback(RowMutationImpl* mu_impl) {
    CHECK_EQ(&_mutation_buffer, mu_impl);
    CHECK_NOTNULL(_user_commit_callback);
    // run user's commit callback
    _user_commit_callback(this);
}

/// 序列化
void SingleRowTxn::Serialize(RowMutationSequence* mu_seq) {
    SingleRowTxnReadInfo* pb_read_info = mu_seq->mutable_txn_read_info();
    pb_read_info->set_has_read(_has_read);

    // serialize read_clumn_list
    RowReader::ReadColumnList::iterator column_it = _read_column_list.begin();
    for (; column_it != _read_column_list.end(); ++column_it) {
        const std::string& family = column_it->first;
        std::set<std::string>& qualifier_set = column_it->second;

        ColumnFamily* pb_column_info = pb_read_info->add_read_column_list();
        pb_column_info->set_family_name(family);

        std::set<std::string>::iterator cq_it = qualifier_set.begin();
        for (; cq_it != qualifier_set.end(); ++cq_it) {
            pb_column_info->add_qualifier_list(*cq_it);
        }
    }

    // serialize read_result (only family & qualifier & timestamp)
    ReadResult::iterator cf_it = _read_result.begin();
    for (; cf_it != _read_result.end(); ++cf_it) {
        const std::string& family = cf_it->first;
        std::map<std::string, int64_t>& qualifier_map = cf_it->second;

        std::map<std::string, int64_t>::iterator cq_it = qualifier_map.begin();
        for (; cq_it != qualifier_map.end(); ++cq_it) {
            const std::string& qualifier = cq_it->first;
            int64_t timestamp = cq_it->second;

            KeyValuePair* kv = pb_read_info->mutable_read_result()->add_key_values();
            kv->set_column_family(family);
            kv->set_qualifier(qualifier);
            kv->set_timestamp(timestamp);
        }
    }

}

} // namespace tera

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
