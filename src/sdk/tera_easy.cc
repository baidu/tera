// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Author: yanshiguang02@baidu.com

#include "tera_easy.h"

#include <iostream>
#include <boost/bind.hpp>
#include <gflags/gflags.h>
#include <glog/logging.h>

#include "common/thread_pool.h"
#include "tera.h"
#include "utils/atomic.h"
#include "utils/counter.h"

DEFINE_int32(tera_easy_ttl, 90 * 24 * 3600, "ttl(s) of key-value writed by tera_easy");
DEFINE_int32(tera_sdk_rpc_max_pending_num, 1024 * 1024, "max num of pending kv");
DECLARE_int32(tera_sdk_rpc_max_pending_buffer_size);
DECLARE_string(flagfile);

namespace teraeasy {

class TableImpl : public Table {
public:
    TableImpl(tera::Table* table, tera::Client* client)
        : _table(table),
          _client(client),
          _scanner(NULL) {
        ThreadPool::Task task = boost::bind(&TableImpl::PrintStatus, this);
        _thread_pool.DelayTask(1000, task);
    }

    ~TableImpl() {
        Flush();
        delete _scanner;
        delete _table;
        delete _client;
    }

    bool Read(const Key& key, Record* record) {
        std::string value;
        tera::ErrorCode err;
        if (!_table->Get(key, "", "", &value, &err)) {
            LOG(ERROR) << "fail to read: " << key
                << ", reason: " << err.GetReason();
            return false;
        }
        return DeSerializeRecord(value, record);
    }

    bool Write(const Key& key, const Record& record) {
        CHECK(_s_pending_num.Get() >= 0) << "pending num < 0: " << _s_pending_num.Get();
        CHECK(_s_pending_size.Get() >= 0) << "pending size < 0: " << _s_pending_size.Get();
        while (_s_pending_num.Get() > FLAGS_tera_sdk_rpc_max_pending_num ||
               _s_pending_size.Get() > FLAGS_tera_sdk_rpc_max_pending_buffer_size * 1024 * 1024) {
            usleep(1000000);
        }

        std::string value;
        SerializeRecord(record, &value);

        {
            tera::RowMutation* mutation = _table->NewRowMutation(key);
            mutation->Put(value, FLAGS_tera_easy_ttl);
            mutation->SetCallBack(TableImpl::WriteCallback);
            _table->ApplyMutation(mutation);
            _s_pending_num.Inc();
            _s_pending_size.Add(mutation->Size());
        }
        return true;
    }

    void Flush() {
        while (_s_pending_num.Get() > 0) {
            usleep(10000);
        }
    }

    // sync delete
    bool Delete(const Key& key) {
        tera::RowMutation* mutation = _table->NewRowMutation(key);
        mutation->DeleteRow();
        _table->ApplyMutation(mutation);
        return true;
    }

    bool SetScanner(const Key& start, const Key& end) {
        if (_scanner != NULL) {
            delete _scanner;
        }
        tera::ErrorCode err;
        tera::ScanDescriptor desc(start);
        desc.SetEnd(end);
        desc.SetAsync(false);

        if ((_scanner = _table->Scan(desc, &err)) == NULL) {
            LOG(ERROR) << "fail to scan the table, reason:" << err.GetReason();
            return false;
        }
        return true;
    }

    bool NextPair(KVPair* pair) {
        if (_scanner == NULL) {
            LOG(ERROR) << "scanner is empty!";
            return false;
        }
        if (!_scanner->Done()) {
            Record record;
            DeSerializeRecord(_scanner->Value(), &record);
            *pair = std::make_pair(_scanner->RowName(), record);
            _scanner->Next();
            return true;
        }
        delete _scanner;
        _scanner = NULL;
        return false;
    }

    static void WriteCallback(tera::RowMutation* mutation) {
        const tera::ErrorCode& error_code = mutation->GetError();
        if (error_code.GetType() != tera::ErrorCode::kOK) {
            _s_write_fail_num.Inc();
            VLOG(5)<< "write key failed: key(" << mutation->RowKey()
                << "), reason:" << error_code.GetReason();
        } else {
            _s_write_succ_num.Inc();
        }

        _s_pending_num.Dec();
        _s_pending_size.Sub(mutation->Size());
        delete mutation;
    }

private:
    void AppendFix32(int32_t v, std::string* str) {
        char buf[sizeof(v)];
        *reinterpret_cast<int32_t*>(buf) = v;
        str->append(buf, sizeof(v));
    }

    int32_t GetFix32(const char* buf) {
        return *(reinterpret_cast<const int32_t*>(buf));
    }

    bool SerializeColumn(const Column& column, std::string* buf) {
        Column::const_iterator it = column.begin();
        AppendFix32(column.size(), buf);
        for (; it != column.end(); ++it) {
            AppendFix32(it->first, buf);
            AppendFix32(it->second.size(), buf);
            buf->append(it->second);
        }
        return true;
    }

    int32_t DeSerializeColumn(const char* buf, Column* column) {
        int num = GetFix32(buf);
        int offset = sizeof(num);
        for (int i = 0; i < num; i++) {
            int ts = GetFix32(buf + offset);
            offset += 4;
            int len = GetFix32(buf + offset);
            offset += 4;
            (*column)[ts].assign(buf + offset, len);
            offset += len;
        }
        return offset;
    }

    bool SerializeRecord(const Record& record, std::string* buf) {
        Record::const_iterator it = record.begin();
        AppendFix32(record.size(), buf);
        for (; it != record.end(); ++it) {
            AppendFix32(it->first.size(), buf);
            buf->append(it->first);
            SerializeColumn(it->second, buf);
        }
        return true;
    }

    int32_t DeSerializeRecord(const std::string& buf, Record* record) {
        int num = GetFix32(buf.data());
        int offset = sizeof(num);
        for (int i = 0; i < num; i++) {
            Key key;
            Column column;
            int len = GetFix32(buf.data()+offset);
            offset += 4;
            key.assign(buf.data()+offset, len);
            offset += len;
            len = DeSerializeColumn(buf.data()+offset, &column);
            offset += len;
            (*record)[key] = column;
        }
        return offset;
    }

    void PrintStatus() {
        LOG(INFO) << "[TeraEasy] pending num " << _s_pending_num.Get()
            << ", pending size " << _s_pending_size.Get()
            << ", success " << _s_write_succ_num.Clear()
            << ", fail " << _s_write_fail_num.Clear();
        ThreadPool::Task task = boost::bind(&TableImpl::PrintStatus, this);
        _thread_pool.DelayTask(1000, task);
    }

private:
    static tera::Counter _s_pending_num;
    static tera::Counter _s_pending_size;
    static tera::Counter _s_write_fail_num;
    static tera::Counter _s_write_succ_num;

    tera::Table* _table;
    tera::Client* _client;
    tera::ResultStream* _scanner;
    ThreadPool _thread_pool;
};

tera::Counter TableImpl::_s_pending_num;
tera::Counter TableImpl::_s_pending_size;
tera::Counter TableImpl::_s_write_fail_num;
tera::Counter TableImpl::_s_write_succ_num;

Table* OpenTable(const std::string& table_name, const std::string& conf_path) {
    std::string conf;
    if (conf_path == "") {
        conf = "./tera.flag";
    } else {
        conf = conf_path;
    }

    tera::Client* client = tera::Client::NewClient(conf, "tera");

    tera::ErrorCode err;
    tera::Table* table = NULL;
    if (client == NULL || (table = client->OpenTable(table_name, &err)) == NULL) {
        LOG(ERROR) << "fail to open table: " << table_name;
        return NULL;
    }
    return new TableImpl(table, client);
}
}
