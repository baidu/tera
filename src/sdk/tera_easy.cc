// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Author: yanshiguang02@baidu.com

#include "tera_easy.h"

#include <functional>
#include <iostream>
#include <gflags/gflags.h>
#include <glog/logging.h>

#include "common/thread_pool.h"
#include "tera.h"
#include "common/atomic.h"
#include "common/counter.h"

DEFINE_int32(tera_easy_ttl, 90 * 24 * 3600, "ttl(s) of key-value writed by tera_easy");
DEFINE_int32(tera_sdk_rpc_max_pending_num, 1024 * 1024, "max num of pending kv");
DECLARE_int32(tera_sdk_rpc_max_pending_buffer_size);
DECLARE_string(flagfile);

namespace teraeasy {

class TableImpl : public Table {
public:
    TableImpl(tera::Table* table, tera::Client* client)
        : table_(table),
          client_(client),
          scanner_(NULL) {
        ThreadPool::Task task = std::bind(&TableImpl::PrintStatus, this);
        thread_pool_.DelayTask(1000, task);
    }

    ~TableImpl() {
        Flush();
        delete scanner_;
        delete table_;
        delete client_;
    }

    bool Read(const Key& key, Record* record) {
        std::string value;
        tera::ErrorCode err;
        if (!table_->Get(key, "", "", &value, &err)) {
            LOG(ERROR) << "fail to read: " << key
                << ", reason: " << err.GetReason();
            return false;
        }
        return DeSerializeRecord(value, record);
    }

    bool Write(const Key& key, const Record& record) {
        CHECK(s_pending_num_.Get() >= 0) << "pending num < 0: " << s_pending_num_.Get();
        CHECK(s_pending_size_.Get() >= 0) << "pending size < 0: " << s_pending_size_.Get();
        while (s_pending_num_.Get() > FLAGS_tera_sdk_rpc_max_pending_num ||
               s_pending_size_.Get() > FLAGS_tera_sdk_rpc_max_pending_buffer_size * 1024 * 1024) {
            usleep(1000000);
        }

        std::string value;
        SerializeRecord(record, &value);

        {
            tera::RowMutation* mutation = table_->NewRowMutation(key);
            mutation->Put(value, FLAGS_tera_easy_ttl);
            mutation->SetCallBack(TableImpl::WriteCallback);
            table_->ApplyMutation(mutation);
            s_pending_num_.Inc();
            s_pending_size_.Add(mutation->Size());
        }
        return true;
    }

    void Flush() {
        while (s_pending_num_.Get() > 0) {
            usleep(10000);
        }
    }

    // sync delete
    bool Delete(const Key& key) {
        tera::RowMutation* mutation = table_->NewRowMutation(key);
        mutation->DeleteRow();
        table_->ApplyMutation(mutation);
        return true;
    }

    bool SetScanner(const Key& start, const Key& end) {
        if (scanner_ != NULL) {
            delete scanner_;
        }
        tera::ErrorCode err;
        tera::ScanDescriptor desc(start);
        desc.SetEnd(end);
        desc.SetAsync(false);

        if ((scanner_ = table_->Scan(desc, &err)) == NULL) {
            LOG(ERROR) << "fail to scan the table, reason:" << err.GetReason();
            return false;
        }
        return true;
    }

    bool NextPair(KVPair* pair) {
        if (scanner_ == NULL) {
            LOG(ERROR) << "scanner is empty!";
            return false;
        }
        if (!scanner_->Done()) {
            Record record;
            DeSerializeRecord(scanner_->Value(), &record);
            *pair = std::make_pair(scanner_->RowName(), record);
            scanner_->Next();
            return true;
        }
        delete scanner_;
        scanner_ = NULL;
        return false;
    }

    static void WriteCallback(tera::RowMutation* mutation) {
        const tera::ErrorCode& error_code = mutation->GetError();
        if (error_code.GetType() != tera::ErrorCode::kOK) {
            s_write_fail_num_.Inc();
            VLOG(5)<< "write key failed: key(" << mutation->RowKey()
                << "), reason:" << error_code.GetReason();
        } else {
            s_write_succ_num_.Inc();
        }

        s_pending_num_.Dec();
        s_pending_size_.Sub(mutation->Size());
        delete mutation;
    }

private:
    union Fix32Converter {
        int32_t v;
        char buf[sizeof(v)];
    };

    void AppendFix32(int32_t v, std::string* str) {
        Fix32Converter u;
        u.v = v;
        str->append(u.buf, sizeof(v));
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
        LOG(INFO) << "[TeraEasy] pending num " << s_pending_num_.Get()
            << ", pending size " << s_pending_size_.Get()
            << ", success " << s_write_succ_num_.Clear()
            << ", fail " << s_write_fail_num_.Clear();
        ThreadPool::Task task = std::bind(&TableImpl::PrintStatus, this);
        thread_pool_.DelayTask(1000, task);
    }

private:
    static tera::Counter s_pending_num_;
    static tera::Counter s_pending_size_;
    static tera::Counter s_write_fail_num_;
    static tera::Counter s_write_succ_num_;

    tera::Table* table_;
    tera::Client* client_;
    tera::ResultStream* scanner_;
    ThreadPool thread_pool_;
};

tera::Counter TableImpl::s_pending_num_;
tera::Counter TableImpl::s_pending_size_;
tera::Counter TableImpl::s_write_fail_num_;
tera::Counter TableImpl::s_write_succ_num_;

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
