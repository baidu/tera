// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "tera_hash.h"

#include <iostream>

#include "common/base/string_format.h"
#include "common/mutex.h"
#include "gflags/gflags.h"
#include "glog/logging.h"

DEFINE_int32(tera_hash_sdk_rpc_max_pending_num, 1024 * 1024, "max num of pending kv");
DEFINE_int64(tera_hash_sdk_scan_buffer_size, 128 * 1024, "max buffer size of scan in hask sdk");
DECLARE_int32(tera_sdk_rpc_max_pending_buffer_size);

namespace tera {

class NullHashMethod : public HashMethod {
public:
    NullHashMethod(int32_t bulk_num = -1) : HashMethod(bulk_num) {}
    ~NullHashMethod() {}
    std::string HashKey(const std::string& key) {
        return key;
    }
    std::string Key(const std::string& hash_key) {
        return hash_key;
    }
};


Mutex s_mutex_;
int32_t s_pending_num_ = 0;
int32_t s_pending_size_ = 0;

HashClient::HashClient(HashMethod* hash_method,
                       const std::string& table_name,
                       Client* client_impl)
    : table_(NULL), scan_stream_(NULL), table_name_(table_name),
      hash_method_(hash_method), is_created_client_(false),
      is_created_hash_method_(false) {
    if (hash_method_ == NULL) {
        hash_method_ = new NullHashMethod();
        is_created_hash_method_ = true;
    }
    ErrorCode err;
    if (client_impl) {
        client_ = client_impl;
    } else {
        client_ = Client::NewClient("./tera.flag", "tera_hash", &err);
        is_created_client_ = true;
    }
    CHECK(client_ && err.GetType() == ErrorCode::kOK) << strerr(err);
}

HashClient::~HashClient() {
    if (is_created_hash_method_) {
        delete hash_method_;
    }
    if (is_created_client_) {
        delete client_;
    }

    if (table_) {
        delete table_;
    }
}

bool HashClient::OpenTable(ErrorCode* err) {
    if (table_) {
        return true;
    }

    if (!client_->IsTableExist(table_name_, err)) {
        return false;
    }

    table_ = client_->OpenTable(table_name_, err);
    if (table_ == NULL) {
        return false;
    }

    CHECK(GetColumnFamilyList(&field_types_, err));

    return true;
}

bool HashClient::Put(const std::string& row_key,
                     const std::string& value,
                     ErrorCode* err) {
    return Put(row_key, "", "", value, err);
}

bool HashClient::Put(const std::string& row_key, const std::string& family,
                     const std::string& qualifier, const std::string& value,
                     ErrorCode* err) {
    if (!table_) {
        LOG(ERROR) << "table not open: " << table_name_;
        SetErrorCode(err, ErrorCode::kSystem, "tail not open: " + table_name_);
        return false;
    }
    return table_->Put(hash_method_->HashKey(row_key), family, qualifier, value, err);
}

bool HashClient::Write(const std::string& row_key, const std::string& family,
                       const std::string& qualifier, const std::string& value,
                       ErrorCode* err) {
    return Write(row_key, family, qualifier, value, NULL, err);
}

bool HashClient::Write(const std::string& row_key, const std::string& family,
                       const std::string& qualifier, const std::string& value,
                       UserContext* context, ErrorCode* err) {
    if (!table_) {
        LOG(ERROR) << "table not open: " << table_name_;
        SetErrorCode(err, ErrorCode::kSystem, "tail not open: " + table_name_);
        return false;
    }

    while (s_pending_num_ > FLAGS_tera_hash_sdk_rpc_max_pending_num
           || s_pending_size_ > FLAGS_tera_sdk_rpc_max_pending_buffer_size * 1024 * 1024) {
        usleep(1000000);
    }


    {
        tera::RowMutation* mutation = NewMutation(row_key);
        mutation->Put(family, qualifier, value);
        ApplyMutation(context, mutation, row_key.length() + family.length()
                      + qualifier.length() + value.length());
    }
    return true;
}

void HashClient::Flush(uint64_t sleep_time) {
    while (s_pending_num_ > 0) {
        usleep(sleep_time);
    }
}

RowMutation* HashClient::NewMutation(const std::string& row_key) {
    return table_->NewRowMutation(hash_method_->HashKey(row_key));
}

void HashClient::ApplyMutation(UserContext* context, RowMutation* mutation,
                               int32_t value_size) {
    mutation->SetCallBack(HashClient::WriteCallback);
    if (context) {
        mutation->SetContext(context);
    } else {
        mutation->SetContext(NULL);
    }
    MutexLock locker(&s_mutex_);
    s_pending_num_++;
    s_pending_size_ += value_size;
    table_->ApplyMutation(mutation);
}

void HashClient::WriteCallback(tera::RowMutation* mutation) {
    MutexLock locker(&s_mutex_);
    const tera::ErrorCode& error_code = mutation->GetError();
    if (error_code.GetType() != tera::ErrorCode::kOK) {
        LOG(ERROR) << "write failed: key = " << mutation->RowKey()
            << "), reason:" << error_code.GetReason();
    }

    UserContext* context = reinterpret_cast<UserContext*>(mutation->GetContext());
    if (context) {
        if (context->callback) {
            context->callback(context->param, error_code.GetType() == tera::ErrorCode::kOK);
        }
        delete context;
    }

    s_pending_num_--;
    s_pending_size_ -= mutation->Size();
    delete mutation;
}

bool HashClient::Get(const std::string& row_key,
                     std::string* value, ErrorCode* err) {
    return Get(row_key, "", "", value, err);
}

bool HashClient::Get(const std::string& row_key, const std::string& family,
                     const std::string& qualifier, std::string* value,
                     ErrorCode* err) {
    if (!table_) {
        LOG(ERROR) << "table not open: " << table_name_;
        SetErrorCode(err, ErrorCode::kSystem, "tail not open: " + table_name_);
        return false;
    }

    return table_->Get(hash_method_->HashKey(row_key), family, qualifier, value, err);
}

bool HashClient::Get(const std::string& row_key, void* obj,
                     void (*callback)(void*, const std::string& family, const std::string& qualifer,
                                      const std::string& value, const std::string& value_type)) {
    RowReader* row_reader = table_->NewRowReader(hash_method_->HashKey(row_key));
    row_reader->SetMaxVersions(1);
    row_reader->SetTimeOut(5000);
    table_->Get(row_reader);

    while(!row_reader->Done()) {
        std::string type;
        std::map<std::string, std::string>::iterator it = field_types_.find(row_reader->Family());
        if (it != field_types_.end()) {
            type = it->second;
        }
        callback(obj, row_reader->Family(), row_reader->Qualifier(), row_reader->Value(), type);
        row_reader->Next();
    }
    delete row_reader;

    return true;
}

bool HashClient::Delete(const std::string& row_key, ErrorCode* err) {
    RowMutation* mutation = table_->NewRowMutation(hash_method_->HashKey(row_key));
    mutation->DeleteRow();
    table_->ApplyMutation(mutation);
    delete mutation;
    return true;
}


bool HashClient::Seek(const HashScanDesc& desc, ErrorCode* err) {
    ScanDescriptor scan_desc(desc.start_rowkey);
    scan_desc.SetEnd(desc.end_rowkey);
    if (desc.buffer_size > 0) {
        scan_desc.SetBufferSize(desc.buffer_size);
    } else if (FLAGS_tera_hash_sdk_scan_buffer_size > 0) {
        scan_desc.SetBufferSize(FLAGS_tera_hash_sdk_scan_buffer_size);
    }
    if (!desc.filter_expression.empty()) {
        scan_desc.SetFilter(desc.filter_expression);
    }

    if (desc.converter) {
        scan_desc.SetValueConverter(desc.converter);
    }
    scan_desc.SetAsync(desc.is_async);

    std::string::size_type pos;
    std::string cf, col;
    for (size_t i = 0; i < desc.fields.size(); ++i) {
        if ((pos = desc.fields[i].find(":", 0)) == std::string::npos) {
            // add columnfamily
            scan_desc.AddColumnFamily(desc.fields[i]);
            VLOG(10) << "add cf: " << desc.fields[i] << " to scan descriptor";
        } else {
            // add column
            cf = desc.fields[i].substr(0, pos);
            col = desc.fields[i].substr(pos + 1);
            scan_desc.AddColumn(cf, col);
            VLOG(10) << "add column: " << cf << ":" << col << " to scan descriptor";
        }
    }

    scan_stream_ = table_->Scan(scan_desc, err);
    return scan_stream_ != NULL;
}

bool HashClient::Current(std::string* key, std::string* value,
                         ErrorCode* err) {
    if (scan_stream_->Done()) {
        SetErrorCode(err, ErrorCode::kSystem, "not more record");
        return false;
    }
    *key = hash_method_->Key(scan_stream_->RowName());
    *value = scan_stream_->Value();
    return true;
}

bool HashClient::Current(std::string* row_key, std::string* family, std::string* qualifier,
                         std::string* value, ErrorCode* err) {
    if (!scan_stream_) {
        SetErrorCode(err, ErrorCode::kSystem, "scan not ready");
        return false;
    }
    if (scan_stream_->Done()) {
        SetErrorCode(err, ErrorCode::kSystem, "not more record");
        return false;
    }

    *row_key = hash_method_->Key(scan_stream_->RowName());
    if (family) {
        *family = scan_stream_->Family();
    }
    if (qualifier) {
        *qualifier = scan_stream_->Qualifier();
    }
    if (value) {
        *value = scan_stream_->Value();
    }
    return true;
}

bool HashClient::Current(std::string* row_key, void* obj,
                         void (*callback)(void*, const std::string& family,
                                          const std::string& qualifier,
                                          const std::string& value,
                                          const std::string& type)) {
    if (scan_stream_->Done()) {
        return false;
    }
    std::string type;
    std::map<std::string, std::string>::iterator it = field_types_.find(scan_stream_->Family());
    if (it != field_types_.end()) {
        type = it->second;
    }
    *row_key = hash_method_->Key(scan_stream_->RowName());
    callback(obj, scan_stream_->Family(), scan_stream_->Qualifier(),
             scan_stream_->Value(), type);
    return true;
}

bool HashClient::Next(ErrorCode* err) {
    if (scan_stream_->Done()) {
        SetErrorCode(err, ErrorCode::kSystem, "not more record");
        return false;
    }
    scan_stream_->Next();

    if (scan_stream_->Done()) {
        return false;
    }
    return true;
}

const Table* HashClient::GetTable() {
    return table_;
}

bool HashClient::CreateTable(const std::map<std::string, std::string>& cf_list,
                             ErrorCode* err) {
    if (client_->IsTableExist(table_name_, err)) {
        LOG(ERROR) << "table '" << table_name_ << "' already exist";
        return false;
    }

    TableDescriptor table_desc(table_name_);
    table_desc.SetRawKey(kBinary);

    std::string sf_lg_name = "sf_lg";
    if (!table_desc.AddLocalityGroup(sf_lg_name)) {
        LOG(ERROR) << "fail to add locality group: " << sf_lg_name;
        return false;
    }
    std::map<std::string, std::string>::const_iterator it = cf_list.begin();
    for (; it != cf_list.end(); ++it) {
        ColumnFamilyDescriptor* cf_desc =
            table_desc.AddColumnFamily(it->first, sf_lg_name);
        if (!cf_desc) {
            LOG(ERROR) << "fail to add column family: " << it->first;
            continue;
        }
        cf_desc->SetType(it->second);
    }

    std::vector<std::string> delimiters;
    for (int32_t i = 0; i < hash_method_->GetBulkNum(); ++i) {
        delimiters.push_back(StringFormat("%08llu", i));
    }

    if (!client_->CreateTable(table_desc, delimiters, err)) {
        return false;
    }
    return true;
}

bool HashClient::DeleteTable(ErrorCode* err) {
    return true;
}

bool HashClient::GetColumnFamilyList(std::map<std::string, std::string>* cf_list,
                                     ErrorCode* err) {
    TableDescriptor* table_desc = client_->GetTableDescriptor(table_name_, err);
    if (!table_desc) {
        return false;
    }

    for (int32_t i = 0; i < table_desc->ColumnFamilyNum(); ++i) {
        const ColumnFamilyDescriptor* cf_desc =  table_desc->ColumnFamily(i);
        (*cf_list)[cf_desc->Name()] = cf_desc->Type();
    }
    return true;
}

void HashClient::SetErrorCode(ErrorCode* err, ErrorCode::ErrorCodeType value,
                              const std::string& err_reason) {
    if (err) {
        err->SetFailed(value, err_reason);
    }
}

} // namespace tera
