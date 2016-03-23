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


Mutex _s_mutex;
int32_t _s_pending_num = 0;
int32_t _s_pending_size = 0;

HashClient::HashClient(HashMethod* hash_method,
                       const std::string& table_name,
                       Client* client_impl)
    : _table(NULL), _scan_stream(NULL), _table_name(table_name),
      _hash_method(hash_method), _is_created_client(false),
      _is_created_hash_method(false) {
    if (_hash_method == NULL) {
        _hash_method = new NullHashMethod();
        _is_created_hash_method = true;
    }
    ErrorCode err;
    if (client_impl) {
        _client = client_impl;
    } else {
        _client = Client::NewClient("./tera.flag", "tera_hash", &err);
        _is_created_client = true;
    }
    CHECK(_client && err.GetType() == ErrorCode::kOK) << strerr(err);
}

HashClient::~HashClient() {
    if (_is_created_hash_method) {
        delete _hash_method;
    }
    if (_is_created_client) {
        delete _client;
    }

    if (_table) {
        delete _table;
    }
}

bool HashClient::OpenTable(ErrorCode* err) {
    if (_table) {
        return true;
    }

    if (!_client->IsTableExist(_table_name, err)) {
        return false;
    }

    _table = _client->OpenTable(_table_name, err);
    if (_table == NULL) {
        return false;
    }

    CHECK(GetColumnFamilyList(&_field_types, err));

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
    if (!_table) {
        LOG(ERROR) << "table not open: " << _table_name;
        SetErrorCode(err, ErrorCode::kSystem, "tail not open: " + _table_name);
        return false;
    }
    return _table->Put(_hash_method->HashKey(row_key), family, qualifier, value, err);
}

bool HashClient::Write(const std::string& row_key, const std::string& family,
                       const std::string& qualifier, const std::string& value,
                       ErrorCode* err) {
    return Write(row_key, family, qualifier, value, NULL, err);
}

bool HashClient::Write(const std::string& row_key, const std::string& family,
                       const std::string& qualifier, const std::string& value,
                       UserContext* context, ErrorCode* err) {
    if (!_table) {
        LOG(ERROR) << "table not open: " << _table_name;
        SetErrorCode(err, ErrorCode::kSystem, "tail not open: " + _table_name);
        return false;
    }

    while (_s_pending_num > FLAGS_tera_hash_sdk_rpc_max_pending_num
           || _s_pending_size > FLAGS_tera_sdk_rpc_max_pending_buffer_size * 1024 * 1024) {
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
    while (_s_pending_num > 0) {
        usleep(sleep_time);
    }
}

RowMutation* HashClient::NewMutation(const std::string& row_key) {
    return _table->NewRowMutation(_hash_method->HashKey(row_key));
}

void HashClient::ApplyMutation(UserContext* context, RowMutation* mutation,
                               int32_t value_size) {
    mutation->SetCallBack(HashClient::WriteCallback);
    if (context) {
        mutation->SetContext(context);
    } else {
        mutation->SetContext(NULL);
    }
    MutexLock locker(&_s_mutex);
    _s_pending_num++;
    _s_pending_size += value_size;
    _table->ApplyMutation(mutation);
}

void HashClient::WriteCallback(tera::RowMutation* mutation) {
    MutexLock locker(&_s_mutex);
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

    _s_pending_num--;
    _s_pending_size -= mutation->Size();
    delete mutation;
}

bool HashClient::Get(const std::string& row_key,
                     std::string* value, ErrorCode* err) {
    return Get(row_key, "", "", value, err);
}

bool HashClient::Get(const std::string& row_key, const std::string& family,
                     const std::string& qualifier, std::string* value,
                     ErrorCode* err) {
    if (!_table) {
        LOG(ERROR) << "table not open: " << _table_name;
        SetErrorCode(err, ErrorCode::kSystem, "tail not open: " + _table_name);
        return false;
    }

    return _table->Get(_hash_method->HashKey(row_key), family, qualifier, value, err);
}

bool HashClient::Get(const std::string& row_key, void* obj,
                     void (*callback)(void*, const std::string& family, const std::string& qualifer,
                                      const std::string& value, const std::string& value_type)) {
    RowReader* row_reader = _table->NewRowReader(_hash_method->HashKey(row_key));
    row_reader->SetMaxVersions(1);
    row_reader->SetTimeOut(5000);
    _table->Get(row_reader);

    while(!row_reader->Done()) {
        std::string type;
        std::map<std::string, std::string>::iterator it = _field_types.find(row_reader->Family());
        if (it != _field_types.end()) {
            type = it->second;
        }
        callback(obj, row_reader->Family(), row_reader->Qualifier(), row_reader->Value(), type);
        row_reader->Next();
    }
    delete row_reader;

    return true;
}

bool HashClient::Delete(const std::string& row_key, ErrorCode* err) {
    RowMutation* mutation = _table->NewRowMutation(_hash_method->HashKey(row_key));
    mutation->DeleteRow();
    _table->ApplyMutation(mutation);
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

    _scan_stream = _table->Scan(scan_desc, err);
    return _scan_stream != NULL;
}

bool HashClient::Current(std::string* key, std::string* value,
                         ErrorCode* err) {
    if (_scan_stream->Done()) {
        SetErrorCode(err, ErrorCode::kSystem, "not more record");
        return false;
    }
    *key = _hash_method->Key(_scan_stream->RowName());
    *value = _scan_stream->Value();
    return true;
}

bool HashClient::Current(std::string* row_key, std::string* family, std::string* qualifier,
                         std::string* value, ErrorCode* err) {
    if (!_scan_stream) {
        SetErrorCode(err, ErrorCode::kSystem, "scan not ready");
        return false;
    }
    if (_scan_stream->Done()) {
        SetErrorCode(err, ErrorCode::kSystem, "not more record");
        return false;
    }

    *row_key = _hash_method->Key(_scan_stream->RowName());
    if (family) {
        *family = _scan_stream->Family();
    }
    if (qualifier) {
        *qualifier = _scan_stream->Qualifier();
    }
    if (value) {
        *value = _scan_stream->Value();
    }
    return true;
}

bool HashClient::Current(std::string* row_key, void* obj,
                         void (*callback)(void*, const std::string& family,
                                          const std::string& qualifier,
                                          const std::string& value,
                                          const std::string& type)) {
    if (_scan_stream->Done()) {
        return false;
    }
    std::string type;
    std::map<std::string, std::string>::iterator it = _field_types.find(_scan_stream->Family());
    if (it != _field_types.end()) {
        type = it->second;
    }
    *row_key = _hash_method->Key(_scan_stream->RowName());
    callback(obj, _scan_stream->Family(), _scan_stream->Qualifier(),
             _scan_stream->Value(), type);
    return true;
}

bool HashClient::Next(ErrorCode* err) {
    if (_scan_stream->Done()) {
        SetErrorCode(err, ErrorCode::kSystem, "not more record");
        return false;
    }
    _scan_stream->Next();

    if (_scan_stream->Done()) {
        return false;
    }
    return true;
}

const Table* HashClient::GetTable() {
    return _table;
}

bool HashClient::CreateTable(const std::map<std::string, std::string>& cf_list,
                             ErrorCode* err) {
    if (_client->IsTableExist(_table_name, err)) {
        LOG(ERROR) << "table '" << _table_name << "' already exist";
        return false;
    }

    TableDescriptor table_desc(_table_name);
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
    for (int32_t i = 0; i < _hash_method->GetBulkNum(); ++i) {
        delimiters.push_back(StringFormat("%08llu", i));
    }

    if (!_client->CreateTable(table_desc, delimiters, err)) {
        return false;
    }
    return true;
}

bool HashClient::DeleteTable(ErrorCode* err) {
    return true;
}

bool HashClient::GetColumnFamilyList(std::map<std::string, std::string>* cf_list,
                                     ErrorCode* err) {
    TableDescriptor* table_desc = _client->GetTableDescriptor(_table_name, err);
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
