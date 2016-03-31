// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef TERA_SDK_TERA_HASH_H_
#define TERA_SDK_TERA_HASH_H_

#include <map>

#include "tera.h"

#pragma GCC visibility push(default)

namespace tera {

class HashMethod {
public:
    HashMethod(int32_t bulk_num = 0)
        : _bulk_num(bulk_num) {}
    virtual ~HashMethod() {}

    virtual std::string HashKey(const std::string& key) = 0;
    virtual std::string Key(const std::string& hash_key) = 0;

    int32_t GetBulkNum() const {
        return _bulk_num;
    }

protected:
    int32_t _bulk_num;
};

struct HashScanDesc {
    std::string start_rowkey;
    std::string end_rowkey;
    std::vector<std::string> fields;
    std::string filter_expression;
    int64_t buffer_size;
    ScanDescriptor::ValueConverter converter;
    bool is_async;

    HashScanDesc() : buffer_size(-1), converter(NULL),
        is_async(true) {}
};

struct UserContext {
    void (*callback)(void*, bool);
    void* param;

    UserContext() : callback(NULL), param(NULL) {}
};

class HashClient {
public:
    HashClient(HashMethod* hash_method,
               const std::string& table_name = "sf_table",
               Client* client_impl = NULL);
    ~HashClient();


    bool GetColumnFamilyList(std::map<std::string, std::string>* cf_list,
                             ErrorCode* err = NULL);

    bool Put(const std::string& row_key, const std::string& value,
             ErrorCode* err = NULL);
    bool Put(const std::string& row_key, const std::string& family,
             const std::string& qualifier, const std::string& value,
             ErrorCode* err = NULL);

    bool Get(const std::string& row_key,
             std::string* value, ErrorCode* err = NULL);
    bool Get(const std::string& row_key, const std::string& family,
             const std::string& qualifier, std::string* value,
             ErrorCode* err = NULL);
    bool Get(const std::string& row_key, void* obj,
             void (*callback)(void*, const std::string& family, const std::string& qualifier,
                              const std::string& value, const std::string& value_type));

    bool Delete(const std::string& row_key, ErrorCode* err = NULL);

    bool Write(const std::string& row_key, const std::string& family,
               const std::string& qualifier, const std::string& value,
               ErrorCode* err = NULL);
    bool Write(const std::string& row_key, const std::string& family,
               const std::string& qualifier, const std::string& value,
               UserContext* context = NULL, ErrorCode* err = NULL);
    void Flush(uint64_t sleep_time = 10000);

    bool Seek(const HashScanDesc& desc,
              ErrorCode* err = NULL);
    bool Current(std::string* key, std::string* value,
                 ErrorCode* err = NULL);
    bool Current(std::string* row_key, std::string* family, std::string* qualifier,
                 std::string* value, ErrorCode* err = NULL);
    bool Current(std::string* row_key, void* obj,
                 void (*callback)(void*, const std::string& family, const std::string& qualifier,
                                  const std::string& value, const std::string& value_type));
    bool Next(ErrorCode* err = NULL);

    const Table* GetTable();

    bool CreateTable(const std::map<std::string, std::string>& cf_list,
                     ErrorCode* err = NULL);
    bool DeleteTable(ErrorCode* err = NULL);

    bool OpenTable(ErrorCode* err = NULL);

    static void WriteCallback(tera::RowMutation* mutation);

    RowMutation* NewMutation(const std::string& row_key);
    void ApplyMutation(UserContext* context, RowMutation* mutation,
                       int32_t value_size);

private:
    void SetErrorCode(ErrorCode* err, ErrorCode::ErrorCodeType value,
                      const std::string& err_reason);

private:
    Client* _client;
    Table* _table;
    ResultStream* _scan_stream;
    std::string _table_name;

    HashMethod* _hash_method;
    int32_t _bulk_num;
    bool _is_created_client;
    bool _is_created_hash_method;
    std::map<std::string, std::string> _field_types;
};

} // namespace tera

#pragma GCC visibility pop

#endif // TERA_SDK_TERA_HASH_H_
