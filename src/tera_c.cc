// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "sdk/tera_c.h"

#include <assert.h>
#include <stdlib.h>
#include <string.h>

#include "sdk/tera.h"

using tera::Client;
using tera::Table;
using tera::ErrorCode;
using tera::RowMutation;

extern "C" {

struct tera_client_t { Client* rep; };
struct tera_table_t { Table* rep; };

static bool SaveError(char** errptr, const ErrorCode& s) {
  assert(errptr != NULL);
  if (s.GetType() == ErrorCode::kOK) {
    return false;
  } else if (*errptr == NULL) {
    *errptr = strdup(s.GetReason().c_str());
  } else {
    free(*errptr);
    *errptr = strdup(s.GetReason().c_str());
  }
  return true;
}

static char* CopyString(const std::string& str) {
  char* result = reinterpret_cast<char*>(malloc(sizeof(char) * str.size()));
  memcpy(result, str.data(), sizeof(char) * str.size());
  return result;
}

tera_client_t* tera_client_open(const char* conf_path, const char* log_prefix, char** errptr) {
    ErrorCode err;
    tera_client_t* result = new tera_client_t;
    result->rep = Client::NewClient(conf_path, log_prefix, &err);
    if (SaveError(errptr, err)) {
        return NULL;
    }
    return result;
}

tera_table_t* tera_table_open(tera_client_t* client, const char* table_name, char** errptr) {
    ErrorCode err;
    tera_table_t* result = new tera_table_t;
    result->rep = client->rep->OpenTable(table_name, &err);
    if (SaveError(errptr, err)) {
        return NULL;
    }
    return result;
}

bool tera_table_get(tera_table_t* table,
                    const char* row_key, uint64_t keylen,
                    const char* family, const char* qualifier,
                    uint64_t qulen, char** value, uint64_t* vallen,
                    char** errptr, uint64_t snapshot_id) {
    ErrorCode err;
    std::string key_str(row_key, keylen);
    std::string qu_str(qualifier, qulen);
    std::string value_str;
    bool result = table->rep->Get(key_str, family, qu_str, &value_str, &err, snapshot_id);
    if (result) {
        *value = CopyString(value_str);
        *vallen = value_str.size();
    }
    if (SaveError(errptr, err)) {
        *vallen = 0;
    }
    return result;
}

bool tera_table_put(tera_table_t* table,
                    const char* row_key, uint64_t keylen,
                    const char* family, const char* qualifier,
                    uint64_t qulen, const char* value, uint64_t vallen,
                    char** errptr) {
    ErrorCode err;
    std::string key_str(row_key, keylen);
    std::string qu_str(qualifier, qulen);
    std::string value_str(value, vallen);
    bool result = table->rep->Put(key_str, family, qu_str, value_str, &err);
    if (SaveError(errptr, err)) {
        return false;
    }
    return result;
}

void tera_table_delete(tera_table_t* table, const char* row_key, uint64_t keylen,
                       const char* family, const char* qualifier, uint64_t qulen) {
    ErrorCode err;
    std::string key_str(row_key, keylen);
    std::string qu_str(qualifier, qulen);
    RowMutation* mutation = table->rep->NewRowMutation(key_str);
    mutation->DeleteColumn(family, qu_str);
    table->rep->ApplyMutation(mutation);
}

}  // end extern "C"
