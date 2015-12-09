// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "sdk/tera_c.h"

#include <iostream>
#include <map>

#include <assert.h>
#include <stdlib.h>
#include <string.h>

#include "common/mutex.h"
#include "sdk/tera.h"

extern "C" {

struct tera_client_t { Client* rep; };
struct tera_table_t { Table* rep; };
struct tera_row_mutation_t { RowMutation* rep; };

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

//           <RowMutation*, <tera_row_mutation_t*, user_callback> >
typedef std::map<int64_t, std::pair<int64_t, int64_t> > mutation_callback_map_t;
static mutation_callback_map_t g_mutation_callback_map;
static Mutex g_mutation_mutex;


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

tera_row_mutation_t* tera_row_mutation(tera_table_t* table, const char* row_key, uint64_t keylen) {
    tera_row_mutation_t* result = new tera_row_mutation_t;
    result->rep = table->rep->NewRowMutation(std::string(row_key, keylen));
    return result;
}

void tera_table_apply_mutation(tera_table_t* table, tera_row_mutation_t* mutation) {
    table->rep->ApplyMutation(mutation->rep);
}

bool tera_table_is_put_finished(tera_table_t* table) {
    return table->rep->IsPutFinished();
}

void tera_row_mutation_put(tera_row_mutation_t* mu, const char* cf,
                           const char* qu, uint64_t qulen,
                           const char* val, uint64_t vallen) {
    mu->rep->Put(cf, std::string(qu, qulen), std::string(val, vallen));
}

void tera_row_mutation_delete_column(tera_row_mutation_t* mu, const char* cf,
                                     const char* qu, uint64_t qulen) {
    mu->rep->DeleteColumn(cf, std::string(qu, qulen));
}

void tera_row_mutation_callback_stub(RowMutation* mu) {
    MutexLock locker(&g_mutation_mutex);
    int64_t sdk_mu = (int64_t)mu; // C++ sdk RowMutation*
    mutation_callback_map_t::iterator it = g_mutation_callback_map.find(sdk_mu);
    assert (it != g_mutation_callback_map.end());

    std::pair<int64_t, int64_t> apair = it->second;
    int64_t c_mu = apair.first; // C tera_row_mutation_t*
    MutationCallbackType callback = (MutationCallbackType)apair.second;

    g_mutation_mutex.Unlock();
    // users use C tera_row_mutation_t* to construct it's own object
    callback((void*)c_mu);
    g_mutation_mutex.Lock();

    g_mutation_callback_map.erase(it);
}

void tera_row_mutation_set_callback(tera_row_mutation_t* mu, MutationCallbackType callback) {
    MutexLock locker(&g_mutation_mutex);
    g_mutation_callback_map.insert( std::pair<int64_t, std::pair<int64_t, int64_t> >(
        (int64_t)mu->rep,
        std::pair<int64_t, int64_t>((int64_t)mu, (int64_t)callback))
    );
    mu->rep->SetCallBack(tera_row_mutation_callback_stub);
}

void tera_row_mutation_rowkey(tera_row_mutation_t* mu, char** val, uint64_t* vallen) {
    std::string row = mu->rep->RowKey();
    *val = CopyString(row);
    *vallen = row.size();
}

}  // end extern "C"
