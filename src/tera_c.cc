// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "sdk/tera_c.h"

#include <assert.h>
#include <stdlib.h>
#include <string.h>

#include <iostream>
#include <map>

#include "common/mutex.h"

#include "sdk/tera.h"

using tera::Client;
using tera::ErrorCode;
using tera::ResultStream;
using tera::RowMutation;
using tera::RowReader;
using tera::ScanDescriptor;
using tera::Table;

extern "C" {

struct tera_client_t          { Client*         rep; };
struct tera_result_stream_t   { ResultStream*   rep; };
struct tera_row_mutation_t    { RowMutation*    rep; };
struct tera_row_reader_t      { RowReader*      rep; };
struct tera_scan_descriptor_t { ScanDescriptor* rep; };
struct tera_table_t           { Table*          rep; };

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
typedef std::map<void*, std::pair<void*, void*> > mutation_callback_map_t;
static mutation_callback_map_t g_mutation_callback_map;
static Mutex g_mutation_mutex;

//           <RowReader*,   <tera_row_reader_t*, user_callback> >
typedef std::map<void*, std::pair<void*, void*> > reader_callback_map_t;
static reader_callback_map_t g_reader_callback_map;
static Mutex g_reader_mutex;

tera_client_t* tera_client_open(const char* conf_path, const char* log_prefix, char** errptr) {
    ErrorCode err;
    tera_client_t* result = new tera_client_t;
    result->rep = Client::NewClient(conf_path, log_prefix, &err);
    if (SaveError(errptr, err) || !result->rep) {
        delete result;
        return NULL;
    }
    return result;
}

tera_table_t* tera_table_open(tera_client_t* client, const char* table_name, char** errptr) {
    ErrorCode err;
    tera_table_t* result = new tera_table_t;
    result->rep = client->rep->OpenTable(table_name, &err);
    if (SaveError(errptr, err) || !result->rep) {
        delete result;
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

bool tera_table_getint64(tera_table_t* table,
                         const char* row_key, uint64_t keylen,
                         const char* family, const char* qualifier,
                         uint64_t qulen, int64_t* value,
                         char** errptr, uint64_t snapshot_id) {
    ErrorCode err;
    std::string key_str(row_key, keylen);
    std::string qu_str(qualifier, qulen);
    bool result = table->rep->Get(key_str, family, qu_str, value, &err, snapshot_id);
    if (SaveError(errptr, err)) {
        return false;
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

bool tera_table_putint64(tera_table_t* table,
                         const char* row_key, uint64_t keylen,
                         const char* family, const char* qualifier,
                         uint64_t qulen, int64_t value,
                         char** errptr) {
    ErrorCode err;
    std::string key_str(row_key, keylen);
    std::string qu_str(qualifier, qulen);
    bool result = table->rep->Put(key_str, family, qu_str, value, &err);
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

int64_t tera_row_mutation_get_status_code(tera_row_mutation_t* mu) {
    return mu->rep->GetError().GetType();
}

void tera_row_mutation_destroy(tera_row_mutation_t* mu) {
    delete mu->rep;
    mu->rep = NULL;
}

void tera_table_apply_mutation(tera_table_t* table, tera_row_mutation_t* mutation) {
    table->rep->ApplyMutation(mutation->rep);
}

tera_row_reader_t* tera_row_reader(tera_table_t* table, const char* row_key, uint64_t keylen) {
    tera_row_reader_t* result = new tera_row_reader_t;
    result->rep = table->rep->NewRowReader(std::string(row_key, keylen));
    return result;
}

void tera_row_reader_rowkey(tera_row_reader_t* reader, char** str, uint64_t* strlen) {
    std::string val = reader->rep->RowName();
    *str = CopyString(val);
    *strlen = val.size();
}

void tera_row_reader_add_column_family(tera_row_reader_t* reader, const char* family) {
    reader->rep->AddColumnFamily(family);
}

bool tera_row_reader_done(tera_row_reader_t* reader) {
    return reader->rep->Done();
}

void tera_row_reader_next(tera_row_reader_t* reader) {
    reader->rep->Next();
}

void tera_row_reader_value(tera_row_reader_t* reader, char** str, uint64_t* strlen) {
    std::string val = reader->rep->Value();
    *str = CopyString(val);
    *strlen = val.size();
}

void tera_row_reader_callback_stub(RowReader* reader) {
    MutexLock locker(&g_reader_mutex);
    void* sdk_reader = reader; // C++ sdk RowReader*
    reader_callback_map_t::iterator it = g_reader_callback_map.find(sdk_reader);
    assert (it != g_reader_callback_map.end());

    std::pair<void*, void*> apair = it->second;
    void* c_reader = apair.first; // C tera_row_reader_t*
    ReaderCallbackType callback = (ReaderCallbackType)apair.second;

    g_reader_mutex.Unlock();
    // users use C tera_row_reader_t* to construct it's own object
    callback(c_reader);
    g_reader_mutex.Lock();

    g_reader_callback_map.erase(it);
}

void tera_row_reader_set_callback(tera_row_reader_t* reader, ReaderCallbackType callback) {
    MutexLock locker(&g_reader_mutex);
    g_reader_callback_map.insert( std::pair<void*, std::pair<void*, void*> >(
        reader->rep,
        std::pair<void*, void*>(reader, (void*)callback))
    );
    reader->rep->SetCallBack(tera_row_reader_callback_stub);
}

void tera_row_reader_add_column(tera_row_reader_t* reader, const char* cf, const char* qu, uint64_t len) {
    reader->rep->AddColumn(cf, std::string(qu, len));
}

void tera_row_reader_set_timestamp(tera_row_reader_t* reader, int64_t ts) {
    reader->rep->SetTimestamp(ts);
}

void tera_row_reader_set_time_range(tera_row_reader_t* reader, int64_t start, int64_t end) {
    reader->rep->SetTimeRange(start, end);
}

void tera_row_reader_set_snapshot(tera_row_reader_t* reader, uint64_t snapshot) {
    reader->rep->SetSnapshot(snapshot);
}

void tera_row_reader_set_max_versions(tera_row_reader_t* reader, uint32_t maxversions) {
    reader->rep->SetMaxVersions(maxversions);
}

void tera_row_reader_set_timeout(tera_row_reader_t* reader, int64_t timeout) {
    reader->rep->SetTimeOut(timeout);
}

void tera_row_reader_family(tera_row_reader_t* reader, char** str, uint64_t* strlen) {
    std::string val = reader->rep->Family();
    *str = CopyString(val);
    *strlen = val.size();
}

void tera_row_reader_qualifier(tera_row_reader_t* reader, char** str, uint64_t* strlen) {
    std::string val = reader->rep->Qualifier();
    *str = CopyString(val);
    *strlen = val.size();
}

int64_t tera_row_reader_timestamp(tera_row_reader_t* reader) {
    return reader->rep->Timestamp();
}

int64_t tera_row_reader_get_status_code(tera_row_reader_t* reader) {
    return reader->rep->GetError().GetType();
}

void tera_row_reader_destroy(tera_row_reader_t* reader) {
    delete reader->rep;
    reader->rep = NULL;
}

void tera_table_apply_reader(tera_table_t* table, tera_row_reader_t* reader) {
    table->rep->Get(reader->rep);
}

bool tera_table_is_put_finished(tera_table_t* table) {
    return table->rep->IsPutFinished();
}

bool tera_table_is_get_finished(tera_table_t* table) {
    return table->rep->IsGetFinished();
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
    void* sdk_mu = mu; // C++ sdk RowMutation*
    mutation_callback_map_t::iterator it = g_mutation_callback_map.find(sdk_mu);
    assert (it != g_mutation_callback_map.end());

    std::pair<void*, void*> apair = it->second;
    void* c_mu = apair.first; // C tera_row_mutation_t*
    MutationCallbackType callback = (MutationCallbackType)apair.second;

    g_mutation_mutex.Unlock();
    // users use C tera_row_mutation_t* to construct it's own object
    callback(c_mu);
    g_mutation_mutex.Lock();

    g_mutation_callback_map.erase(it);
}

void tera_row_mutation_set_callback(tera_row_mutation_t* mu, MutationCallbackType callback) {
    MutexLock locker(&g_mutation_mutex);
    g_mutation_callback_map.insert( std::pair<void*, std::pair<void*, void*> >(
        mu->rep,
        std::pair<void*, void*>(mu, (void*)callback))
    );
    mu->rep->SetCallBack(tera_row_mutation_callback_stub);
}

void tera_row_mutation_rowkey(tera_row_mutation_t* mu, char** val, uint64_t* vallen) {
    std::string row = mu->rep->RowKey();
    *val = CopyString(row);
    *vallen = row.size();
}

tera_result_stream_t* tera_table_scan(tera_table_t* table,
                                      const tera_scan_descriptor_t* desc,
                                      char** errptr) {
    ErrorCode err;
    tera_result_stream_t* result = new tera_result_stream_t;
    result->rep = table->rep->Scan(*desc->rep, &err);
    if (SaveError(errptr, err)) {
        return NULL;
    }
    return result;
}

tera_scan_descriptor_t* tera_scan_descriptor(const char* start_key, uint64_t keylen) {
    std::string key(start_key, keylen);
    tera_scan_descriptor_t* result = new tera_scan_descriptor_t;
    result->rep = new ScanDescriptor(key);
    return result;
}

void tera_scan_descriptor_add_column(tera_scan_descriptor_t* desc, const char* cf,
                                     const char* qualifier, uint64_t qulen) {
    std::string qu(qualifier, qulen);
    desc->rep->AddColumn(cf, qu);
}

void tera_scan_descriptor_add_column_family(tera_scan_descriptor_t* desc, const char* cf) {
    desc->rep->AddColumnFamily(cf);
}

bool tera_scan_descriptor_is_async(tera_scan_descriptor_t* desc) {
    return desc->rep->IsAsync();
}

void tera_scan_descriptor_set_is_async(tera_scan_descriptor_t* desc, bool is_async) {
    desc->rep->SetAsync(is_async);
}

void tera_scan_descriptor_set_buffer_size(tera_scan_descriptor_t* desc, int64_t size) {
    desc->rep->SetBufferSize(size);
}

void tera_scan_descriptor_set_end(tera_scan_descriptor_t* desc, const char* end_key, uint64_t keylen) {
    std::string key(end_key, keylen);
    desc->rep->SetEnd(key);
}

void tera_scan_descriptor_set_pack_interval(tera_scan_descriptor_t* desc, int64_t interval) {
    desc->rep->SetPackInterval(interval);
}

void tera_scan_descriptor_set_max_versions(tera_scan_descriptor_t* desc, int32_t versions) {
    desc->rep->SetMaxVersions(versions);
}

void tera_scan_descriptor_set_snapshot(tera_scan_descriptor_t* desc, uint64_t snapshot_id) {
    desc->rep->SetSnapshot(snapshot_id);
}

// NOTE: arguments order is different from C++ sdk(tera.h)
void tera_scan_descriptor_set_time_range(tera_scan_descriptor_t* desc, int64_t ts_start, int64_t ts_end) {
    desc->rep->SetTimeRange(ts_end, ts_start);
}

bool tera_scan_descriptor_set_filter(tera_scan_descriptor_t* desc, char* filter_str) {
    return desc->rep->SetFilter(filter_str);
}

bool tera_result_stream_done(tera_result_stream_t* stream, char** errptr) {
    ErrorCode err;
    if (!stream->rep->Done(&err)) {
        SaveError(errptr, err);
        return false;
    }
    return true;
}

int64_t tera_result_stream_timestamp(tera_result_stream_t* stream) {
    int64_t ts = stream->rep->Timestamp();
    //fprintf(stderr, "%lld\n", ts);
    return ts;
}

void tera_result_stream_qualifier(tera_result_stream_t* stream, char** str, uint64_t* strlen) {
    std::string val = stream->rep->Qualifier();
    *str = CopyString(val);
    *strlen = val.size();
}

void tera_result_stream_column_name(tera_result_stream_t* stream, char** str, uint64_t* strlen) {
    std::string val = stream->rep->ColumnName();
    *str = CopyString(val);
    *strlen = val.size();
}

void tera_result_stream_family(tera_result_stream_t* stream, char** str, uint64_t* strlen) {
    std::string val = stream->rep->Family();
    *str = CopyString(val);
    *strlen = val.size();
}

void tera_result_stream_next(tera_result_stream_t* stream) {
    stream->rep->Next();
}

void tera_result_stream_row_name(tera_result_stream_t* stream, char** str, uint64_t* strlen) {
    std::string val = stream->rep->RowName();
    *str = CopyString(val);
    *strlen = val.size();
}

void tera_result_stream_value(tera_result_stream_t* stream, char** str, uint64_t* strlen) {
    std::string val = stream->rep->Value();
    *str = CopyString(val);
    *strlen = val.size();
}

int64_t tera_result_stream_value_int64(tera_result_stream_t* stream) {
    return stream->rep->ValueInt64();
}

}  // end extern "C"
