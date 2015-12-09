// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef TEAR_C_H_
#define TEAR_C_H_

#include "tera.h"

using tera::Client;
using tera::Table;
using tera::ErrorCode;
using tera::RowMutation;


#ifdef __cplusplus
extern "C" {
#endif

typedef struct tera_client_t tera_client_t;
typedef struct tera_table_t tera_table_t;
typedef struct tera_row_mutation_t tera_row_mutation_t;
typedef struct tera_scan_descriptor_t tera_scan_descriptor_t;
typedef struct tera_result_stream_t tera_result_stream_t;

typedef void (*MutationCallbackType)(void* param);
extern tera_client_t* tera_client_open(const char* conf_path, const char* log_prefix, char** err);

extern tera_table_t* tera_table_open(tera_client_t* client, const char* table_name, char** err);

extern bool tera_table_get(tera_table_t* table,
                           const char* row_key, uint64_t keylen,
                           const char* family, const char* qualifier,
                           uint64_t qulen, char** value, uint64_t* vallen,
                           char** errptr, uint64_t snapshot_id);

extern bool tera_table_put(tera_table_t* table,
                           const char* row_key, uint64_t keylen,
                           const char* family, const char* qualifier,
                           uint64_t qulen, const char* value, uint64_t vallen,
                           char** errptr);

void tera_table_delete(tera_table_t* table, const char* row_key, uint64_t keylen,
                       const char* family, const char* qualifier, uint64_t qulen);

bool tera_table_is_put_finished(tera_table_t* table);

tera_row_mutation_t* tera_row_mutation(tera_table_t* table, const char* row_key, uint64_t keylen);
void tera_table_apply_mutation(tera_table_t* table, tera_row_mutation_t* mutation);
void tera_row_mutation_put(tera_row_mutation_t* mu, const char* cf,
                           const char* qu, uint64_t qulen,
                           const char* val, uint64_t vallen);
void tera_row_mutation_delete_column(tera_row_mutation_t* mu, const char* cf,
                                     const char* qu, uint64_t qulen);
void tera_row_mutation_set_callback(tera_row_mutation_t* mu, MutationCallbackType callback);
void tera_row_mutation_rowkey(tera_row_mutation_t* mu, char** val, uint64_t* vallen);

tera_result_stream_t* tera_table_scan(tera_table_t* table,
                                      const tera_scan_descriptor_t* desc,
                                      char** errptr);

// scan descriptor
tera_scan_descriptor_t* tera_scan_descriptor(const char* start_key, uint64_t keylen);
void tera_scan_descriptor_add_column(tera_scan_descriptor_t* desc, const char* cf,
                                     const char* qualifier, uint64_t qulen);
void tera_scan_descriptor_add_column_family(tera_scan_descriptor_t* desc, const char* cf);
bool tera_scan_descriptor_is_async(tera_scan_descriptor_t* desc);
void tera_scan_descriptor_set_buffer_size(tera_scan_descriptor_t* desc, int64_t size);
void tera_scan_descriptor_set_end(tera_scan_descriptor_t* desc, const char* end_key, uint64_t keylen);
void tera_scan_descriptor_set_filter_string(tera_scan_descriptor_t* desc, const char* filter_string);
void tera_scan_descriptor_set_pack_interval(tera_scan_descriptor_t* desc, int64_t interval);
void tera_scan_descriptor_set_is_async(tera_scan_descriptor_t* desc, bool is_async);
void tera_scan_descriptor_set_max_versions(tera_scan_descriptor_t* desc, int32_t versions);
void tera_scan_descriptor_set_snapshot(tera_scan_descriptor_t* desc, uint64_t snapshot_id);
void tera_scan_descriptor_set_time_range(tera_scan_descriptor_t* desc, int64_t ts_start, int64_t ts_end);

// scan result stream
bool tera_result_stream_done(tera_result_stream_t* stream, char** errptr);
bool tera_result_stream_look_up(tera_result_stream_t* stream, const char* str, uint64_t strlen);
int64_t tera_result_stream_timestamp(tera_result_stream_t* stream);
void tera_result_stream_column_name(tera_result_stream_t* stream, char** str, uint64_t* strlen);
void tera_result_stream_family(tera_result_stream_t* stream, char** str, uint64_t* strlen);
void tera_result_stream_next(tera_result_stream_t* stream);
void tera_result_stream_qualifier(tera_result_stream_t* stream, char** str, uint64_t* strlen);
void tera_result_stream_row_name(tera_result_stream_t* stream, char** str, uint64_t* strlen);
void tera_result_stream_value(tera_result_stream_t* stream, char** str, uint64_t* strlen);
void tera_result_stream_value(tera_result_stream_t* stream, char** str, uint64_t* strlen);

#ifdef __cplusplus
}  /* end extern "C" */
#endif

#endif
