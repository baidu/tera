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

#ifdef __cplusplus
}  /* end extern "C" */
#endif

#endif
