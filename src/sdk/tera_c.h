#ifndef TEAR_C_H_
#define TEAR_C_H_

#include "tera.h"

using tera::ErrorCode;

#ifdef __cplusplus
extern "C" {
#endif

typedef struct tera_client_t tera_client_t;
typedef struct tera_table_t tera_table_t;

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

#ifdef __cplusplus
}  /* end extern "C" */
#endif

#endif
