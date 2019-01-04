// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef  TERA_TABLE_H_
#define  TERA_TABLE_H_

#include <stdint.h>
#include <string>
#include <vector>
#include <functional>

#include "error_code.h"
#include "batch_mutation.h"
#include "mutation.h"
#include "reader.h"
#include "scan.h"
#include "table_descriptor.h"
#include "hash.h"

#pragma GCC visibility push(default)
namespace tera {

struct TableInfo {
    TableDescriptor* table_desc;
    std::string status;
};

struct TabletInfo {
    std::string table_name;
    std::string path;
    std::string server_addr;
    std::string start_key;
    std::string end_key;
    int64_t data_size;
    std::string status;
};

class BatchMutation;
class RowMutation;
class RowReader;
class Transaction;
class Table {
public:
    // Return the name of table.
    virtual const std::string GetName() = 0;

    // Return a row mutation handle. User should delete it when it is no longer
    // needed.
    virtual RowMutation* NewRowMutation(const std::string& row_key) = 0;

    // Return a batch mutation handle. User should delete it when it is no longer
    // needed.
    virtual BatchMutation* NewBatchMutation() = 0;
    // Apply the specified row_mutation(s) to the database. Support batch put.
    // Users can set a callback in "row_mutation" to activate async put.
    // Use RowMutation::GetError() to check return code.
    virtual void Put(RowMutation* row_mutation) = 0;
    virtual void Put(const std::vector<RowMutation*>& row_mutations) = 0;
    // Check if all put operations are finished.
    virtual bool IsPutFinished() = 0;
    // Easy synchronised interface. Returns true on success.
    virtual bool Put(const std::string& row_key, const std::string& family,
                     const std::string& qualifier, const std::string& value,
                     ErrorCode* err) = 0;
    virtual bool Put(const std::string& row_key, const std::string& family,
                     const std::string& qualifier, const int64_t value,
                     ErrorCode* err) = 0;
    virtual bool Add(const std::string& row_key, const std::string& family,
                     const std::string& qualifier, int64_t delta,
                     ErrorCode* err) = 0;
    virtual bool PutIfAbsent(const std::string& row_key, const std::string& family,
                             const std::string& qualifier, const std::string& value,
                             ErrorCode* err) = 0;
    virtual bool Append(const std::string& row_key, const std::string& family,
                        const std::string& qualifier, const std::string& value,
                        ErrorCode* err) = 0;

    // Return a row reader handle. User should delete it when it is no longer
    // needed.
    virtual RowReader* NewRowReader(const std::string& row_key) = 0;
    // Apply the specified reader to the database. Support batch get.
    // Users can set a callback in "row_reader" to activate async get.
    // Use RowReader::GetError() to check return code.
    virtual void Get(RowReader* row_reader) = 0;
    virtual void Get(const std::vector<RowReader*>& row_readers) = 0;
    // Check if all get operations are finished.
    virtual bool IsGetFinished() = 0;
    // Easy synchronized interface. Returns true on success.
    virtual bool Get(const std::string& row_key, const std::string& family,
                     const std::string& qualifier, std::string* value,
                     ErrorCode* err) = 0;
    virtual bool Get(const std::string& row_key, const std::string& family,
                     const std::string& qualifier, int64_t* value,
                     ErrorCode* err) = 0;

    // Return a result stream described by "desc".
    virtual ResultStream* Scan(const ScanDescriptor& desc, ErrorCode* err) = 0;

    // EXPERIMENTAL
    // Return a row transaction handle.
    virtual Transaction* StartRowTransaction(const std::string& row_key) = 0;
    // Commit a row transaction.
    virtual void CommitRowTransaction(Transaction* transaction) = 0;

    // DEVELOPING
    virtual void SetMaxMutationPendingNum(uint64_t max_pending_num) = 0;
    virtual void SetMaxReaderPendingNum(uint64_t max_pending_num) = 0;
    virtual bool Get(const std::string& row_key, const std::string& family,
                     const std::string& qualifier, std::string* value,
                     uint64_t snapshot_id, ErrorCode* err) = 0;
    virtual bool Get(const std::string& row_key, const std::string& family,
                     const std::string& qualifier, int64_t* value,
                     uint64_t snapshot_id, ErrorCode* err) = 0;
    virtual bool GetDescriptor(TableDescriptor* desc, ErrorCode* err) = 0;

    // DEPRECATED
    // Use Put() instead.
    virtual void ApplyMutation(RowMutation* row_mu) = 0;
    virtual void ApplyMutation(const std::vector<RowMutation*>& row_mu_list) = 0;

    virtual bool Put(const std::string& row_key, const std::string& family,
                     const std::string& qualifier, const std::string& value,
                     int32_t ttl, ErrorCode* err) = 0;
    virtual bool Put(const std::string& row_key, const std::string& family,
                     const std::string& qualifier, const std::string& value,
                     int64_t timestamp, int32_t ttl, ErrorCode* err) = 0;
    virtual bool AddInt64(const std::string& row_key, const std::string& family,
                     const std::string& qualifier, int64_t delta,
                     ErrorCode* err) = 0;
    virtual bool GetTabletLocation(std::vector<TabletInfo>* tablets, ErrorCode* err) = 0;
    virtual bool GetStartEndKeys(std::string* start_key, std::string* end_key,
                                 ErrorCode* err) = 0;
    virtual bool LockRow(const std::string& rowkey, RowLock* lock, ErrorCode* err) = 0;
    virtual void SetWriteTimeout(int64_t timeout_ms) = 0;
    virtual void SetReadTimeout(int64_t timeout_ms) = 0;
    virtual int64_t IncrementColumnValue(const std::string& row, const std::string& family,
                                         const std::string& qualifier, int64_t amount,
                                         ErrorCode* err) = 0;
    virtual bool CheckAndApply(const std::string& rowkey, const std::string& cf_c,
                               const std::string& value, const RowMutation& row_mu,
                               ErrorCode* err) = 0;
    virtual bool Flush() = 0;
    virtual bool Get(const std::string& row_key, const std::string& family,
                     const std::string& qualifier, std::string* value,
                     ErrorCode* err, uint64_t snapshot_id) = 0;
    virtual bool Get(const std::string& row_key, const std::string& family,
                     const std::string& qualifier, int64_t* value,
                     ErrorCode* err, uint64_t snapshot_id) = 0;
    virtual bool IsHashTable() = 0;
    virtual std::function<std::string(const std::string&)> GetHashMethod() = 0;
    virtual bool GetTablet(const std::string& row_key, std::string* tablet) = 0;

    // For BatchMutation
    virtual void ApplyMutation(BatchMutation* batch_mutation) = 0;

    Table() {}
    virtual ~Table() {}

private:
    Table(const Table&);
    void operator=(const Table&);
};

} // namespace tera
#pragma GCC visibility pop

#endif  // TERA_TABLE_H_
