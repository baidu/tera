// Copyright (c) 2015-2018, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Author: baorenyi@baidu.com
//
// BatchMutation only ensure all the 'mutation' in this batch will sent to same
// 'tabletserver' and same 'tablet' through one rpc request. 
// 

#ifndef  TERA_BATCH_MUTATION_H_
#define  TERA_BATCH_MUTATION_H_

#include <stdint.h>
#include <string>

#include "error_code.h"

#pragma GCC visibility push(default)
namespace tera {

class Table;

class BatchMutation {
public:
    // Set the database entry for "key" to "value". The database should be
    // created as a key-value storage.
    // "ttl"(time-to-live) is optional, "value" will expire after "ttl"
    // second. If ttl <= 0, "value" never expire.
    virtual void Put(const std::string& row_key,
                     const std::string& value,
                     int32_t ttl = -1) = 0;

    // Set the database entry for the specified column to "value".
    // "timestamp"(us) is optional, current time by default.
    virtual void Put(const std::string& row_key,
                     const std::string& family,
                     const std::string& qualifier,
                     const std::string& value,
                     int64_t timestamp = -1) = 0;

    // Put an integer into a cell. This cell can be used as a counter.
    virtual void Put(const std::string& row_key,
                     const std::string& family,
                     const std::string& qualifier,
                     const int64_t value,
                     int64_t timestamp = -1) = 0;

    // Add "delta" to a specified cell. "delta" can be negative.
    virtual void Add(const std::string& row_key,
                     const std::string& family,
                     const std::string& qualifier,
                     const int64_t delta) = 0;

    // "value" will take effect when specified cell does not exist.
    // Otherwise, "value" will be discarded.
    virtual void PutIfAbsent(const std::string& row_key,
                             const std::string& family,
                             const std::string& qualifier,
                             const std::string& value) = 0;

    // Append "value" to a specified cell.
    virtual void Append(const std::string& row_key,
                        const std::string& family,
                        const std::string& qualifier,
                        const std::string& value) = 0;

    // Delete updates of a specified row/columnfamily/qualifier before "timestamp"(us).
    // Delete all versions by default.
    // "timestamp" will be ignored in key-value mode.
    virtual void DeleteRow(const std::string& row_key,
                           int64_t timestamp = -1) = 0;
    virtual void DeleteFamily(const std::string& row_key,
                              const std::string& family,
                              int64_t timestamp = -1) = 0;
    virtual void DeleteColumns(const std::string& row_key,
                               const std::string& family,
                               const std::string& qualifier,
                               int64_t timestamp = -1) = 0;
    // Delete the cell specified by "family"&"qualifier"&"timestamp".
    virtual void DeleteColumn(const std::string& row_key,
                              const std::string& family,
                              const std::string& qualifier,
                              int64_t timestamp = -1) = 0;

    // The status of this batch mutation. Returns kOK on success and a non-OK
    // status on error.
    virtual const ErrorCode& GetError() = 0;

    // Users are allowed to register callback/context a two-tuples that
    // will be invoked when this batch mutation is finished.
    typedef void (*Callback)(BatchMutation* param);
    virtual void SetCallBack(Callback callback) = 0;
    virtual Callback GetCallBack() = 0;
    virtual void SetContext(void* context) = 0;
    virtual void* GetContext() = 0;

    // Set/get timeout(ms).
    virtual void SetTimeOut(int64_t timeout_ms) = 0;
    virtual int64_t TimeOut() = 0;

    // Get the mutation count of one row in this batch mutaion.
    virtual uint32_t MutationNum(const std::string& row_key) = 0;
    // Get total size of all mutations, including size of rowkey, columnfamily,
    // qualifier, value and timestamp.
    virtual uint32_t Size() = 0;

    virtual bool IsAsync() = 0;

    // reset all status and context of this BatchMutation to init
    virtual void Reset() = 0;

    BatchMutation() {};
    virtual ~BatchMutation() {};

private:
    BatchMutation(const BatchMutation&);
    void operator=(const BatchMutation&);
};
} // namespace tera
#pragma GCC visibility pop

#endif  // TERA_BATCH_MUTATION_H_
