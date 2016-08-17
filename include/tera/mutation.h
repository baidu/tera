// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef  TERA_MUTATION_H_
#define  TERA_MUTATION_H_

#pragma GCC visibility push(default)
namespace tera {

class Table;
class Transaction;
class RowLock;

class RowMutation {
public:
    virtual const std::string& RowKey() = 0;

    // Put/delete a value in kv mode
    // <ttl> is optional, default is never expired.
    virtual void Put(const std::string& value, int32_t ttl = -1) = 0;

    // Put a cell in table mode
    // <timestamp> is optional, default is kLatestTimestamp
    virtual void Put(const std::string& family, const std::string& qualifier,
                     const std::string& value, int64_t timestamp = -1) = 0;
    virtual void Put(const std::string& family, const std::string& qualifier,
                     const int64_t value, int64_t timestamp = -1) = 0;
    virtual void Add(const std::string& family, const std::string& qualifier,
                     const int64_t delta) = 0;
    virtual void PutIfAbsent(const std::string& family, const std::string& qualifier,
                             const std::string& value) = 0;
    virtual void Append(const std::string& family, const std::string& qualifier,
                        const std::string& value) = 0;

    // Delete op will take effect until <timestamp>
    // Default is from current timestamp
    // Kv mode will discard <timestamp>
    virtual void DeleteRow(int64_t timestamp = -1) = 0;
    virtual void DeleteFamily(const std::string& family, int64_t timestamp = -1) = 0;
    virtual void DeleteColumns(const std::string& family, const std::string& qualifier,
                               int64_t timestamp = -1) = 0;
    virtual void DeleteColumn(const std::string& family, const std::string& qualifier,
                              int64_t timestamp) = 0;

    virtual const ErrorCode& GetError() = 0;

    virtual void SetTimeOut(int64_t timeout_ms) = 0;
    virtual int64_t TimeOut() = 0;

    // If callback is set, mutation will be put async
    typedef void (*Callback)(RowMutation* param);
    virtual void SetCallBack(Callback callback) = 0;
    virtual Callback GetCallBack() = 0;
    virtual bool IsAsync() = 0;

    // Pass context to callback
    virtual void SetContext(void* context) = 0;
    virtual void* GetContext() = 0;

    enum Type {
        kPut,
        kDeleteColumn,
        kDeleteColumns,
        kDeleteFamily,
        kDeleteRow,
        kAdd,
        kPutIfAbsent,
        kAppend,
        kAddInt64
    };
    struct Mutation {
        Type type;
        std::string family;
        std::string qualifier;
        std::string value;
        int64_t timestamp;
        int32_t ttl;
    };

    virtual uint32_t MutationNum() = 0;
    virtual uint32_t Size() = 0;
    virtual uint32_t RetryTimes() = 0;
    virtual const RowMutation::Mutation& GetMutation(uint32_t index) = 0;

    // Pre-release
    // Return transaction if exist
    virtual Transaction* GetTransaction() = 0;

    // Developing
    virtual void Reset(const std::string& row_key) = 0;
    virtual bool IsFinished() const = 0;

    // Do-not-use
    RowMutation() {};
    virtual ~RowMutation() {};
    virtual void Put(const std::string& family, const std::string& qualifier,
                     const std::string& value, int32_t ttl) = 0;
    virtual void AddInt64(const std::string& family, const std::string& qualifier,
                     const int64_t delta) = 0;
    virtual void Put(int64_t timestamp, const std::string& value) = 0;
    virtual void Put(const int64_t value) = 0;
    virtual void Put(const std::string& family, const std::string& qualifier,
                     int64_t timestamp, const std::string& value, int32_t ttl) = 0;
    virtual void Put(const std::string& family, const std::string& qualifier,
                     int64_t timestamp, const std::string& value) = 0;
    virtual void DeleteColumn(const std::string& family,
                              const std::string& qualifier) = 0;
    virtual void SetLock(RowLock* rowlock) = 0;

private:
    RowMutation(const RowMutation&);
    void operator=(const RowMutation&);
};

class RowLock {};
} // namespace tera
#pragma GCC visibility pop

#endif  // TERA_MUTATION_H_
