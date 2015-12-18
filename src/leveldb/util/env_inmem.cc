// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <errno.h>
#include <stdio.h>
#include <sys/time.h>
#include <time.h>
#include <algorithm>
#include <set>
#include <iostream>
#include <sstream>
#include "leveldb/env.h"
#include "leveldb/status.h"
#include "leveldb/env_dfs.h"
#include "leveldb/table_utils.h"
#include "util/mutexlock.h"
#include "helpers/memenv/memenv.h"
#include "../utils/counter.h"

#include "leveldb/env_inmem.h"


namespace leveldb {

// Log error message
static Status IOError(const std::string& context, int err_number)
{
    return Status::IOError(context, strerror(err_number));
}

class InMemorySequentialFile: public SequentialFile {
private:
    SequentialFile* dfs_file_;
    SequentialFile* mem_file_;
public:
    InMemorySequentialFile(Env* mem_env, Env* dfs_env, const std::string& fname)
        :dfs_file_(NULL), mem_file_(NULL) {
        dfs_env->NewSequentialFile(fname, &dfs_file_);
    }

    virtual ~InMemorySequentialFile() {
        delete dfs_file_;
        delete mem_file_;
    }

    virtual Status Read(size_t n, Slice* result, char* scratch) {
        if (mem_file_) {
            return mem_file_->Read(n, result, scratch);
        }
        return dfs_file_->Read(n, result, scratch);
    }

    virtual Status Skip(uint64_t n) {
        if (mem_file_) {
            return mem_file_->Skip(n);
        }
        return dfs_file_->Skip(n);
    }

    bool isValid() {
        return (dfs_file_ || mem_file_);
    }

};

// A file abstraction for randomly reading the contents of a file.
class InMemoryRandomAccessFile :public RandomAccessFile{
private:
    RandomAccessFile* dfs_file_;
    RandomAccessFile* mem_file_;
public:
    InMemoryRandomAccessFile(Env* mem_env, Env* dfs_env, const std::string& fname)
        :dfs_file_(NULL), mem_file_(NULL) {
        Status s = mem_env->NewRandomAccessFile(fname, &mem_file_);
        if (s.ok()) {
            return;
        }
        mem_file_ = NULL;
        s = dfs_env->NewRandomAccessFile(fname, &dfs_file_);
        if (!s.ok()) {
            return;
        }
    }
    ~InMemoryRandomAccessFile() {
        delete dfs_file_;
        delete mem_file_;
    }
    Status Read(uint64_t offset, size_t n, Slice* result,
                      char* scratch) const {
        if (mem_file_) {
            return mem_file_->Read(offset, n, result, scratch);
        }
        return dfs_file_->Read(offset, n, result, scratch);
    }
    bool isValid() {
        return (dfs_file_ || mem_file_);
    }
};

// WritableFile
class InMemoryWritableFile: public WritableFile {
private:
    WritableFile* dfs_file_;
    WritableFile* mem_file_;
public:
    InMemoryWritableFile(Env* mem_env, Env* dfs_env, const std::string& fname)
        :dfs_file_(NULL), mem_file_(NULL) {
        Status s = dfs_env->NewWritableFile(fname, &dfs_file_);
        if (!s.ok()) {
            return;
        }
        if (fname.rfind(".sst") != fname.size()-4) {
            return;
        }
        s = mem_env->NewWritableFile(fname, &mem_file_);
        assert(s.ok());
    }
    virtual ~InMemoryWritableFile() {
        delete dfs_file_;
        delete mem_file_;
    }
    virtual Status Append(const Slice& data) {
        Status s = dfs_file_->Append(data);
        if (!s.ok()) {
            return s;
        }
        if (mem_file_) {
            s = mem_file_->Append(data);
            assert(s.ok());
        }
        return s;
    }

    bool isValid() {
        return (dfs_file_ || mem_file_);
    }

    virtual Status Flush() {
        Status s = dfs_file_->Flush();
        if (!s.ok()) {
            return s;
        }
        if (mem_file_) {
            s = mem_file_->Flush();
            assert(s.ok());
        }
        return s;
    }

    virtual Status Sync() {
        Status s = dfs_file_->Sync();
        if (!s.ok()) {
            return s;
        }
        if (mem_file_) {
            s = mem_file_->Sync();
            assert(s.ok());
        }
        return s;
    }

    virtual Status Close() {
        if (mem_file_) {
            Status s = mem_file_->Close();
            assert(s.ok());
        }
        return dfs_file_->Close();
    }
};

InMemoryEnv::InMemoryEnv(Env* base_env) : EnvWrapper(Env::Default())
{
    dfs_env_ = base_env;
    mem_env_ = NewMemEnv(dfs_env_);
}

InMemoryEnv::~InMemoryEnv()
{
    delete mem_env_;
}

// SequentialFile
Status InMemoryEnv::NewSequentialFile(const std::string& fname, SequentialFile** result)
{
    InMemorySequentialFile* f = new InMemorySequentialFile(mem_env_, dfs_env_, fname);
    if (!f->isValid()) {
        delete f;
        *result = NULL;
        return IOError(fname, errno);
    }
    *result = f;
    return Status::OK();
}

// random read file
Status InMemoryEnv::NewRandomAccessFile(const std::string& fname,
        RandomAccessFile** result)
{
    InMemoryRandomAccessFile* f = new InMemoryRandomAccessFile(mem_env_, dfs_env_, fname);
    if (f == NULL || !f->isValid()) {
        *result = NULL;
        delete f;
        return IOError(fname, errno);
    }
    *result = f;
    return Status::OK();
}

// writable
Status InMemoryEnv::NewWritableFile(const std::string& fname,
        WritableFile** result)
{
    Status s;
    InMemoryWritableFile* f = new InMemoryWritableFile(mem_env_, dfs_env_, fname);
    if (f == NULL || !f->isValid()) {
        *result = NULL;
        delete f;
        return IOError(fname, errno);
    }
    *result = f;
    return Status::OK();
}

// FileExists
bool InMemoryEnv::FileExists(const std::string& fname)
{
    return dfs_env_->FileExists(fname);
}

//
Status InMemoryEnv::GetChildren(const std::string& path, std::vector<std::string>* result)
{
    return dfs_env_->GetChildren(path, result);
}

Status InMemoryEnv::DeleteFile(const std::string& fname)
{
    mem_env_->DeleteFile(fname);
    return dfs_env_->DeleteFile(fname);
}

Status InMemoryEnv::CreateDir(const std::string& name)
{
    mem_env_->CreateDir(name);
    return dfs_env_->CreateDir(name);
};

Status InMemoryEnv::DeleteDir(const std::string& name)
{
    mem_env_->DeleteDir(name);
    return dfs_env_->DeleteDir(name);
};

Status InMemoryEnv::GetFileSize(const std::string& fname, uint64_t* size)
{
    return dfs_env_->GetFileSize(fname, size);
}

///
Status InMemoryEnv::RenameFile(const std::string& src, const std::string& target)
{
    mem_env_->RenameFile(src, target);
    return dfs_env_->RenameFile(src, target);
}

Status InMemoryEnv::LockFile(const std::string& fname, FileLock** lock)
{
    *lock = NULL;
    return Status::OK();
}

Status InMemoryEnv::UnlockFile(FileLock* lock)
{
    return Status::OK();
}

Env* NewInMemoryEnv(Env* base_env)
{
    return new InMemoryEnv(base_env);
}

}  // namespace leveldb
