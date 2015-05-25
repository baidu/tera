// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

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
#include "util/hash.h"
#include "util/mutexlock.h"
#include "helpers/memenv/memenv.h"
#include "../utils/counter.h"

#include "leveldb/env_flash.h"


namespace leveldb {

tera::Counter ssd_read_counter;
tera::Counter ssd_read_size_counter;
tera::Counter ssd_write_counter;
tera::Counter ssd_write_size_counter;

// Log error message
static Status IOError(const std::string& context, int err_number)
{
    return Status::IOError(context, strerror(err_number));
}

/// copy file from env to local
Status CopyToLocal(const std::string& local_fname, Env* env,
                   const std::string& fname, uint64_t fsize)
{
    uint64_t time_s = env->NowMicros();

    uint64_t local_size = 0;
    Status s = Env::Default()->GetFileSize(local_fname, &local_size);
    if (s.ok() && fsize == local_size) {
        return s;
    }
    fprintf(stderr, "local file mismatch, expect %lu, actual %lu, delete %s\n",
            fsize, local_size, local_fname.c_str());
    Env::Default()->DeleteFile(local_fname);

    fprintf(stderr, "open %s\n", fname.c_str());
    SequentialFile* hdfs_file = NULL;
    s = env->NewSequentialFile(fname, &hdfs_file);
    if (!s.ok()) {
        return s;
    }

    for (size_t i = 1; i < local_fname.size(); i++) {
        if (local_fname.at(i) == '/') {
            Env::Default()->CreateDir(local_fname.substr(0,i));
        }
    }

    fprintf(stderr, "open %s\n", local_fname.c_str());
    WritableFile* local_file = NULL;
    s = Env::Default()->NewWritableFile(local_fname, &local_file);
    if (!s.ok()) {
        delete hdfs_file;
        return s;
    }

    char buf[4096];
    Slice result;
    local_size = 0;
    while (hdfs_file->Read(4096, &result, buf).ok() && result.size() > 0
        && local_file->Append(result).ok()) {
        local_size += result.size();
    }
    delete hdfs_file;
    delete local_file;

    if (local_size == fsize) {
        uint64_t time_used = env->NowMicros() - time_s;
        //if (time_used > 200000) {
        if (true) {
            fprintf(stderr, "copy %s to local used %llu ms\n",
                fname.c_str(), static_cast<unsigned long long>(time_used) / 1000);
        }
        return s;
    }

    uint64_t file_size = 0;
    s = env->GetFileSize(fname, &file_size);
    if (!s.ok()) {
        return Status::IOError("hdfs GetFileSize fail", s.ToString());
    }

    if (fsize == file_size) {
        // dfs fsize match but local doesn't match
        s = IOError("local fsize mismatch", file_size);
    } else {
        s = IOError("dfs fsize mismatch", file_size);
    }
    Env::Default()->DeleteFile(local_fname);
    return s;
}

class FlashSequentialFile: public SequentialFile {
private:
    SequentialFile* hdfs_file_;
    SequentialFile* flash_file_;
public:
    FlashSequentialFile(Env* posix_env, Env* hdfs_env, const std::string& fname)
        :hdfs_file_(NULL), flash_file_(NULL) {
        hdfs_env->NewSequentialFile(fname, &hdfs_file_);
    }

    virtual ~FlashSequentialFile() {
        delete hdfs_file_;
        delete flash_file_;
    }

    virtual Status Read(size_t n, Slice* result, char* scratch) {
        if (flash_file_) {
            return flash_file_->Read(n, result, scratch);
        }
        return hdfs_file_->Read(n, result, scratch);
    }

    virtual Status Skip(uint64_t n) {
        if (flash_file_) {
            return flash_file_->Skip(n);
        }
        return hdfs_file_->Skip(n);
    }

    bool isValid() {
        return (hdfs_file_ || flash_file_);
    }

};

// A file abstraction for randomly reading the contents of a file.
class FlashRandomAccessFile :public RandomAccessFile{
private:
    RandomAccessFile* hdfs_file_;
    RandomAccessFile* flash_file_;
public:
    FlashRandomAccessFile(Env* posix_env, Env* hdfs_env,
                          const std::string& fname, uint64_t fsize)
        :hdfs_file_(NULL), flash_file_(NULL) {
        std::string local_fname = FlashEnv::FlashPath(fname) + fname;

        // copy from hdfs with seq read
        Status copy_status = CopyToLocal(local_fname, hdfs_env, fname, fsize);
        if (!copy_status.ok()) {
            fprintf(stderr, "copy to local fail [%s]: %s\n",
                copy_status.ToString().c_str(), local_fname.c_str());
            // no flash file, use hdfs file
            hdfs_env->NewRandomAccessFile(fname, &hdfs_file_);
            return;
        }

        Status s = posix_env->NewRandomAccessFile(local_fname, &flash_file_);
        if (s.ok()) {
            return;
        }
        fprintf(stderr, "local file exists, but open for RandomAccess fail: %s\n",
            local_fname.c_str());
        unlink(local_fname.c_str());
        hdfs_env->NewRandomAccessFile(fname, &hdfs_file_);
    }
    ~FlashRandomAccessFile() {
        delete hdfs_file_;
        delete flash_file_;
    }
    Status Read(uint64_t offset, size_t n, Slice* result,
                      char* scratch) const {
        if (flash_file_) {
            Status read_status = flash_file_->Read(offset, n, result, scratch);
            if (read_status.ok()) {
                ssd_read_counter.Inc();
                ssd_read_size_counter.Add(result->size());
            }
            return read_status;
        }
        return hdfs_file_->Read(offset, n, result, scratch);
    }
    bool isValid() {
        return (hdfs_file_ || flash_file_);
    }
};

// WritableFile
class FlashWritableFile: public WritableFile {
private:
    WritableFile* hdfs_file_;
    WritableFile* flash_file_;
    std::string local_fname_;
public:
    FlashWritableFile(Env* posix_env, Env* hdfs_env, const std::string& fname)
        :hdfs_file_(NULL), flash_file_(NULL) {
        Status s = hdfs_env->NewWritableFile(fname, &hdfs_file_);
        if (!s.ok()) {
            return;
        }
        if (fname.rfind(".sst") != fname.size()-4) {
            // fprintf(stderr, "[EnvFlash] Don't cache %s\n", fname.c_str());
            return;
        }
        local_fname_ = FlashEnv::FlashPath(fname) + fname;
        for(size_t i=1 ;i<local_fname_.size(); i++) {
            if (local_fname_.at(i) == '/') {
                posix_env->CreateDir(local_fname_.substr(0,i));
            }
        }
        s = posix_env->NewWritableFile(local_fname_, &flash_file_);
        if (!s.ok()) {
            fprintf(stderr, "Open local flash file for write fail: %s\n",
                    local_fname_.c_str());
        }
    }
    virtual ~FlashWritableFile() {
        delete hdfs_file_;
        delete flash_file_;
    }
    void DeleteLocal() {
        delete flash_file_;
        flash_file_ = NULL;
        unlink(local_fname_.c_str());
    }
    virtual Status Append(const Slice& data) {
        Status s = hdfs_file_->Append(data);
        if (!s.ok()) {
            return s;
        }
        if (flash_file_) {
            Status local_s = flash_file_->Append(data);
            if (!local_s.ok()) {
                DeleteLocal();
            }else{
                ssd_write_counter.Inc();
                ssd_write_size_counter.Add(data.size());
            }
        }
        return s;
    }

    bool isValid() {
        return (hdfs_file_ || flash_file_);
    }

    virtual Status Flush() {
        Status s = hdfs_file_->Flush();
        if (!s.ok()) {
            return s;
        }
        // Don't flush cache file
        /*
        if (flash_file_) {
            Status local_s = flash_file_->Flush();
            if (!local_s.ok()) {
                DeleteLocal();
            }
        }*/
        return s;
    }

    virtual Status Sync() {
        Status s = hdfs_file_->Sync();
        if (!s.ok()) {
            return s;
        }
        /* Don't sync cache file
        if (flash_file_) {
            Status local_s = flash_file_->Sync();
            if (!local_s.ok()) {
                DeleteLocal();
            }
        }*/
        return s;
    }

    virtual Status Close() {
        if (flash_file_) {
            Status local_s = flash_file_->Close();
            if (!local_s.ok()) {
                DeleteLocal();
            }
        }
        return hdfs_file_->Close();
    }
};

std::vector<std::string> FlashEnv::flash_paths_(1, "./flash");

FlashEnv::FlashEnv() : EnvWrapper(Env::Default())
{
    dfs_env_ = EnvDfs();
    posix_env_ = Env::Default();
}

FlashEnv::~FlashEnv()
{
}

// SequentialFile
Status FlashEnv::NewSequentialFile(const std::string& fname, SequentialFile** result)
{
    FlashSequentialFile* f = new FlashSequentialFile(posix_env_, dfs_env_, fname);
    if (!f->isValid()) {
        delete f;
        *result = NULL;
        return IOError(fname, errno);
    }
    *result = f;
    return Status::OK();
}

// random read file
Status FlashEnv::NewRandomAccessFile(const std::string& fname,
        uint64_t fsize, RandomAccessFile** result)
{
    FlashRandomAccessFile* f = new FlashRandomAccessFile(posix_env_, dfs_env_,
                                                         fname, fsize);
    if (f == NULL || !f->isValid()) {
        *result = NULL;
        return IOError(fname, errno);
    }
    *result = f;
    return Status::OK();
}

Status FlashEnv::NewRandomAccessFile(const std::string& fname,
                                     RandomAccessFile** result) {
    // not implement
    abort();
}

// writable
Status FlashEnv::NewWritableFile(const std::string& fname,
        WritableFile** result)
{
    Status s;
    FlashWritableFile* f = new FlashWritableFile(posix_env_, dfs_env_, fname);
    if (f == NULL || !f->isValid()) {
        *result = NULL;
        return IOError(fname, errno);
    }
    *result = f;
    return Status::OK();
}

// FileExists
bool FlashEnv::FileExists(const std::string& fname)
{
    return dfs_env_->FileExists(fname);
}

//
Status FlashEnv::GetChildren(const std::string& path,
        std::vector<std::string>* result)
{
    return dfs_env_->GetChildren(path, result);
}

Status FlashEnv::DeleteFile(const std::string& fname)
{
    posix_env_->DeleteFile(FlashEnv::FlashPath(fname) + fname);
    return dfs_env_->DeleteFile(fname);
}

Status FlashEnv::CreateDir(const std::string& name)
{
    std::string local_name = FlashEnv::FlashPath(name) + name;
    for(size_t i=1 ;i<local_name.size(); i++) {
        if (local_name.at(i) == '/') {
            posix_env_->CreateDir(local_name.substr(0,i));
        }
    }
    posix_env_->CreateDir(local_name);
    return dfs_env_->CreateDir(name);
};

Status FlashEnv::DeleteDir(const std::string& name)
{
    posix_env_->DeleteDir(FlashEnv::FlashPath(name) + name);
    return dfs_env_->DeleteDir(name);
};

Status FlashEnv::ListDir(const std::string& name,
        std::vector<std::string>* result)
{
    return dfs_env_->ListDir(name, result);
}

Status FlashEnv::GetFileSize(const std::string& fname, uint64_t* size)
{
    return dfs_env_->GetFileSize(fname, size);
}

///
Status FlashEnv::RenameFile(const std::string& src, const std::string& target)
{
    posix_env_->RenameFile(FlashEnv::FlashPath(src) + src, FlashEnv::FlashPath(target) + target);
    return dfs_env_->RenameFile(src, target);
}

Status FlashEnv::LockFile(const std::string& fname, FileLock** lock)
{
    *lock = NULL;
    return Status::OK();
}

Status FlashEnv::UnlockFile(FileLock* lock)
{
    return Status::OK();
}

void FlashEnv::SetFlashPath(const std::string& path) {
    std::vector<std::string> backup;
    flash_paths_.swap(backup);

    size_t beg = 0;
    const char *str = path.c_str();
    for (size_t i = 0; i <= path.size(); ++i) {
        if ((str[i] == '\0' || str[i] == ';') && i - beg > 0) {
            flash_paths_.push_back(std::string(str + beg, i - beg));
            beg = i +1;
        }
    }
    if (!flash_paths_.size()) {
        flash_paths_.swap(backup);
    }
}

const std::string& FlashEnv::FlashPath(const std::string& fname) {
    if (flash_paths_.size() == 1) {
        return flash_paths_[0];
    }
    uint32_t hash = Hash(fname.c_str(), fname.size(), 13);
    return flash_paths_[hash % flash_paths_.size()];
}

static pthread_once_t once = PTHREAD_ONCE_INIT;
static Env* flash_env;
static void InitFlashEnv()
{
    flash_env = new FlashEnv();
}

Env* EnvFlash()
{
    pthread_once(&once, InitFlashEnv);
    return flash_env;
}

Env* NewFlashEnv()
{
    return new FlashEnv();
}

}  // namespace leveldb


