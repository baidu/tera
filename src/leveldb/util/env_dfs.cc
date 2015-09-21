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

#include "hdfs.h"
#include "leveldb/env.h"
#include "leveldb/status.h"
#include "leveldb/env_dfs.h"
#include "leveldb/table_utils.h"
#include "nfs.h"
#include "util/mutexlock.h"
#include "../utils/counter.h"

namespace leveldb {

tera::Counter dfs_read_size_counter;
tera::Counter dfs_write_size_counter;

tera::Counter dfs_read_delay_counter;
tera::Counter dfs_write_delay_counter;
tera::Counter dfs_sync_delay_counter;

tera::Counter dfs_read_counter;
tera::Counter dfs_write_counter;
tera::Counter dfs_sync_counter;
tera::Counter dfs_flush_counter;
tera::Counter dfs_list_counter;
tera::Counter dfs_other_counter;
tera::Counter dfs_exists_counter;
tera::Counter dfs_open_counter;
tera::Counter dfs_close_counter;
tera::Counter dfs_delete_counter;
tera::Counter dfs_tell_counter;
tera::Counter dfs_info_counter;

tera::Counter dfs_read_hang_counter;
tera::Counter dfs_write_hang_counter;
tera::Counter dfs_sync_hang_counter;
tera::Counter dfs_flush_hang_counter;
tera::Counter dfs_list_hang_counter;
tera::Counter dfs_other_hang_counter;
tera::Counter dfs_exists_hang_counter;
tera::Counter dfs_open_hang_counter;
tera::Counter dfs_close_hang_counter;
tera::Counter dfs_delete_hang_counter;
tera::Counter dfs_tell_hang_counter;
tera::Counter dfs_info_hang_counter;

bool split_filename(const std::string filename,
        std::string* path, std::string* file)
{
    size_t pos = filename.rfind('/');
    if (pos == std::string::npos) {
        return false;
    }
    *path = filename.substr(0, pos);
    *file = filename.substr(pos + 1);
    return true;
}

char* get_time_str(char* p, size_t len)
{
    const uint64_t thread_id = DfsEnv::gettid();
    struct timeval now_tv;
    gettimeofday(&now_tv, NULL);
    const time_t seconds = now_tv.tv_sec;
    struct tm t;
    localtime_r(&seconds, &t);
    p += snprintf(p, len,
            "%04d/%02d/%02d-%02d:%02d:%02d.%06d %llu",
            t.tm_year + 1900,
            t.tm_mon + 1,
            t.tm_mday,
            t.tm_hour,
            t.tm_min,
            t.tm_sec,
            static_cast<int>(now_tv.tv_usec),
            static_cast<long long unsigned int>(thread_id));
    return p;
}

// Log error message
static Status IOError(const std::string& context, int err_number)
{
    return Status::IOError(context, strerror(err_number));
}


class DfsReadableFile: virtual public SequentialFile, virtual public RandomAccessFile {
private:
    Dfs* fs_;
    std::string filename_;
    DfsFile* file_;
    mutable ssize_t now_pos;
    //mutable port::Mutex mu_;
public:
    DfsReadableFile(Dfs* fs, const std::string& fname)
        : fs_(fs), filename_(fname), file_(NULL),
          now_pos(-1) {
        tera::AutoCounter ac(&dfs_open_hang_counter, "OpenFile", filename_.c_str());
        dfs_open_counter.Inc();
        file_ = fs->OpenFile(filename_, RDONLY);
        // assert(hfile_ != NULL);
        if (file_ == NULL) {
            fprintf(stderr, "[env_dfs]: open file fail: %s\n", filename_.c_str());
        }
        now_pos = 0;
    }

    virtual ~DfsReadableFile() {
        if (file_) {
            tera::AutoCounter ac(&dfs_close_hang_counter, "CloseFile", filename_.c_str());
            dfs_close_counter.Inc();
            file_->CloseFile();
        }
        delete file_;
        file_ = NULL;
    }

    bool isValid() {
        return (file_ != NULL);
    }

    virtual Status Read(size_t n, Slice* result, char* scratch) {
        now_pos = -1;
        Status s;
        int64_t t = tera::get_micros();
        tera::AutoCounter ac(&dfs_read_hang_counter, "Read", filename_.c_str());
        int32_t bytes_read = file_->Read(scratch, (int32_t)n);
        dfs_read_delay_counter.Add(tera::get_micros() - t);
        dfs_read_counter.Inc();
        *result = Slice(scratch, (bytes_read < 0) ? 0 : bytes_read);
        if (bytes_read < static_cast<int32_t>(n)) {
            if (feof()) {
                // end of the file
            } else {
                s = IOError(filename_, errno);
            }
        }
        dfs_read_size_counter.Add(bytes_read);
        return s;
    }

    virtual Status Read(uint64_t offset, size_t n, Slice* result, char* scratch) const {
        Status s;
        int64_t t = tera::get_micros();
        tera::AutoCounter ac(&dfs_read_hang_counter, "Read", filename_.c_str());
        int32_t bytes_read = file_->Pread(offset, scratch, n);
        dfs_read_delay_counter.Add(tera::get_micros() - t);
        dfs_read_counter.Inc();
        *result = Slice(scratch, (bytes_read < 0) ? 0 : bytes_read);
        if (bytes_read < 0) {
            s = IOError(filename_, errno);
        }
        dfs_read_size_counter.Add(bytes_read);
        return s;
    }

    virtual Status Skip(uint64_t n) {
        int64_t current = 0;
        {
            tera::AutoCounter ac(&dfs_tell_hang_counter, "Skip", filename_.c_str());
            dfs_tell_counter.Inc();
            current = file_->Tell();
        }
        if (current < 0) {
            return IOError(filename_, errno);
        }
        // seek to new offset
        int64_t newoffset = current + n;

        tera::AutoCounter ac(&dfs_other_hang_counter, "Seek", filename_.c_str());
        dfs_other_counter.Inc();
        int val = file_->Seek(newoffset);
        if (val < 0) {
            return IOError(filename_, errno);
        }
        return Status::OK();
    }

private:
    // at the end of file ?
    bool feof() {
        tera::AutoCounter ac(&dfs_tell_hang_counter, "feof", filename_.c_str());
        dfs_tell_counter.Inc();
        if (file_ && file_->Tell() >= fileSize()) {
            return true;
        }
        return false;
    }
    // file size
    int64_t fileSize() {
        tera::AutoCounter ac(&dfs_info_hang_counter, "GetFileSize", filename_.c_str());
        dfs_info_counter.Inc();
        uint64_t size = 0;
        fs_->GetFileSize(filename_, &size);
        return size;
    }
};

// WritableFile
class DfsWritableFile: public WritableFile {
private:
    Dfs* fs_;
    std::string filename_;
    DfsFile* file_;
public:
    DfsWritableFile(Dfs* fs, const std::string& fname)
        : fs_(fs), filename_(fname) , file_(NULL) {
        fs_->Delete(filename_);
        tera::AutoCounter ac(&dfs_open_hang_counter, "OpenFile", filename_.c_str());
        dfs_open_counter.Inc();
        file_ = fs_->OpenFile(filename_, WRONLY);
        if (file_ == NULL) {
            fprintf(stderr, "[env_dfs]: open file for write fail: %s\n", fname.c_str());
        }
    }
    virtual ~DfsWritableFile() {
        if (file_ != NULL) {
            tera::AutoCounter ac(&dfs_close_hang_counter, "CloseFile", filename_.c_str());
            dfs_close_counter.Inc();
            file_->CloseFile();
        }
        delete file_;
    }

    bool isValid() {
        return file_ != NULL;
    }

    const std::string& getName() {
        return filename_;
    }

    virtual Status Append(const Slice& data) {
        const char* src = data.data();
        size_t left = data.size();

        int64_t s = tera::get_micros();
        tera::AutoCounter ac(&dfs_write_hang_counter, "Write", filename_.c_str());
        int32_t ret = file_->Write(src, left);
        dfs_write_delay_counter.Add(tera::get_micros() - s);
        dfs_write_counter.Inc();

        if (ret != static_cast<int32_t>(left)) {
            return IOError(filename_, errno);
        }
        dfs_write_size_counter.Add(ret);
        return Status::OK();
    }

    virtual Status Flush() {
        //tera::AutoCounter ac(&dfs_flush_hang_counter, "Flush", filename_.c_str());
        //dfs_flush_counter.Inc();
        // dfs flush efficiency is too low, close it
        //if (file_->Flush() != 0) {
        //    return IOError(filename_, errno);
        //}
        return Status::OK();
    }

    virtual Status Sync() {
        tera::AutoCounter ac(&dfs_sync_hang_counter, "Sync", filename_.c_str());
        dfs_sync_counter.Inc();
        Status s;
        tera::Counter dfs_sync_counter;
        uint64_t t = EnvDfs()->NowMicros();
        if (file_->Sync() == -1) {
            fprintf(stderr, "dfs sync fail: %s\n", filename_.c_str());
            s = IOError(filename_, errno);
        }
        uint64_t diff = EnvDfs()->NowMicros() - t;
        dfs_sync_delay_counter.Add(diff);
        if (diff > 2000000) {
            char buf[128];
            get_time_str(buf, 128);
            fprintf(stderr, "%s dfs sync for %s use %.2fms\n",
                buf, filename_.c_str(), diff / 1000.0);
        }
        return s;
    }

    virtual Status Close() {
        Status result;
        if (file_ != NULL && file_->CloseFile() != 0) {
            result = IOError(filename_, errno);
        }
        delete file_;
        file_ = NULL;
        return result;
    }
};

DfsEnv::DfsEnv(Dfs* dfs)
  : EnvWrapper(Env::Default()), dfs_(dfs) {
}

DfsEnv::~DfsEnv()
{
}

// SequentialFile
Status DfsEnv::NewSequentialFile(const std::string& fname, SequentialFile** result)
{
    DfsReadableFile* f = new DfsReadableFile(dfs_, fname);
    if (!f->isValid()) {
        delete f;
        *result = NULL;
        return IOError(fname, errno);
    }
    *result = dynamic_cast<SequentialFile*>(f);
    return Status::OK();
}

// random read file
Status DfsEnv::NewRandomAccessFile(const std::string& fname, RandomAccessFile** result)
{
    DfsReadableFile* f = new DfsReadableFile(dfs_, fname);
    if (f == NULL || !f->isValid()) {
        delete f;
        *result = NULL;
        return IOError(fname, errno);
    }
    *result = dynamic_cast<RandomAccessFile*>(f);
    return Status::OK();
}

// writable
Status DfsEnv::NewWritableFile(const std::string& fname,
        WritableFile** result)
{
    Status s;
    DfsWritableFile* f = new DfsWritableFile(dfs_, fname);
    if (f == NULL || !f->isValid()) {
        delete f;
        *result = NULL;
        return IOError(fname, errno);
    }
    *result = dynamic_cast<WritableFile*>(f);
    return Status::OK();
}

// FileExists
bool DfsEnv::FileExists(const std::string& fname)
{
    tera::AutoCounter ac(&dfs_exists_hang_counter, "Exists", fname.c_str());
    dfs_exists_counter.Inc();
    return (0 == dfs_->Exists(fname));
}

Status DfsEnv::CopyFile(const std::string& from, const std::string& to) {
    tera::AutoCounter ac(&dfs_other_hang_counter, "Copy", from.c_str());
    dfs_other_counter.Inc();
    std::cerr << "DfsEnv: " << from << " --> " << to << std::endl;
    if (from != to && dfs_->Copy(from, to) != 0) {
        return Status::IOError("DFS Copy", from);
    }
    return Status::OK();
}


Status DfsEnv::GetChildren(const std::string& path, std::vector<std::string>* result)
{
    {
        tera::AutoCounter ac(&dfs_exists_hang_counter, "Exists", path.c_str());
        dfs_exists_counter.Inc();
        if (0 != dfs_->Exists(path)) {
            fprintf(stderr, "GetChildren call with path not exists: %s\n",
                    path.data());
            return Status::IOError("Path not exist", path);
        }
    }

    tera::AutoCounter ac(&dfs_list_hang_counter, "ListDirectory", path.c_str());
    dfs_list_counter.Inc();
    if (0 != dfs_->ListDirectory(path, result)) {
        abort();
    }
    return Status::OK();
}

bool DfsEnv::CheckDelete(const std::string& fname, std::vector<std::string>* flags)
{
    std::string path, file;
    bool r = split_filename(fname, &path, &file);
    assert(r);
    std::string prefix = file + "_del_";
    std::vector<std::string> files;
    dfs_->ListDirectory(path, &files);
    size_t max_len = 0;
    size_t value = 0;
    for (size_t i = 0; i < files.size(); i++) {
        if (files[i].compare(0, prefix.size(), prefix) != 0) {
            continue;
        }
        flags->push_back(path + "/" + files[i]);
        std::string id_str = files[i].substr(prefix.size());
        if (id_str.size() > 64) {
            return false;
        }
        if (max_len < id_str.size()) {
            value <<= (id_str.size() - max_len);
            value ++;
            max_len = id_str.size();
        } else {
            value += (1ULL << (max_len - id_str.size()));
        }
    }
    return (value == (1ULL << max_len));
}

Status DfsEnv::DeleteFile(const std::string& fname)
{
    tera::AutoCounter ac(&dfs_delete_hang_counter, "DeleteFile", fname.c_str());
    dfs_delete_counter.Inc();
    if (dfs_->Delete(fname) == 0) {
        return Status::OK();
    }
    return IOError(fname, errno);
};

Status DfsEnv::CreateDir(const std::string& name)
{
    tera::AutoCounter ac(&dfs_other_hang_counter, "CreateDirectory", name.c_str());
    dfs_other_counter.Inc();
    if (dfs_->CreateDirectory(name) == 0) {
        return Status::OK();
    }
    return IOError(name, errno);
};

Status DfsEnv::DeleteDir(const std::string& name)
{
    tera::AutoCounter ac(&dfs_delete_hang_counter, "DeleteDirectory", name.c_str());
    dfs_delete_counter.Inc();
    if (dfs_->DeleteDirectory(name) == 0) {
        return Status::OK();
    }
    return IOError(name, errno);
};

Status DfsEnv::GetFileSize(const std::string& fname, uint64_t* size)
{
    tera::AutoCounter ac(&dfs_info_hang_counter, "GetFileSize", fname.c_str());
    dfs_info_counter.Inc();
    *size = 0L;
    if (0 != dfs_->GetFileSize(fname, size)) {
        return IOError(fname, errno);
    } else {
        return Status::OK();
    }
}

///
Status DfsEnv::RenameFile(const std::string& src, const std::string& target)
{
    tera::AutoCounter ac(&dfs_other_hang_counter, "RenameFile", src.c_str());
    dfs_other_counter.Inc();
    if (dfs_->Rename(src, target) == 0) {

    }
    Status result;
    return result;
}

Status DfsEnv::LockFile(const std::string& fname, FileLock** lock)
{
    *lock = NULL;
    return Status::OK();
}

Status DfsEnv::UnlockFile(FileLock* lock)
{
    return Status::OK();
}

Status DfsEnv::NewLogger(const std::string& fname, Logger** result)
{
    return IOError(fname, errno);
}

static bool inited = false;
static port::Mutex mutex;
static Env* dfs_env;

void InitDfsEnv(const std::string& so_path, const std::string& conf) {
    MutexLock l(&mutex);
    if (inited) {
        return;
    }
    Dfs* dfs = Dfs::NewDfs(so_path, conf);
    if (dfs == NULL) {
        abort();
    }
    dfs_env = new DfsEnv(dfs);
    inited = true;
}

void InitHdfsEnv()
{
    MutexLock l(&mutex);
    if (inited) {
        return;
    }
    Dfs* dfs = new Hdfs();
    dfs_env = new DfsEnv(dfs);
    inited = true;
}

void InitHdfs2Env(const std::string& namenode_list)
{
    MutexLock l(&mutex);
    if (inited) {
        return;
    }
    Dfs* dfs = new Hdfs2(namenode_list);
    dfs_env = new DfsEnv(dfs);
    inited = true;
}

void InitNfsEnv(const std::string& mountpoint,
                const std::string& conf_path)
{
    MutexLock l(&mutex);
    if (inited) {
        return;
    }
    Nfs::Init(mountpoint, conf_path);
    Dfs* dfs = Nfs::GetInstance();
    dfs_env = new DfsEnv(dfs);
    inited = true;
}

Env* NewDfsEnv(Dfs* dfs)
{
    return new DfsEnv(dfs);
}

Env* EnvDfs()
{
    MutexLock l(&mutex);
    if (inited) {
        return dfs_env;
    }
    Dfs* dfs = new Hdfs();
    dfs_env = new DfsEnv(dfs);
    inited = true;
    return dfs_env;
}

}  // namespace leveldb
