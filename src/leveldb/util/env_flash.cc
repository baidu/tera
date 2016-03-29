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

const int64_t kFlashFileCheckIntervalMicros = 30 * 1000000;
const int64_t kUpdateFlashRetryIntervalMillis = 60 * 1000;

// Log error message
static Status IOError(const std::string& context, int err_number) {
    return Status::IOError(context, strerror(err_number));
}

/// copy file from env to local
Status CopyToLocal(const std::string& local_fname, Env* env,
                   const std::string& fname, uint64_t fsize, bool vanish_allowed) {
    uint64_t time_s = env->NowMicros();

    uint64_t local_size = 0;
    Status s = Env::Default()->GetFileSize(local_fname, &local_size);
    if (s.ok() && fsize == local_size) {
        return s;
    }
    Log("[env_flash] local file mismatch, expect %lu, actual %lu, delete %s\n",
        fsize, local_size, local_fname.c_str());
    Env::Default()->DeleteFile(local_fname);

//    Log("[env_flash] open dfs_file %s\n", fname.c_str());
    SequentialFile* dfs_file = NULL;
    s = env->NewSequentialFile(fname, &dfs_file);
    if (!s.ok()) {
        return s;
    }

    for (size_t i = 1; i < local_fname.size(); i++) {
        if (local_fname.at(i) == '/') {
            Env::Default()->CreateDir(local_fname.substr(0,i));
        }
    }

//    Log("[env_flash] open local %s\n", local_fname.c_str());
    WritableFile* local_file = NULL;
    s = Env::Default()->NewWritableFile(local_fname, &local_file);
    if (!s.ok()) {
        if (!vanish_allowed) {
            Log("[env_flash] local env error, exit: %s\n",
                local_fname.c_str());
            exit(-1);
        }
        delete dfs_file;
        return s;
    }

    char* buf = new char[1048576];
    Slice result;
    local_size = 0;
    while (dfs_file->Read(1048576, &result, buf).ok() && result.size() > 0
        && local_file->Append(result).ok()) {
        local_size += result.size();
    }
    delete [] buf;
    delete dfs_file;
    delete local_file;

    if (local_size == fsize) {
        uint64_t time_used = env->NowMicros() - time_s;
        //if (time_used > 200000) {
        if (true) {
            Log("[env_flash] copy %s to local used %llu ms\n",
                fname.c_str(), static_cast<unsigned long long>(time_used) / 1000);
        }
        return s;
    }

    uint64_t file_size = 0;
    s = env->GetFileSize(fname, &file_size);
    if (!s.ok()) {
        return Status::IOError("dfs GetFileSize fail", s.ToString());
    }

    Log("[env_flash] copy %s to local fail, size %ld, dfs size %ld, local size %ld\n",
        fname.c_str(), fsize, file_size, local_size);
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
    SequentialFile* dfs_file_;
    SequentialFile* flash_file_;

public:
    FlashSequentialFile(FlashEnv* flash_env, const std::string& fname)
        :dfs_file_(NULL), flash_file_(NULL) {
        flash_env->BaseEnv()->NewSequentialFile(fname, &dfs_file_);
    }

    virtual ~FlashSequentialFile() {
        delete dfs_file_;
        delete flash_file_;
    }

    virtual Status Read(size_t n, Slice* result, char* scratch) {
        if (flash_file_) {
            return flash_file_->Read(n, result, scratch);
        }
        return dfs_file_->Read(n, result, scratch);
    }

    virtual Status Skip(uint64_t n) {
        if (flash_file_) {
            return flash_file_->Skip(n);
        }
        return dfs_file_->Skip(n);
    }

    bool isValid() {
        return (dfs_file_ || flash_file_);
    }

};

// A file abstraction for randomly reading the contents of a file.
class FlashRandomAccessFile :public RandomAccessFile{
private:
    FlashEnv* flash_env_;
    mutable RandomAccessFile* dfs_file_;
    mutable RandomAccessFile* flash_file_;
    std::string fname_;
    std::string local_fname_;
    uint64_t fsize_;

    mutable port::Mutex mutex_;
    mutable bool flash_file_is_checking_;
    mutable uint64_t flash_file_last_check_micros_;
    mutable uint64_t flash_file_check_interval_micros_;
    mutable uint64_t read_dfs_count_;
public:
    FlashRandomAccessFile(FlashEnv* flash_env, const std::string& fname, uint64_t fsize)
        : flash_env_(flash_env), dfs_file_(NULL), flash_file_(NULL), fname_(fname),
          local_fname_(flash_env->FlashPath(fname) + fname), fsize_(fsize),
          flash_file_is_checking_(false), flash_file_last_check_micros_(0),
          flash_file_check_interval_micros_(kFlashFileCheckIntervalMicros),
          read_dfs_count_(0) {

        // copy file to cache if force read from cache
        if (flash_env_->ForceReadFromCache()) {
            Status copy_status = CopyToLocal(local_fname_, flash_env_->BaseEnv(), fname, fsize,
                                             flash_env_->VanishAllowed());
            if (!copy_status.ok()) {
                Log("[env_flash] copy to local fail [%s]: %s\n",
                    copy_status.ToString().c_str(), local_fname_.c_str());
            }
        }

        // if cache file is identical with dfs file, use cache file
        if (flash_env_->FlashFileIdentical(fname, fsize)) {
            Status s = flash_env_->CacheEnv()->NewRandomAccessFile(local_fname_, &flash_file_);
            if (s.ok()) {
                return;
            }
            Log("[env_flash] local file check pass, but open for RandomAccess fail [%s]: %s\n",
                s.ToString().c_str(), local_fname_.c_str());
        } else {
            Log("[env_flash] local file check fail: %s\n", local_fname_.c_str());
        }

        // else, use dfs file
        flash_env_->ScheduleUpdateFlash(fname, fsize, 1);
        flash_env_->BaseEnv()->NewRandomAccessFile(fname, &dfs_file_);
        flash_file_last_check_micros_ = Env::Default()->NowMicros();
    }
    ~FlashRandomAccessFile() {
        delete dfs_file_;
        delete flash_file_;
    }
    Status Read(uint64_t offset, size_t n, Slice* result, char* scratch) const {
        bool use_flash = false;
        {
            MutexLock l(&mutex_);
            // evenry 30 seconds, check if flash file is identical to dfs file.
            // if so, try open flash file;
            // else, reschedule update it with a higher priority
            if (flash_file_ == NULL &&
                    !flash_file_is_checking_ &&
                    flash_file_last_check_micros_ + flash_file_check_interval_micros_
                        <= Env::Default()->NowMicros()) {
                flash_file_is_checking_ = true;
                mutex_.Unlock();
                RandomAccessFile* tmp_file = NULL;
                if (flash_env_->FlashFileIdentical(fname_, fsize_)) {
                    flash_env_->CacheEnv()->NewRandomAccessFile(local_fname_, &tmp_file);
                }
                mutex_.Lock();
                if (tmp_file != NULL) {
                    flash_file_ = tmp_file;
                    Log("[env_flash] switch to local file: %s\n", local_fname_.c_str());
                } else {
                    flash_env_->ScheduleUpdateFlash(fname_, fsize_, read_dfs_count_);
                    read_dfs_count_ = 0;
                }
                flash_file_is_checking_ = false;
                flash_file_last_check_micros_ = Env::Default()->NowMicros();
            }
            if (flash_file_ != NULL) {
                use_flash = true;
            } else {
                ++read_dfs_count_;
            }
        }

        if (use_flash) {
            Status read_status = flash_file_->Read(offset, n, result, scratch);
            if (read_status.ok()) {
                ssd_read_counter.Inc();
                ssd_read_size_counter.Add(result->size());
            }
            return read_status;
        }
        return dfs_file_->Read(offset, n, result, scratch);
    }
    bool isValid() {
        return (dfs_file_ || flash_file_);
    }
};

// WritableFile
class FlashWritableFile: public WritableFile {
private:
    WritableFile* dfs_file_;
    WritableFile* flash_file_;
    std::string local_fname_;
public:
    FlashWritableFile(FlashEnv* flash_env, const std::string& fname)
        :dfs_file_(NULL), flash_file_(NULL) {
        Status s = flash_env->BaseEnv()->NewWritableFile(fname, &dfs_file_);
        if (!s.ok()) {
            return;
        }
        if (fname.rfind(".sst") != fname.size()-4) {
            // Log(logger, "[env_flash] Don't cache %s\n", fname.c_str());
            return;
        }
        local_fname_ = flash_env->FlashPath(fname) + fname;
        for(size_t i = 1; i < local_fname_.size(); i++) {
            if (local_fname_.at(i) == '/') {
                flash_env->CacheEnv()->CreateDir(local_fname_.substr(0,i));
            }
        }
        s = flash_env->CacheEnv()->NewWritableFile(local_fname_, &flash_file_);
        if (!s.ok()) {
            Log("[env_flash] Open local flash file for write fail: %s\n",
                local_fname_.c_str());
        }
    }
    virtual ~FlashWritableFile() {
        delete dfs_file_;
        delete flash_file_;
    }
    void DeleteLocal() {
        delete flash_file_;
        flash_file_ = NULL;
        Env::Default()->DeleteFile(local_fname_);
    }
    virtual Status Append(const Slice& data) {
        Status s = dfs_file_->Append(data);
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
        return (dfs_file_ || flash_file_);
    }

    virtual Status Flush() {
        Status s = dfs_file_->Flush();
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
        Status s = dfs_file_->Sync();
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
        return dfs_file_->Close();
    }
};


FlashEnv::FlashEnv(Env* base_env)
  : EnvWrapper(Env::Default()), dfs_env_(base_env), posix_env_(Env::Default()),
    flash_paths_(1, "./flash"), vanish_allowed_(false), force_read_from_cache_(true),
    update_flash_retry_interval_millis_(kUpdateFlashRetryIntervalMillis)
{
}

FlashEnv::~FlashEnv()
{
}

// SequentialFile
Status FlashEnv::NewSequentialFile(const std::string& fname, SequentialFile** result)
{
    FlashSequentialFile* f = new FlashSequentialFile(this, fname);
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
    FlashRandomAccessFile* f =
        new FlashRandomAccessFile(this, fname, fsize);
    if (f == NULL || !f->isValid()) {
        *result = NULL;
        delete f;
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
    FlashWritableFile* f = new FlashWritableFile(this, fname);
    if (f == NULL || !f->isValid()) {
        *result = NULL;
        delete f;
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

void FlashEnv::SetFlashPath(const std::string& path, bool vanish_allowed) {
    std::vector<std::string> backup;
    flash_paths_.swap(backup);
    vanish_allowed_ = vanish_allowed;

    size_t beg = 0;
    const char *str = path.c_str();
    for (size_t i = 0; i <= path.size(); ++i) {
        if ((str[i] == '\0' || str[i] == ';') && i - beg > 0) {
            flash_paths_.push_back(std::string(str + beg, i - beg));
            beg = i +1;
            if (!vanish_allowed
                && !Env::Default()->FileExists(flash_paths_.back())
                && !Env::Default()->CreateDir(flash_paths_.back()).ok()) {
                Log("[env_flash] cannot access cache dir: %s\n",
                    flash_paths_.back().c_str());
                exit(-1);
            }
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

void FlashEnv::SetIfForceReadFromCache(bool force) {
    force_read_from_cache_ = force;
}

bool FlashEnv::ForceReadFromCache() {
    return force_read_from_cache_;
}

void FlashEnv::SetUpdateFlashThreadNumber(int thread_num) {
    update_flash_threads_.SetBackgroundThreads(thread_num);
}

bool FlashEnv::FlashFileIdentical(const std::string& fname, uint64_t fsize) {
    uint64_t local_size = 0;
    std::string local_fname = FlashEnv::FlashPath(fname) + fname;
    Status s = Env::Default()->GetFileSize(local_fname, &local_size);
    if (s.ok() && fsize == local_size) {
        return true;
    }
    return false;
}

struct UpdateFlashFileParam {
    FlashEnv* flash_env;
    std::string fname;
    uint64_t fsize;
};

void UpdateFlashFileFunc(void* arg) {
    UpdateFlashFileParam* update_arg = (UpdateFlashFileParam*)arg;
    update_arg->flash_env->UpdateFlashFile(update_arg->fname, update_arg->fsize);
    delete update_arg;
}

void FlashEnv::ScheduleUpdateFlash(const std::string& fname, uint64_t fsize, int64_t priority) {
    MutexLock l(&update_flash_mutex_);
    if (update_flash_waiting_files_.find(fname) == update_flash_waiting_files_.end()) {
        UpdateFlashFileParam* param = new UpdateFlashFileParam;
        param->flash_env = this;
        param->fname = fname;
        param->fsize = fsize;

        UpdateFlashTask& task = update_flash_waiting_files_[fname];
        task.priority = priority;
        task.id = update_flash_threads_.Schedule(UpdateFlashFileFunc, param, (double)task.priority, 0);
        Log("[env_flash] schedule copy to local, id: %ld, prio: %ld, file: %s, pend: %ld\n",
            task.id, task.priority, fname.c_str(), update_flash_threads_.GetPendingTaskNum());
    } else {
        UpdateFlashTask& task = update_flash_waiting_files_[fname];
        task.priority += priority;
        update_flash_threads_.ReSchedule(task.id, (double)task.priority, 0);
        Log("[env_flash] reschedule copy to local, id: %ld, prio: %ld, file: %s, pend: %ld\n",
            task.id, task.priority, fname.c_str(), update_flash_threads_.GetPendingTaskNum());
    }
}

void FlashEnv::UpdateFlashFile(const std::string& fname, uint64_t fsize) {
    std::string local_fname = FlashEnv::FlashPath(fname) + fname;
    Status copy_status = CopyToLocal(local_fname, dfs_env_, fname, fsize, vanish_allowed_);

    MutexLock l(&update_flash_mutex_);
    if (copy_status.ok()) {
        UpdateFlashTask& task = update_flash_waiting_files_[fname];
        Log("[env_flash] copy to local success, id: %ld, prio: %ld, file: %s, pend: %ld\n",
            task.id, task.priority, local_fname.c_str(), update_flash_threads_.GetPendingTaskNum());
        update_flash_waiting_files_.erase(fname);
    } else {
        UpdateFlashTask& task = update_flash_waiting_files_[fname];
        Log("[env_flash] copy to local fail [%s], id: %ld, prio: %ld, file: %s, pend: %ld\n",
            copy_status.ToString().c_str(), task.id, task.priority,
            local_fname.c_str(), update_flash_threads_.GetPendingTaskNum());

        UpdateFlashFileParam* param = new UpdateFlashFileParam;
        param->flash_env = this;
        param->fname = fname;
        param->fsize = fsize;

        task.priority >>= 1; // cut down priority to half
        if (task.priority > 0) {
            task.id = update_flash_threads_.Schedule(UpdateFlashFileFunc, param, (double)task.priority,
                                                     update_flash_retry_interval_millis_);
            Log("[env_flash] schedule copy to local after %ld ms, id: %ld, prio: %ld, file: %s\n",
                update_flash_retry_interval_millis_, task.id, task.priority, local_fname.c_str());
        } else {
            Log("[env_flash] abort schedule copy to local, file: %s\n", local_fname.c_str());
            update_flash_waiting_files_.erase(fname);
        }
    }
}

Env* NewFlashEnv(Env* base_env)
{
    return new FlashEnv(base_env);
}

}  // namespace leveldb


