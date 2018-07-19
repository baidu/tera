// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
 
#include "common/log/log_cleaner.h"

#include <algorithm>
#include <map>
#include <vector>
#include <unistd.h>

#include <glog/logging.h>

#include "common/file/file_path.h"
#include "common/timer.h"

DECLARE_string(log_dir);
DECLARE_string(tera_log_prefix);
DECLARE_string(tera_leveldb_log_path);
DECLARE_int64(tera_info_log_clean_period_second);
DECLARE_int64(tera_info_log_expire_second);
DECLARE_string(ins_log_file);
 
namespace common {

static const int64_t kMinCleanPeriodMs = 1000; // 1s
static const int64_t kMinInfoLogExpireSec = 1; // 1s
static const size_t kPathMaxLen = 64;

Mutex LogCleaner::inst_init_mutex_;
LogCleaner* LogCleaner::singleton_instance_ = NULL;

static std::string GetProcFdPath() {
    char path_buf[kPathMaxLen];
    snprintf(path_buf, kPathMaxLen, "/proc/%d/fd", getpid());
    return std::string(path_buf);
}

static std::string GetFileNameFromPath(const std::string& path) {
    std::string::size_type pos = path.rfind("/");
    if (pos == std::string::npos) {
        return path;
    } else {
        return path.substr(pos + 1);
    }
}


LogCleaner* LogCleaner::GetInstance(ThreadPool *thread_pool) {
    if (singleton_instance_ == NULL) {
        singleton_instance_ = new LogCleaner(FLAGS_log_dir,
            FLAGS_tera_info_log_clean_period_second, 
            FLAGS_tera_info_log_expire_second, 
            thread_pool);
        singleton_instance_->AddPrefix(FLAGS_tera_log_prefix);
        singleton_instance_->AddPrefix(GetFileNameFromPath(FLAGS_tera_leveldb_log_path));
        singleton_instance_->AddPrefix(GetFileNameFromPath(FLAGS_ins_log_file));
    }
    return singleton_instance_;
}

bool LogCleaner::StartCleaner(ThreadPool *thread_pool) {
    return GetInstance()->Start();
}

void LogCleaner::StopCleaner() {
    MutexLock l(&inst_init_mutex_, "Destroy log cleaner");
    if (singleton_instance_ != NULL) {
        singleton_instance_->Stop();
        delete singleton_instance_;
        singleton_instance_ = NULL;
    }
}
 
LogCleaner::LogCleaner(const std::string& log_dir, 
                       int64_t period_second,
                       int64_t expire_second, 
                       ThreadPool *thread_pool)
    : thread_pool_(thread_pool),
      thread_pool_own_(false),
      mutex_(),
      info_log_dir_(log_dir),
      log_prefix_list_(),
      info_log_clean_period_ms_(std::max(period_second * 1000, kMinCleanPeriodMs)),
      info_log_expire_sec_(std::max(expire_second, kMinInfoLogExpireSec)),
      stop_(false),
      bg_exit_(false),
      bg_cond_(&mutex_),
      bg_func_(std::bind(&LogCleaner::CleanTaskWrap, this)),
      bg_task_id_(-1), 
      proc_fd_path_(GetProcFdPath()) {}
 
LogCleaner::~LogCleaner() {
    DestroyOwnThreadPool();
}

static bool CheckDirPath(const std::string &dir_path) {
    return !dir_path.empty() && IsDir(dir_path);
}

bool LogCleaner::CheckOptions() const {
    return CheckDirPath(info_log_dir_) && 
           info_log_clean_period_ms_ > 0 && 
           info_log_expire_sec_ > 0;
}

bool LogCleaner::Start() {
    if (!CheckOptions()) {
        return false;
    }
    
    MutexLock l(&mutex_, "Start info log cleaner");
    
    // double check
    if (IsRunning()) {
        return true;
    }
    
    stop_ = false;
    bg_exit_ = false;
    if (nullptr == thread_pool_) {
        NewThreadPool();
    }

    if (bg_task_id_ <= 0) {
        // start immediately
        bg_task_id_ = thread_pool_->DelayTask(0, bg_func_);
    }
    return true;
}

void LogCleaner::Stop() {
    MutexLock l(&mutex_, "Stop info log cleaner");
    stop_ = true;
    bool is_running = false;
    if (bg_task_id_ > 0) {
        bg_exit_ = thread_pool_->CancelTask(bg_task_id_, true, &is_running);
    } else {
        bg_exit_ = true;
    }
    
    CHECK(is_running || bg_exit_);
    while(!bg_exit_) {
        bg_cond_.Wait();
    }
    bg_task_id_ = -1;
}

void LogCleaner::CleanTaskWrap() {
    MutexLock l(&mutex_);
    DoCleanLocalLogs();
    if (stop_) {
        bg_task_id_ = -1;
        bg_exit_ = true;
    } else {
        bg_task_id_ = thread_pool_->DelayTask(info_log_clean_period_ms_, bg_func_);
    }
    bg_cond_.Signal();
}

bool LogCleaner::CheckLogPrefix(const std::string& filename) const {
    std::set<std::string>::const_iterator prefix_iter = log_prefix_list_.begin();
    for (; prefix_iter != log_prefix_list_.end(); ++prefix_iter) {
        const std::string& prefix = *prefix_iter;
        if (filename.size() < prefix.size()) {
            // do not need to compare
            continue;
        }

        if (strncmp(prefix.c_str(), filename.c_str(), prefix.size()) == 0) {
            // return true if match any prefix
            return true;
        }
    }
    return false;
}

bool LogCleaner::DoCleanLocalLogs() {
    if (log_prefix_list_.empty()) {
        LOG(WARNING) << "[LogCleaner] Log prefix is not set yet.";
        return false;
    }
    if (!CheckDirPath(info_log_dir_) || IsEmpty(info_log_dir_)) {
        LOG(WARNING) << "[LogCleaner] Log dir " << info_log_dir_ << " not exsit logs.";
        return false;
    }
    int64_t now_time = tera::get_millis() / 1000;
    int64_t clean_time = now_time - info_log_expire_sec_;
    LOG(INFO) << "[LogCleaner] Start clean log dir: " << info_log_dir_ 
              << ", now_time = " << now_time 
              << ", clean_time = " << clean_time;

    long path_maxlen = pathconf(info_log_dir_.c_str(), _PC_PATH_MAX);
    std::vector<std::string> log_file_list;
    if (!ListCurrentDir(info_log_dir_, &log_file_list)) {
        // list failed
        LOG(WARNING) << "[LogCleaner] List log dir " << info_log_dir_
                     << " failed. Cancel clean.";
        return false;
    }

    // reserved_set: filenames that should not to be clean
    std::set<std::string> reserved_set;
    if (!GetCurrentOpendLogs(&reserved_set)) {
        LOG(WARNING) << "[LogCleaner] GetCurrentOpendLogs failed. Cancel clean.";
        return false;
    }

    std::vector<std::string>::const_iterator it = log_file_list.begin();
    for (; it != log_file_list.end(); ++it) {
        if (reserved_set.find(*it) != reserved_set.end()) {
            // already reserved
            continue;
        }
            
        const std::string& file_name = *it;
            
        // check if filename start with log_prefix_
        // if leveldb_log_prefix_ is not empty, check also
        if (!CheckLogPrefix(file_name)) {
            VLOG(16) << "[LogCleaner] Reserve log file: " << file_name
                     << ", which not match prefix.";
            reserved_set.insert(file_name);
            continue;
        }
            
        // get file stat
        std::string file_path = info_log_dir_ + "/" + file_name;
        struct stat file_st;
        if (lstat(file_path.c_str(), &file_st) != 0) {
            // cancel clean if any file stat failed
            LOG(WARNING) << "[LogCleaner] Stat log file: " << file_path << " fail. Cancel log clean.";
            return false;
        }
            
        if (S_ISLNK(file_st.st_mode)) {
            // handle symbolic link
            VLOG(16) << "[LogCleaner] Reserve symbolic link log: " << file_name;
            reserved_set.insert(file_name);
            char path_buf[path_maxlen];
            int ret = readlink(file_path.c_str(), path_buf, path_maxlen);
            if (ret < 0 || ret >= path_maxlen) {
                continue;
            } else {
                // reserve link target
                path_buf[ret] = '\0';
                std::string target_filename = GetFileNameFromPath(path_buf);
                VLOG(16) << "[LogCleaner] Reserve link target: " << target_filename
                         << " for link: " << file_path;
                reserved_set.insert(target_filename);
            }
        } else if (!S_ISREG(file_st.st_mode)) {
            VLOG(16)  << "[LogCleaner] Reserve not regular file: " << file_name;
            reserved_set.insert(file_name);
        } else if (file_st.st_mtime >= clean_time) {
            VLOG(16)  << "[LogCleaner] Reserve not expire log: " << file_name
                      << ", mtime: " << file_st.st_mtime << ", clean_time: " << clean_time;
            reserved_set.insert(file_name);
        }
        VLOG(16) << "stat filename: " << file_name
                 << ", is_symbolic_link: " << S_ISLNK(file_st.st_mode)
                 << ", is_dir: " << S_ISDIR(file_st.st_mode)
                 << ", is_regular_file: " << S_ISREG(file_st.st_mode)
                 << ", last mod time: " << file_st.st_mtime
                 << ", link number: " << file_st.st_nlink
                 << ", reserve: " << (reserved_set.find(file_name) != reserved_set.end());
    }

    // clean log
    size_t clean_cnt = 0;
    it = log_file_list.begin();
    for (; it != log_file_list.end(); ++it) {
        const std::string &file_name = *it;
        std::string file_path = info_log_dir_ + "/" + file_name;
        if (reserved_set.find(file_name) == reserved_set.end()) {
            LOG(INFO) << "[LogCleaner] log: " << file_path << " will be clean";
            if (!RemoveLocalFile(file_path)){
                LOG(WARNING) << "[LogCleaner] log clean fail: " << file_path;
            } else {
                ++clean_cnt;
            }
        }
    }
    LOG(INFO) << "[LogCleaner] Found log: " << log_file_list.size()
              << ", clean: " << clean_cnt;
    return true;
}

bool LogCleaner::GetCurrentOpendLogs(std::set<std::string>* opend_logs) {
    long path_maxlen = pathconf(proc_fd_path_.c_str(), _PC_PATH_MAX);
    if (path_maxlen < 0) {
        LOG(ERROR) << "[LogCleaner] Get Path Max Len Failed";
        return false;
    }
    std::vector<FileStateInfo> opend_logs_list;
    VLOG(16) << "[LogCleaner] Search fd_path: " << proc_fd_path_;
    if (!ListCurrentDirWithStat(proc_fd_path_, &opend_logs_list)) {
        VLOG(16) << "[LogCleaner] list fd_path: " << proc_fd_path_ << " failed.";
        return false;
    }

    std::vector<FileStateInfo>::const_iterator it = opend_logs_list.begin();
    for (; it != opend_logs_list.end(); ++it) {
        const std::string& filename = it->first;
        const struct stat& st = it->second;
        if (S_ISLNK(st.st_mode)) {
            char path_buf[path_maxlen];
            int ret = readlink(filename.c_str(), path_buf, path_maxlen);
            if (ret > 0 && ret < path_maxlen && path_buf[0] == '/') {
                path_buf[ret] = '\0';
                std::string target_filename = GetFileNameFromPath(path_buf);
                VLOG(16) << "[LogCleaner] Reserve log in use: " << target_filename;
                opend_logs->insert(target_filename);
            }
        }
    }
    return true;
}
 
} // end namespace common
 
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
