// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
#include <map>
#include <queue>
#include <set>
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <malloc.h>
#include <sys/syscall.h>
#include <sys/time.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>
#include "leveldb/env.h"
#include "leveldb/slice.h"
#include "port/port.h"
#include "util/logging.h"
#include "util/mutexlock.h"
#include "util/posix_logger.h"
#include "util/string_ext.h"
#include "util/thread_pool.h"
#include "../common/counter.h"
#include "util/env_posix.h"

namespace leveldb {

tera::Counter posix_read_size_counter;
tera::Counter posix_write_size_counter;

tera::Counter posix_read_counter;
tera::Counter posix_write_counter;
tera::Counter posix_sync_counter;
tera::Counter posix_list_counter;
tera::Counter posix_exists_counter;
tera::Counter posix_open_counter;
tera::Counter posix_close_counter;
tera::Counter posix_delete_counter;
tera::Counter posix_tell_counter;
tera::Counter posix_seek_counter;
tera::Counter posix_info_counter;
tera::Counter posix_other_counter;

// Reference from rocksdb
namespace {
size_t GetLogicalBufferSize(int fd) {
  struct stat buf;
  int result = fstat(fd, &buf);
  if (result == -1) {
    return kDefaultPageSize;
  }
  if (major(buf.st_dev) == 0) {
    // Unnamed devices (e.g. non-device mounts), reserved as null device number.
    // These don't have an entry in /sys/dev/block/. Return a sensible default.
    return kDefaultPageSize;
  }

  // Reading queue/logical_block_size does not require special permissions.
  const int kBufferSize = 100;
  char path[kBufferSize];
  char real_path[PATH_MAX + 1];
  snprintf(path, kBufferSize, "/sys/dev/block/%u:%u", major(buf.st_dev),
           minor(buf.st_dev));
  if (realpath(path, real_path) == nullptr) {
    return kDefaultPageSize;
  }
  std::string device_dir(real_path);
  if (!device_dir.empty() && device_dir.back() == '/') {
    device_dir.pop_back();
  }
  // NOTE: sda3 does not have a `queue/` subdir, only the parent sda has it.
  // $ ls -al '/sys/dev/block/8:3'
  // lrwxrwxrwx. 1 root root 0 Jun 26 01:38 /sys/dev/block/8:3 ->
  // ../../block/sda/sda3
  size_t parent_end = device_dir.rfind('/', device_dir.length() - 1);
  if (parent_end == std::string::npos) {
    return kDefaultPageSize;
  }
  size_t parent_begin = device_dir.rfind('/', parent_end - 1);
  if (parent_begin == std::string::npos) {
    return kDefaultPageSize;
  }
  if (device_dir.substr(parent_begin + 1, parent_end - parent_begin - 1) !=
      "block") {
    device_dir = device_dir.substr(0, parent_end);
  }
  std::string fname = device_dir + "/queue/logical_block_size";
  FILE* fp;
  size_t size = 0;
  fp = fopen(fname.c_str(), "r");
  if (fp != nullptr) {
    char* line = nullptr;
    size_t len = 0;
    if (getline(&line, &len, fp) != -1) {
      sscanf(line, "%zu", &size);
    }
    free(line);
    fclose(fp);
  }
  if (size != 0 && (size & (size - 1)) == 0) {
    return size;
  }
  return kDefaultPageSize;
}
} //  namespace

namespace {

static Status IOError(const std::string& context, int err_number) {
  if (err_number == EACCES) {
    return Status::IOPermissionDenied(context, strerror(err_number));
  }
  return Status::IOError(context, strerror(err_number));
}

class PosixSequentialFile: public SequentialFile {
 private:
  std::string filename_;
  FILE* file_;

 public:
  PosixSequentialFile(const std::string& fname, FILE* f)
      : filename_(fname), file_(f) { }
  virtual ~PosixSequentialFile() { fclose(file_); }

  virtual Status Read(size_t n, Slice* result, char* scratch) {
    posix_read_counter.Inc();
    Status s;
    size_t r = fread_unlocked(scratch, 1, n, file_);
    *result = Slice(scratch, r);
    if (r < n) {
      if (feof(file_)) {
        // We leave status as ok if we hit the end of the file
      } else {
        // A partial read with an error: return a non-ok status
        s = IOError(filename_, errno);
      }
    }
    posix_read_size_counter.Add(r);
    posix_fadvise(fileno(file_), 0, 0, POSIX_FADV_DONTNEED);
    return s;
  }

  virtual Status Skip(uint64_t n) {
    posix_seek_counter.Inc();
    if (fseek(file_, n, SEEK_CUR)) {
      return IOError(filename_, errno);
    }
    return Status::OK();
  }
};

// pread() based random-access
class PosixRandomAccessFile: public RandomAccessFile {
 private:
  std::string filename_;
  int fd_;
  EnvOptions env_opt_;
  int logical_sector_size_;

 public:
  PosixRandomAccessFile(const std::string& fname, int fd, const EnvOptions& options)
      : filename_(fname),
        fd_(fd),
        env_opt_(options),
        logical_sector_size_(GetLogicalBufferSize(fd_)) { }

  virtual ~PosixRandomAccessFile() { close(fd_); }

  virtual Status Read(uint64_t offset, size_t n, Slice* result,
                      char* scratch) const {
    posix_read_counter.Inc();
    Status s;
    ssize_t r = pread(fd_, scratch, n, static_cast<off_t>(offset));
    *result = Slice(scratch, (r < 0) ? 0 : r);
    if (r < 0) {
      // An error: return a non-ok status
      s = IOError(filename_, errno);
    } else {
      posix_read_size_counter.Add(r);
    }
    if (!env_opt_.use_direct_io_read) {
        posix_fadvise(fd_, 0, 0, POSIX_FADV_DONTNEED);
    }
    return s;
  }

  virtual size_t GetRequiredBufferAlignment() const {
    return logical_sector_size_;
  }

};

// Helper class to limit mmap file usage so that we do not end up
// running out virtual memory or running into kernel performance
// problems for very large databases.
class MmapLimiter {
 public:
  MmapLimiter() {
    //Disable mmap in tera for reducing memory use.
    SetAllowed(0);

    // Up to 1000 mmaps for 64-bit binaries; none for smaller pointer sizes.
    //SetAllowed(sizeof(void*) >= 8 ? 1000 : 0);
    //If you want to enable mmap, uncomment the line above.
  }

  // If another mmap slot is available, acquire it and return true.
  // Else return false.
  bool Acquire() {
    if (GetAllowed() <= 0) {
      return false;
    }
    MutexLock l(&mu_);
    intptr_t x = GetAllowed();
    if (x <= 0) {
      return false;
    } else {
      SetAllowed(x - 1);
      return true;
    }
  }

  // Release a slot acquired by a previous call to Acquire() that returned true.
  void Release() {
    MutexLock l(&mu_);
    SetAllowed(GetAllowed() + 1);
  }

 private:
  port::Mutex mu_;
  port::AtomicPointer allowed_;

  intptr_t GetAllowed() const {
    return reinterpret_cast<intptr_t>(allowed_.Acquire_Load());
  }

  // REQUIRES: mu_ must be held
  void SetAllowed(intptr_t v) {
    allowed_.Release_Store(reinterpret_cast<void*>(v));
  }

  MmapLimiter(const MmapLimiter&);
  void operator=(const MmapLimiter&);
};

// mmap() based random-access
class PosixMmapReadableFile: public RandomAccessFile {
 private:
  std::string filename_;
  void* mmapped_region_;
  size_t length_;
  MmapLimiter* limiter_;

 public:
  // base[0,length-1] contains the mmapped contents of the file.
  PosixMmapReadableFile(const std::string& fname, void* base, size_t length,
                        MmapLimiter* limiter)
      : filename_(fname), mmapped_region_(base), length_(length),
        limiter_(limiter) {
  }

  virtual ~PosixMmapReadableFile() {
    munmap(mmapped_region_, length_);
    limiter_->Release();
  }

  virtual Status Read(uint64_t offset, size_t n, Slice* result,
                      char* scratch) const {
    posix_read_counter.Inc();
    Status s;
    if (offset + n > length_) {
      *result = Slice();
      s = IOError(filename_, EINVAL);
    } else {
      *result = Slice(reinterpret_cast<char*>(mmapped_region_) + offset, n);
      posix_read_size_counter.Add(n);
    }
    return s;
  }
};

// We preallocate up to an extra megabyte and use memcpy to append new
// data to the file.  This is safe since we either properly close the
// file before reading from it, or for log files, the reading code
// knows enough to skip zero suffixes.
class PosixMmapFile : public WritableFile {
 private:
  std::string filename_;
  int fd_;
  size_t page_size_;
  size_t map_size_;       // How much extra memory to map at a time
  char* base_;            // The mapped region
  char* limit_;           // Limit of the mapped region
  char* dst_;             // Where to write next  (in range [base_,limit_])
  char* last_sync_;       // Where have we synced up to
  uint64_t file_offset_;  // Offset of base_ in file

  // Have we done an munmap of unsynced data?
  bool pending_sync_;

  // Roundup x to a multiple of y
  static size_t Roundup(size_t x, size_t y) {
    return ((x + y - 1) / y) * y;
  }

  size_t TruncateToPageBoundary(size_t s) {
    s -= (s & (page_size_ - 1));
    assert((s % page_size_) == 0);
    return s;
  }

  bool UnmapCurrentRegion() {
    bool result = true;
    if (base_ != NULL) {
      if (last_sync_ < limit_) {
        // Defer syncing this data until next Sync() call, if any
        pending_sync_ = true;
      }
      if (munmap(base_, limit_ - base_) != 0) {
        result = false;
      }
      file_offset_ += limit_ - base_;
      base_ = NULL;
      limit_ = NULL;
      last_sync_ = NULL;
      dst_ = NULL;

      // Increase the amount we map the next time, but capped at 1MB
      if (map_size_ < (1<<20)) {
        map_size_ *= 2;
      }
    }
    return result;
  }

  bool MapNewRegion() {
    assert(base_ == NULL);
    if (ftruncate(fd_, file_offset_ + map_size_) < 0) {
      return false;
    }
    void* ptr = mmap(NULL, map_size_, PROT_READ | PROT_WRITE, MAP_SHARED,
                     fd_, file_offset_);
    if (ptr == MAP_FAILED) {
      return false;
    }
    base_ = reinterpret_cast<char*>(ptr);
    limit_ = base_ + map_size_;
    dst_ = base_;
    last_sync_ = base_;
    return true;
  }

 public:
  PosixMmapFile(const std::string& fname, int fd, size_t page_size)
      : filename_(fname),
        fd_(fd),
        page_size_(page_size),
        map_size_(Roundup(65536, page_size)),
        base_(NULL),
        limit_(NULL),
        dst_(NULL),
        last_sync_(NULL),
        file_offset_(0),
        pending_sync_(false) {
    assert((page_size & (page_size - 1)) == 0);
  }


  ~PosixMmapFile() {
    if (fd_ >= 0) {
      PosixMmapFile::Close();
    }
  }

  virtual Status Append(const Slice& data) {
    posix_write_counter.Inc();
    const char* src = data.data();
    size_t left = data.size();
    while (left > 0) {
      assert(base_ <= dst_);
      assert(dst_ <= limit_);
      size_t avail = limit_ - dst_;
      if (avail == 0) {
        if (!UnmapCurrentRegion() ||
            !MapNewRegion()) {
          return IOError(filename_, errno);
        }
      }

      size_t n = (left <= avail) ? left : avail;
      memcpy(dst_, src, n);
      dst_ += n;
      src += n;
      left -= n;
      posix_write_size_counter.Add(n);
    }
    return Status::OK();
  }

  virtual Status Close() {
    posix_close_counter.Inc();
    Status s;
    size_t unused = limit_ - dst_;
    if (!UnmapCurrentRegion()) {
      s = IOError(filename_, errno);
    } else if (unused > 0) {
      // Trim the extra space at the end of the file
      if (ftruncate(fd_, file_offset_ - unused) < 0) {
        s = IOError(filename_, errno);
      }
    }

    if (close(fd_) < 0) {
      if (s.ok()) {
        s = IOError(filename_, errno);
      }
    }

    fd_ = -1;
    base_ = NULL;
    limit_ = NULL;
    return s;
  }

  virtual Status Flush() {
    return Status::OK();
  }

  virtual Status Sync() {
    posix_sync_counter.Inc();
    Status s;

    if (pending_sync_) {
      // Some unmapped data was not synced
      pending_sync_ = false;
      if (fdatasync(fd_) < 0) {
        s = IOError(filename_, errno);
      }
    }

    if (dst_ > last_sync_) {
      // Find the beginnings of the pages that contain the first and last
      // bytes to be synced.
      size_t p1 = TruncateToPageBoundary(last_sync_ - base_);
      size_t p2 = TruncateToPageBoundary(dst_ - base_ - 1);
      last_sync_ = dst_;
      if (msync(base_ + p1, p2 - p1 + page_size_, MS_SYNC) < 0) {
        s = IOError(filename_, errno);
      }
    }
    return s;
  }
};



static int LockOrUnlock(int fd, bool lock) {
  errno = 0;
  struct flock f;
  memset(&f, 0, sizeof(f));
  f.l_type = (lock ? F_WRLCK : F_UNLCK);
  f.l_whence = SEEK_SET;
  f.l_start = 0;
  f.l_len = 0;        // Lock/unlock entire file
  return fcntl(fd, F_SETLK, &f);
}

class PosixFileLock : public FileLock {
 public:
  int fd_;
  std::string name_;
};

// Set of locked files.  We keep a separate set instead of just
// relying on fcntrl(F_SETLK) since fcntl(F_SETLK) does not provide
// any protection against multiple uses from the same process.
class PosixLockTable {
 private:
  port::Mutex mu_;
  std::set<std::string> locked_files_;
 public:
  bool Insert(const std::string& fname) {
    MutexLock l(&mu_);
    return locked_files_.insert(fname).second;
  }
  void Remove(const std::string& fname) {
    MutexLock l(&mu_);
    locked_files_.erase(fname);
  }
};

class PosixEnv : public Env {
 public:
  PosixEnv();
  virtual ~PosixEnv() {
    for (size_t i=0; i<threads_to_join_.size(); i++) {
      pthread_join(threads_to_join_[i], NULL);
    }
  }

  virtual Status NewSequentialFile(const std::string& fname,
                                   SequentialFile** result) {
    posix_open_counter.Inc();
    FILE* f = fopen(fname.c_str(), "r");
    if (f == NULL) {
      *result = NULL;
      return IOError(fname, errno);
    } else {
      *result = new PosixSequentialFile(fname, f);
      return Status::OK();
    }
  }

  virtual Status NewRandomAccessFile(const std::string& fname,
                                     RandomAccessFile** result,
                                     const EnvOptions& options) {
    posix_open_counter.Inc();
    *result = NULL;
    Status s;
    int flags = O_RDONLY;
    if (options.use_direct_io_read) {
        flags |= O_DIRECT;
    }
    int fd = open(fname.c_str(), flags);
    if (fd < 0) {
      s = IOError(fname, errno);
    } else if (!options.use_direct_io_read && mmap_limit_.Acquire()) {
      uint64_t size;
      s = GetFileSize(fname, &size);
      if (s.ok()) {
        void* base = mmap(NULL, size, PROT_READ, MAP_SHARED, fd, 0);
        if (base != MAP_FAILED) {
          *result = new PosixMmapReadableFile(fname, base, size, &mmap_limit_);
        } else {
          s = IOError(fname, errno);
        }
      }
      close(fd);
      if (!s.ok()) {
        mmap_limit_.Release();
      }
    } else {
      *result = new PosixRandomAccessFile(fname, fd, options);
    }
    return s;
  }

  virtual Status NewRandomAccessFile(const std::string& fname, uint64_t fsize,
                                     RandomAccessFile** result,
                                     const EnvOptions& options) {
    return NewRandomAccessFile(fname, result, options);
  }

  virtual Status NewWritableFile(const std::string& fname,
                                 WritableFile** result,
                                 const EnvOptions& options) {
    posix_open_counter.Inc();
    Status s;
    const int fd = options.use_direct_io_write ?
                   open(fname.c_str(), O_CREAT | O_WRONLY | O_TRUNC | O_DIRECT, 0644) :
                   open(fname.c_str(), O_CREAT | O_WRONLY | O_TRUNC, 0644);
    if (fd < 0) {
      *result = NULL;
      s = IOError(fname, errno);
    } else {
      *result = new PosixWritableFile(fname, fd, options);
    }
    return s;
  }

  virtual Status FileExists(const std::string& fname) {
    posix_exists_counter.Inc();
    int32_t retval = access(fname.c_str(), F_OK);
    if (retval == 0) {
      return Status::OK();
    } else if (errno == ENOENT) {
      return Status::NotFound("filestatus", fname);
    } else {
      return Status::IOError(fname);
    }
  }

  virtual Status GetChildren(const std::string& dir, std::vector<std::string>* result) {
    posix_list_counter.Inc();
    result->clear();
    DIR* d = opendir(dir.c_str());
    if (d == NULL) {
      return IOError(dir, errno);
    }
    struct dirent* entry;
    while ((entry = readdir(d)) != NULL) {
      if (strcmp(entry->d_name, ".") == 0 ||
          strcmp(entry->d_name, "..") == 0) {
        continue;
      }
      result->push_back(entry->d_name);
    }
    closedir(d);
    return Status::OK();
  }

  virtual Status DeleteFile(const std::string& fname) {
    posix_delete_counter.Inc();
    Status result;
    if (unlink(fname.c_str()) != 0) {
      result = IOError(fname, errno);
    }
    return result;
  }

  virtual Status CreateDir(const std::string& name) {
    posix_other_counter.Inc();
    Status result;
    std::vector<std::string> items;
    SplitString(name, "/", &items);
    std::string path;
    if (name[0] == '/') {
        path = "/";
    }
    for (uint32_t i = 0; i < items.size() && result.ok(); ++i) {
        path += items[i];
        if (mkdir(path.c_str(), 0755) != 0 && errno != EEXIST) {
            result = IOError(path, errno);
        }
        path += "/";
    }

    return result;
  }

  Status DeleteDirRecursive(const std::string& name) {
    posix_delete_counter.Inc();
    Status s;
    std::vector<std::string> files;
    s = GetChildren(name, &files);
    if (!s.ok()) {
      return s;
    }
    for (size_t i=0; i<files.size(); i++) {
      if (files[i] == "." || files[i] == "..") {
        continue;
      }
      std::string entry_name = name+'/'+files[i];
      struct stat info;
      if (0 != stat(entry_name.c_str(), &info)) {
        s = IOError(entry_name, errno);
        break;
      }
      if (S_ISDIR(info.st_mode)) {
        //fprintf(stderr, "Recursive delete %s\n", entry_name.c_str());
        s = DeleteDirRecursive(entry_name);
        if (!s.ok()) {
          break;
        }
      } else {
        s = DeleteFile(entry_name);
        if (!s.ok()) {
          break;
        }
      }
    }
    if (s.ok()) {
      s = DeleteDir(name);
    }
    return s;
  }
  virtual Status DeleteDir(const std::string& name) {
    posix_delete_counter.Inc();
    Status result;
    if (rmdir(name.c_str()) != 0) {
      result = IOError(name, errno);
    }
    return result;
  }

  virtual Status CopyFile(const std::string& from, const std::string& to) {
    if (from != to) {
      posix_other_counter.Inc();
      std::string cmd("cp -rf ");
      cmd += (from + " " + to);
      system(cmd.c_str());
    }
    return Status::OK();
  }

  virtual Status GetFileSize(const std::string& fname, uint64_t* size) {
    posix_info_counter.Inc();
    Status s;
    struct stat sbuf;
    if (stat(fname.c_str(), &sbuf) != 0) {
      *size = 0;
      s = IOError(fname, errno);
    } else {
      *size = sbuf.st_size;
    }
    return s;
  }

  virtual Status RenameFile(const std::string& src, const std::string& target) {
    posix_other_counter.Inc();
    Status result;
    if (rename(src.c_str(), target.c_str()) != 0) {
      result = IOError(src, errno);
    }
    return result;
  }

  virtual Status LockFile(const std::string& fname, FileLock** lock) {
    *lock = NULL;
    Status result;
    int fd = open(fname.c_str(), O_RDWR | O_CREAT, 0644);
    if (fd < 0) {
      result = IOError(fname, errno);
    } else if (!locks_.Insert(fname)) {
      close(fd);
      result = Status::IOError("lock " + fname, "already held by process");
    } else if (LockOrUnlock(fd, true) == -1) {
      result = IOError("lock " + fname, errno);
      close(fd);
      locks_.Remove(fname);
    } else {
      PosixFileLock* my_lock = new PosixFileLock;
      my_lock->fd_ = fd;
      my_lock->name_ = fname;
      *lock = my_lock;
    }
    return result;
  }

  virtual Status UnlockFile(FileLock* lock) {
    PosixFileLock* my_lock = reinterpret_cast<PosixFileLock*>(lock);
    Status result;
    if (LockOrUnlock(my_lock->fd_, false) == -1) {
      result = IOError("unlock", errno);
    }
    locks_.Remove(my_lock->name_);
    close(my_lock->fd_);
    if (remove(my_lock->name_.c_str()) == -1) {
      result = IOError("unlock", errno);
    }
    delete my_lock;
    return result;
  }

  virtual Env* CacheEnv() { return this; }

  virtual int64_t Schedule(void (*function)(void*), void* arg, double prio,
                           int64_t wait_time_millisec);

  virtual void ReSchedule(int64_t id, double prio, int64_t millisec);

  virtual void StartThread(void (*function)(void* arg), void* arg);

  virtual Status GetTestDirectory(std::string* result) {
    const char* env = getenv("TEST_TMPDIR");
    if (env && env[0] != '\0') {
      *result = env;
    } else {
      char buf[100];
      snprintf(buf, sizeof(buf), "/tmp/leveldbtest-%d", int(gettid()));
      *result = buf;
    }
    // Directory may already exist
    CreateDir(*result);
    return Status::OK();
  }

  static uint64_t gettid() {
#ifdef OS_LINUX
    pid_t tid = syscall(SYS_gettid);
#else
    pthread_t tid = pthread_self();
#endif
    uint64_t thread_id = 0;
    memcpy(&thread_id, &tid, std::min(sizeof(thread_id), sizeof(tid)));
    return thread_id;
  }

  virtual Status NewLogger(const std::string& fname, Logger** result) {
    *result = new EnhancePosixLogger(fname, &PosixEnv::gettid);
    return Status::OK();

    /*
    FILE* f = fopen(fname.c_str(), "w");
    if (f == NULL) {
      *result = NULL;
      return IOError(fname, errno);
    } else {
      *result = new PosixLogger(f, &PosixEnv::gettid);
      return Status::OK();
    }
    */
  }
  virtual void SetLogger(Logger* logger) {
    Logger::SetDefaultLogger(logger);
    info_log_ = logger;
    thread_pool_.SetLogger(logger);
  }

  virtual uint64_t NowMicros() {
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    return static_cast<int64_t>(ts.tv_sec) * 1000000 + static_cast<int64_t>(ts.tv_nsec) / 1000;
  }

  virtual void SleepForMicroseconds(int micros) {
    usleep(micros);
  }

  // Allow increasing the number of worker threads.
  virtual int SetBackgroundThreads(int num) {
    thread_pool_.SetBackgroundThreads(num);
    return thread_pool_.GetThreadNumber();
  }

 private:
  static void PthreadCall(const char* label, int result) {
    if (result != 0) {
      fprintf(stderr, "pthread %s: %s\n", label, strerror(result));
      abort();
    }
  }

  // BGThread() is the body of the background thread
  void BGThread();
  static void* BGThreadWrapper(void* arg) {
    reinterpret_cast<PosixEnv*>(arg)->BGThread();
    return NULL;
  }

  size_t page_size_;

  PosixLockTable locks_;
  MmapLimiter mmap_limit_;

  static Logger* info_log_;
  ThreadPool thread_pool_;

  pthread_mutex_t mu_;
  std::vector<pthread_t> threads_to_join_;

};

Logger* PosixEnv::info_log_ = NULL;

PosixEnv::PosixEnv() : page_size_(getpagesize()) {
  PthreadCall("mutex_init", pthread_mutex_init(&mu_, NULL));
}

int64_t PosixEnv::Schedule(void (*function)(void*), void* arg, double prio,
                           int64_t wait_time_millisec = 0) {
  return thread_pool_.Schedule(function, arg, prio, wait_time_millisec);
}

void PosixEnv::ReSchedule(int64_t id, double prio, int64_t millisec = -1) {
  return thread_pool_.ReSchedule(id, prio, millisec);
}

namespace {
  struct StartThreadState {
    void (*user_function)(void*);
    void* arg;
  };
}
static void* StartThreadWrapper(void* arg) {
  StartThreadState* state = reinterpret_cast<StartThreadState*>(arg);
  state->user_function(state->arg);
  delete state;
  return NULL;
}

void PosixEnv::StartThread(void (*function)(void* arg), void* arg) {
  pthread_t t;
  StartThreadState* state = new StartThreadState;
  state->user_function = function;
  state->arg = arg;
  PthreadCall("start thread",
      pthread_create(&t, NULL,  &StartThreadWrapper, state));
  PthreadCall("lock", pthread_mutex_lock(&mu_));
  threads_to_join_.push_back(t);
  PthreadCall("unlock", pthread_mutex_unlock(&mu_));
}

}  // namespace

static pthread_once_t once = PTHREAD_ONCE_INIT;
static Env* default_env;
static void InitDefaultEnv() { default_env = new PosixEnv; }

Env* Env::Default() {
  pthread_once(&once, InitDefaultEnv);
  return default_env;
}

Env* NewPosixEnv() {
  return new PosixEnv;
}

PosixWritableFile::PosixWritableFile(const std::string& fname,
                  int fd,
                  const EnvOptions& options)
  : filename_(fname),
    fd_(fd),
    pos_(0),
    is_dio_(options.use_direct_io_write),
    buffer_size_(options.posix_write_buffer_size),
    align_size_(GetLogicalBufferSize(fd_)) {
  //Buffer size should never be set to zero
  assert(buffer_size_ > 0);
  if (is_dio_) {
    //See format.cc:76 DirectIOAlign() about this code.
    buffer_size_ = buffer_size_ % align_size_ == 0 ? buffer_size_ :
                   (buffer_size_ + align_size_ - 1) & (~(align_size_ - 1));
    buf_ = (char*)memalign(align_size_, buffer_size_);
    //fprintf(stderr, "Dio: %s, Aligned Buffer Size: %lu\n", filename_.c_str(), buffer_size_);
  } else {
    buf_ = (char*)malloc(buffer_size_);
  }
}

PosixWritableFile::~PosixWritableFile() {
  if (fd_ >= 0) {
    // Ignoring any potential errors
    posix_close_counter.Inc();
    Close();
  }
  free(buf_);
  buf_ = NULL;
}

Status PosixWritableFile::Append(const Slice& data) {
  size_t n = data.size();
  const char* p = data.data();

  // Fit as much as possible into buffer.
  size_t copy = std::min(n, buffer_size_ - pos_);
  memcpy(buf_ + pos_, p, copy);
  p += copy;
  n -= copy;
  pos_ += copy;
  if (n == 0) {
    return Status::OK();
  }

  // Can't fit in buffer, so need to do at least one write.
  Status s = FlushBuffered();
  if (!s.ok()) {
    return s;
  }

  // In DIO: Small writes go to buffer, large writes are appended again for aligned write.
  // Not In DIO: Small writes go to buffer, large writes are written directly.
  if (n < buffer_size_) {
    memcpy(buf_, p, n);
    pos_ = n;
    return Status::OK();
  }

  if (is_dio_) {
    return Append(Slice(p, n));
  } else {
    return WriteRaw(p, n);
  }
}

Status PosixWritableFile::Close() {
  posix_close_counter.Inc();
  Status result = FlushBuffered();
  const int r = close(fd_);
  if (r < 0 && result.ok()) {
    result = IOError(filename_, errno);
  }
  fd_ = -1;
  return result;
}

Status PosixWritableFile::Flush() {
  return FlushBuffered();
}

Status PosixWritableFile::SyncDirIfManifest() {
  const char* f = filename_.c_str();
  const char* sep = strrchr(f, '/');
  Slice basename;
  std::string dir;
  if (sep == nullptr) {
    dir = ".";
    basename = f;
  } else {
    dir = std::string(f, sep - f);
    basename = sep + 1;
  }
  Status s;
  if (basename.starts_with("MANIFEST")) {
    int fd = open(dir.c_str(), O_RDONLY);
    if (fd < 0) {
      s = IOError(dir, errno);
    } else {
      if (fsync(fd) < 0) {
        s = IOError(dir, errno);
      }
      close(fd);
    }
  }
  return s;
}

Status PosixWritableFile::Sync() {
  posix_sync_counter.Inc();
  // Ensure new files referred to by the manifest are in the filesystem.
  Status s = SyncDirIfManifest();
  if (!s.ok()) {
    return s;
  }
  s = FlushBuffered();
  if (s.ok()) {
    if (fdatasync(fd_) != 0) {
      s = IOError(filename_, errno);
    }
  }
  return s;
}

Status PosixWritableFile::LeaveDio() {
  int flags = fcntl(fd_, F_GETFL, 0);
  if(flags < 0) {
    return IOError(filename_, errno);
  }
  flags &= ~O_DIRECT;
  if(fcntl(fd_, F_SETFL, flags) < 0) {
    return IOError(filename_, errno);
  }
  is_dio_ = false;
  return Status::OK();
}

Status PosixWritableFile::FlushBuffered() {
  Status s;
  // Once flushed with not aligned buffer_size_, 
  // we flush the last aligned buffer, and leave
  // dio mode, and never enter again.
  if (is_dio_ && 
      (pos_ & (align_size_ - 1)) != 0) {
    // For example, assume buffer_size_ is 524288 (aka 512kB), and align_size is 512 bytes.
    // When user write 400000 bytes to buffer and call Flush(). We do followed steps:
    // 1. get the maximum aligned size for a dio write by caculationg 400000 & (~(512 -1)) = 399872.
    size_t aligned_pos = pos_ & (~(align_size_ - 1));
    if (aligned_pos != 0) {
    // 2. Write this 399872 bytes to file in dio mode.
      s = WriteRaw(buf_, aligned_pos);
    }
    // 3. Leave Dio Mode (never enter again).
    s = LeaveDio();
    if (!s.ok()) {
      return s;
    }
    // 4. Write the last 400000 - 399872 = 128 bytes to file in page io mode.
    s = WriteRaw(buf_ + aligned_pos, pos_ - aligned_pos);
  } else {
    s = WriteRaw(buf_, pos_);
  }
  pos_ = 0;
  return s;
}

Status PosixWritableFile::WriteRaw(const char* p, size_t n) {
  posix_write_counter.Inc();
  posix_write_size_counter.Add(n);
  while (n > 0) {
    ssize_t r = write(fd_, p, n);
    if (r < 0) {
      if (errno == EINTR) {
        continue;  // Retry
      }
      return IOError(filename_, errno);
    }
    p += r;
    n -= r;
  }
  return Status::OK();
}

}  // namespace leveldb
