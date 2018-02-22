// Copyright (c) 2016, Baidu.com, Inc. All Rights Reserved
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
#include <sys/syscall.h>
#include <sys/time.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>

#include "leveldb/env_mock.h"
#include "leveldb/env.h"
#include "leveldb/slice.h"
#include "port/port.h"
#include "util/logging.h"
#include "util/mutexlock.h"
#include "util/posix_logger.h"
#include "util/string_ext.h"
#include "util/thread_pool.h"

namespace leveldb {

static std::string mock_path_prefix = "/tmp/mock-env/";

static std::string MockPath(const std::string& o)
{
    return mock_path_prefix + o;
}

void MockEnv::SetPrefix(const std::string& p)
{
    mock_path_prefix = p + "/";
}

// Log error message
static Status IOError(const std::string& context, int err_number)
{
    if (err_number == EACCES) {
        return Status::IOPermissionDenied(context, strerror(err_number));
    }
    return Status::IOError(context, strerror(err_number));
}


MockEnv::MockEnv() : EnvWrapper(Env::Default())
{
}

MockEnv::~MockEnv()
{
}

static bool (*SequentialFileRead)(int32_t i, char* scratch, size_t* mock_size);
static int32_t iSequentialFileRead;
void MockEnv::SetSequentialFileReadCallback(bool (*p)(int32_t i, char* scratch, size_t* mock_size))
{
    SequentialFileRead = p;
}

class MockSequentialFile: public SequentialFile {
 private:
  std::string filename_;
  FILE* file_;

 public:
  MockSequentialFile(const std::string& fname, FILE* f)
      : filename_(fname), file_(f) { }
  virtual ~MockSequentialFile() { fclose(file_); }

  virtual Status Read(size_t n, Slice* result, char* scratch) {
    // std::cerr << "[debug] SequentialFileRead:" << filename_ << std::endl;
    iSequentialFileRead++;

    Status s;

    size_t r = fread_unlocked(scratch, 1, n, file_);

    if (SequentialFileRead && SequentialFileRead(iSequentialFileRead, scratch, &r)) {
      *result = Slice(scratch, r);
      std::cerr << "[mockenv] SequentialFileRead:" << result->ToString() << std::endl;
    } else {
      *result = Slice(scratch, r);
    }

    if (r < n) {
      if (feof(file_)) {
        // We leave status as ok if we hit the end of the file
      } else {
        // A partial read with an error: return a non-ok status
        s = IOError(filename_, errno);
      }
    }
    return s;
  }

  virtual Status Skip(uint64_t n) {
    if (fseek(file_, n, SEEK_CUR)) {
      return IOError(filename_, errno);
    }
    return Status::OK();
  }
};

class MockWritableFile : public WritableFile {
 private:
  std::string filename_;
  FILE* file_;

 public:
  MockWritableFile(const std::string& fname, FILE* f)
      : filename_(fname), file_(f) { }

  ~MockWritableFile() {
    if (file_ != NULL) {
      // Ignoring any potential errors
      fclose(file_);
    }
  }

  virtual Status Append(const Slice& data) {
    size_t r = fwrite_unlocked(data.data(), 1, data.size(), file_);
    if (r != data.size()) {
      return IOError(filename_, errno);
    }
    return Status::OK();
  }

  virtual Status Close() {
    Status result;
    if (fclose(file_) != 0) {
      result = IOError(filename_, errno);
    }
    file_ = NULL;
    return result;
  }

  virtual Status Flush() {
    if (fflush_unlocked(file_) != 0) {
      return IOError(filename_, errno);
    }
    return Status::OK();
  }

  Status SyncDirIfManifest() {
    const char* f = filename_.c_str();
    const char* sep = strrchr(f, '/');
    Slice basename;
    std::string dir;
    if (sep == NULL) {
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

  virtual Status Sync() {
    // Ensure new files referred to by the manifest are in the filesystem.
    Status s = SyncDirIfManifest();
    if (!s.ok()) {
      return s;
    }
    if (fflush_unlocked(file_) != 0 ||
        fdatasync(fileno(file_)) != 0) {
      s = Status::IOError(filename_, strerror(errno));
    }
    return s;
  }
};

// pread() based random-access
class MockRandomAccessFile: public RandomAccessFile {
 private:
  std::string filename_;
  int fd_;

 public:
  MockRandomAccessFile(const std::string& fname, int fd)
      : filename_(fname), fd_(fd) { }
  virtual ~MockRandomAccessFile() { close(fd_); }

  virtual Status Read(uint64_t offset, size_t n, Slice* result,
                      char* scratch) const {
    Status s;
    ssize_t r = pread(fd_, scratch, n, static_cast<off_t>(offset));
    *result = Slice(scratch, (r < 0) ? 0 : r);
    if (r < 0) {
      // An error: return a non-ok status
      s = IOError(filename_, errno);
    } else {
    }
    return s;
  }
};

static bool (*NewSequentialFileFailed)(int32_t i, const std::string& fname);
static int32_t iNewSequentialFile;
void MockEnv::SetNewSequentialFileFailedCallback(bool (*p)(int32_t i, const std::string& fname))
{
    NewSequentialFileFailed = p;
}

// SequentialFile
Status MockEnv::NewSequentialFile(const std::string& fname, SequentialFile** result)
{
    iNewSequentialFile++;

    FILE* f = fopen(MockPath(fname).c_str(), "r");
    if (f == NULL) {
      *result = NULL;
      return IOError(MockPath(fname), errno);
    } else {
      if (NewSequentialFileFailed && NewSequentialFileFailed(iNewSequentialFile, fname)) {
        std::cerr << "[mockenv] NewSequentialFile failed" << std::endl;
        fclose(f);
        return Status::IOError("open failed: " + fname);
      } else {
        *result = new MockSequentialFile(MockPath(fname), f);
        return Status::OK();
      }
    }
}

// random read file
Status MockEnv::NewRandomAccessFile(const std::string& fname, RandomAccessFile** result)
{
    *result = NULL;
    Status s;
    int fd = open(MockPath(fname).c_str(), O_RDONLY);
    if (fd < 0) {
      s = IOError(MockPath(fname), errno);
    } else {
      *result = new MockRandomAccessFile(MockPath(fname), fd);
    }
    return s;
}

// writable
Status MockEnv::NewWritableFile(const std::string& fname,
        WritableFile** result)
{
    Status s;
    FILE* f = fopen(MockPath(fname).c_str(), "w");
    if (f == NULL) {
      *result = NULL;
      s = IOError(MockPath(fname), errno);
    } else {
      *result = new MockWritableFile(MockPath(fname), f);
    }
    return s;
}

// FileExists
Status MockEnv::FileExists(const std::string& fname)
{
    int32_t retval = access(MockPath(fname).c_str(), F_OK);
    if (retval == 0) {
        return Status::OK();
    } else if (errno == ENOENT) {
        return Status::NotFound("filestatus", MockPath(fname));
    } else {
        return Status::IOError(MockPath(fname));
    }
}

Status MockEnv::CopyFile(const std::string& from, const std::string& to) {
    abort();
    return Status::OK();
}

static bool (*GetChildrenDrop)(int32_t i, const std::string& fname);
static int32_t iGetChildren;
void MockEnv::SetGetChildrenCallback(bool (*p)(int32_t i, const std::string& fname))
{
    GetChildrenDrop = p;
}
Status MockEnv::GetChildren(const std::string& dir, std::vector<std::string>* result)
{
    iGetChildren++;

    result->clear();
    DIR* d = opendir(MockPath(dir).c_str());
    if (d == NULL) {
      abort();
      return IOError(MockPath(dir), errno);
    }
    struct dirent* entry;
    while ((entry = readdir(d)) != NULL) {
      if (strcmp(entry->d_name, ".") == 0 ||
          strcmp(entry->d_name, "..") == 0) {
        continue;
      }
      if (GetChildrenDrop && GetChildrenDrop(iGetChildren, entry->d_name)) {
          std::cerr << "[mockenv] GetChildren drop:" << entry->d_name << std::endl;
      } else {
          result->push_back(entry->d_name);
      }
    }
    closedir(d);
    return Status::OK();
}

Status MockEnv::DeleteFile(const std::string& fname)
{
    Status result;
    if (unlink(MockPath(fname).c_str()) != 0) {
      result = IOError(MockPath(fname), errno);
    }
    return result;
};

Status MockEnv::CreateDir(const std::string& name)
{
    Status result;
    std::vector<std::string> items;
    SplitString(MockPath(name), "/", &items);
    std::string path;
    if (MockPath(name)[0] == '/') {
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
};

Status MockEnv::DeleteDir(const std::string& name)
{
    Status result;
    if (rmdir(MockPath(name).c_str()) != 0) {
      result = IOError(MockPath(name), errno);
    }
    return result;
};

Status MockEnv::GetFileSize(const std::string& fname, uint64_t* size)
{
    Status s;
    struct stat sbuf;
    if (stat(MockPath(fname).c_str(), &sbuf) != 0) {
      *size = 0;
      s = IOError(MockPath(fname), errno);
    } else {
      *size = sbuf.st_size;
    }
    return s;
}

///
Status MockEnv::RenameFile(const std::string& src, const std::string& target)
{
    Status result;
    if (rename(MockPath(src).c_str(), MockPath(target).c_str()) != 0) {
      result = IOError(MockPath(src), errno);
    }
    return result;
}

Status MockEnv::LockFile(const std::string& fname, FileLock** lock)
{
    *lock = NULL;
    return Status::OK();
}

Status MockEnv::UnlockFile(FileLock* lock)
{
    return Status::OK();
}

void MockEnv::ResetMock()
{
    iGetChildren = 0;
    GetChildrenDrop = NULL;

    iNewSequentialFile = 0;
    NewSequentialFileFailed = NULL;

    iSequentialFileRead = 0;
    SequentialFileRead = NULL;
}


static pthread_once_t g_once = PTHREAD_ONCE_INIT;
static Env* g_mock_env;
static void InitMockEnv() { g_mock_env = new MockEnv; }

Env* NewMockEnv() {
  assert(pthread_once(&g_once, InitMockEnv) == 0);
  return g_mock_env;
}

}  // namespace leveldb
