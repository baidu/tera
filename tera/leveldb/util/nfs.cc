// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include <dlfcn.h>
#include <errno.h>
#include <fcntl.h>
#include <stdlib.h>

#include "nfs.h"
#include "util/mutexlock.h"
#include "util/string_ext.h"
#include "../utils/counter.h"
#include "../../deps/nfs_wrapper.h"

namespace leveldb {

extern tera::Counter dfs_read_delay_counter;
extern tera::Counter dfs_write_delay_counter;

extern tera::Counter dfs_read_counter;
extern tera::Counter dfs_write_counter;
extern tera::Counter dfs_sync_counter;
extern tera::Counter dfs_flush_counter;
extern tera::Counter dfs_list_counter;
extern tera::Counter dfs_other_counter;
extern tera::Counter dfs_exists_counter;
extern tera::Counter dfs_open_counter;
extern tera::Counter dfs_close_counter;
extern tera::Counter dfs_delete_counter;
extern tera::Counter dfs_tell_counter;
extern tera::Counter dfs_info_counter;

extern tera::Counter dfs_read_hang_counter;
extern tera::Counter dfs_write_hang_counter;
extern tera::Counter dfs_sync_hang_counter;
extern tera::Counter dfs_flush_hang_counter;
extern tera::Counter dfs_list_hang_counter;
extern tera::Counter dfs_other_hang_counter;
extern tera::Counter dfs_exists_hang_counter;
extern tera::Counter dfs_open_hang_counter;
extern tera::Counter dfs_close_hang_counter;
extern tera::Counter dfs_delete_hang_counter;
extern tera::Counter dfs_tell_hang_counter;
extern tera::Counter dfs_info_hang_counter;

static const char* (*printVersion)();
static int (*nfsInit)(const char* mountpoint, const char* config_file_path);
static void (*nfsSetComlogLevel)(int loglevel);
static int (*nfsGetErrno)();

static int (*nfsMkdir)(const char* path);
static int (*nfsRmdir)(const char* path);
static nfs::NFSDIR* (*nfsOpendir)(const char* path);
static struct ::dirent* (*nfsReaddir)(nfs::NFSDIR* dir);
static int (*nfsClosedir)(nfs::NFSDIR* dir);

static int (*nfsStat)(const char* path, struct ::stat* stat);
static int (*nfsUnlink)(const char* path);
static int (*nfsAccess)(const char* path, int mode);
static int (*nfsRename)(const char* oldpath, const char* newpath);

static nfs::NFSFILE* (*nfsOpen)(const char* path, const char* mode);
static int (*nfsClose)(nfs::NFSFILE* stream);

static ssize_t (*nfsRead)(nfs::NFSFILE* stream, void* ptr, size_t size);
static ssize_t (*nfsPRead)(nfs::NFSFILE* stream, void* ptr, size_t size,
                        uint64_t offset);
static ssize_t (*nfsWrite)(nfs::NFSFILE* stream, const void* ptr, size_t size);

static int (*nfsFsync)(nfs::NFSFILE* stream);
static int64_t (*nfsTell)(nfs::NFSFILE* stream);
static int (*nfsSeek)(nfs::NFSFILE* stream, uint64_t offset);

void* ResolveSymbol(void* dl, const char* sym) {
  dlerror();
  void* sym_ptr = dlsym(dl, sym);
  const char* error = dlerror();
  if (error != NULL) {
    fprintf(stderr, "resolve symbol %s from libnfs.so error: %s\n",
            sym, error);
    abort();
  }
  return sym_ptr;
}

void Nfs::LoadSymbol() {
  dlerror();
  void* dl = dlopen("libnfs.so", RTLD_NOW | RTLD_GLOBAL);
  if (dl == NULL) {
    fprintf(stderr, "dlopen libnfs.so error: %s\n", dlerror());
    abort();
  }

  *(void**)(&printVersion) = ResolveSymbol(dl, "PrintNfsVersion");
  fprintf(stderr, "libnfs.so version: \n%s\n\n", (*printVersion)());

  *(void**)(&nfsInit) = ResolveSymbol(dl, "Init");
  *(void**)(&nfsSetComlogLevel) = ResolveSymbol(dl, "SetComlogLevel");
  *(void**)(&nfsGetErrno) = ResolveSymbol(dl, "GetErrno");
  *(void**)(&nfsMkdir) = ResolveSymbol(dl, "Mkdir");
  *(void**)(&nfsRmdir) = ResolveSymbol(dl, "Rmdir");
  *(void**)(&nfsOpendir) = ResolveSymbol(dl, "Opendir");
  *(void**)(&nfsReaddir) = ResolveSymbol(dl, "Readdir");
  *(void**)(&nfsClosedir) = ResolveSymbol(dl, "Closedir");
  *(void**)(&nfsStat) = ResolveSymbol(dl, "Stat");
  *(void**)(&nfsUnlink) = ResolveSymbol(dl, "Unlink");
  *(void**)(&nfsAccess) = ResolveSymbol(dl, "Access");
  *(void**)(&nfsRename) = ResolveSymbol(dl, "Rename");
  *(void**)(&nfsOpen) = ResolveSymbol(dl, "Open");
  *(void**)(&nfsClose) = ResolveSymbol(dl, "Close");
  *(void**)(&nfsRead) = ResolveSymbol(dl, "Read");
  *(void**)(&nfsPRead) = ResolveSymbol(dl, "PRead");
  *(void**)(&nfsWrite) = ResolveSymbol(dl, "Write");
  *(void**)(&nfsFsync) = ResolveSymbol(dl, "Fsync");
  *(void**)(&nfsTell) = ResolveSymbol(dl, "Tell");
  *(void**)(&nfsSeek) = ResolveSymbol(dl, "Seek");
}

NFile::NFile(nfs::NFSFILE* file, const std::string& name)
  : file_(file), name_(name) {
}
NFile::~NFile() {
  if (file_) {
    CloseFile();
  }
}

int32_t NFile::Write(const char* buf, int32_t len) {
  int64_t s = tera::get_micros();
  tera::AutoCounter ac(&dfs_write_hang_counter, "Write", name_.c_str());
  int32_t retval = (*nfsWrite)(file_, buf, len);
  dfs_write_counter.Inc();
  dfs_write_delay_counter.Add(tera::get_micros() - s);
  if (retval < 0) {
    errno = (*nfsGetErrno)();
  }
  return retval;
}
int32_t NFile::Flush() {
  tera::AutoCounter ac(&dfs_flush_hang_counter, "Flush", name_.c_str());
  dfs_flush_counter.Inc();
  int32_t retval = 0;
  // hdfsFlush有什么意义么, 我没想明白~
  // retval = hdfsFlush(fs_, file_);
  return retval;
}
int32_t NFile::Sync() {
  tera::AutoCounter ac(&dfs_sync_hang_counter, "Sync", name_.c_str());
  dfs_sync_counter.Inc();
  int32_t retval = (*nfsFsync)(file_);
  if (retval != 0) {
    errno = (*nfsGetErrno)();
  }
  return retval;
}
int32_t NFile::Read(char* buf, int32_t len) {
  int64_t s = tera::get_micros();
  tera::AutoCounter ac(&dfs_read_hang_counter, "Read", name_.c_str());
  int32_t retval = (*nfsRead)(file_, buf, len);
  dfs_read_counter.Inc();
  dfs_read_delay_counter.Add(tera::get_micros() - s);
  if (retval < 0) {
    errno = (*nfsGetErrno)();
  }
  return retval;
}
int32_t NFile::Pread(int64_t offset, char* buf, int32_t len) {
  tera::AutoCounter ac(&dfs_read_hang_counter, "Pread", name_.c_str());
  dfs_read_counter.Inc();
  int32_t retval = (*nfsPRead)(file_, buf, len, offset);
  if (retval < 0) {
    errno = (*nfsGetErrno)();
  }
  return retval;
}
int64_t NFile::Tell() {
  tera::AutoCounter ac(&dfs_tell_hang_counter, "Tell", name_.c_str());
  dfs_tell_counter.Inc();
  int64_t retval = (*nfsTell)(file_);
  if (retval < 0) {
    errno = (*nfsGetErrno)();
  }
  return retval;
}
int32_t NFile::Seek(int64_t offset) {
  tera::AutoCounter ac(&dfs_other_hang_counter, "Seek", name_.c_str());
  dfs_other_counter.Inc();
  int32_t retval = (*nfsSeek)(file_, offset);
  if (retval != 0) {
    errno = (*nfsGetErrno)();
  }
  return retval;
}

int32_t NFile::CloseFile() {
  tera::AutoCounter ac(&dfs_close_hang_counter, "CloseFile", name_.c_str());
  dfs_close_counter.Inc();
  int32_t retval = 0;
  if (file_ != NULL) {
    retval = (*nfsClose)(file_);
  }
  if (retval != 0) {
    errno = (*nfsGetErrno)();
    fprintf(stderr, "[ClosFile] %s fail: %d\n", name_.c_str(), errno);
  }
  // WARNING: ignore nfs close error; may cause memory leak
  file_ = NULL;
  return retval;
}

bool Nfs::dl_init_ = false;
port::Mutex Nfs::mu_;
static Nfs* instance = NULL;

void Nfs::Init(const std::string& mountpoint, const std::string& conf_path)
{
  MutexLock l(&mu_);
  if (!dl_init_) {
    LoadSymbol();
    dl_init_ = true;
  }
  (*nfsSetComlogLevel)(2);
  if (0 != (*nfsInit)(mountpoint.c_str(), conf_path.c_str())) {
    char err[256];
    strerror_r((*nfsGetErrno)(), err, 256);
    fprintf(stderr, "init nfs fail: %s\n", err);
    abort();
  }
}

Nfs* Nfs::GetInstance() {
  MutexLock l(&mu_);
  if (instance == NULL) {
    instance = new Nfs();
  }
  return instance;
}

Nfs::Nfs() {}

Nfs::~Nfs() {}

int32_t Nfs::CreateDirectory(const std::string& name) {
  tera::AutoCounter ac(&dfs_other_hang_counter, "CreateDirectory", name.c_str());
  dfs_other_counter.Inc();
  std::vector<std::string> items;
  SplitString(name, "/", &items);
  std::string path;
  if (name[0] == '/') {
    path = "/";
  }
  for (uint32_t i = 0; i < items.size(); ++i) {
    path += items[i];
    if (0 != (*nfsAccess)(path.c_str(), F_OK) && (*nfsGetErrno)() == ENOENT) {
      if (0 != (*nfsMkdir)(path.c_str()) && (*nfsGetErrno)() != EEXIST) {
        errno = (*nfsGetErrno)();
        return -1;
      }
    }
    path += "/";
  }
  return 0;
}
int32_t Nfs::DeleteDirectory(const std::string& name) {
  tera::AutoCounter ac(&dfs_delete_hang_counter, "DeleteDirectory", name.c_str());
  dfs_delete_counter.Inc();
  int32_t retval = (*nfsRmdir)(name.c_str());
  if (retval != 0) {
    errno = (*nfsGetErrno)();
  }
  return retval;
}
int32_t Nfs::Exists(const std::string& filename) {
  tera::AutoCounter ac(&dfs_exists_hang_counter, "Exists", filename.c_str());
  dfs_exists_counter.Inc();
  int32_t retval = (*nfsAccess)(filename.c_str(), F_OK);
  if (retval != 0) {
    errno = (*nfsGetErrno)();
  }
  return retval;
}
int32_t Nfs::Delete(const std::string& filename) {
  tera::AutoCounter ac(&dfs_delete_hang_counter, "Delete", filename.c_str());
  dfs_delete_counter.Inc();
  int32_t retval = (*nfsUnlink)(filename.c_str());
  if (retval != 0) {
    errno = (*nfsGetErrno)();
  }
  return retval;
}
int32_t Nfs::GetFileSize(const std::string& filename, uint64_t* size) {
  tera::AutoCounter ac(&dfs_info_hang_counter, "GetFileSize", filename.c_str());
  dfs_info_counter.Inc();
  struct stat fileinfo;
  int32_t retval = (*nfsStat)(filename.c_str(), &fileinfo);
  if (retval == 0) {
    *size = fileinfo.st_size;
  } else {
    errno = (*nfsGetErrno)();
  }
  return retval;
}
int32_t Nfs::Rename(const std::string& from, const std::string& to) {
  tera::AutoCounter ac(&dfs_other_hang_counter, "Rename", from.c_str());
  dfs_other_counter.Inc();
  int32_t retval = (*nfsRename)(from.c_str(), to.c_str());
  if (retval != 0) {
    errno = (*nfsGetErrno)();
  }
  return retval;
}

DfsFile* Nfs::OpenFile(const std::string& filename, int32_t flags) {
  tera::AutoCounter ac(&dfs_open_hang_counter, "OpenFile", filename.c_str());
  dfs_open_counter.Inc();
  //fprintf(stderr, "OpenFile %s %d\n", filename.c_str(), flags);
  nfs::NFSFILE* file = NULL;
  if (flags == RDONLY) {
    file = (*nfsOpen)(filename.c_str(), "r");
  } else {
    file = (*nfsOpen)(filename.c_str(), "w");
  }
  if (file != NULL) {
    return new NFile(file, filename);
  }
  errno = (*nfsGetErrno)();
  return NULL;
}

int32_t Nfs::Copy(const std::string& from, const std::string& to) {
  tera::AutoCounter ac(&dfs_other_hang_counter, "Copy", from.c_str());
  dfs_other_counter.Inc();
  // not support
  return -1;
}
int32_t Nfs::ListDirectory(const std::string& path, std::vector<std::string>* result) {
  tera::AutoCounter ac(&dfs_list_hang_counter, "ListDirectory", path.c_str());
  dfs_list_counter.Inc();
  nfs::NFSDIR* dir = (*nfsOpendir)(path.c_str());
  if (NULL == dir) {
    fprintf(stderr, "Opendir %s fail\n", path.c_str());
    errno = (*nfsGetErrno)();
    return -1;
  }
  struct ::dirent* dir_info = NULL;
  while (NULL != (dir_info = (*nfsReaddir)(dir))) {
    const char* pathname = dir_info->d_name;
    if (strcmp(pathname, ".") != 0 && strcmp(pathname, "..") != 0) {
      result->push_back(pathname);
    }
  }
  if (0 != (*nfsGetErrno)()) {
    fprintf(stderr, "List %s error: %d\n", path.c_str(), (*nfsGetErrno)());
    errno = (*nfsGetErrno)();
    (*nfsClosedir)(dir);
    return -1;
  }
  (*nfsClosedir)(dir);
  return 0;
}

}
/* vim: set expandtab ts=2 sw=2 sts=2 tw=100: */
