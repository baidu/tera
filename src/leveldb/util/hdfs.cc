// Copyright (c) 2014, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Author: yanshiguang02@baidu.com

#include <assert.h>
#include <dlfcn.h>

#include "hdfs.h"
#include "include/hdfs.h"
#include "../utils/counter.h"

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

static hdfsFS (*hdfsConnect)(const char* nn, tPort port);
static int (*hdfsDisconnect)(hdfsFS fs);

static int (*hdfsCreateDirectory)(hdfsFS fs, const char* path);
static hdfsFileInfo* (*hdfsListDirectory)(hdfsFS fs, const char* path,
                                          int *numEntries);
static hdfsFileInfo* (*hdfsGetPathInfo)(hdfsFS fs, const char* path);
static void (*hdfsFreeFileInfo)(hdfsFileInfo *hdfsFileInfo, int numEntries);

static int (*hdfsDelete)(hdfsFS fs, const char* path);
static int (*hdfsExists)(hdfsFS fs, const char *path);
static int (*hdfsRename)(hdfsFS fs, const char* oldPath, const char* newPath);
static int (*hdfsCopy)(hdfsFS srcFS, const char* src,
                        hdfsFS dstFS, const char* dst);

static hdfsFile (*hdfsOpenFile)(hdfsFS fs, const char* path, int flags,
                                 int bufferSize, short replication,
                                 tSize blocksize);
static int (*hdfsCloseFile)(hdfsFS fs, hdfsFile file);

static tSize (*hdfsRead)(hdfsFS fs, hdfsFile file, void* buffer, tSize length);
static tSize (*hdfsPread)(hdfsFS fs, hdfsFile file, tOffset position,
                           void* buffer, tSize length);
static tSize (*hdfsWrite)(hdfsFS fs, hdfsFile file, const void* buffer,
                           tSize length);
static int (*hdfsFlush)(hdfsFS fs, hdfsFile file);
static int (*hdfsSync)(hdfsFS fs, hdfsFile file);
static tOffset (*hdfsTell)(hdfsFS fs, hdfsFile file);
static int (*hdfsSeek)(hdfsFS fs, hdfsFile file, tOffset desiredPos);

bool Hdfs::LoadSymbol() {
  dlerror();
  void* dl = dlopen("libhdfs.so", RTLD_NOW | RTLD_GLOBAL);
  if (dl == NULL) {
    fprintf(stderr, "dlopen libhdfs.so error: %s\n", dlerror());
    return false;
  }

  dlerror();
  *(void**)(&hdfsConnect) = dlsym(dl, "hdfsConnect");
  *(void**)(&hdfsDisconnect) = dlsym(dl, "hdfsDisconnect");
  *(void**)(&hdfsCreateDirectory) = dlsym(dl, "hdfsCreateDirectory");
  *(void**)(&hdfsListDirectory) = dlsym(dl, "hdfsListDirectory");
  *(void**)(&hdfsGetPathInfo) = dlsym(dl, "hdfsGetPathInfo");
  *(void**)(&hdfsFreeFileInfo) = dlsym(dl, "hdfsFreeFileInfo");
  *(void**)(&hdfsDelete) = dlsym(dl, "hdfsDelete");
  *(void**)(&hdfsExists) = dlsym(dl, "hdfsExists");
  *(void**)(&hdfsRename) = dlsym(dl, "hdfsRename");
  *(void**)(&hdfsCopy) = dlsym(dl, "hdfsCopy");
  *(void**)(&hdfsOpenFile) = dlsym(dl, "hdfsOpenFile");
  *(void**)(&hdfsCloseFile) = dlsym(dl, "hdfsCloseFile");
  *(void**)(&hdfsRead) = dlsym(dl, "hdfsRead");
  *(void**)(&hdfsPread) = dlsym(dl, "hdfsPread");
  *(void**)(&hdfsWrite) = dlsym(dl, "hdfsWrite");
  *(void**)(&hdfsFlush) = dlsym(dl, "hdfsFlush");
  *(void**)(&hdfsSync) = dlsym(dl, "hdfsSync");
  *(void**)(&hdfsTell) = dlsym(dl, "hdfsTell");
  *(void**)(&hdfsSeek) = dlsym(dl, "hdfsSeek");

  const char* error = dlerror();
  if (error != NULL) {
    fprintf(stderr, "resolve symbol from libhdfs.so error: %s\n", error);
    return false;
  }

  return true;
}

HFile::HFile(void* fs, void* file, const std::string& name)
  : fs_(fs), file_(file), name_(name) {
}
HFile::~HFile() {
  if (file_) {
    CloseFile();
  }
}

int32_t HFile::Write(const char* buf, int32_t len) {
  int64_t s = tera::get_micros();
  tera::AutoCounter ac(&dfs_write_hang_counter, "Write", name_.c_str());
  int32_t retval = (*hdfsWrite)((hdfsFS)fs_, (hdfsFile)file_, buf, len);
  dfs_write_delay_counter.Add(tera::get_micros() - s);
  dfs_write_counter.Inc();
  return retval;
}
int32_t HFile::Flush() {
  tera::AutoCounter ac(&dfs_flush_hang_counter, "Flush", name_.c_str());
  dfs_flush_counter.Inc();
  return (*hdfsFlush)((hdfsFS)fs_, (hdfsFile)file_);
}
int32_t HFile::Sync() {
  tera::AutoCounter ac(&dfs_sync_hang_counter, "Sync", name_.c_str());
  dfs_sync_counter.Inc();
  return (*hdfsSync)((hdfsFS)fs_, (hdfsFile)file_);
}
int32_t HFile::Read(char* buf, int32_t len) {
  int64_t s = tera::get_micros();
  tera::AutoCounter ac(&dfs_read_hang_counter, "Read", name_.c_str());
  int32_t retval = (*hdfsRead)((hdfsFS)fs_, (hdfsFile)file_, buf, len);
  dfs_read_delay_counter.Add(tera::get_micros() - s);
  dfs_read_counter.Inc();
  return retval;
}
int32_t HFile::Pread(int64_t offset, char* buf, int32_t len) {
  tera::AutoCounter ac(&dfs_read_hang_counter, "Pread", name_.c_str());
  dfs_read_counter.Inc();
  int32_t retval = (*hdfsPread)((hdfsFS)fs_, (hdfsFile)file_, offset, buf, len);
  return retval;
}
int64_t HFile::Tell() {
  tera::AutoCounter ac(&dfs_tell_hang_counter, "Tell", name_.c_str());
  dfs_tell_counter.Inc();
  int64_t retval = (*hdfsTell)((hdfsFS)fs_, (hdfsFile)file_);
  return retval;
}
int32_t HFile::Seek(int64_t offset) {
  tera::AutoCounter ac(&dfs_other_hang_counter, "Seek", name_.c_str());
  dfs_other_counter.Inc();
  return (*hdfsSeek)((hdfsFS)fs_, (hdfsFile)file_, offset);
}

int32_t HFile::CloseFile() {
  tera::AutoCounter ac(&dfs_close_hang_counter, "CloseFile", name_.c_str());
  dfs_close_counter.Inc();
  int32_t retval = 0;
  if ((hdfsFile)file_ != NULL) {
    retval = (*hdfsCloseFile)((hdfsFS)fs_, (hdfsFile)file_);
  }
  if (retval != 0) {
    fprintf(stderr, "[ClosFile] %s fail: %d\n", name_.c_str(), retval);
  }
  // WARNING: in libhdfs, close file success even retval != 0
  file_ = NULL;
  return retval;
}

port::Mutex Hdfs::dl_mu_;
bool Hdfs::dl_init_ = false;

Hdfs::Hdfs() {
  {
    MutexLock l(&dl_mu_);
    if (!dl_init_) {
      bool r = LoadSymbol();
      assert(r);
      dl_init_ = true;
    }
  }
  fs_ =  (*hdfsConnect)("default", 0);
}
Hdfs::~Hdfs() {
  if (fs_) {
    (*hdfsDisconnect)((hdfsFS)fs_);
    fs_ = NULL;
  }
}
int32_t Hdfs::CreateDirectory(const std::string& path) {
  tera::AutoCounter ac(&dfs_other_hang_counter, "CreateDirectory", path.c_str());
  dfs_other_counter.Inc();
  return (*hdfsCreateDirectory)((hdfsFS)fs_, path.c_str());
}
int32_t Hdfs::DeleteDirectory(const std::string& path) {
  tera::AutoCounter ac(&dfs_delete_hang_counter, "DeleteDirectory", path.c_str());
  dfs_delete_counter.Inc();
  return (*hdfsDelete)((hdfsFS)fs_, path.c_str());
}
int32_t Hdfs::Exists(const std::string& filename) {
  tera::AutoCounter ac(&dfs_exists_hang_counter, "Exists", filename.c_str());
  dfs_exists_counter.Inc();
  int32_t retval = (*hdfsExists)((hdfsFS)fs_, filename.c_str());
  return retval;
}
int32_t Hdfs::Delete(const std::string& filename) {
  tera::AutoCounter ac(&dfs_delete_hang_counter, "Delete", filename.c_str());
  dfs_delete_counter.Inc();
  return (*hdfsDelete)((hdfsFS)fs_, filename.c_str());
}
int32_t Hdfs::GetFileSize(const std::string& filename, uint64_t* size) {
  tera::AutoCounter ac(&dfs_info_hang_counter, "GetFileSize", filename.c_str());
  dfs_info_counter.Inc();
  hdfsFileInfo* pFileInfo = (*hdfsGetPathInfo)((hdfsFS)fs_, filename.c_str());
  if (pFileInfo != NULL) {
    *size = pFileInfo->mSize;
    (*hdfsFreeFileInfo)(pFileInfo, 1);
    return 0;
  }
  return -1;
}
int32_t Hdfs::Rename(const std::string& from, const std::string& to) {
  tera::AutoCounter ac(&dfs_other_hang_counter, "Rename", from.c_str());
  dfs_other_counter.Inc();
  return (*hdfsRename)((hdfsFS)fs_, from.c_str(), to.c_str());
}

DfsFile* Hdfs::OpenFile(const std::string& filename, int32_t flags) {
  tera::AutoCounter ac(&dfs_open_hang_counter, "OpenFile", filename.c_str());
  dfs_open_counter.Inc();
  // fprintf(stderr, "OpenFile %s %d\n", filename.c_str(), flags);
  int32_t hflags = (flags == RDONLY ? O_RDONLY : O_WRONLY);
  hdfsFile file = (*hdfsOpenFile)((hdfsFS)fs_, filename.c_str(), hflags, 0 ,0 ,0);
  if (!file) {
    if (hflags == O_WRONLY) {
      // open failed, delete and reopen it
      Delete(filename);
      file = (*hdfsOpenFile)((hdfsFS)fs_, filename.c_str(), hflags, 0 ,0 ,0);
      if (!file) {
        return NULL;
      }
    } else {
      return NULL;
    }
  }
  return new HFile(fs_, file, filename);
}

int32_t Hdfs::Copy(const std::string& from, const std::string& to) {
  tera::AutoCounter ac(&dfs_other_hang_counter, "Copy", from.c_str());
  dfs_other_counter.Inc();
  return (*hdfsCopy)((hdfsFS)fs_, from.c_str(), (hdfsFS)fs_, to.c_str());
}
int32_t Hdfs::ListDirectory(const std::string& path, std::vector<std::string>* result) {
  tera::AutoCounter ac(&dfs_list_hang_counter, "ListDirectory", path.c_str());
  dfs_list_counter.Inc();
  int numEntries = 0;
  hdfsFileInfo* pHdfsFileInfo = 0;
  pHdfsFileInfo = (*hdfsListDirectory)((hdfsFS)fs_, path.c_str(), &numEntries);
  if (numEntries >= 0) {
    for (int i = 0; i < numEntries; i++) {
      char* pathname = pHdfsFileInfo[i].mName;
      char* filename = rindex(pathname, '/');
      if (filename != NULL) {
        result->push_back(filename + 1);
      }
    }
    if (pHdfsFileInfo != NULL) {
      (*hdfsFreeFileInfo)(pHdfsFileInfo, numEntries);
    }
  }
  return 0;
}

}
/* vim: set expandtab ts=2 sw=2 sts=2 tw=100: */
