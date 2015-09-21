// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include <assert.h>
#include <dlfcn.h>

#include "hdfs.h"
#include "include/hdfs2.h"
#include "util/hash.h"
#include "../utils/counter.h"

namespace leveldb {

static hdfsFS (*hdfsConnect)(const char* nn, tPort port);
static int (*hdfsDisconnect)(hdfsFS fs);

static int (*hdfsCreateDirectory)(hdfsFS fs, const char* path);
static hdfsFileInfo* (*hdfsListDirectory)(hdfsFS fs, const char* path,
                                          int *numEntries);
static hdfsFileInfo* (*hdfsGetPathInfo)(hdfsFS fs, const char* path);
static void (*hdfsFreeFileInfo)(hdfsFileInfo *hdfsFileInfo, int numEntries);

static int (*hdfsDelete)(hdfsFS fs, const char* path, int recursive);
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
static int (*hdfsHSync)(hdfsFS fs, hdfsFile file);
static tOffset (*hdfsTell)(hdfsFS fs, hdfsFile file);
static int (*hdfsSeek)(hdfsFS fs, hdfsFile file, tOffset desiredPos);

bool Hdfs2::LoadSymbol() {
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
  *(void**)(&hdfsHSync) = dlsym(dl, "hdfsHSync");
  *(void**)(&hdfsTell) = dlsym(dl, "hdfsTell");
  *(void**)(&hdfsSeek) = dlsym(dl, "hdfsSeek");

  const char* error = dlerror();
  if (error != NULL) {
    fprintf(stderr, "resolve symbol from libhdfs.so error: %s\n", error);
    return false;
  }
  return true;
}

H2File::H2File(void* fs, void* file, const std::string& name)
  : fs_(fs), file_(file), name_(name) {
}
H2File::~H2File() {
  if (file_) {
    CloseFile();
  }
}

int32_t H2File::Write(const char* buf, int32_t len) {
  return (*hdfsWrite)((hdfsFS)fs_, (hdfsFile)file_, buf, len);
}
int32_t H2File::Flush() {
  return (*hdfsFlush)((hdfsFS)fs_, (hdfsFile)file_);
}
int32_t H2File::Sync() {
  return (*hdfsHSync)((hdfsFS)fs_, (hdfsFile)file_);
}
int32_t H2File::Read(char* buf, int32_t len) {
  return (*hdfsRead)((hdfsFS)fs_, (hdfsFile)file_, buf, len);
}
int32_t H2File::Pread(int64_t offset, char* buf, int32_t len) {
  return (*hdfsPread)((hdfsFS)fs_, (hdfsFile)file_, offset, buf, len);
}
int64_t H2File::Tell() {
  int64_t retval = (*hdfsTell)((hdfsFS)fs_, (hdfsFile)file_);
  return retval;
}
int32_t H2File::Seek(int64_t offset) {
  return (*hdfsSeek)((hdfsFS)fs_, (hdfsFile)file_, offset);
}

int32_t H2File::CloseFile() {
  int32_t retval = 0;
  if (file_ != NULL) {
    retval = (*hdfsCloseFile)((hdfsFS)fs_, (hdfsFile)file_);
  }
  if (retval != 0) {
    fprintf(stderr, "[ClosFile] %s fail: %d\n", name_.c_str(), retval);
  }
  // WARNING: in libhdfs, close file success even retval != 0
  file_ = NULL;
  return retval;
}

port::Mutex Hdfs2::dl_mu_;
bool Hdfs2::dl_init_ = false;

Hdfs2::Hdfs2(const std::string& namenode_list) {
  {
    MutexLock l(&dl_mu_);
    if (!dl_init_) {
      bool r = LoadSymbol();
      assert(r);
      dl_init_ = true;
    }
  }

  assert(!namenode_list.empty());
  size_t begin = 0, end = 0;
  while (begin < namenode_list.size()) {
    end = namenode_list.find(',', begin);
    if (end == std::string::npos) {
      end = namenode_list.size();
    }
    assert(end > begin);
    std::string namenode = namenode_list.substr(begin, end - begin);
    fprintf(stderr, "connect to %s\n", namenode.c_str());
    hdfsFS fs = (*hdfsConnect)(namenode.c_str(), 0);
    assert(fs);
    fs_list_.push_back(fs);
    begin = end + 1;
  }
}

Hdfs2::~Hdfs2() {
  for (uint32_t i = 0; i < fs_list_.size(); i++) {
    (*hdfsDisconnect)((hdfsFS)fs_list_[i]);
  }
}

int32_t Hdfs2::CreateDirectory(const std::string& path) {
  return (*hdfsCreateDirectory)((hdfsFS)GetFSHandle(path), path.c_str());
}

int32_t Hdfs2::DeleteDirectory(const std::string& path) {
  return (*hdfsDelete)((hdfsFS)GetFSHandle(path), path.c_str(), 1);
}

int32_t Hdfs2::Exists(const std::string& filename) {
  return (*hdfsExists)((hdfsFS)GetFSHandle(filename), filename.c_str());
}

int32_t Hdfs2::Delete(const std::string& filename) {
  return (*hdfsDelete)((hdfsFS)GetFSHandle(filename), filename.c_str(), 1);
}

int32_t Hdfs2::GetFileSize(const std::string& filename, uint64_t* size) {
  hdfsFileInfo* pFileInfo = (*hdfsGetPathInfo)((hdfsFS)GetFSHandle(filename), filename.c_str());
  if (pFileInfo != NULL) {
    *size = pFileInfo->mSize;
    (*hdfsFreeFileInfo)(pFileInfo, 1);
    return 0;
  }
  return -1;
}

int32_t Hdfs2::Rename(const std::string& from, const std::string& to) {
  assert(GetFSHandle(from) == GetFSHandle(to));
  // Hdfs not support rename to an exist path, so we delete target path first.
  // But this may cause Rename not a atomic operation.
  (*hdfsDelete)((hdfsFS)GetFSHandle(to), to.c_str(), 1);
  return (*hdfsRename)((hdfsFS)GetFSHandle(from), from.c_str(), to.c_str());
}

DfsFile* Hdfs2::OpenFile(const std::string& filename, int32_t flags) {
  //fprintf(stderr, "OpenFile %s %d\n", filename.c_str(), flags);
  int32_t hflags = (flags == RDONLY ? O_RDONLY : O_WRONLY);
  hdfsFile file = (*hdfsOpenFile)((hdfsFS)GetFSHandle(filename), filename.c_str(), hflags, 0 ,0 ,0);
  if (!file) {
    return NULL;
  } else {
    return new H2File(GetFSHandle(filename), file, filename);
  }
}

int32_t Hdfs2::Copy(const std::string& from, const std::string& to) {
  return (*hdfsCopy)((hdfsFS)GetFSHandle(from), from.c_str(), (hdfsFS)GetFSHandle(to), to.c_str());
}

int32_t Hdfs2::ListDirectory(const std::string& path,
                             std::vector<std::string>* result) {
  int numEntries = 0;
  hdfsFileInfo* pHdfsFileInfo = 0;
  pHdfsFileInfo = (*hdfsListDirectory)((hdfsFS)GetFSHandle(path), path.c_str(), &numEntries);
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

void* Hdfs2::GetFSHandle(const std::string& path) {
  size_t pos = path.rfind("tablet");
  if (pos == std::string::npos) {
    // fprintf(stderr, "hash path: %s, index: 0\n", path.c_str());
    return fs_list_[0];
  }
  pos = path.find('/', pos);
  if (pos == std::string::npos) {
    pos = path.size();
  }
  std::string hash_path = path.substr(0, pos);
  uint32_t index = Hash(hash_path.c_str(), hash_path.size(), 12345) % fs_list_.size();
  // fprintf(stderr, "hash path: %s, index: %d\n", hash_path.c_str(), index);
  return fs_list_[index];
}

} // namespace leveldb

/* vim: set expandtab ts=2 sw=2 sts=2 tw=100: */
