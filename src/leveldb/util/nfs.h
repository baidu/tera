// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef  TERA_LEVELDB_NFS_H_
#define  TERA_LEVELDB_NFS_H_

#include <string>
#include <vector>
#include "leveldb/dfs.h"
#include "port/port_posix.h"

namespace nfs {
  struct NFSFILE;
  typedef int (*AssignNamespaceIdFunc)(const char* path, int max_namespaces);
}

namespace leveldb {

class NFile : public DfsFile {
public:
  NFile(nfs::NFSFILE* file, const std::string& name);
  ~NFile();
  int32_t Write(const char* buf, int32_t len);
  int32_t Flush();
  int32_t Sync();
  int32_t Read(char* buf, int32_t len);
  int32_t Pread(int64_t offset, char* buf, int32_t len);
  int64_t Tell();
  int32_t Seek(int64_t offset);
  int32_t CloseFile();
private:
  nfs::NFSFILE* file_;
  std::string name_;
};

class Nfs : public Dfs {
public:
  static void Init(const std::string& mountpoint, const std::string& conf_path);
  static int CalcNamespaceId(const char* c_path, int max_namespaces);
  static Nfs* GetInstance();
  ~Nfs();
  int32_t CreateDirectory(const std::string& path);
  int32_t DeleteDirectory(const std::string& path);
  int32_t Exists(const std::string& filename);
  int32_t Delete(const std::string& filename);
  int32_t GetFileSize(const std::string& filename, uint64_t* size);
  int32_t Rename(const std::string& from, const std::string& to);
  int32_t Copy(const std::string& from, const std::string& to);
  int32_t ListDirectory(const std::string& path, std::vector<std::string>* result);
  DfsFile* OpenFile(const std::string& filename, int32_t flags);
private:
  Nfs();
  static port::Mutex mu_;
  static void LoadSymbol();
  static bool dl_init_;
};

}

#endif  //TERA_LEVELDB_NFS_H_

/* vim: set expandtab ts=2 sw=2 sts=2 tw=100: */
