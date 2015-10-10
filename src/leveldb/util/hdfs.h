// Copyright (c) 2014, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Author: yanshiguang02@baidu.com

#ifndef  TERA_LEVELDB_AHDFS_H_
#define  TERA_LEVELDB_AHDFS_H_

#include <string>
#include <vector>
#include "leveldb/dfs.h"
#include "util/mutexlock.h"
#include "port/port.h"

namespace leveldb {

class HFile : public DfsFile {
public:
  HFile(void* fs, void* file, const std::string& name);
  ~HFile();
  int32_t Write(const char* buf, int32_t len);
  int32_t Flush();
  int32_t Sync();
  int32_t Read(char* buf, int32_t len);
  int32_t Pread(int64_t offset, char* buf, int32_t len);
  int64_t Tell();
  int32_t Seek(int64_t offset);
  int32_t CloseFile();

private:
  void* fs_;
  void* file_;
  std::string name_;
};

class Hdfs : public Dfs {
public:
  Hdfs();
  ~Hdfs();
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
  void* fs_;

  // for dynamic library
  static bool LoadSymbol();
  static port::Mutex dl_mu_;
  static bool dl_init_;
};

class H2File : public DfsFile {
public:
  H2File(void* fs, void* file, const std::string& name);
  ~H2File();
  int32_t Write(const char* buf, int32_t len);
  int32_t Flush();
  int32_t Sync();
  int32_t Read(char* buf, int32_t len);
  int32_t Pread(int64_t offset, char* buf, int32_t len);
  int64_t Tell();
  int32_t Seek(int64_t offset);
  int32_t CloseFile();

private:
  void* fs_;
  void* file_;
  std::string name_;
};

class Hdfs2 : public Dfs {
public:
  Hdfs2(const std::string& namenode_list);
  ~Hdfs2();
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
  void* GetFSHandle(const std::string& path);
  std::vector<void*> fs_list_;

  // for dynamic library
  static bool LoadSymbol();
  static port::Mutex dl_mu_;
  static bool dl_init_;
};

}

#endif  //TERA_LEVELDB_AHDFS_H_

/* vim: set expandtab ts=2 sw=2 sts=2 tw=100: */
