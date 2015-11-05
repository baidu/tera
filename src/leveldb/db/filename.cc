// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include <ctype.h>
#include <limits.h>
#include <stdint.h>
#include <stdio.h>

#include "db/filename.h"
#include "db/dbformat.h"
#include "leveldb/env.h"
#include "util/string_ext.h"

#include <iostream>
namespace leveldb {

// A utility routine: write "data" to the named file and Sync() it.
extern Status WriteStringToFileSync(Env* env, const Slice& data,
                                    const std::string& fname);

static std::string MakeFileName(const std::string& name, uint64_t number,
                                const char* suffix) {
  char buf[100];
  snprintf(buf, sizeof(buf), "/%08llu.%s",
           static_cast<unsigned long long>(number),
           suffix);
  return name + buf;
}

bool ParseDbName(const std::string& dbname, std::string* prefix,
                 uint64_t* tablet, uint64_t* lg) {
  std::string::size_type pos1, pos2;

  assert(dbname[dbname.size() - 1] != '/');
  pos2 = dbname.find_last_of("/");
  assert(pos2 != std::string::npos);
  if (lg) {
    Slice lg_str(dbname.data() + pos2 + 1);
    assert(ConsumeDecimalNumber(&lg_str, lg));
  }

  pos1 = dbname.find_last_of("/", pos2 - 1);
  assert(pos1 != std::string::npos);

  if (pos1 + 1 != dbname.find("tablet", pos1)) {
    if (prefix) {
      prefix->assign(dbname.substr(0, pos2));
    }
    // have no tablet
    return false;
  } else {
    if (prefix) {
      prefix->assign(dbname.substr(0, pos1));
    }
    if (tablet) {
      pos1 += 7;
      Slice tablet_str(dbname.data() + pos1, pos2 - pos1);
      assert(ConsumeDecimalNumber(&tablet_str, tablet));
    }
    return true;
  }
}

std::string LogFileName(const std::string& name, uint64_t number) {
  assert(number > 0);
  return MakeFileName(name, number, "log");
}

std::string LogHexFileName(const std::string& name, uint64_t number) {
  assert(number > 0);
  return name + std::string("/H") + Uint64ToString(number, 16) + ".log";
}

std::string TableFileName(const std::string& name, uint64_t number) {
  assert(number > 0);
  if (number < (1ull << 63)) {
    return MakeFileName(name, number, "sst");
  }
  uint64_t tablet = number >> 32 & 0x7fffffff;
  std::string dbname = RealDbName(name, tablet);
  return MakeFileName(dbname, number & 0xffffffff, "sst");
}

std::string DescriptorFileName(const std::string& dbname, uint64_t number) {
  assert(number > 0);
  char buf[100];
  snprintf(buf, sizeof(buf), "/MANIFEST-%06llu",
           static_cast<unsigned long long>(number));
  return dbname + buf;
}

std::string CurrentFileName(const std::string& dbname) {
  return dbname + "/CURRENT";
}

std::string LockFileName(const std::string& dbname) {
  return dbname + "/LOCK";
}

std::string TempFileName(const std::string& dbname, uint64_t number) {
  assert(number > 0);
  return MakeFileName(dbname, number, "dbtmp");
}

std::string InfoLogFileName(const std::string& dbname) {
  return dbname + "/LOG";
}

// Return the name of the old info log file for "dbname".
std::string OldInfoLogFileName(const std::string& dbname) {
  return dbname + "/LOG.old";
}


// Owned filenames have the form:
//    dbname/CURRENT
//    dbname/LOCK
//    dbname/LOG
//    dbname/LOG.old
//    dbname/MANIFEST-[0-9]+
//    dbname/[0-9]+.(log|sst)
bool ParseFileName(const std::string& fname,
                   uint64_t* number,
                   FileType* type) {
  Slice rest(fname);
  if (rest == "CURRENT") {
    *number = 0;
    *type = kCurrentFile;
  } else if (rest == "LOCK") {
    *number = 0;
    *type = kDBLockFile;
  } else if (rest == "LOG" || rest == "LOG.old") {
    *number = 0;
    *type = kInfoLogFile;
  } else if (rest.starts_with("MANIFEST-")) {
    rest.remove_prefix(strlen("MANIFEST-"));
    uint64_t num;
    if (!ConsumeDecimalNumber(&rest, &num)) {
      return false;
    }
    if (!rest.empty()) {
      return false;
    }
    *type = kDescriptorFile;
    *number = num;
  } else {
    // Avoid strtoull() to keep filename format independent of the
    // current locale
    uint64_t num;
    if (!ConsumeDecimalNumber(&rest, &num)) {
      return false;
    }
    Slice suffix = rest;
    if (suffix == Slice(".log")) {
      *type = kLogFile;
    } else if (suffix == Slice(".sst")) {
      *type = kTableFile;
    } else if (suffix == Slice(".dbtmp")) {
      *type = kTempFile;
    } else {
      return false;
    }
    *number = num;
  }
  return true;
}

Status SetCurrentFile(Env* env, const std::string& dbname,
                      uint64_t descriptor_number) {
  // Remove leading "dbname/" and add newline to manifest file name
  std::string manifest = DescriptorFileName(dbname, descriptor_number);
  Slice contents = manifest;
  assert(contents.starts_with(dbname + "/"));
  contents.remove_prefix(dbname.size() + 1);
  std::string tmp = TempFileName(dbname, descriptor_number);
  Status s = WriteStringToFileSync(env, contents.ToString() + "\n", tmp);
  if (s.ok()) {
    s = env->RenameFile(tmp, CurrentFileName(dbname));
  } else {
    Log("[dfs error] open dbtmp[%s] error, status[%s].\n",
        tmp.c_str(), s.ToString().c_str());
  }
  if (!s.ok()) {
    env->DeleteFile(tmp);
  } else {
    Log("[dfs error] rename CURRENT[%s] error, status[%s].\n",
        tmp.c_str(), s.ToString().c_str());
  }
  return s;
}

const char* FileTypeToString(FileType type) {
  switch (type) {
  case kLogFile:
    return "kLogFile";
  case kDBLockFile:
    return "kDBLockFile";
  case kTableFile:
    return "kTableFile";
  case kDescriptorFile:
    return "kDescriptorFile";
  case kCurrentFile:
    return "kCurrentFile";
  case kTempFile:
    return "kTempFile";
  case kInfoLogFile:
    return "kInfoLogFile";
  default:
    return "kUnknown";
  }
  return "kUnknown";
}

uint64_t BuildFullFileNumber(const std::string& dbname, uint64_t number) {
  assert(number < UINT_MAX);

  uint64_t tablet, lg;
  std::string prefix;
  if (!ParseDbName(dbname, &prefix, &tablet, &lg)) {
    // have no tablet
    return number;
  } else {
    return (1UL << 63 | tablet << 32 | number);
  }
}

bool ParseFullFileNumber(uint64_t full_number, uint64_t* tablet, uint64_t* file) {
  if (tablet) {
    *tablet = (full_number >> 32 & 0x7FFFFFFF);
  }
  if (file) {
    *file = full_number & 0xffffffff;
  }
  return true;
}

std::string BuildTableFilePath(const std::string& prefix,
                               uint64_t lg, uint64_t number) {
  uint64_t tablet = (number >> 32 & 0x7FFFFFFF);

  char buf[100];
  snprintf(buf, sizeof(buf), "/tablet%08llu/%llu",
           static_cast<unsigned long long>(tablet),
           static_cast<unsigned long long>(lg));
  std::string dbname = prefix + buf;
  return MakeFileName(dbname, number & 0xffffffff, "sst");
}

std::string RealDbName(const std::string& dbname, uint64_t tablet) {
  assert(tablet < UINT_MAX);
  uint64_t tablet_old, lg;
  std::string prefix;
  if (!ParseDbName(dbname, &prefix, &tablet_old, &lg)) {
    return dbname;
  }
  char buf[100];
  snprintf(buf, sizeof(buf), "/tablet%08llu/%llu",
           static_cast<unsigned long long>(tablet),
           static_cast<unsigned long long>(lg));
  return prefix + buf;
}

std::string GetTabletPathFromNum(const std::string& tablename, uint64_t tablet) {
  assert(tablet > 0);
  char buf[32];
  snprintf(buf, sizeof(buf), "/tablet%08llu",
           static_cast<unsigned long long>(tablet));
  return tablename + buf;
}

std::string GetChildTabletPath(const std::string& parent_path, uint64_t tablet) {
  assert(tablet > 0);
  char buf[32];
  snprintf(buf, sizeof(buf), "%08llu",
           static_cast<unsigned long long>(tablet));
  return parent_path.substr(0, parent_path.size() - 8) + buf;
}

uint64_t GetTabletNumFromPath(const std::string& tabletpath) {
  Slice path = tabletpath;
  if (path.size() <= 14) {
    return 0;
  }
  path.remove_prefix(path.size() - 14);
  if (!path.starts_with("tablet")) {
    return 0;
  }
  path.remove_prefix(6);
  uint64_t tablet = 0;
  assert(ConsumeDecimalNumber(&path, &tablet));
  return tablet;
}

bool IsTableFileInherited(uint64_t tablet, uint64_t number) {
  assert(number > UINT_MAX);
  uint64_t file_tablet = (number >> 32 & 0x7FFFFFFF);
  return (tablet == file_tablet) ? false : true;
}
}  // namespace leveldb
