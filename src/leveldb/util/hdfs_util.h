// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//

#ifndef TERA_LEVELDB_HDFS_UTIL_H
#define TERA_LEVELDB_HDFS_UTIL_H
#include <sys/types.h>
#include <pwd.h>
#include <grp.h>
#include <unistd.h>
#include <stdlib.h>
namespace leveldb {

static void HdfsFileInfo2PosixFileStat(hdfsFileInfo* info, struct stat* st) {
  memset(st, 0, sizeof(struct stat));
  // by default: set to 0 to indicate not support for directory because we can
  // not get this info
  st->st_nlink = (info->mKind == kObjectKindDirectory) ? 0 : 1;
  uid_t owner_id = 99;  // no body, magic number in linux
  if (info->mOwner != NULL) {
    struct passwd passwd_info;
    struct passwd* result = NULL;
    ssize_t buf_size = sysconf(_SC_GETPW_R_SIZE_MAX);
    buf_size = buf_size == -1 ? 16384 : buf_size;
    char* pwbuf = new char[buf_size];
    if (0 == getpwnam_r(info->mOwner, &passwd_info, pwbuf, buf_size, &result)) {
      if (result != NULL) {
        owner_id = passwd_info.pw_uid;
      }
    }
    delete[] pwbuf;
  }
  gid_t group_id = 99;  // no body, magic number in posix
  if (info->mGroup != NULL) {
    struct group result;
    struct group* resultp;
    ssize_t len = sysconf(_SC_GETGR_R_SIZE_MAX);
    len = len == -1 ? 16384 : len;
    char* group_buf = new char[len];
    if (0 == getgrnam_r(info->mGroup, &result, group_buf, len, &resultp)) {
      if (resultp != NULL) {
        group_id = result.gr_gid;
      }
    }
    delete[] group_buf;
  }
  short file_mode = (info->mKind == kObjectKindDirectory) ? (S_IFDIR | 0777) : (S_IFREG | 0666);
  if (info->mPermissions > 0) {
    file_mode = (info->mKind == kObjectKindDirectory) ? S_IFDIR : S_IFREG;
    file_mode |= info->mPermissions;
  }
  st->st_size = (info->mKind == kObjectKindDirectory) ? 4096 : info->mSize;
  st->st_blksize = 512;  // posix default block size
  st->st_blocks = (st->st_size + st->st_blksize - 1) / st->st_blksize;
  st->st_mode = file_mode;
  st->st_uid = owner_id;
  st->st_gid = group_id;
  st->st_atime = info->mLastAccess;
  st->st_ctime = info->mLastMod;
  st->st_mtime = info->mLastMod;
  return;
}
}
#endif
