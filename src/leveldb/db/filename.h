// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// File names used by DB code

#ifndef STORAGE_LEVELDB_DB_FILENAME_H_
#define STORAGE_LEVELDB_DB_FILENAME_H_

#include <stdint.h>
#include <string>
#include "leveldb/slice.h"
#include "leveldb/status.h"
#include "port/port.h"
#include "util/logging.h"

namespace leveldb {

class Env;

enum FileType {
  kUnknown,
  kLogFile,
  kDBLockFile,
  kTableFile,
  kDescriptorFile,
  kCurrentFile,
  kTempFile,
  kInfoLogFile  // Either the current one, or an old one
};

// Return the name of the log file with the specified number
// in the db named by "dbname".  The result will be prefixed with
// "dbname".
extern std::string LogFileName(const std::string& dbname, uint64_t number);

// for qinan
extern std::string LogHexFileName(const std::string& dbname, uint64_t number);

// Return the name of the sstable with the specified number
// in the db named by "dbname".  The result will be prefixed with
// "dbname".
extern std::string TableFileName(const std::string& dbname, uint64_t number);

// Return the name of the descriptor file for the db named by
// "dbname" and the specified incarnation number.  The result will be
// prefixed with "dbname".
extern std::string DescriptorFileName(const std::string& dbname, uint64_t number);

// Return the name of the current file.  This file contains the name
// of the current manifest file.  The result will be prefixed with
// "dbname".
extern std::string CurrentFileName(const std::string& dbname);

// Return the name of the lock file for the db named by
// "dbname".  The result will be prefixed with "dbname".
extern std::string LockFileName(const std::string& dbname);

// Return the name of a temporary file owned by the db named "dbname".
// The result will be prefixed with "dbname".
extern std::string TempFileName(const std::string& dbname, uint64_t number);

// Return the name of the info log file for "dbname".
extern std::string InfoLogFileName(const std::string& dbname);

// Return the name of the old info log file for "dbname".
extern std::string OldInfoLogFileName(const std::string& dbname);

// If filename is a leveldb file, store the type of the file in *type.
// The number encoded in the filename is stored in *number.  If the
// filename was successfully parsed, returns true.  Else return false.
extern bool ParseFileName(const std::string& filename, uint64_t* number, FileType* type);

// Make the CURRENT file point to the descriptor file with the
// specified number.
extern Status SetCurrentFile(Env* env, const std::string& dbname, uint64_t descriptor_number);

const char* FileTypeToString(FileType type);

// build a full path file number from dbname&filenumber, format:
// |--tabletnum(4B)--|--filenum(4B)--|
// tabletnum = 0x80000000|real_tablet_num
extern uint64_t BuildFullFileNumber(const std::string& dbname, uint64_t number);

// Build tablet path from tablet_num
// E.g. construct "/table1/tablet000003"
//      from (/table1, 3)
std::string BuildTabletPath(const std::string& prefix, uint64_t tablet);

std::string BuildTabletLgPath(const std::string& prfix, uint64_t tablet, uint64_t lg);

// Build file path from tablet_num & lg_num & file number
// E.g. construct "/table1/tablet000003/0/00000001.sst"
//      from (/table1, 3, 0, 1)
std::string BuildTableFilePath(const std::string& prefix, uint64_t tablet, uint64_t lg,
                               uint64_t number);

// Build trash file path from tablet_num & lg & file number & time
// E.g. construct "/table1/tablet000003/0/00000001.sst.20170718-17-08-30"
//      from (/table1, 3, 0, 1, 20170718-17-08-30)
std::string BuildTrashTableFilePath(const std::string& prefix, uint64_t tablet, uint32_t lg_id,
                                    uint64_t number, const std::string& time);

// get time string from trash file path
// E.g. get "20170718-17-08-30"
//      from  "/table1/tablet000003/0/00000001.sst.20170718-17-08-30"
// if path is invalid, return ""
std::string GetTimeStrFromTrashFile(const std::string& path);

// Build file path from lg_num & full file number
// E.g. construct "/table1/tablet000003/0/00000001.sst"
//      from (/table1, 0, 0x8000000300000001)
std::string BuildTableFilePath(const std::string& prefix, uint64_t lg, uint64_t full_number);

// Parse a db_impl name to prefix, tablet number, lg number...
// db_impl name format maybe:
// /.../tablename/tablet000012/2    (have tablet name, allow split)
// or /.../tablename/2              (have none tablet name, donot allow split)
bool ParseDbName(const std::string& dbname, std::string* prefix, uint64_t* tablet, uint64_t* lg);

// Parse a full file number to tablet number & file number
bool ParseFullFileNumber(uint64_t full_number, uint64_t* tablet, uint64_t* file);

// Construct a db_impl name from a cur-db_impl name and a tablet number.
// E.g. construct "/table1/tablet000003/0" from (/table1/tablet000001/0, 3)
std::string RealDbName(const std::string& dbname, uint64_t tablet);

// Construct a db_table name from a cur-db_table name and a tablet number.
// E.g. construct "/table1/tablet000003" from (/table1/tablet000001, 3)
std::string GetChildTabletPath(const std::string& parent_path, uint64_t tablet);

// Construct a db_table name from a table name and a tablet number.
// E.g. construct "/table1/tablet000003" from (/table1, 3)
std::string GetTabletPathFromNum(const std::string& tablename, uint64_t tablet);

// Parse tablet number from a db_table name.
// E.g. get 3 from "table1/tablet000003"
uint64_t GetTabletNumFromPath(const std::string& tabletpath);

// Check if this table file is inherited.
bool IsTableFileInherited(uint64_t tablet, uint64_t number);

std::string FileNumberDebugString(uint64_t full_number);
}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_FILENAME_H_
