// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef TERA_LEVELDB_STRING_EXT_H
#define TERA_LEVELDB_STRING_EXT_H

#include <stdint.h>

#include <string>
#include <vector>

namespace leveldb {

void SplitString(const std::string& full,
                 const std::string& delim,
                 std::vector<std::string>* result);

void SplitStringEnd(const std::string& full,
                    std::string* begin_part,
                    std::string* end_part,
                    std::string delim = ".");

void SplitStringStart(const std::string& full,
                      std::string* begin_part,
                      std::string* end_part,
                      std::string delim = ".");

std::string ReplaceString(const std::string& str,
                          const std::string& src,
                          const std::string& dest);


std::string TrimString(const std::string& str,
                       const std::string& trim = " ");

bool StringEndsWith(const std::string& str,
                    const std::string& sub_str);

bool StringStartWith(const std::string& str,
                    const std::string& sub_str);

char* StringAsArray(std::string* str);

std::string Uint64ToString(uint64_t i, int base = 10);

uint64_t StringToUint64(const std::string& int_str, int base = 10);

// file path

void SplitStringPath(const std::string& full_path,
                     std::string* dir_part,
                     std::string* file_part);

bool IsExist(const std::string& file_path);

} // namespace leveldb

#endif // TERA_LEVELDB_STRING_EXT_H
