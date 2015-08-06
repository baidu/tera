// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef TERA_COMMON_STRING_EXT_H_
#define TERA_COMMON_STRING_EXT_H_

#include <string>
#include <vector>

void SplitString(const std::string& full,
                 const std::string& delim,
                 std::vector<std::string>* result);

void SplitStringEnd(const std::string& full,
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

#endif // TERA_COMMON_STRING_EXT_H_
