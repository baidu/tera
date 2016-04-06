// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef  TERA_UTIL_STRING_UTIL_H_
#define  TERA_UTIL_STRING_UTIL_H_

#include <string>

namespace tera {

    extern const size_t kNameLenMin;
    extern const size_t kNameLenMax;

    std::string DebugString(const std::string& src);
    bool IsValidName(const std::string& str);
    bool IsValidTableName(const std::string& str);
    bool IsValidGroupName(const std::string& name);
    bool IsValidUserName(const std::string& name);

    bool IsValidColumnFamilyName(const std::string& str);
    std::string RoundNumberToNDecimalPlaces(double n, int d);
    int EditDistance(const std::string& a, const std::string& b);
} // namespace tera

#endif  // TERA_UTIL_STRING_UTIL_H_
