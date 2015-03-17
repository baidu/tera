// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "string_ext.h"

#include <unistd.h>
#include <iomanip>
#include <sstream>

namespace leveldb {

void SplitString(const std::string& full,
                 const std::string& delim,
                 std::vector<std::string>* result) {
    result->clear();
    if (full.empty()) {
        return;
    }

    std::string tmp;
    std::string::size_type pos_begin = full.find_first_not_of(delim);
    std::string::size_type comma_pos = 0;

    while (pos_begin != std::string::npos) {
        comma_pos = full.find(delim, pos_begin);
        if (comma_pos != std::string::npos) {
            tmp = full.substr(pos_begin, comma_pos - pos_begin);
            pos_begin = comma_pos + delim.length();
        } else {
            tmp = full.substr(pos_begin);
            pos_begin = comma_pos;
        }

        if (!tmp.empty()) {
            result->push_back(tmp);
            tmp.clear();
        }
    }
}

void SplitStringEnd(const std::string& full, std::string* begin_part,
                    std::string* end_part, std::string delim) {
    std::string::size_type pos = full.find_last_of(delim);
    if (pos != std::string::npos && pos != 0) {
        if (end_part) {
            *end_part = full.substr(pos + 1);
        }
        if (begin_part) {
            *begin_part = full.substr(0, pos);
        }
    } else {
        if (end_part) {
            *end_part = full;
        }
    }
}

void SplitStringStart(const std::string& full, std::string* begin_part,
                      std::string* end_part, std::string delim) {
    std::string::size_type pos = full.find_first_of(delim);
    if (pos != std::string::npos && pos != 0) {
        if (end_part) {
            *end_part = full.substr(pos + 1);
        }
        if (begin_part) {
            *begin_part = full.substr(0, pos);
        }
    } else if (pos == 0) {
        if (end_part) {
            *end_part = full;
        }
    } else if (pos != std::string::npos) {
        if (begin_part) {
            *begin_part = full;
        }
    }
}

std::string ReplaceString(const std::string& str, const std::string& src,
                          const std::string& dest) {
    std::string ret;

    std::string::size_type pos_begin = 0;
    std::string::size_type pos = str.find(src);
    while (pos != std::string::npos) {
        // cout <<"replacexxx:" << pos_begin <<" " << pos <<"\n";
        ret.append(str.data() + pos_begin, pos - pos_begin);
        ret += dest;
        pos_begin = pos + src.length();
        pos = str.find(src, pos_begin);
    }
    if (pos_begin < str.length()) {
        ret.append(str.begin() + pos_begin, str.end());
    }
    return ret;
}

std::string TrimString(const std::string& str, const std::string& trim) {
    std::string::size_type pos = str.find_first_not_of(trim);
    if (pos == std::string::npos) {
        return str;
    }
    std::string::size_type pos2 = str.find_last_not_of(trim);
    if (pos2 != std::string::npos) {
        return str.substr(pos, pos2 - pos + 1);
    }
    return str.substr(pos);
}

bool StringEndsWith(const std::string& str, const std::string& sub_str) {
    if (str.length() < sub_str.length()) {
        return false;
    }
    if (str.substr(str.length() - sub_str.length()) != sub_str) {
        return false;
    }
    return true;
}

bool StringStartWith(const std::string& str, const std::string& sub_str) {
    if (str.length() < sub_str.length()) {
        return false;
    }
    if (str.substr(0, sub_str.length()) != sub_str) {
        return false;
    }
    return true;
}

char* StringAsArray(std::string* str) {
    return str->empty() ? NULL : &*str->begin();
}

std::string Uint64ToString(uint64_t i, int base) {
    std::stringstream ss;
    if (base == 16) {
        ss << std::hex << std::setfill('0') << std::setw(16) << i;
    } else if (base == 8) {
        ss << std::oct << std::setfill('0') << std::setw(8) << i;
    } else {
        ss << i;
    }
    return ss.str();
}

uint64_t StringToUint64(const std::string& int_str, int base) {
    uint64_t value;
    std::istringstream buffer(int_str);
    if (base == 16) {
        buffer >> std::hex >> value;
    } else if (base == 8) {
        buffer >> std::oct >> value;
    } else {
        buffer >> value;
    }
    return value;
}

void SplitStringPath(const std::string& full_path,
                     std::string* dir_part,
                     std::string* file_part) {
    std::string::size_type pos = full_path.rfind("/");
    if (pos != std::string::npos) {
        if (dir_part) {
            *dir_part = full_path.substr(0, pos);
        }
        if (file_part) {
            *file_part = full_path.substr(pos + 1);
        }
    } else {
        if (dir_part) {
            *dir_part = full_path;
        }
    }
}

bool IsExist(const std::string& path) {
    return access(path.c_str(), R_OK) == 0;
}

} // namespace leveldb
