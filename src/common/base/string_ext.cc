// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "common/base/string_ext.h"

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
