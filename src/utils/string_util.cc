// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "string_util.h"

#include <stdint.h>

namespace tera {

bool IsVisible(char c) {
    return (c >= 0x20 && c <= 0x7E);
}

char ToHex(uint8_t i) {
    char j = 0;
    if (i < 10) {
        j = i + '0';
    } else {
        j = i - 10 + 'a';
    }
    return j;
}

std::string DebugString(const std::string& src) {
    size_t src_len = src.size();
    std::string dst;
    dst.resize(src_len << 2);

    size_t j = 0;
    for (size_t i = 0; i < src_len; i++) {
        uint8_t c = src[i];
        if (IsVisible(c)) {
            dst[j++] = c;
        } else {
            dst[j++] = '\\';
            dst[j++] = 'x';
            dst[j++] = ToHex(c >> 4);
            dst[j++] = ToHex(c & 0xF);
        }
    }

    return dst.substr(0, j);
}

bool IsValidTableName(const std::string& str) {
    return IsValidName(str);
}

const size_t kNameLenMin = 1;
const size_t kNameLenMax = 512;

bool IsValidName(const std::string& str) {
    if (str.size() < kNameLenMin || kNameLenMax < str.size()) {
        return false;
    }
    if (!(isupper(str[0]) || islower(str[0]))) {
        return false;
    }
    for (size_t i = 0; i < str.size(); ++i) {
        char c = str[i];
        if (!(isdigit(c) || isupper(c) || islower(c)
              || (c == '_') || (c == '.') || (c == '-'))) {
            return false;
        }
    }
    return true;
}

} // namespace tera
