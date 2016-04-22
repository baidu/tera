// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "string_util.h"

#include <iostream>
#include <sstream>

#include <stdint.h>
#include <stdlib.h>

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

bool IsValidGroupName(const std::string& str) {
    return IsValidName(str);
}

bool IsValidUserName(const std::string& str) {
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

bool IsValidColumnFamilyName(const std::string& str) {
    if ((64 * 1024 - 1) < str.size()) { // [0, 64KB)
        return false;
    }
    for (size_t i = 0; i < str.size(); ++i) {
        char c = str[i];
        if (!isprint(c)) {
            return false;
        }
    }
    return true;
}

std::string RoundNumberToNDecimalPlaces(double n, int d) {
    if (d < 0 || 9 < d) {
        return "(null)";
    }
    std::stringstream ss;
    ss << std::fixed;
    ss.precision(d);
    ss << n;
    return ss.str();
}

struct EditDistanceMatrix {
    EditDistanceMatrix(int row, int col)
        : matrix_((int*)malloc(sizeof(int) * row * col)),
          n_(col) {}
    int& At(int row, int col) {return matrix_[row * n_ + col];}
    ~EditDistanceMatrix() {
        free(matrix_);
        matrix_ = NULL;
    }
    int* matrix_;
private:
    int n_; // columns(row size)
    EditDistanceMatrix(const EditDistanceMatrix& m);
    EditDistanceMatrix& operator=(const EditDistanceMatrix& m);
};

static int MinOfThreeNum(int a, int b, int c) {
    int min = (a < b) ? a : b;
    min = (min < c) ? min : c;
    return min;
}

/*
        a[0] a[1] a[2] a[3] . . . a[n-1]
  b[0]
  b[1]
  b[2]        +    +
  b[3]        +    *
  .
  .
  .
  b[m-1]
*/

// https://en.wikipedia.org/wiki/Edit_distance
// https://en.wikipedia.org/wiki/Levenshtein_distance
int EditDistance(const std::string& a, const std::string& b) {
    int n = a.size();
    int m = b.size();
    if ((n == 0) || (m == 0)) {
        return (n == 0) ? m : n;
    }
    EditDistanceMatrix matrix(m, n);
    matrix.At(0, 0) = (a[0] == b[0]) ? 0 : 1;
    for (size_t i = 1; i < a.size(); i++) {
        matrix.At(0, i) = matrix.At(0, i-1) + 1;
    }
    for (size_t j = 1; j < b.size(); j++) {
        matrix.At(j, 0) = matrix.At(j-1, 0) + 1;
    }
    for (size_t j = 1; j < b.size(); j++) {
        for (size_t i = 1; i < a.size(); i++) {
            int min = MinOfThreeNum(matrix.At(j-1, i-1),
                                    matrix.At(j,   i-1),
                                    matrix.At(j-1, i));
            if (a[i] == b[j]) {
                matrix.At(j, i) = min;
            } else {
                matrix.At(j, i) = min + 1;
            }
        }
    }
    return matrix.At(m-1, n-1);
}

} // namespace tera
