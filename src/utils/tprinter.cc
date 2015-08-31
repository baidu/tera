// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Author: Xu Peilin (xupeilin@baidu.com)

#include "tprinter.h"

#include <stdarg.h>
#include <iomanip>
#include <iostream>
#include <sstream>

namespace tera {

TPrinter::TPrinter(int cols, ...) : cols_(cols) {
    assert (cols > 0);
    va_list args;
    va_start(args, cols);
    for (int i = 0; i < cols; ++i) {
        string item = va_arg(args, char*);
        string name;
        CellType type;
        if (!ParseColType(item, &name, &type)) {
            name = item;
            type = STRING;
        }
        head_.push_back(std::make_pair(name, type));
    }
    va_end(args);
}

TPrinter::~TPrinter() {
}

bool TPrinter::AddRow(int cols, ...) {
    if (cols != cols_) {
        return false;
    }
    Line line;
    va_list args;
    va_start(args, cols);
    for (int i = 0; i < cols; ++i) {
        switch (head_[i].second) {
        case INT:
            line.push_back(Cell((int64_t)va_arg(args, int64_t), INT));
            break;
        case DOUBLE:
            line.push_back(Cell((double)va_arg(args, double), DOUBLE));
            break;
        case STRING:
            line.push_back(Cell((char*)va_arg(args, char*), STRING));
            break;
        default:
            abort();
        }
    }
    va_end(args);
    body_.push_back(line);
    return true;
}

void TPrinter::Print(const PrintOpt& opt) {
#if 0
    if (_table.size() < 1) {
        return;
    }
    int line_len = 0;
    for (size_t i = 0; i < _cols; ++i) {
        line_len += 2 + _col_width[i];
        std::cout << "  " << std::setfill(' ')
            << std::setw(_col_width[i])
            << std::setiosflags(std::ios::left)
            << _table[0][i];
    }
    std::cout << std::endl;
    if (has_head) {
        for (int i = 0; i < line_len + 2; ++i) {
            std::cout << "-";
        }
        std::cout << std::endl;
    }

    for (size_t i = 1; i < _table.size(); ++i) {
        for (size_t j = 0; j < _cols; ++j) {
            std::cout << "  " << std::setfill(' ')
                << std::setw(_col_width[j])
                << std::setiosflags(std::ios::left)
                << _table[i][j];
        }
        std::cout << std::endl;
    }
#endif
}

string TPrinter::ToString(const PrintOpt& opt) {
#if 0
    std::ostringstream ostr;
    if (_table.size() < 1) {
        return "";
    }
    int line_len = 0;
    for (size_t i = 0; i < _cols; ++i) {
        line_len += 2 + _col_width[i];
        ostr << "  " << std::setfill(' ')
            << std::setw(_col_width[i])
            << std::setiosflags(std::ios::left)
            << _table[0][i];
    }
    ostr << std::endl;
    if (has_head) {
        for (int i = 0; i < line_len + 2; ++i) {
            ostr << "-";
        }
        ostr << std::endl;
    }

    for (size_t i = 1; i < _table.size(); ++i) {
        for (size_t j = 0; j < _cols; ++j) {
            ostr << "  " << std::setfill(' ')
                << std::setw(_col_width[j])
                << std::setiosflags(std::ios::left)
                << _table[i][j];
        }
        ostr << std::endl;
    }
    return ostr.str();
#endif
    return "";
}

bool TPrinter::ParseColType(const string& item, string* name, CellType* type) {
    string::size_type pos1;
    pos1 = item.find('<');
    if (pos1 == string::npos) {
        return false;
    }
    if (item[item.size() - 1] != '>') {
        return false;
    }
    string type_str = item.substr(pos1 + 1, item.size() - pos1 - 2);
    if (type_str == "int") {
        *type = INT;
    } else if (type_str == "double") {
        *type = DOUBLE;
    } else if (type_str == "string") {
        *type = STRING;
    } else {
        return false;
    }
    *name = item.substr(0, pos1);
    return true;
}

string TPrinter::NumToStr(const int64_t num) {
    const int64_t kKB = 1000;
    const int64_t kMB = kKB * 1000;
    const int64_t kGB = kMB * 1000;
    const int64_t kTB = kGB * 1000;
    const int64_t kPB = kTB * 1000;

    std::string unit;
    double res;
    if (num > kPB) {
        res = (1.0 * num) / kPB;
        unit = "P";
    } else if (num > kTB) {
        res = (1.0 * num) / kTB;
        unit = "T";
    } else if (num > kGB) {
        res = (1.0 * num) / kGB;
        unit = "G";
    } else if (num > kMB) {
        res = (1.0 * num) / kMB;
        unit = "M";
    } else if (num > kKB) {
        res = (1.0 * num) / kKB;
        unit = "K";
    } else {
        res = num;
        unit = "";
    }
    const int buflen = 16;
    char buf[buflen];
    if ((int)res - res == 0) {
        snprintf(buf, buflen, "%d%s", (int)res, unit.c_str());
    } else {
        snprintf(buf, buflen, "%.2f%s", res, unit.c_str());
    }
    return string(buf);
}

} // namespace tera
