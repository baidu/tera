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
#include "common/base/string_number.h"

namespace tera {

TPrinter::TPrinter(int cols, ...) : cols_(cols) {
    assert (cols > 0);
    va_list args;
    va_start(args, cols);
    for (int i = 0; i < cols; ++i) {
        string item = va_arg(args, char*);
        string name, type;
        if (!ParseColType(item, &name, &type)) {
            name = item;
            type = "string";
        }
        if (type == "int") {
            head_.push_back(std::make_pair(name, INT));
        } else if (type == "double") {
            head_.push_back(std::make_pair(name, DOUBLE));
        } else if (type == "string") {
            head_.push_back(std::make_pair(name, STRING));
        } else {
            abort();
        }
    }
    va_end(args);
}

TPrinter::~TPrinter() {
}

bool TPrinter::AddRow(int cols, ...) {
    if (cols != cols_) {
        return false;
    }
    va_list args;
    va_start(args, cols);
    for (int i = 0; i < cols; ++i) {
        switch (head_[i].second)
        string item = va_arg(args, char*);
        string name, type;
        if (!ParseColType(item, &name, &type)) {
            name = item;
            type = "string";
        }

        if (type == "int") {
            head_.push_back(std::make_pair(name, INT));
        } else if (type == "double") {
            head_.push_back(std::make_pair(name, DOUBLE));
        } else if (type == "string") {
            head_.push_back(std::make_pair(name, STRING));
        } else {
            abort();
        }
    }
    va_end(args);
    return true;
}

void TPrinter::Print(bool has_head) {
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

string TPrinter::ToString(bool has_head) {
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

bool TPrinter::ParseColType(const string& item, string* name, string* type) {
    string::size_type pos1;
    pos1 = item.find('<');
    if (pos1 == string::npos) {
        return false;
    }
    if (item[item.size() - 1] != '>') {
        return false;
    }
    *name = item.substr(0, pos1);
    *type = item.substr(pos1 + 1, item.size() - pos1 - 2);
    return true;
}

string TPrinter::RemoveSubString(const string& input, const string& substr) {
    string ret;
    string::size_type p = 0;
    string tmp = input;
    while (1) {
        tmp = tmp.substr(p);
        p = tmp.find(substr);
        ret.append(tmp.substr(0, p));
        if (p == string::npos) {
            break;
        }
        p += substr.size();
    }

    return ret;
}
} // namespace tera
