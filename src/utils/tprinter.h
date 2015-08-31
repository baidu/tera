// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Author: Xu Peilin (xupeilin@baidu.com)

#ifndef  TERA_UTILS_T_PRINTER_H_
#define  TERA_UTILS_T_PRINTER_H_

#include <stdint.h>

#include <string>
#include <vector>
#include <iostream>

using std::string;

namespace tera {

class TPrinter {
public:
    struct PrintOpt {
    public:
        bool print_head; // if print table header

        // >0 for positive order, <0 for reverse order, 0 for not sort
        int  sort_dir;
        int  sort_col;   // select column num for sorting

        PrintOpt() : print_head(true), sort_dir(0) {}
    };

    TPrinter(int cols, ...);
    ~TPrinter();

    bool AddRow(int cols, ...);

    void Print(const PrintOpt& opt = PrintOpt());

    string ToString(const PrintOpt& opt = PrintOpt());

private:
    enum CellType {
        INT,
        DOUBLE,
        STRING
    };
    struct Cell {
        CellType type;
        union {
            int64_t i;
            double  d;
            string* s;
        } value;

        Cell (int64_t v,       CellType t) { value.i = v; type = t; }
        Cell (double  v,       CellType t) { value.d = v; type = t; }
        Cell (const string& v, CellType t) { value.s = new string(v); type = t; }
        Cell (const Cell& ref) { *this = ref; }
        ~Cell () { if (type == STRING) delete value.s; }
        Cell& operator=(const Cell& ref) {
            type = ref.type;
            if (type == STRING && this != &ref) {
                value.s = new string(*ref.value.s);
            } else {
                value = ref.value;
            }
            return *this;
        }
    };

    // column format: "name<int>"
    static bool ParseColType(const string& item, string* name, CellType* type);
    static string NumToStr(const int64_t num);

private:
    typedef std::vector<Cell> Line;
    std::vector<std::pair<std::string, CellType> > head_;
    std::vector<Line> body_;
//    std::vector<int> col_width_;
    int cols_;
    static const uint32_t kMaxColWidth = 50;
};

} // namespace tera
#endif // TERA_UTILS_T_PRINTER_H_
