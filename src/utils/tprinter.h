// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Author: Xu Peilin (xupeilin@baidu.com)

#ifndef  TERA_UTILS_T_PRINTER_H_
#define  TERA_UTILS_T_PRINTER_H_

#include <stdint.h>

#include <iostream>
#include <map>
#include <string>
#include <vector>

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

    TPrinter();
    TPrinter(int cols, ...);
    ~TPrinter();

    bool AddRow(int cols, ...);
    bool AddRow(const std::vector<string>& row);
    bool AddRow(const std::vector<int64_t>& row);

    void Print(const PrintOpt& opt = PrintOpt());

    string ToString(const PrintOpt& opt = PrintOpt());

    void Reset(int cols, ...);
    void Reset(const std::vector<string>& head);

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

        string ToString();

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
    typedef std::vector<Cell> Line;

    // column format: "name<int[,unit]>"
    // e.g. "name<string>", "money<int,yuan>", "speed<int_1024,B>"
    bool ParseColType(const string& item, string* name, CellType* type);
    void FormatOneLine(Line& ori, std::vector<string>* dst);
    static string NumToStr(const double num);

private:
    int cols_;
    std::vector<std::pair<string, CellType> > head_;
    std::vector<Line> body_;
    std::vector<size_t> col_width_;
};

} // namespace tera
#endif // TERA_UTILS_T_PRINTER_H_
