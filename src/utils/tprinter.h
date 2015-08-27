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

using std::string;

namespace tera {

class TPrinter {
public:
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

        Cell (int64_t v, CellType t) {
            value.i = v;
            type = t;
        }
        Cell (double v, CellType t)  {
            value.d = v;
            type = t;
        }
        Cell (const string& v, CellType t)  {
            value.s = new string(v);
            type = t;
        }
        ~Cell () {
            if (type == STRING) {
                delete value.s;
            }
        }
    };

    TPrinter(int cols, ...);
    ~TPrinter();

    bool AddRow(int cols, ...);

    void Print(bool has_head = true);

    string ToString(bool has_head = true);

    static string RemoveSubString(const string& input, const string& substr);

private:
    // type format: "name<int>"
    static bool ParseColType(const string& item, string* name, string* type);

private:
    typedef std::vector<Cell> Line;
    std::vector<std::pair<std::string, CellType> > head_;
    std::vector<Line> body_;
    std::vector<int> col_width_;
    size_t cols_;
    static const uint32_t kMaxColWidth = 50;
};

} // namespace tera
#endif // TERA_UTILS_T_PRINTER_H_
