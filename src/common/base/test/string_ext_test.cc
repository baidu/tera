// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "bvs/common/base/string_ext.h"

int main(int argc, char* argv[])
{
    cout << strtool::trim(" nihao ") <<"\n";

    vector<string> vt;
    strtool::split(",o h,,,nice,,,,,,,", vt);
    for (size_t i = 0; i < vt.size(); ++i) {
        cout <<"out:" << vt[i] <<"\n";
    }

    string ret = strtool::replace("xxAxxxAxxAxx", "A", "B");
    cout <<"replace:" << ret <<"\n";
    return 0;
}
