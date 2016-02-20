// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef  TERA_UTILS_SCAN_FILTER_H_
#define  TERA_UTILS_SCAN_FILTER_H_

#include <set>
#include "proto/tabletnode_rpc.pb.h"

using std::string;

namespace tera {

bool CheckCell(const KeyValuePair& kv, const Filter& filter);

class ScanFilter {
public:
    ScanFilter(const FilterList& filter_list);
    ~ScanFilter();

    bool Check(const KeyValuePair& kv);

    bool IsSuccess();

    void GetAllCfs(std::set<string>* cf_set);

private:
    int BinCompCheck(const KeyValuePair& kv, const Filter& filter);
    bool DoBinCompCheck(BinCompOp op, const string& l_value, const string& r_value);

private:
    ScanFilter();
    FilterList _filter_list;
    int _suc_num;
    int _filter_num;
};

} // namespace tera
#endif // TERA_UTILS_SCAN_FILTER_H_
