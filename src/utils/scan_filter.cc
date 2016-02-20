// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "scan_filter.h"

#include <glog/logging.h>

namespace tera {

static bool CheckValue(const KeyValuePair& kv, const Filter& filter) {
    int64_t v1 = *(int64_t*)kv.value().c_str();
    int64_t v2 = *(int64_t*)filter.ref_value().c_str();
    BinCompOp op = filter.bin_comp_op();
    switch (op) {
    case EQ:
        return v1 == v2;
        break;
    case NE:
        return v1 != v2;
        break;
    case LT:
        return v1 < v2;
        break;
    case LE:
        return v1 <= v2;
        break;
    case GT:
        return v1 > v2;
        break;
    case GE:
        return v1 >= v2;
        break;
    default:
        LOG(ERROR) << "illegal compare operator: " << op;
    }
    return false;
}

bool CheckCell(const KeyValuePair& kv, const Filter& filter) {
    switch (filter.type()) {
    case BinComp: {
        if (filter.field() == ValueFilter) {
            if (!CheckValue(kv, filter)) {
                return false;
            }
        } else {
            LOG(ERROR) << "only support value-compare.";
        }
        break;
    }
    default: {
        LOG(ERROR) << "only support compare.";
        break;
    }}
    return true;
}


ScanFilter::ScanFilter(const FilterList& filter_list)
    : _filter_list(filter_list),
      _suc_num(0),
      _filter_num(filter_list.filter_size()) {
}

ScanFilter::~ScanFilter() {}

bool ScanFilter::Check(const KeyValuePair& kv) {
    for (int i = 0; i < _filter_num; ++i) {
        const Filter& filter = _filter_list.filter(i);
        switch (filter.type()) {
        case BinComp: {
            int res = BinCompCheck(kv, filter);
            if (res > 0) {
                _suc_num++;
                return true;
            } else if (res == 0) {
                continue;
            } else {
                return false;
            }
        } break;
        default: {
            LOG(ERROR) << "not support.";
            return false;
        }}
    }
    return true;
}

bool ScanFilter::IsSuccess() {
    if (_suc_num == _filter_num) {
        return true;
    }
    return false;
}

void ScanFilter::GetAllCfs(std::set<string>* cf_set) {
    CHECK(cf_set != NULL);

    for (int i = 0; i < _filter_num; ++i) {
        const Filter& filter = _filter_list.filter(i);
        switch (filter.type()) {
        case BinComp:
            if (filter.field() == ValueFilter) {
                cf_set->insert(filter.content());
            }
            break;
        default:
            LOG(ERROR) << "not support.";
        }
    }
}

int ScanFilter::BinCompCheck(const KeyValuePair& kv, const Filter& filter) {
    if (filter.field() == ValueFilter) {
        if (kv.column_family() == filter.content() && kv.qualifier().size() == 0) {
            if (DoBinCompCheck(filter.bin_comp_op(), kv.value(), filter.ref_value())) {
                return 1;
            } else {
                return -1;
            }
        } else {
            // not the proper column family
            // only support filter on qualifier-empty cf
            return 0;
        }

    } else {
        LOG(ERROR) << "not support";
        return -1;
    }
}

bool ScanFilter::DoBinCompCheck(BinCompOp op, const string& l_value, const string& r_value) {
    int res = l_value.compare(r_value);
    switch (op) {
    case EQ:
        if (res == 0) { return true; }
        break;
    case NE:
        if (res != 0) { return true; }
        break;
    case LT:
        if (res < 0) { return true; }
        break;
    case LE:
        if (res <= 0) { return true; }
        break;
    case GT:
        if (res > 0) { return true; }
        break;
    case GE:
        if (res >= 0) { return true; }
        break;
    default:
        LOG(ERROR) << "illegal compare operator: " << op;
    }
    return false;
}
} // namespace tera
