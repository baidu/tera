// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "io/timekey_comparator.h"

#include <glog/logging.h>

#include "io/coding.h"
#include "types.h"

namespace tera {
namespace io {

TimekeyComparator::TimekeyComparator(const leveldb::Comparator* comparator)
    : comparator_(comparator) {}

TimekeyComparator::~TimekeyComparator() {}

const char* TimekeyComparator::Name() const {
    return "TimekeyComparator";
}

int TimekeyComparator::Compare(const leveldb::Slice& akey,
                               const leveldb::Slice& bkey) const {
    if (akey.size() < sizeof(uint64_t) || bkey.size() < sizeof(uint64_t)) {
        return static_cast<int>(akey.size()) - static_cast<int>(bkey.size());
    }
    int r = comparator_->Compare(ExtractShortKey(akey), ExtractShortKey(bkey));
    if (r == 0) {
        const uint64_t anum = DecodeFixed64(akey.data() + akey.size() - 8);
        const uint64_t bnum = DecodeFixed64(bkey.data() + bkey.size() - 8);
        if (anum > bnum) {
            r = -1;
        } else if (anum < bnum) {
            r = +1;
        }
    }
    return r;
}

void TimekeyComparator::FindShortestSeparator(std::string* start,
                                              const leveldb::Slice& limit) const {
    leveldb::Slice user_start = ExtractTimeKey(*start);
    leveldb::Slice user_limit = ExtractTimeKey(limit);
    std::string tmp(user_start.data(), user_start.size());

    comparator_->FindShortestSeparator(&tmp, user_limit);
    if (tmp.size() < user_start.size() &&
        comparator_->Compare(user_start, tmp) < 0) {
        PutFixed64(&tmp, PackTimestampAndType(kMaxTimeStamp, UKT_FORSEEK));
        CHECK(this->Compare(*start, tmp) < 0);
        CHECK(this->Compare(tmp, limit) < 0);
        start->swap(tmp);
    }
}

void TimekeyComparator::FindShortSuccessor(std::string* key) const {
    leveldb::Slice user_key = ExtractTimeKey(*key);
    std::string tmp(user_key.data(), user_key.size());

    comparator_->FindShortSuccessor(&tmp);
    if (tmp.size() < user_key.size() &&
        comparator_->Compare(user_key, tmp) < 0) {
        PutFixed64(&tmp, PackTimestampAndType(kMaxTimeStamp, UKT_FORSEEK));
        CHECK(this->Compare(*key, tmp) < 0);
        key->swap(tmp);
    }
}

const TimekeyComparator* NewTimekeyComparator(const leveldb::Comparator* comparator) {
    return new TimekeyComparator(comparator);
}
} // namespace io
} // namespace tera
