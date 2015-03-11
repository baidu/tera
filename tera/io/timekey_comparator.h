// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef TERA_IO_TIMEKEY_COMARATOR_H
#define TERA_IO_TIMEKEY_COMARATOR_H

#include "leveldb/slice.h"
#include "leveldb/comparator.h"

namespace tera {
namespace io {

class TimekeyComparator : public leveldb::Comparator {
public:
    TimekeyComparator(const leveldb::Comparator* comparator);
    ~TimekeyComparator();

    int Compare(const leveldb::Slice& a, const leveldb::Slice& b) const;

    const char* Name() const;

    void FindShortestSeparator(std::string* start,
                               const leveldb::Slice& limit) const;

    void FindShortSuccessor(std::string* key) const;

private:
    const leveldb::Comparator* m_comparator;
};

const TimekeyComparator* NewTimekeyComparator(const leveldb::Comparator* comparator);

} // namespace io
} // namespace tera

#endif // TERA_IO_TIMEKEY_COMARATOR_H
