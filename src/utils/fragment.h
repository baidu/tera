// Copyright (c) 2016, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef  TERA_FRAGMENT_UTIL_H_
#define  TERA_FRAGMENT_UTIL_H_

#include <list>
#include <string>

namespace tera {

class RangeFragment {
public:
    // caller should use Lock to avoid data races
    // On success, return true. Otherwise, return false due to invalid argumetns
    bool AddToRange(const std::string& start, const std::string& end);

    bool IsCompleteRange() const;

    bool IsCoverRange(const std::string& start, const std::string& end) const;

    std::string DebugString() const;

private:
    std::list<std::pair<std::string, std::string> > range_;
};

} // namespace tera

#endif // TERA_FRAGMENT_UTIL_H_
