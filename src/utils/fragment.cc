// Copyright (c) 2016, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//

#include "utils/fragment.h"

#include <sstream>

namespace tera {

static int CompareTwoEndKey(const std::string& a, const std::string& b) {
    if (a == "" && b == "") {
        return 0;
    }
    if (a == "") {
        return 1;
    }
    if (b == "") {
        return -1;
    }
    return a.compare(b);
}

bool RangeFragment::IsCoverRange(const std::string& start, const std::string& end) const {
    std::list<std::pair<std::string, std::string> >::const_iterator it=range_.begin();
    for ( ; it != range_.end(); ++it ) {
        if (it->second != ""
            && start.compare(it->second) > 0) {
            continue;
        }
        break;
    }

    if (it == range_.end()) {
        return false;
    }
    return (start.compare(it->first) >= 0)
            && (CompareTwoEndKey(end, it->second) <= 0);
}

bool RangeFragment::AddToRange(const std::string& start, const std::string& end) {
    if (end != "" && start.compare(end) > 0) {
        return false;
    }
    std::list<std::pair<std::string, std::string> >::iterator it=range_.begin();
    for ( ; it != range_.end(); ++it ) {
        if (it->second != ""
            && start.compare(it->second) > 0) {
            continue;
        }
        break;
    }
    if (it == range_.end()) {
        range_.push_back(std::pair<std::string, std::string>(start, end));
        return true;
    }
    /*
     *                         [  )
     *              [-------)          [------)
     *
     *
     *                                   [                  )
     *              [-------)          [------)  .....   [----)
     *
     *
     *                                   [                      )
     *              [-------)          [------)  .....   [----)   [----)
     *
     *
     *                            [                         )
     *              [-------)          [------)  .....   [----)
     *
     *
     *                            [                             )
     *              [-------)          [------)  .....   [----)   [-----)
     */
    std::string new_start = start.compare(it->first) < 0 ? start : it->first;
    std::string new_end = end;
    while (it != range_.end()) {
        if (end == ""
            || end.compare(it->first) >= 0 ) {
            new_end = CompareTwoEndKey(end, it->second) > 0 ? end : it->second;
            it = range_.erase(it);
            continue;
        }
        break;
    }
    range_.insert(it, std::pair<std::string, std::string>(new_start, new_end));
    return true;
}

std::string RangeFragment::DebugString() const {
    std::list<std::pair<std::string, std::string> >::const_iterator it = range_.begin();
    std::stringstream ss;
    for (; it != range_.end(); ++it) {
        ss << it->first << ":" << it->second << " ";
    }
    return ss.str();
}

bool RangeFragment::IsCompleteRange() const {
    return (range_.size() == 1)
           && (range_.begin()->first == "")
           && (range_.begin()->second == "");
}

} // namespace tera
