// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef TERA_MASTER_TABLET_AVAILABILITY_H_
#define TERA_MASTER_TABLET_AVAILABILITY_H_

#include <string>

#include "master/tablet_manager.h"

#include "common/mutex.h"

namespace tera {
namespace master {

class TabletAvailability {
public:
    TabletAvailability(boost::shared_ptr<TabletManager> t) : tablet_manager_(t) {}
    void LogAvailability();
    void AddNotReadyTablet(const std::string& id);
    void EraseNotReadyTablet(const std::string& id);

private:
    Mutex mutex_;
    boost::shared_ptr<TabletManager> tablet_manager_;
    std::map<std::string, int64_t> tablets_;
};

} // master
} // tera

#endif // TERA_MASTER_TABLET_AVAILABILITY_H_
