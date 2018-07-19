// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef TERA_LOAD_BALANCER_PLAN_H_
#define TERA_LOAD_BALANCER_PLAN_H_

#include <string>

#include "master/tablet_manager.h"
#include "master/tabletnode_manager.h"

namespace tera {
namespace load_balancer {

class Plan {
public:
    Plan() {}

    Plan(const tera::master::TabletPtr& tablet,
         const tera::master::TabletNodePtr& source,
         const tera::master::TabletNodePtr& dest) {
        tablet_ = tablet;
        source_ = source;
        dest_ = dest;
    }

    virtual ~Plan() {}

    virtual std::string TabletPath() const {
        if (tablet_) {
            return tablet_->GetPath();
        } else {
            return "";
        }
    }

    virtual std::string SourceAddr() const {
        if (source_) {
            return source_->GetAddr();
        } else {
            return "";
        }
    }

    virtual std::string DestAddr() const {
        if (dest_) {
            return dest_->GetAddr();
        } else {
            return "";
        }
    }

    virtual std::string ToString() const {
        std::string str = "tablet:" + (tablet_ ? tablet_->GetPath() : "")
                + " source:" + (source_ ? source_->GetAddr() : "")
                + " dest:" + (dest_ ? dest_->GetAddr() : "");

        return str;
    }

private:
    tera::master::TabletPtr tablet_;
    tera::master::TabletNodePtr source_;
    tera::master::TabletNodePtr dest_;
};

} // namespace load_balancer
} // namespace tera

#endif // TERA_LOAD_BALANCER_PLAN_H_
