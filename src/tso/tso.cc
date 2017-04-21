// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "tso/tso.h"

#include "tso_client_impl.h"

namespace tera {
namespace tso {

TimestampOracle::TimestampOracle() {
    impl_ = new TsoClientImpl;
}

TimestampOracle::~TimestampOracle() {
    delete impl_;
}

int64_t TimestampOracle::GetTimestamp() {
    return impl_->GetTimestamp();
}

void TimestampOracle::GetTimestamp(std::function<void (int64_t)> callback) {
    impl_->GetTimestamp(callback);
}

} // namespace tso
} // namespace tera
