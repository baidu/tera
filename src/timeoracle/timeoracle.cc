// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "timeoracle/timeoracle.h"

namespace tera {
namespace timeoracle {

std::atomic<int64_t>    Timeoracle::s_last_timestamp_ms;

} // namespace timeoracle
} // namespace tera
