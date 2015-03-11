// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef  TERA_SDK_TASK_H_
#define  TERA_SDK_TASK_H_

#include "tera.h"

namespace tera {

class SdkTask {

public:
    enum TYPE {
        READ,
        MUTATION,
        SCAN
    };
    TYPE Type() { return _type; }

    void SetInternalError(StatusCode err) { _internal_err = err; }
    StatusCode GetInternalError() { return _internal_err; }

    void SetMetaTimeStamp(int64_t meta_ts) { _meta_timestamp = meta_ts; }
    int64_t GetMetaTimeStamp() { return _meta_timestamp; }

protected:
    SdkTask(TYPE type)
        : _type(type),
          _internal_err(kTabletNodeOk),
          _meta_timestamp(0) {}
    virtual ~SdkTask() {};

private:
    TYPE _type;
    StatusCode _internal_err;
    int64_t _meta_timestamp;
};

} // namespace tera

#endif  //TERA_SDK_TASK_H_

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
