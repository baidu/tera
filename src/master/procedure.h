// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#pragma once
#include <common/timer.h>
#include <glog/logging.h>
namespace tera {
namespace master {

class Procedure {
public:
    // ProcId, Should be unique for each Procedure
    virtual std::string ProcId() const = 0;
    // the whole lifecycle of a Procedure may be consist of several different stages, 
    // and RunNextState() may be called several times before Procedure is Done, so some status info should 
    // be saved in Your Procedure subclass at the end of each stage, and you should do detrmine
    // what to do according the lastest status saved each time RunNextState() is called
    virtual void RunNextStage() = 0;
    // whether the Procedure is finished
    virtual bool Done() = 0;
    virtual ~Procedure() {}

protected:
    static std::string TimeStamp() {
        int64_t ts = get_micros();
        char buf[128] = {0};
        snprintf(buf, 128, "%ld", ts);
        return buf;
    }
};

// below macros should be only used inside subclasses of Procedure
#ifndef TEST
#define PROC_ID (!this ? std::string("") : ProcId())
#else 
#define PROC_ID std::string("test")
#endif
#define PROC_LOG(level) LOG(level) << "[" << PROC_ID << "] "
#define PROC_VLOG(level) VLOG(level) << "[" << PROC_ID << "] "
#define PROC_LOG_IF(level, condition) LOG_IF(level, condition) << "[" << PROC_ID << "] "
#define PROC_VLOG_IF(level, condition) VLOG_IF(level, condition) << "[" << PROC_ID<< "] "
#define PROC_CHECK(condition) CHECK(condition) << "[" << PROC_ID << "] "
}
}

