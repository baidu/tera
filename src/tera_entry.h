// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef  TERA_TERA_ENTRY_H_
#define  TERA_TERA_ENTRY_H_

#include "common/mutex.h"

namespace tera {

class TeraEntry {
public:
    TeraEntry();
    virtual ~TeraEntry();

    virtual bool Start();
    virtual bool Run();
    virtual bool Shutdown();

protected:
    virtual bool StartServer() = 0;
    virtual void ShutdownServer() = 0;

private:
    bool ShouldStart();
    bool ShouldShutdown();

private:
    Mutex mutex_;
    bool started_;
};

}  // namespace tera

#endif  // TERA_TERA_ENTRY_H_
