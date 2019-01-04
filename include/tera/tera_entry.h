// Copyright (c) 2015-2018, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#pragma once

#include <atomic>

#pragma GCC visibility push(default)
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
    std::atomic<bool> started_;
};

}  // namespace tera
#pragma GCC visibility pop
