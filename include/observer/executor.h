// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef OBSERVER_EXECUTOR_H_
#define OBSERVER_EXECUTOR_H_

#include "observer.h"

#pragma GCC visibility push(default)
namespace observer {

/// 执行器
class Executor {
public:
    static Executor* NewExecutor();
    
    // 注册需要运行的Observer
    virtual bool RegisterObserver(Observer* observer) = 0;
    
    // 启动接口
    virtual bool Run() = 0;

    Executor() {}
    virtual ~Executor() {}

private:
    Executor(const Executor&);
    void operator=(const Executor&);
};

} // namespace observer
#pragma GCC visibility pop

#endif  // OBSERVER_EXECUTOR_H_
