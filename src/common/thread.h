// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Author: yanshiguang02@baidu.com

#ifndef  TERA_COMMON_THREAD_H_
#define  TERA_COMMON_THREAD_H_

#include <pthread.h>

#include <boost/function.hpp>

namespace common {

class Thread {
public:
    Thread() : tid_(0) {}
    bool Start(boost::function<void()> thread_proc) {
        user_proc_ = thread_proc;
        int ret = pthread_create(&tid_, NULL, ProcWrapper, this);
        return (ret == 0);
    }
    bool Join() {
        int ret = pthread_join(tid_, NULL);
        return (ret == 0);
    }

private:
    static void* ProcWrapper(void* arg) {
        reinterpret_cast<Thread*>(arg)->user_proc_();
        return NULL;
    }

private:
    boost::function<void()> user_proc_;
    pthread_t tid_;
};

}  // namespace common

#endif  // TERA_COMMON_THREAD_H_
