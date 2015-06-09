// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Author: yanshiguang02@baidu.com

#include "leveldb/dfs.h"

#include <dlfcn.h>
#include <stdio.h>
#include <string>

namespace leveldb {

Dfs* Dfs::NewDfs(const std::string& so_path, const std::string& conf) {
    dlerror();
    fprintf(stderr, "Open %s\n", so_path.c_str());
    void* handle = dlopen(so_path.c_str(), RTLD_LAZY | RTLD_LOCAL | RTLD_DEEPBIND);
    const char* err = dlerror();
    if (handle == NULL) {
        fprintf(stderr, "Open %s fail: %s\n", so_path.c_str(), err);
        return NULL;
    }


    DfsCreator creator = (DfsCreator)dlsym(handle, "NewDfs");
    err = dlerror();
    if (err != NULL) {
        fprintf(stderr, "Load NewDfs from %s fail: %s\n", so_path.c_str(), err);
        return NULL;
    }

    return (*creator)(conf.c_str());
}

}

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
