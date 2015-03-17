// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef COMMON_FILE_FILE_DEF_H
#define COMMON_FILE_FILE_DEF_H

#include <stdint.h>


enum FileOpenMode {
    FILE_READ = 0x01,
    FILE_WRITE = 0x02,
    FILE_APPEND = 0x04
};

enum FileErrorCode {
    kFileSuccess,
    kFileErrParameter,
    kFileErrOpenFail,
    kFileErrNotOpen,
    kFileErrWrite,
    kFileErrRead,
    kFileErrClose,
    kFileErrNotExit
};

#endif // COMMON_FILE_FILE_DEF_H
