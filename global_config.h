// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef GLOBAL_CONFIG_H

// for support both GCC3.4.5 & GCC4.8 of Redhat AS4.5 environment in Baidu
#define GCC_VERSION (__GNUC__ * 10000 \
                     + __GNUC_MINOR__ * 100 \
                     + __GNUC_PATCHLEVEL__)
// check gcc versin >= 4.8.0
#if GCC_VERSION > 40800
    #undef BAIDU_REDHAT_ENV
# else
//    #warning "GCC 4.8 not support, simulate some advanced functionalities"
    #define BAIDU_REDHAT_ENV
#endif


// for support COMAKE
#define USE_BAIDU_COMAKE

// for support sofa
#define USE_SOFA

#endif // GLOBAL_CONFIG_H
