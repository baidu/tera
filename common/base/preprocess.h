// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef COMMON_BASE_PREPROCESS_H
#define COMMON_BASE_PREPROCESS_H

#include "common/base/static_assert.h"

/// Converts the parameter X to a string after macro replacement
/// on X has been performed.
/// example: PP_STRINGIZE(UCHAR_MAX) -> "255"
#define PP_STRINGIZE(X) PP_DO_STRINGIZE(X)
#define PP_DO_STRINGIZE(X) #X

/// Helper macro to join 2 tokens
/// example: PP_JOIN(UCHAR_MAX, SCHAR_MIN) -> 255(-128)
/// The following piece of macro magic joins the two
/// arguments together, even when one of the arguments is
/// itself a macro (see 16.3.1 in C++ standard). The key
/// is that macro expansion of macro arguments does not
/// occur in PP_DO_JOIN2 but does in PP_DO_JOIN.
#define PP_JOIN(X, Y) PP_DO_JOIN(X, Y)
#define PP_DO_JOIN(X, Y) PP_DO_JOIN2(X, Y)
#define PP_DO_JOIN2(X, Y) X##Y

/// prevent macro substitution for function-like macros
/// if macro 'min()' was defined:
/// 'int min()' whill be substituted, but
/// 'int min PP_PREVENT_MACRO_SUBSTITUTION()' will not be substituted.
#define PP_PREVENT_MACRO_SUBSTITUTION

/// disallow macro be used in header files
///
/// @example
/// #define SOMEMACRO() PP_DISALLOW_IN_HEADER_FILE()
/// A compile error will be issued if SOMEMACRO() is used in header files
#ifdef __GNUC__
# define PP_DISALLOW_IN_HEADER_FILE() \
    STATIC_ASSERT(__INCLUDE_LEVEL__ == 0, "This macro can not be used in header files");
#else
# define PP_DISALLOW_IN_HEADER_FILE()
#endif

#endif // COMMON_BASE_PREPROCESS_H

