// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Author: An Qin (qinan@baidu.com)

#ifndef TERA_COMMON_BASE_STDINT_H_
#define TERA_COMMON_BASE_STDINT_H_

#ifndef __STDC_LIMIT_MACROS
#define __STDC_LIMIT_MACROS
#endif
#if defined __GNUC__
#include_next "stdint.h"
#elif defined _MSC_VER
#include "common/base/vc_stdint.h"
#else
#error Unknown compiler
#endif

// complete all missing definitions in case previous <stdint.h> inclusion
// without __STDC_LIMIT_MACROS

#ifndef __WORDSIZE
#ifndef _WIN32
#ifdef _WIN64
#define __WORDSIZE 64
#else
#define __WORDSIZE 32
#endif
#endif
#endif

#ifndef __INT64_C
#if __WORDSIZE == 64
#define __INT64_C(c) c##L
#define __UINT64_C(c) c##UL
#else
#define __INT64_C(c) c##LL
#define __UINT64_C(c) c##ULL
#endif
#endif

#ifndef INT64_MIN
/* Minimum of signed integral types.  */
#define INT8_MIN (-128)
#define INT16_MIN (-32767 - 1)
#define INT32_MIN (-2147483647 - 1)
#define INT64_MIN (-__INT64_C(9223372036854775807) - 1)
/* Maximum of signed integral types.  */
#define INT8_MAX (127)
#define INT16_MAX (32767)
#define INT32_MAX (2147483647)
#define INT64_MAX (__INT64_C(9223372036854775807))

/* Maximum of unsigned integral types.  */
#define UINT8_MAX (255)
#define UINT16_MAX (65535)
#define UINT32_MAX (4294967295U)
#define UINT64_MAX (__UINT64_C(18446744073709551615))

/* Values to test for integral types holding `void *' pointer.  */
#if __WORDSIZE == 64
#define INTPTR_MIN (-9223372036854775807L - 1)
#define INTPTR_MAX (9223372036854775807L)
#define UINTPTR_MAX (18446744073709551615UL)
#else
#define INTPTR_MIN (-2147483647 - 1)
#define INTPTR_MAX (2147483647)
#define UINTPTR_MAX (4294967295U)
#endif

/* Minimum for largest signed integral type.  */
#define INTMAX_MIN (-__INT64_C(9223372036854775807) - 1)
/* Maximum for largest signed integral type.  */
#define INTMAX_MAX (__INT64_C(9223372036854775807))

/* Maximum for largest unsigned integral type.  */
#define UINTMAX_MAX (__UINT64_C(18446744073709551615))

/* Limits of other integer types.  */

/* Limits of `ptrdiff_t' type.  */
#if __WORDSIZE == 64
#define PTRDIFF_MIN (-9223372036854775807L - 1)
#define PTRDIFF_MAX (9223372036854775807L)
#else
#define PTRDIFF_MIN (-2147483647 - 1)
#define PTRDIFF_MAX (2147483647)
#endif

/* Limit of `size_t' type.  */
#ifndef SIZE_MAX
#if __WORDSIZE == 64
#define SIZE_MAX (18446744073709551615UL)
#else
#define SIZE_MAX (4294967295U)
#endif
#endif

/* Limits of `wchar_t'.  */
#ifndef WCHAR_MIN
/* These constants might also be defined in <wchar.h>.  */
#define WCHAR_MIN __WCHAR_MIN
#define WCHAR_MAX __WCHAR_MAX
#endif

/* Limits of `wint_t'.  */
#define WINT_MIN (0u)
#define WINT_MAX (4294967295u)

#endif

#ifndef INT8_C
/* Signed.  */
#define INT8_C(c) c
#define INT16_C(c) c
#define INT32_C(c) c
#if __WORDSIZE == 64
#define INT64_C(c) c##L
#else
#define INT64_C(c) c##LL
#endif

/* Unsigned.  */
#define UINT8_C(c) c##U
#define UINT16_C(c) c##U
#define UINT32_C(c) c##U
#if __WORDSIZE == 64
#define UINT64_C(c) c##UL
#else
#define UINT64_C(c) c##ULL
#endif

/* Maximal type.  */
#if __WORDSIZE == 64
#define INTMAX_C(c) c##L
#define UINTMAX_C(c) c##UL
#else
#define INTMAX_C(c) c##LL
#define UINTMAX_C(c) c##ULL
#endif
#endif

#endif  // TERA_COMMON_BASE_STDINT_H_
