// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "common/base/ascii.h"
#include <ctype.h>
#include <limits.h>
#include <locale.h>
#include "thirdparty/gtest/gtest.h"

// namespace common {

TEST(Ascii, Init)
{
    setlocale(LC_ALL, "C");
}

#define ASCII_TEST_CTYPE_FUNCTION_EQUIVALENCE(type, New, std) \
TEST(Ascii, New) \
{ \
    for (int c = 0; c <= UCHAR_MAX; ++c) \
        EXPECT_EQ(static_cast<type>(std(c)), Ascii::New(c)) \
            << c << "(" << (isprint(c) ? static_cast<char>(c): ' ') << ")"; \
}

ASCII_TEST_CTYPE_FUNCTION_EQUIVALENCE(bool, IsLower, islower)
ASCII_TEST_CTYPE_FUNCTION_EQUIVALENCE(bool, IsUpper, isupper)
ASCII_TEST_CTYPE_FUNCTION_EQUIVALENCE(bool, IsAlpha, isalpha)
ASCII_TEST_CTYPE_FUNCTION_EQUIVALENCE(bool, IsDigit, isdigit)
ASCII_TEST_CTYPE_FUNCTION_EQUIVALENCE(bool, IsAlphaNumber, isalnum)
ASCII_TEST_CTYPE_FUNCTION_EQUIVALENCE(bool, IsHexDigit, isxdigit)

#ifdef __GNUC__ // windows has no function named 'isblank'
ASCII_TEST_CTYPE_FUNCTION_EQUIVALENCE(bool, IsBlank, isblank)
#endif

ASCII_TEST_CTYPE_FUNCTION_EQUIVALENCE(bool, IsSpace, isspace)
ASCII_TEST_CTYPE_FUNCTION_EQUIVALENCE(bool, IsPunct, ispunct)
ASCII_TEST_CTYPE_FUNCTION_EQUIVALENCE(bool, IsControl, iscntrl)
ASCII_TEST_CTYPE_FUNCTION_EQUIVALENCE(bool, IsGraph, isgraph)
ASCII_TEST_CTYPE_FUNCTION_EQUIVALENCE(bool, IsPrint, isprint)
ASCII_TEST_CTYPE_FUNCTION_EQUIVALENCE(char, ToUpper, toupper)
ASCII_TEST_CTYPE_FUNCTION_EQUIVALENCE(char, ToLower, tolower)
ASCII_TEST_CTYPE_FUNCTION_EQUIVALENCE(char, ToAscii, toascii)

// } // namespace common
