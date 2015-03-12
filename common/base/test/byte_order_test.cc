// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "common/base/byte_order.h"

#include <stdint.h>
#include "thirdparty/gtest/gtest.h"

TEST(ByteOrder, SwapShort)
{
    short x = 0x1234;
    EXPECT_EQ(0x3412, ByteOrder::Swap(x));
    ByteOrder::Swap(&x);
    EXPECT_EQ(0x3412, x);
}

TEST(ByteOrder, SwapUShort)
{
    unsigned short x = 0x1234;
    EXPECT_EQ(0x3412, ByteOrder::Swap(x));
    ByteOrder::Swap(&x);
    EXPECT_EQ(0x3412, x);
}

TEST(ByteOrder, SwapInt)
{
    int x = 0x12345678;
    EXPECT_EQ(0x78563412, ByteOrder::Swap(x));
    ByteOrder::Swap(&x);
    EXPECT_EQ(0x78563412, x);
}

TEST(ByteOrder, SwapUInt)
{
    unsigned int x = 0x12345678U;
    EXPECT_EQ(0x78563412U, ByteOrder::Swap(x));
    ByteOrder::Swap(&x);
    EXPECT_EQ(0x78563412U, x);
}

TEST(ByteOrder, SwapLong)
{
#ifdef _LP64
    long x = 0x0123456789ABCDEFL;
    long expected = 0xEFCDAB8967452301L;
#else
    long x = 0x12345678L;
    long expected = 0x78563412L;
#endif
    EXPECT_EQ(expected, ByteOrder::Swap(x));
    ByteOrder::Swap(&x);
    EXPECT_EQ(expected, x);
}

TEST(ByteOrder, SwapULong)
{
#ifdef _LP64
    unsigned long x = 0x0123456789ABCDEFULL;
    unsigned long expected = 0xEFCDAB8967452301L;
#else
    unsigned long x = 0x12345678L;
    unsigned long expected = 0x78563412L;
#endif
    EXPECT_EQ(expected, ByteOrder::Swap(x));
    ByteOrder::Swap(&x);
    EXPECT_EQ(expected, x);
}

TEST(ByteOrder, SwapULLong)
{
    unsigned long long x = 0x0123456789ABCDEFULL;
    EXPECT_EQ(0xEFCDAB8967452301ULL, ByteOrder::Swap(x));
    ByteOrder::Swap(&x);
    EXPECT_EQ(0xEFCDAB8967452301ULL, x);
}

TEST(ByteOrder, SwapLLong)
{
    long long x = 0x0123456789ABCDEFLL;
    EXPECT_EQ((long long)0xEFCDAB8967452301LL, ByteOrder::Swap(x));
    ByteOrder::Swap(&x);
    EXPECT_EQ((long long)0xEFCDAB8967452301LL, x);
}

TEST(ByteOrder, htonll)
{
    ASSERT_EQ(0x0100000000000000ULL, htonll(0x1ULL));
    ASSERT_EQ(0x0807060504030201ULL, htonll(0x0102030405060708ULL));
    ASSERT_NE(1ULL, htonll(0x1ULL));
}

#undef htons
#undef htonl

TEST(ByteOrder, LocalToNet)
{
    ASSERT_EQ((htons(0x1234)), ByteOrder::LocalToNet((short)0x1234));
    ASSERT_EQ(htons(0x1234), ByteOrder::LocalToNet((unsigned short)0x1234));
    ASSERT_EQ(htonl(0x12345678), (unsigned int)ByteOrder::LocalToNet(0x12345678));
    ASSERT_EQ(htonl(0x12345678), ByteOrder::LocalToNet(0x12345678U));
    ASSERT_EQ(htonll(0x1234567890ABCDEFLL),
        (unsigned long long)ByteOrder::LocalToNet(0x1234567890ABCDEFLL));
    ASSERT_EQ(htonll(0x1234567890ABCDEFULL), ByteOrder::LocalToNet(0x1234567890ABCDEFULL));
}

#undef ntohs
#undef ntohl

TEST(ByteOrder, NetToLocal)
{
    ASSERT_EQ((ntohs(0x1234)), ByteOrder::NetToLocal((short)0x1234));
    ASSERT_EQ(ntohs(0x1234), ByteOrder::NetToLocal((unsigned short)0x1234));
    ASSERT_EQ(ntohl(0x12345678), (unsigned int)ByteOrder::NetToLocal(0x12345678));
    ASSERT_EQ(ntohl(0x12345678), ByteOrder::NetToLocal(0x12345678U));
    ASSERT_EQ(ntohll(0x1234567890ABCDEFLL),
        (unsigned long long)ByteOrder::NetToLocal(0x1234567890ABCDEFLL));
    ASSERT_EQ(ntohll(0x1234567890ABCDEFULL), ByteOrder::NetToLocal(0x1234567890ABCDEFULL));
}

