// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "common/base/closure.h"

#include <stdio.h>
#include <stdlib.h>
#include "global_config.h"

#if !defined(BAIDU_REDHAT_ENV)
#include <functional>
#include <tr1/functional>
#endif

#include "common/base/scoped_ptr.h"
#include <gtest/gtest.h>

// GLOBAL_NOLINT(runtime/int)
// GLOBAL_NOLINT(readability/casting)

//       preArg
void test0() {
    printf("test0()\r\n");
}
void test1(char preA) {
    printf("test1(),preA=%c\r\n", preA);
}
void test2(char preA, short preB) {
    printf("test2(),preA=%c,preB=%d\r\n", preA, preB);
}
void test3(char preA, short preB, int preC) {
    printf("test3(),preA=%c,preB=%d,preC=%d\r\n", preA, preB, preC);
}

//   preArg_Arg(last one)
void test0_1(char a) {
    printf("test0_1(),a=%c\r\n", a);
}
void test1_1(char preA, short b) {
    printf("test1_1(),preA=%c,b=%d\r\n", preA, b);
}
void test2_1(char preA, short preB, int c) {
    printf("test2_1(),preA=%c,preB=%d,c=%d\r\n", preA, preB, c);
}

//   preArg_Arg(last)
void test0_2(char a, short b) {
    printf("test0_2(),a=%c,b=%d\r\n", a, b);
}
void test1_2(char preA, char a, int b) {
    printf("test1_2(),preA=%c,a=%c,b=%d\n", preA, a, b);
}

//   preArg_Arg(last)
void test0_3(char a, short b, int c) {
    printf("test0_3(),a=%c,b=%d,c=%d\r\n", a, b, c);
}

//----------Closure---------
//       preArg
char Rtest0() {
    printf("Rtest0()\r\n");
    return 'a';
}
char Rtest1(char preA) {
    printf("Rtest1(),preA=%c\r\n", preA);
    return preA;
}
char Rtest2(char preA, short preB) {
    printf("Rtest2(),preA=%c,preB=%d\r\n", preA, preB);
    return preA;
}
char Rtest3(char preA, short preB, int preC) {
    printf("Rtest3(),preA=%c,preB=%d,preC=%d\r\n", preA, preB, preC);
    return preA;
}

//   preArg_Arg(last one)
short Rtest0_1(char a) {
    printf("Rtest0_1(),a=%c\r\n", a);
    return short(980);
}

short Rtest1_1(char preA, short b) {
    printf("Rtest1_1(),preA=%c,b=%d\r\n", preA, b);
    return b;
}

short Rtest2_1(char preA, short preB, int c) {
    printf("Rtest2_1(),preA=%c,preB=%d,c=%d\r\n", preA, preB, c);
    return preB;
}

//   preArg_Arg(last)
int Rtest0_2(char a, short b) {
    printf("Rtest0_2(),a=%c,b=%d\r\n", a, b);
    return int(9900);
}
int Rtest1_2(char preA, char a, int b) {
    printf("Rtest1_2(),preA=%c,a=%c,b=%d\r\n", preA, a, b);
    return b;
}

//   preArg_Arg(last)
unsigned int Rtest0_3(char a, short b, int c) {
    printf("Rtest0_3(),a=%c,b=%d,c=%d\r\n", a, b, c);
    return (unsigned int)c;
}

class CTest
{
public:
    CTest() {}
    virtual ~CTest() {}

    //       preArg
    void test0() {
        printf("test0()\r\n");
    }
    void test1(char preA) {
        printf("test1(),preA=%c\r\n", preA);
    }
    void test2(char preA, short preB) {
        printf("test2(),preA=%c,preB=%d\r\n", preA, preB);
    }
    void test3(char preA, short preB, int preC) {
        printf("test3(),preA=%c,preB=%d,preC=%d\r\n", preA, preB, preC);
    }

    //   preArg_Arg(last one)
    void test0_1(char a) {
        printf("test0_1(),a=%c\r\n", a);
    }
    void test1_1(char preA, short b) {
        printf("test1_1(),preA=%c,b=%d\r\n", preA, b);
    }
    void test2_1(char preA, short preB, int c) {
        printf("test2_1(),preA=%c,preB=%d,c=%d\r\n", preA, preB, c);
    }

    //   preArg_Arg(last)
    void test0_2(char a, short b) {
        printf("test0_2(),a=%c,b=%d\r\n", a, b);
    }
    void test1_2(char preA, char a, int b) {
        printf("test1_2(),preA=%c,a=%c,b=%d\n", preA, a, b);
    }

    //   preArg_Arg(last)
    void test0_3(char a, short b, int c) {
        printf("test0_3(),a=%c,b=%d,c=%d\r\n", a, b, c);
    }

    //----------Closure---------
    //       preArg
    char Rtest0() {
        printf("Rtest0()\r\n");
        return 'a';
    }
    char Rtest1(char preA) {
        printf("Rtest1(),preA=%c\r\n", preA);
        return preA;
    }
    char Rtest2(char preA, short preB) {
        printf("Rtest2(),preA=%c,preB=%d\r\n", preA, preB);
        return preA;
    }
    char Rtest3(char preA, short preB, int preC) {
        printf("Rtest3(),preA=%c,preB=%d,preC=%d\r\n", preA, preB, preC);
        return preA;
    }

    //   preArg_Arg(last one)
    short Rtest0_1(char a) {
        printf("Rtest0_1(),a=%c\r\n", a);
        return short(980);
    }
    short Rtest1_1(char preA, short b) {
        printf("Rtest1_1(),preA=%c,b=%d\r\n", preA, b);
        return b;
    }
    short Rtest2_1(char preA, short preB, int c) {
        printf("Rtest2_1(),preA=%c,preB=%d,c=%d\r\n", preA, preB, c);
        return preB;
    }

    //   preArg_Arg(last)
    int Rtest0_2(char a, short b) {
        printf("Rtest0_2(),a=%c,b=%d\r\n", a, b);
        return int(9900);
    }
    int Rtest1_2(char preA, char a, int b) {
        printf("Rtest1_2(),preA=%c,a=%c,b=%d\r\n", preA, a, b);
        return b;
    }

    //   preArg_Arg(last)
    unsigned int Rtest0_3(char a, short b, int c) {
        printf("Rtest0_3(),a=%c,b=%d,c=%d\r\n", a, b, c);
        return (unsigned int) c;
    }
};

TEST(Closure, Closure)
{
    printf("\r\n==============test NewClosure===============\r\n");

    // Closure0
    Closure<void> * pcb = NewClosure(test0);
    ASSERT_TRUE(pcb->IsSelfDelete());
    pcb->Run();

    pcb = NewClosure(test1, 'a');
    pcb->Run();

    pcb = NewClosure(test2, char('a'), short(456));
    pcb->Run();

    pcb = NewClosure(test3, char('a'), short(456), int(9800));
    pcb->Run();

    // Closure1
    Closure<void, char>* pcb0_1 = NewClosure(test0_1);
    pcb0_1->Run('a');

    Closure<void, short>* pcb1_1 = NewClosure(test1_1, 'a');
    pcb1_1->Run(780);

    Closure<void, int>* pcb2_1 = NewClosure(test2_1, char('a'), short(7800));
    pcb2_1->Run(9800);

    // Closure2
    Closure<void, char, short>* pcb0_2 = NewClosure(test0_2);
    pcb0_2->Run('a', 8900);

    Closure<void, char, int>* pcb1_2 = NewClosure(test1_2, 'a');
    pcb1_2->Run('b', 780);

    // Closure3
    Closure<void, char, short, int>* pcb0_3 = NewClosure(test0_3);
    pcb0_3->Run('a', 456, 78909);

    //------Closure------------
    printf("\r\n------Closure------------\r\n");

    // Closure0
    Closure<char>* pRcb = NewClosure(Rtest0);
    char r0 = pRcb->Run();
    EXPECT_EQ('a', r0);

    pRcb = NewClosure(Rtest1, 'a');
    r0 = pRcb->Run();
    EXPECT_EQ('a', r0);

    pRcb = NewClosure(Rtest2, char('a'), short(456));
    r0 = pRcb->Run();
    EXPECT_EQ('a', r0);

    pRcb = NewClosure(Rtest3, char('a'), short(456), int(9800));
    r0 = pRcb->Run();
    EXPECT_EQ('a', r0);

    // Closure1
    Closure<short, char>* pRcb0_1 = NewClosure(Rtest0_1);
    short r1 = pRcb0_1->Run('a');
    EXPECT_EQ(980, r1);

    Closure<short, short>* pRcb1_1 = NewClosure(Rtest1_1, 'a');
    r1 = pRcb1_1->Run(780);
    EXPECT_EQ(780, r1);

    Closure<short, int>* pRcb2_1 = NewClosure(Rtest2_1, char('a'), short(7800));
    r1 = pRcb2_1->Run(9800);
    EXPECT_EQ(7800, r1);

    // Closure2
    Closure<int, char, short>* pRcb0_2 = NewClosure(Rtest0_2);
    int r2 = pRcb0_2->Run('a', 8900);
    EXPECT_EQ(9900, r2);

    Closure<int, char, int>* pRcb1_2 = NewClosure(Rtest1_2, 'a');
    r2 = pRcb1_2->Run('b', 780);
    EXPECT_EQ(780, r2);

    // Closure3
    Closure<unsigned int, char, short, int>* pRcb0_3 = NewClosure(Rtest0_3);
    unsigned int r3 = pRcb0_3->Run('a', 456, 78909);
    EXPECT_EQ(78909U, r3);
}

TEST(Closure, PermanentClosure)
{
    printf("\r\n==============test NewPermanentClosure===============\r\n");
    // Closure0
    Closure<void>* pcb = NewPermanentClosure(test0);
    ASSERT_FALSE(pcb->IsSelfDelete());
    pcb->Run();
    delete pcb;

    pcb = NewPermanentClosure(test1, 'a');
    pcb->Run();
    delete pcb;

    pcb = NewPermanentClosure(test2, char('a'), short(456));
    pcb->Run();
    delete pcb;

    pcb = NewPermanentClosure(test3, char('a'), short(456), int(9800));
    pcb->Run();
    delete pcb;

    // Closure1
    Closure<void, char>* pcb0_1 = NewPermanentClosure(test0_1);
    pcb0_1->Run('a');
    delete pcb0_1;

    Closure<void, short>* pcb1_1 = NewPermanentClosure(test1_1, 'a');
    pcb1_1->Run(780);
    delete pcb1_1;

    Closure<void, int>* pcb2_1 = NewPermanentClosure(test2_1, char('a'), short(7800));
    pcb2_1->Run(9800);
    delete pcb2_1;

    // Closure2
    Closure<void, char, short>* pcb0_2 = NewPermanentClosure(test0_2);
    pcb0_2->Run('a', 8900);
    delete pcb0_2;

    Closure<void, char, int>* pcb1_2 = NewPermanentClosure(test1_2, 'a');
    pcb1_2->Run('b', 780);
    delete pcb1_2;

    // Closure3
    Closure<void, char, short, int>* pcb0_3 = NewPermanentClosure(test0_3);
    pcb0_3->Run('a', 456, 78909);
    delete pcb0_3;

    //------Closure------------
    printf("\r\n------Closure------------\r\n");

    // Closure0
    Closure<char>* pRcb = NewPermanentClosure(Rtest0);
    char r0 = pRcb->Run();
    EXPECT_EQ('a', r0);
    delete pRcb;

    pRcb = NewPermanentClosure(Rtest1, 'a');
    r0 = pRcb->Run();
    EXPECT_EQ('a', r0);
    delete pRcb;

    pRcb = NewPermanentClosure(Rtest2, char('a'), short(456));
    r0 = pRcb->Run();
    EXPECT_EQ('a', r0);
    delete pRcb;

    pRcb = NewPermanentClosure(Rtest3, char('a'), short(456), int(9800));
    r0 = pRcb->Run();
    EXPECT_EQ('a', r0);
    delete pRcb;

    // Closure1
    Closure<short, char>* pRcb0_1 = NewPermanentClosure(Rtest0_1);
    short r1 = pRcb0_1->Run('a');
    EXPECT_EQ(980, r1);
    delete pRcb0_1;

    Closure<short, short>* pRcb1_1 = NewPermanentClosure(Rtest1_1, 'a');
    r1 = pRcb1_1->Run(780);
    EXPECT_EQ(780, r1);
    delete pRcb1_1;

    Closure<short, int>* pRcb2_1 = NewPermanentClosure(Rtest2_1, char('a'), short(7800));
    r1 = pRcb2_1->Run(9800);
    EXPECT_EQ(7800, r1);
    delete pRcb2_1;

    // Closure2
    Closure<int, char, short>* pRcb0_2 = NewPermanentClosure(Rtest0_2);
    int r2 = pRcb0_2->Run('a', 8900);
    EXPECT_EQ(9900, r2);
    delete pRcb0_2;

    Closure<int, char, int>* pRcb1_2 = NewPermanentClosure(Rtest1_2, 'a');
    r2 = pRcb1_2->Run('b', 780);
    EXPECT_EQ(780, r2);
    delete pRcb1_2;

    // Closure3
    Closure<unsigned int, char, short, int>* pRcb0_3 = NewPermanentClosure(Rtest0_3);
    unsigned int r3 = pRcb0_3->Run('a', 456, 78909);
    EXPECT_EQ(78909U, r3);
    delete pRcb0_3;
}

TEST(Closure, ClassClosure)
{
    printf("\r\n==============test NewClosure(Class)===============\r\n");

    CTest obj;

    // Closure0
    Closure<void>* pcb = NewClosure(&obj, &CTest::test0);
    pcb->Run();

    pcb = NewClosure(&obj, &CTest::test1, 'a');
    pcb->Run();

    pcb = NewClosure(&obj, &CTest::test2, char('a'), short(456));
    pcb->Run();

    pcb = NewClosure(&obj, &CTest::test3, char('a'), short(456), int(9800));
    pcb->Run();

    // Closure1
    Closure<void, char>* pcb0_1 = NewClosure(&obj, &CTest::test0_1);
    pcb0_1->Run('a');

    Closure<void, short>* pcb1_1 = NewClosure(&obj, &CTest::test1_1, 'a');
    pcb1_1->Run(short(780));

    Closure<void, int>* pcb2_1 = NewClosure(&obj, &CTest::test2_1, char('a'), short(7800));
    pcb2_1->Run(9800);

    // Closure2
    Closure<void, char, short>* pcb0_2 = NewClosure(&obj, &CTest::test0_2);
    pcb0_2->Run('a', 8900);

    Closure<void, char, int>* pcb1_2 = NewClosure(&obj, &CTest::test1_2, 'a');
    pcb1_2->Run('b', 780);

    // Closure3
    Closure<void, char, short, int>* pcb0_3 = NewClosure(&obj, &CTest::test0_3);
    pcb0_3->Run('a', 456, 78909);

    //------Closure------------
    printf("\r\n------Closure------------\r\n");

    // Closure0
    Closure<char>* pRcb = NewClosure(&obj, &CTest::Rtest0);
    char r0 = pRcb->Run();
    EXPECT_EQ('a', r0);

    pRcb = NewClosure(&obj, &CTest::Rtest1, 'a');
    r0 = pRcb->Run();
    EXPECT_EQ('a', r0);

    pRcb = NewClosure(&obj, &CTest::Rtest2, char('a'), short(456));
    r0 = pRcb->Run();
    EXPECT_EQ('a', r0);

    pRcb = NewClosure(&obj, &CTest::Rtest3, char('a'), short(456), int(9800));
    r0 = pRcb->Run();
    EXPECT_EQ('a', r0);

    // Closure1
    Closure<short, char>* pRcb0_1 = NewClosure(&obj, &CTest::Rtest0_1);
    short r1 = pRcb0_1->Run('a');
    EXPECT_EQ(980, r1);

    Closure<short, short>* pRcb1_1 = NewClosure(&obj, &CTest::Rtest1_1, 'a');
    r1 = pRcb1_1->Run(780);
    EXPECT_EQ(780, r1);

    Closure<short, int>* pRcb2_1 = NewClosure(&obj, &CTest::Rtest2_1, char('a'), short(7800));
    r1 = pRcb2_1->Run(9800);
    EXPECT_EQ(7800, r1);

    // Closure2
    Closure<int, char, short>* pRcb0_2 =
        NewClosure(&obj, &CTest::Rtest0_2);
    int r2 = pRcb0_2->Run('a', 8900);
    EXPECT_EQ(9900, r2);

    Closure<int, char, int>* pRcb1_2 = NewClosure(&obj, &CTest::Rtest1_2, 'a');
    r2 = pRcb1_2->Run('b', 780);
    EXPECT_EQ(780, r2);

    // Closure3
    Closure<void, char, short, int>* pRcb0_3 = NewClosure(&obj, &CTest::test0_3);
    pRcb0_3->Run('a', 456, 78909);
}

TEST(Closure, ClassPermanentClosure)
{
    printf("\r\n==============test NewClosure(Class)===============\r\n");

    CTest obj;

    // Closure0
    Closure<void>* pcb = NewPermanentClosure(&obj, &CTest::test0);
    pcb->Run();
    delete pcb;

    pcb = NewPermanentClosure(&obj, &CTest::test1, 'a');
    pcb->Run();
    delete pcb;

    pcb = NewPermanentClosure(&obj, &CTest::test2, char('a'), short(456));
    pcb->Run();
    delete pcb;

    pcb = NewPermanentClosure(&obj, &CTest::test3, char('a'), short(456), int(9800));
    pcb->Run();
    delete pcb;

    // Closure1
    Closure<void, char>* pcb0_1 = NewPermanentClosure(&obj, &CTest::test0_1);
    pcb0_1->Run('a');
    delete pcb0_1;

    Closure<void, short>* pcb1_1 = NewPermanentClosure(&obj, &CTest::test1_1, 'a');
    pcb1_1->Run(short(780));
    delete pcb1_1;

    Closure<void, int>* pcb2_1 =
        NewPermanentClosure(&obj, &CTest::test2_1, char('a'), short(7800));
    pcb2_1->Run(9800);
    delete pcb2_1;

    // Closure2
    Closure<void, char, short>* pcb0_2 = NewPermanentClosure(&obj, &CTest::test0_2);
    pcb0_2->Run('a', 8900);
    delete pcb0_2;

    Closure<void, char, int>* pcb1_2 = NewPermanentClosure(&obj, &CTest::test1_2, 'a');
    pcb1_2->Run('b', 780);
    delete pcb1_2;

    // Closure3
    Closure<void, char, short, int>* pcb0_3 = NewPermanentClosure(&obj, &CTest::test0_3);
    pcb0_3->Run('a', 456, 78909);
    delete pcb0_3;

    //------Closure------------
    printf("\r\n------Closure------------\r\n");

    // Closure0
    Closure<char>* pRcb = NewPermanentClosure(&obj, &CTest::Rtest0);
    char r0 = pRcb->Run();
    EXPECT_EQ('a', r0);
    delete pRcb;

    pRcb = NewPermanentClosure(&obj, &CTest::Rtest1, 'a');
    r0 = pRcb->Run();
    EXPECT_EQ('a', r0);
    delete pRcb;

    pRcb = NewPermanentClosure(&obj, &CTest::Rtest2, char('a'), short(456));
    r0 = pRcb->Run();
    EXPECT_EQ('a', r0);
    delete pRcb;

    pRcb = NewPermanentClosure(&obj, &CTest::Rtest3, char('a'), short(456), int(9800));
    r0 = pRcb->Run();
    EXPECT_EQ('a', r0);
    delete pRcb;

    // Closure1
    Closure<short, char>* pRcb0_1 = NewPermanentClosure(&obj, &CTest::Rtest0_1);
    short r1 = pRcb0_1->Run('a');
    EXPECT_EQ(980, r1);
    delete pRcb0_1;

    Closure<short, short>* pRcb1_1 = NewPermanentClosure(&obj, &CTest::Rtest1_1, 'a');
    r1 = pRcb1_1->Run(780);
    EXPECT_EQ(780, r1);
    delete pRcb1_1;

    Closure<short, int>* pRcb2_1 =
        NewPermanentClosure(&obj, &CTest::Rtest2_1, char('a'), short(7800));
    r1 = pRcb2_1->Run(9800);
    EXPECT_EQ(7800, r1);
    delete pRcb2_1;

    // Closure2
    Closure<int, char, short>* pRcb0_2 =
        NewPermanentClosure(&obj, &CTest::Rtest0_2);
    int r2 = pRcb0_2->Run('a', 8900);
    EXPECT_EQ(9900, r2);
    delete pRcb0_2;

    Closure<int, char, int>* pRcb1_2 = NewPermanentClosure(&obj, &CTest::Rtest1_2, 'a');
    r2 = pRcb1_2->Run('b', 780);
    EXPECT_EQ(780, r2);
    delete pRcb1_2;

    // Closure3
    Closure<unsigned int, char, short, int>* pRcb0_3 = NewPermanentClosure(&obj, &CTest::Rtest0_3);
    unsigned int r3 = pRcb0_3->Run('a', 456, 78909);
    EXPECT_EQ(78909U, r3);
    delete pRcb0_3;
}

void NullFunction()
{
}

class Performance : public testing::Test {};

const int kLoopCount = 10000000;

TEST_F(Performance, CxxFunction)
{
    for (int i = 0; i < kLoopCount; ++i)
    {
        NullFunction();
    }
}

TEST_F(Performance, Closure)
{
    for (int i = 0; i < kLoopCount; ++i)
    {
        Closure<void>* closure = NewClosure(NullFunction);
        closure->Run();
    }
}

TEST_F(Performance, PermantClosure)
{
    scoped_ptr<Closure<void> > closure(NewPermanentClosure(&NullFunction));
    for (int i = 0; i < kLoopCount; ++i)
    {
        closure->Run();
    }
}

#if !defined(BAIDU_REDHAT_ENV)
// for compability in RedHat AS4
TEST_F(Performance, StdFunction)
{
    std::tr1::function<void ()> function = NullFunction;
    for (int i = 0; i < kLoopCount; ++i)
    {
        function();
    }
}

TEST_F(Performance, OnceStdFunction)
{
    for (int i = 0; i < kLoopCount; ++i)
    {
        std::tr1::function<void ()> function = NullFunction;
        function();
    }
}
#endif

class ClosureInheritTest : public testing::Test
{
public:
    int Foo() { return 42; }
};

TEST_F(ClosureInheritTest, Test)
{
    Closure<int>* closure = NewClosure(this, &ClosureInheritTest::Foo);
    EXPECT_EQ(42, closure->Run());
}

#if 0
void RefTest(int& a, const int& b)
{
    printf("a = %d, b = %d\n", a, b);
    a = b;
}

TEST(Closure, Reference)
{
    Closure<void, int&, const int&>* c1 = NewClosure(&RefTest);
    int a, b = 2;
    c1->Run(a, b);
    printf("a = %d, b = %d\n", a, b);
}
#endif

