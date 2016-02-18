// Copyright (c) 2016, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.


#include "utils/fragment.h"

#include <gtest/gtest.h>

namespace tera {

TEST(FragmentTest, Head) {
    RangeFragment all;

//  a b c d e f g h i j k l m n o p q r s t u v w x y z
//  (null)
//  ----------->g
    all.AddToRange("", "g");
    ASSERT_EQ(all.DebugString(), ":g ");

//  a b c d e f g h i j k l m n o p q r s t u v w x y z
//  ----------->g
//                      k<--------p
    all.AddToRange("k", "p");
    ASSERT_EQ(all.DebugString(), ":g k:p ");

//  a b c d e f g h i j k l m n o p q r s t u v w x y z
//  ----------->g       k<--------p
//                                        t-------x
    all.AddToRange("t", "x");
    ASSERT_EQ(all.DebugString(), ":g k:p t:x ");

//  a b c d e f g h i j k l m n o p q r s t u v w x y z
//  ----------->g       k<--------p       t-------x
//                                  q---s
    all.AddToRange("q", "s");
    ASSERT_EQ(all.DebugString(), ":g k:p q:s t:x ");

//  a b c d e f g h i j k l m n o p q r s t u v w x y z
//  ----------->g       k<--------p q---s t-------x
//                                                  y--
    all.AddToRange("y", "");
    ASSERT_EQ(all.DebugString(), ":g k:p q:s t:x y: ");

    all.AddToRange("p", "q");
    ASSERT_EQ(all.DebugString(), ":g k:s t:x y: ");

    all.AddToRange("s", "t");
    ASSERT_EQ(all.DebugString(), ":g k:x y: ");

    all.AddToRange("g", "k");
    ASSERT_EQ(all.DebugString(), ":x y: ");

    all.AddToRange("x", "y");
    ASSERT_EQ(all.DebugString(), ": ");

    ASSERT_TRUE(all.IsCompleteRange());
}

TEST(FragmentTest, Tail) {
    RangeFragment all;
//  a b c d e f g h i j k l m n o p q r s t u v w x y z
//  (null)
//                                        t------------
    all.AddToRange("t", "");
    ASSERT_EQ(all.DebugString(), "t: ");

//  a b c d e f g h i j k l m n o p q r s t u v w x y z
//                                        t------------
//  --------e
    all.AddToRange("", "e");
    ASSERT_EQ(all.DebugString(), ":e t: ");

//  a b c d e f g h i j k l m n o p q r s t u v w x y z
//  --------e                             t------------
//                h-----------n
    all.AddToRange("h", "n");
    ASSERT_EQ(all.DebugString(), ":e h:n t: ");
}

TEST(FragmentTest, OverlapFormer) {
    RangeFragment all;

//  a b c d e f g h i j k l m n o p q r s t u v w x y z
//  (null)
//  ------d
    all.AddToRange("", "d");
    ASSERT_EQ(all.DebugString(), ":d ");

//  a b c d e f g h i j k l m n o p q r s t u v w x y z
//  ------d
//                                p-------t
    all.AddToRange("p", "t");
    ASSERT_EQ(all.DebugString(), ":d p:t ");

//  a b c d e f g h i j k l m n o p q r s t u v w x y z
//  ------d                       p-------t
//                            n-o
    all.AddToRange("n", "o");
    ASSERT_EQ(all.DebugString(), ":d n:o p:t ");

//  a b c d e f g h i j k l m n o p q r s t u v w x y z
//  ------d                   n-o p-------t
//                              o-p
    all.AddToRange("o", "p");
    ASSERT_EQ(all.DebugString(), ":d n:t ");

//  a b c d e f g h i j k l m n o p q r s t u v w x y z
//  ------d                   n-----------t
//                          m---o
    all.AddToRange("m", "o");
    ASSERT_EQ(all.DebugString(), ":d m:t ");

//  a b c d e f g h i j k l m n o p q r s t u v w x y z
//  ------d                 m-------------t
//                        l-------------s
    all.AddToRange("l", "s");
    ASSERT_EQ(all.DebugString(), ":d l:t ");

//  a b c d e f g h i j k l m n o p q r s t u v w x y z
//  ------d               l---------------t
//                      k-----------------t
    all.AddToRange("k", "t");
    ASSERT_EQ(all.DebugString(), ":d k:t ");

//  a b c d e f g h i j k l m n o p q r s t u v w x y z
//  ------d             k-----------------t
//                    j---------------------u
    all.AddToRange("j", "u");
    ASSERT_EQ(all.DebugString(), ":d j:u ");
}

TEST(FragmentTest, OverlapLater) {
    RangeFragment all;
//  a b c d e f g h i j k l m n o p q r s t u v w x y z
//  (null)
//                          m-------q
    all.AddToRange("m", "q");
    ASSERT_EQ(all.DebugString(), "m:q ");

//  a b c d e f g h i j k l m n o p q r s t u v w x y z
//                          m-------q
//                                                  y--
    all.AddToRange("y", "");
    ASSERT_EQ(all.DebugString(), "m:q y: ");

//  a b c d e f g h i j k l m n o p q r s t u v w x y z
//                          m-------q               y--
//                          m-n
    all.AddToRange("m", "n");
    ASSERT_EQ(all.DebugString(), "m:q y: ");

//  a b c d e f g h i j k l m n o p q r s t u v w x y z
//                          m-------q               y--
//                          m-------q
    all.AddToRange("m", "q");
    ASSERT_EQ(all.DebugString(), "m:q y: ");

//  a b c d e f g h i j k l m n o p q r s t u v w x y z
//                          m-------q               y--
//                          m---------r
    all.AddToRange("m", "r");
    ASSERT_EQ(all.DebugString(), "m:r y: ");

//  a b c d e f g h i j k l m n o p q r s t u v w x y z
//                          m---------r             y--
//                              o-p
    all.AddToRange("o", "p");
    ASSERT_EQ(all.DebugString(), "m:r y: ");

//  a b c d e f g h i j k l m n o p q r s t u v w x y z
//                          m---------r             y--
//                              o-----r
    all.AddToRange("o", "r");
    ASSERT_EQ(all.DebugString(), "m:r y: ");

//  a b c d e f g h i j k l m n o p q r s t u v w x y z
//                          m---------r             y--
//                              o-------s
    all.AddToRange("o", "s");
    ASSERT_EQ(all.DebugString(), "m:s y: ");

//  a b c d e f g h i j k l m n o p q r s t u v w x y z
//                          m-----------s           y--
//                                      s-t
    all.AddToRange("s", "t");
    ASSERT_EQ(all.DebugString(), "m:t y: ");

//  a b c d e f g h i j k l m n o p q r s t u v w x y z
//                          m-------------t         y--
//                                          u-v
    all.AddToRange("u", "v");
    ASSERT_EQ(all.DebugString(), "m:t u:v y: ");

//  a b c d e f g h i j k l m n o p q r s t u v w x y z
//                          m-------------t u-v     y--
//                                        t------------
    all.AddToRange("t", "");
    ASSERT_EQ(all.DebugString(), "m: ");
}

TEST(FragmentTest, CommonMutilFragment) {
    RangeFragment all;
//  a b c d e f g h i j k l m n o p q r s t u v w x y z
//  (null)
//                    j-------------q
    all.AddToRange("j", "q");
    ASSERT_EQ(all.DebugString(), "j:q ");

//  a b c d e f g h i j k l m n o p q r s t u v w x y z
//                    j-------------q
//  ------------------j
    all.AddToRange("", "j");
    ASSERT_EQ(all.DebugString(), ":q ");

//  a b c d e f g h i j k l m n o p q r s t u v w x y z
//  --------------------------------q
//                                  q------------------
    all.AddToRange("q", "");
    ASSERT_EQ(all.DebugString(), ": ");

    ASSERT_TRUE(all.IsCompleteRange());
}

TEST(FragmentTest, CommonOneFragment) {
    RangeFragment all;
    all.AddToRange("", "");
    ASSERT_EQ(all.DebugString(), ": ");

    ASSERT_TRUE(all.IsCompleteRange());

    all.AddToRange("", "");
    ASSERT_EQ(all.DebugString(), ": ");

    all.AddToRange("a", "b");
    ASSERT_EQ(all.DebugString(), ": ");

    all.AddToRange("a", "");
    ASSERT_EQ(all.DebugString(), ": ");

    all.AddToRange("", "b");
    ASSERT_EQ(all.DebugString(), ": ");

    ASSERT_TRUE(all.IsCompleteRange());
}

TEST(FragmentTest, Endkey) {
    RangeFragment all;

//  a b c d e f g h i j k l m n o p q r s t u v w x y z
//  (null)
//                          m--------------------------
    all.AddToRange("m", "");
    ASSERT_EQ(all.DebugString(), "m: ");

//  a b c d e f g h i j k l m n o p q r s t u v w x y z
//                          m--------------------------
//                                        t------------
    all.AddToRange("t", "");
    ASSERT_EQ(all.DebugString(), "m: ");

//  a b c d e f g h i j k l m n o p q r s t u v w x y z
//                          m--------------------------
//                                        t-----------z
    all.AddToRange("t", "z");
    ASSERT_EQ(all.DebugString(), "m: ");

//  a b c d e f g h i j k l m n o p q r s t u v w x y z
//                          m--------------------------
//                          m--------------------------
    all.AddToRange("m", "");
    ASSERT_EQ(all.DebugString(), "m: ");

//  a b c d e f g h i j k l m n o p q r s t u v w x y z
//                          m--------------------------
//                          m-------------------------z
    all.AddToRange("m", "z");
    ASSERT_EQ(all.DebugString(), "m: ");

//  a b c d e f g h i j k l m n o p q r s t u v w x y z
//                          m--------------------------
//                        l----------------------------
    all.AddToRange("l", "");
    ASSERT_EQ(all.DebugString(), "l: ");

//  a b c d e f g h i j k l m n o p q r s t u v w x y z
//                        l----------------------------
//                        l-----------r
    all.AddToRange("l", "r");
    ASSERT_EQ(all.DebugString(), "l: ");

//  a b c d e f g h i j k l m n o p q r s t u v w x y z
//                        l----------------------------
//                      k---m
    all.AddToRange("k", "m");
    ASSERT_EQ(all.DebugString(), "k: ");

//  a b c d e f g h i j k l m n o p q r s t u v w x y z
//                      k------------------------------
//  ---------------------------------------------------
    all.AddToRange("", "");
    ASSERT_EQ(all.DebugString(), ": ");

    ASSERT_TRUE(all.IsCompleteRange());
}

TEST(CoverTest, CompleteRange) {
    RangeFragment all;
    all.AddToRange("", "");
    ASSERT_TRUE(all.IsCoverRange("", ""));
    ASSERT_TRUE(all.IsCoverRange("", "a"));
    ASSERT_TRUE(all.IsCoverRange("a", ""));
    ASSERT_TRUE(all.IsCoverRange("a", "b"));
}

TEST(CoverTest, Start) {
//  a b c d e f g h i j k l m n o p q r s t u v w x y z
//                *
    RangeFragment all;
    all.AddToRange("", "h");

    ASSERT_TRUE(all.IsCoverRange("", "g"));
    ASSERT_TRUE(all.IsCoverRange("", "h"));
    ASSERT_FALSE(all.IsCoverRange("", "i"));
    ASSERT_FALSE(all.IsCoverRange("", ""));

    ASSERT_TRUE(all.IsCoverRange("a", "g"));
    ASSERT_TRUE(all.IsCoverRange("a", "h"));
    ASSERT_FALSE(all.IsCoverRange("a", "i"));
    ASSERT_FALSE(all.IsCoverRange("a", ""));

    ASSERT_FALSE(all.IsCoverRange("h", "i"));
    ASSERT_FALSE(all.IsCoverRange("h", ""));
}

TEST(CoverTest, End) {
//  a b c d e f g h i j k l m n o p q r s t u v w x y z
//                *
    RangeFragment all;
    all.AddToRange("h", "");
    ASSERT_FALSE(all.IsCoverRange("", "g"));
    ASSERT_FALSE(all.IsCoverRange("", "h"));
    ASSERT_FALSE(all.IsCoverRange("", "i"));
    ASSERT_FALSE(all.IsCoverRange("", ""));

    ASSERT_FALSE(all.IsCoverRange("a", "g"));
    ASSERT_FALSE(all.IsCoverRange("a", "h"));
    ASSERT_FALSE(all.IsCoverRange("a", "i"));
    ASSERT_FALSE(all.IsCoverRange("a", ""));

    ASSERT_TRUE(all.IsCoverRange("h", "i"));
    ASSERT_TRUE(all.IsCoverRange("h", ""));
}

TEST(CoverTest, Common) {
//  a b c d e f g h i j k l m n o p q r s t u v w x y z
//                *             *
    RangeFragment all;
    all.AddToRange("h", "o");

    ASSERT_FALSE(all.IsCoverRange("a", "g"));
    ASSERT_FALSE(all.IsCoverRange("a", "h"));
    ASSERT_FALSE(all.IsCoverRange("a", "i"));
    ASSERT_FALSE(all.IsCoverRange("a", "n"));
    ASSERT_FALSE(all.IsCoverRange("a", "o"));
    ASSERT_FALSE(all.IsCoverRange("a", "p"));
    ASSERT_FALSE(all.IsCoverRange("a", ""));

    ASSERT_TRUE(all.IsCoverRange("h", "i"));
    ASSERT_TRUE(all.IsCoverRange("h", "o"));
    ASSERT_FALSE(all.IsCoverRange("h", "p"));
    ASSERT_FALSE(all.IsCoverRange("h", ""));

    ASSERT_TRUE(all.IsCoverRange("i", "n"));
    ASSERT_TRUE(all.IsCoverRange("i", "o"));
    ASSERT_FALSE(all.IsCoverRange("i", "p"));
    ASSERT_FALSE(all.IsCoverRange("i", ""));

    ASSERT_FALSE(all.IsCoverRange("o", "p"));
    ASSERT_FALSE(all.IsCoverRange("o", ""));

    ASSERT_FALSE(all.IsCoverRange("p", "q"));
    ASSERT_FALSE(all.IsCoverRange("p", ""));
}

} // namespace tera


int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
