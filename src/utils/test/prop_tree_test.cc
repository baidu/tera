// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#define private public

#include "utils/prop_tree.h"

#include <gtest/gtest.h>

namespace tera {

//class TokenizerTest : public ::testing::Test, public Tokenizer {
//public:
//   TokenizerTest() : TPrinter(3) {}
//    ~TokenizerTest() {}
//};

TEST(TokenizerTest, ConsumeUselessChars) {
    std::string input;
    input = "hello";
    Tokenizer t(input);
    t.ConsumeUselessChars();
    ASSERT_EQ(t.cur_pos_, 0u);

    input = "    hello";
    t.Reset(input);
    t.ConsumeUselessChars();
    ASSERT_EQ(t.origin_[t.cur_pos_], 'h');

    input = "\thello";
    t.Reset(input);
    t.ConsumeUselessChars();
    ASSERT_EQ(t.origin_[t.cur_pos_], 'h');

    input = "  # this is a comment;\n hello";
    t.Reset(input);
    t.ConsumeUselessChars();
    ASSERT_EQ(t.origin_[t.cur_pos_], 'h');
}

TEST(TokenizerTest, Next) {
    std::string input;
    input = "hello";
    Tokenizer t(input);
    ASSERT_TRUE(t.Next());
    ASSERT_EQ(t.current().text, "hello");
    ASSERT_FALSE(t.Next());

    input = "hello  world";
    t.Reset(input);
    ASSERT_TRUE(t.Next());
    ASSERT_EQ(t.current().text, "hello");
    ASSERT_TRUE(t.Next());
    ASSERT_EQ(t.current().text, "world");
    ASSERT_FALSE(t.Next());

    input = "int main (int64_t ar.gc, char* arg-v[])";
    t.Reset(input);
    ASSERT_TRUE(t.Next());
    ASSERT_EQ(t.current().type, Tokenizer::IDENTIFIER);
    ASSERT_EQ(t.current().text, "int");
    ASSERT_TRUE(t.Next());
    ASSERT_EQ(t.current().text, "main");
    ASSERT_TRUE(t.Next());
    ASSERT_EQ(t.current().type, Tokenizer::SYMBOL);
    ASSERT_EQ(t.current().text, "(");
    ASSERT_TRUE(t.Next());
    ASSERT_EQ(t.current().text, "int64_t");
    ASSERT_TRUE(t.Next());
    ASSERT_EQ(t.current().text, "ar.gc");
    ASSERT_TRUE(t.Next());
    ASSERT_EQ(t.current().text, ",");
    ASSERT_TRUE(t.Next());
    ASSERT_EQ(t.current().text, "char");
    ASSERT_TRUE(t.Next());
    ASSERT_EQ(t.current().text, "*");
    ASSERT_TRUE(t.Next());
    ASSERT_EQ(t.current().text, "arg-v");
    ASSERT_TRUE(t.Next());
    ASSERT_EQ(t.current().text, "[");
    ASSERT_TRUE(t.Next());
    ASSERT_EQ(t.current().text, "]");
    ASSERT_TRUE(t.Next());
    ASSERT_EQ(t.current().text, ")");
    ASSERT_FALSE(t.Next());
}

TEST(PropTreeTest, ParseFromString) {
    std::string input;
    PropTree pt;
    PropTree::Node* proot;

    input = "root<props{child1}";
    EXPECT_FALSE(pt.ParseFromString(input));
    // LOG(ERROR) << pt.State();

    input = "root<props> child1}";
    EXPECT_FALSE(pt.ParseFromString(input));
    // LOG(ERROR) << pt.State();

    input = "root>props<child1}";
    EXPECT_FALSE(pt.ParseFromString(input));
    // LOG(ERROR) << pt.State();

    input = "root";
    EXPECT_TRUE(pt.ParseFromString(input));

    input = "root<prop1=value1>";
    EXPECT_TRUE(pt.ParseFromString(input));

    input = "root<prop1=value1, prop2=value2>";
    EXPECT_TRUE(pt.ParseFromString(input));

    input = "root{}";
    EXPECT_FALSE(pt.ParseFromString(input));

    input = "root{child1}";
    EXPECT_TRUE(pt.ParseFromString(input));

    input = "root{child1, child2, child3,}";
    EXPECT_TRUE(pt.ParseFromString(input));
    proot = pt.GetRootNode();
    EXPECT_EQ(proot->children_.size(), 3u);

    input = "root{child1<prop1=value1>}";
    EXPECT_TRUE(pt.ParseFromString(input));

    input = "root{child1{child11, child12}, child2{child21},}";
    EXPECT_TRUE(pt.ParseFromString(input));
    EXPECT_EQ(pt.GetRootNode()->children_.size(), 2u);
    EXPECT_EQ(pt.MaxDepth(), 3);
    EXPECT_EQ(pt.MinDepth(), 3);

    input = "root:hahh{child1{child11, child12}, child2{child21},}";
    EXPECT_FALSE(pt.ParseFromString(input));
    // LOG(ERROR) << pt.FormatString();

    input = "root{child1<prop1 = value1, prop2 = value2>, child2,}";
    EXPECT_TRUE(pt.ParseFromString(input));

    input = "root<prop1=value1, prop2=value2> { \
                child1<prop3=value3> { \
                    child11<prop4=value4>, \
                    child12<prop5=value5>, \
                }, \
                child2, \
             }";
    EXPECT_TRUE(pt.ParseFromString(input));
    EXPECT_EQ(pt.GetRootNode()->children_.size(), 2u);
    EXPECT_EQ(pt.MaxDepth(), 3);
    EXPECT_EQ(pt.MinDepth(), 2);
    // LOG(ERROR) << pt.FormatString();
}
} // namespace tera
