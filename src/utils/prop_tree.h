// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef TERA_UTILS_PROP_TREE_H
#define TERA_UTILS_PROP_TREE_H

#include <deque>
#include <map>
#include <string>
#include <vector>

namespace tera {

/*
 * An agile tree structure.
 * We can specify user-defined properties to every node.
 *
 * Syntax:
 *  root<prop1=value1, prop2=value2> {
 *      child1<prop3=value3> {
 *          child11<prop4=value4>,
 *          child12<prop5=value5>
 *      },
 *      child2
 *  }
 */

class Tokenizer {
public:
    Tokenizer(const std::string& input);
    ~Tokenizer();

    enum TokenType {
        INIT,
        IDENTIFIER,
        SYMBOL
    };

    struct Token {
        TokenType type;
        std::string text;

        size_t size() { return text.size(); }
        void clear() { text.clear(); type = INIT; }
        void push_back(char c) { text.push_back(c); }
    };

    bool Next();

    const Token& current() { return current_; }

    void Reset(const std::string& input) {
        origin_ = input;
        cur_pos_ = 0;
        current_.clear();
    }

private:
    void ConsumeUselessChars();
    void ConsumeIdentifier();
    void ConsumeSymbol();

private:
    Token current_;
    std::string origin_;
    std::string::size_type cur_pos_;
};

class PropTree {
public:
    PropTree();
    ~PropTree();

    struct Node {
        std::string name_;
        std::map<std::string, std::string> properties_;
        std::vector<Node*> children_;
        Node* mother_;

        Node() : mother_(NULL) {}
        ~Node() {
            for (size_t i = 0; i < children_.size(); ++i) {
                delete children_[i];
            }
        }
    };

    void Reset();

    bool ParseFromString(const std::string& input);

    bool ParseFromFile(const std::string& file);

    Node* GetRootNode() { return root_; }

    std::string FormatString();

    int MaxDepth() { return max_depth_; }

    int MinDepth() { return min_depth_; }

    const std::string& State() { return state_; }

private:
    bool ParseNodeFromTokens(std::deque<Tokenizer::Token>& tokens,
                             int depth, Node** node);

    bool ParsePropsFromTokens(std::deque<Tokenizer::Token>& tokens, Node* node);

    bool ParseChildrenFromTokens(std::deque<Tokenizer::Token>& tokens,
                                 int depth, Node* node);

    void FormatNode(const std::string& line_prefix, Node* node,
                    std::stringstream* ss);

    void AddError(const std::string& error_str);

private:
    Node* root_;
    int max_depth_;
    int min_depth_;
    std::string state_;
};

}  // namespace tera
#endif // TERA_UTILS_PROP_TREE_H
