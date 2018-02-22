// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef TERA_LOAD_BALANCER_ACTION_H_
#define TERA_LOAD_BALANCER_ACTION_H_

#include <memory>
#include <string>

namespace tera {
namespace load_balancer {

class Action {
public:
    enum class Type {
        ASSIGN,
        MOVE,
        SWAP,
        EMPTY,
    };

    Type GetType() const {
        return type_;
    }

public:
    Action(Type t) {
        type_ = t;
    }

    virtual ~Action() {}

    virtual Action* UndoAction() = 0;

    virtual std::string ToString() const = 0;

private:
    Type type_;
};

} // namespace load_balancer
} // namespace tera

#endif // TERA_LOAD_BALANCER_ACTION_H_
