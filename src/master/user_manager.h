// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef TERA_MASTER_USER_MANAGER_H_
#define TERA_MASTER_USER_MANAGER_H_

#include <boost/shared_ptr.hpp>

#include "proto/master_rpc.pb.h"

#include "common/mutex.h"
#include "common/base/scoped_ptr.h"

namespace tera {
namespace master {

class User {
    friend std::ostream& operator << (std::ostream& o, const User& user);
    friend class UserManager;

public:
    User(const std::string& name);
    const std::string& GetToken();

private:
    User(const User&) {}
    User& operator=(const User&) {return *this;}

    mutable Mutex m_mutex;
    std::string m_name;
    UserInfo m_user_info;
};
typedef boost::shared_ptr<User> UserPtr;

class UserManager {
public:
    bool AddUser(const std::string& user_name, UserInfo& user_info);
    bool DeleteUser(const std::string& user_name);
    bool IsUserExist(const std::string& user_name);

    // load a user meta entry(memtable) into user_manager(memory)
    void LoadUserMeta(const std::string& key,
                      const std::string& value); 

    // valid user name:
    // 1. kLenMin <= user_name.length() <= kLenMax 
    // 2. first char of user_name is alphabet
    // 3. contains only alphabet or digit
    bool IsUserNameValid(const std::string& user_name);

    // setups root user if root not found in metatable after master inits
    // e.g. the tera cluster first starts.
    void SetupRootUser();

    std::string GetUserToken(const std::string& user_name);
    bool IsUserAndTokenMatch(const std::string& user_name, const std::string& token);

    // for debug
    void ListAll();
private:
    mutable Mutex m_mutex;
    typedef std::map<std::string, UserPtr> UserList;
    UserList m_all_users;
};

} // namespace master
} // namespace tera
#endif // TERA_MASTER_USER_MANAGER_H_
