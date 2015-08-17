// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "user_manager.h"

#include <glog/logging.h>

namespace tera {
namespace master {

User::User(const std::string& name) : m_name(name) {}

const std::string& User::GetToken() {
    MutexLock locker(&m_mutex);
    return m_user_info.token();
}

std::ostream& operator << (std::ostream& o, const User& user) {
    MutexLock locker(&user.m_mutex);
    o << "user:" << user.m_name
      << ", token:" << user.m_user_info.token();
    return o;
}

bool UserManager::AddUser(const std::string& user_name, UserInfo& user_info) {
    MutexLock locker(&m_mutex);
    boost::shared_ptr<User> user(new User(user_name));
    user->m_user_info.CopyFrom(user_info);

    std::pair<UserList::iterator, bool> ret =
        m_all_users.insert(std::pair<std::string, UserPtr>(user_name, user));
    if (ret.second) {
        LOG(INFO) << "add user: " << user_name << " success";
    } else {
        LOG(INFO) << "add user: " << user_name << " failed: user exists";
    }
    return ret.second;
}

bool UserManager::DeleteUser(const std::string& user_name) {
    MutexLock locker(&m_mutex);
    UserList::iterator it = m_all_users.find(user_name);
    if (it == m_all_users.end()) {
        LOG(ERROR) << "delete user: " << user_name << " failed: user not found";
        return false;
    } else {
        m_all_users.erase(user_name);
        LOG(INFO) << "delete user: " << user_name << " success";
        return true;
    }
}

bool UserManager::IsUserExist(const std::string& user_name) {
    MutexLock locker(&m_mutex);
    UserList::iterator it = m_all_users.find(user_name);
    return it != m_all_users.end();
}

void UserManager::LoadUserMeta(const std::string& key,
                               const std::string& value) {
    if (key.length() <= 1 || key[0] != '~') {
        LOG(ERROR) << "[acl] invalid argument";
        return;
    }
    std::string user_name = key.substr(1);

    UserInfo user_info;
    user_info.ParseFromString(value);

    AddUser(user_name, user_info);

    LOG(INFO) << "user_name:" << user_name << ", token:" << user_info.token();    
}

void UserManager::SetupRootUser() {
    // there is no races, so there is no lock
    UserList::iterator it = m_all_users.find("root");
    if (it == m_all_users.end()) {
        UserInfo user_info;
        user_info.set_user_name("root");
        user_info.set_token("af6a89c2");
        AddUser("root", user_info);
        LOG(INFO) << "root not found in meta table, add a root with default password.";
    }
}

bool UserManager::IsUserNameValid(const std::string& user_name) {
    const size_t kLenMin = 2;
    const size_t kLenMax = 32;
    if (user_name.length() < kLenMin || user_name.length() > kLenMax
        || !isalpha(user_name[0])) {
        return false;
    }
    for (size_t i = 0; i < user_name.length(); ++i) {
        if (!isalnum(user_name[i])) { 
            return false;
        }
    }
    return true;
}

bool UserManager::IsUserAndTokenMatch(const std::string& user_name, const std::string& token) {
    return token == GetUserToken(user_name);
}

std::string UserManager::GetUserToken(const std::string& user_name) {
    MutexLock locker(&m_mutex);
    UserList::iterator it = m_all_users.find(user_name);
    if (it == m_all_users.end()) {
        return "";        
    }
    return it->second->GetToken();
}

void UserManager::ListAll() {
    MutexLock locker(&m_mutex);
    for (UserList::const_iterator it = m_all_users.begin(); 
         it != m_all_users.end(); ++it) {
        LOG(INFO) << *it->second;        
    }
}

} // namespace master
} // namespace tera
