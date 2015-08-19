// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "user_manager.h"

#include <glog/logging.h>

namespace tera {
namespace master {

User::User(const std::string& name) : m_name(name) {}

std::ostream& operator << (std::ostream& o, const User& user) {
    MutexLock locker(&user.m_mutex);
    o << "user:" << user.m_name
      << ", token:" << user.m_user_info.token()
      << ", group(" << user.m_user_info.group_name_size() << "):";
    for (int i = 0; i < user.m_user_info.group_name_size(); ++i) {
        o << user.m_user_info.group_name(i) << " ";
    }
    return o;
}

std::string User::GetUserName() {
    MutexLock locker(&m_mutex);
    return m_name;
}

UserInfo& User::GetUserInfo() {
    MutexLock locker(&m_mutex);
    return m_user_info;
}

const std::string User::GetToken() {
    MutexLock locker(&m_mutex);
    return m_user_info.token();
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

bool UserManager::IsUserInGroup(const std::string& user_name, const std::string& group_name) {
    MutexLock locker(&m_mutex);
    UserList::iterator it = m_all_users.find(user_name);
    if (it == m_all_users.end()) {
        return false;        
    }
    UserInfo user_info = it->second->GetUserInfo();
    for (int i = 0; i < user_info.group_name_size(); ++i) {
        if (user_info.group_name(i) == group_name) {
            LOG(INFO) << "found group:" << group_name << " for:" << user_name;
            return true;
        }
    }
    LOG(INFO) << "not found group:" << group_name << " for:" << user_name;
    return false;
}

bool UserManager::DeleteUser(const std::string& user_name) {
    MutexLock locker(&m_mutex);
    UserList::iterator it = m_all_users.find(user_name);
    if (it == m_all_users.end()) {
        LOG(ERROR) << "delete user: " << user_name << " failed: user not found";
    } else {
        m_all_users.erase(user_name);
        LOG(INFO) << "delete user: " << user_name << " success";
    }
    return it == m_all_users.end();
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
}

void UserManager::SetupRootUser() {
    // there is no races, so there is no lock
    UserList::iterator it = m_all_users.find("root");
    if (it == m_all_users.end()) {
        UserInfo user_info;
        user_info.set_user_name("root");
        user_info.set_token("af6a89c2");
        AddUser("root", user_info);
        LOG(INFO) << "root not found in meta table, add a root with default password";
    }
    LOG(INFO) << "root restored";
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

bool UserManager::IsValidForCreate(const std::string& token, 
                                   const std::string& user_name) {
    LOG(INFO) << user_name << ", " << token;
    return IsUserNameValid(user_name) 
           && !IsUserExist(user_name)
           && TokenToUserName(token) == "root";
}

bool UserManager::IsValidForDelete(const std::string& token, 
                                   const std::string& user_name) {
    LOG(INFO) << user_name << ", " << token;
    return IsUserExist(user_name) 
           && user_name != "root"
           && TokenToUserName(token) == "root";
}

bool UserManager::IsValidForChangepwd(const std::string& token, 
                                      const std::string& user_name) {
    LOG(INFO) << user_name << ", " << token << ", who call:" << TokenToUserName(token);
    return IsUserExist(user_name)
           && (TokenToUserName(token) == "root" || TokenToUserName(token) == user_name);
}

bool UserManager::IsValidForAddToGroup(const std::string& token, 
                                       const std::string& user_name, 
                                       const std::string& group_name) {
    return IsUserExist(user_name) 
           && !IsUserInGroup(user_name, group_name)
           && TokenToUserName(token) == "root";
}

bool UserManager::IsValidForDeleteFromGroup(const std::string& token, 
                                            const std::string& user_name, 
                                            const std::string& group_name) {
    return IsUserExist(user_name) 
           && IsUserInGroup(user_name, group_name)
           && (TokenToUserName(token) == "root" || TokenToUserName(token) == user_name);
}

std::string UserManager::UserNameToToken(const std::string& user_name) {
    MutexLock locker(&m_mutex);
    UserList::iterator it = m_all_users.find(user_name);
    if (it == m_all_users.end()) {
        return "UnknownUser";        
    }
    return it->second->GetToken();
}

std::string UserManager::TokenToUserName(const std::string& token) {
    MutexLock locker(&m_mutex);
    for (UserList::const_iterator it = m_all_users.begin();
         it != m_all_users.end(); ++it) {
        if (token == it->second->GetToken()) {
            return it->second->GetUserName();
        }
    }
    return "UnknownUser";
}

bool UserManager::GetUserInfo(const std::string& user_name, UserInfo* user_info) {
    MutexLock locker(&m_mutex);
    UserList::iterator it = m_all_users.find(user_name);
    if (it == m_all_users.end()) {
        LOG(ERROR) << "user:" << user_name << " not found";        
        return false;
    }
    user_info->CopyFrom(it->second->GetUserInfo());
    return true;
}

bool UserManager::DeleteGroupFromUserInfo(UserInfo& user_info,
                                          const std::string& group) {
    MutexLock locker(&m_mutex);
    std::string user_name = user_info.user_name();
    UserList::iterator it = m_all_users.find(user_name);
    if (it == m_all_users.end()) {
        LOG(ERROR) << "user:" << user_name << " not found";        
        return false;
    }
    user_info.clear_group_name();
    UserInfo orig_user = it->second->GetUserInfo();
    for (int i = 0; i < orig_user.group_name_size(); ++i) {
        if (orig_user.group_name(i) == group) {
            LOG(INFO) << "found delete group:" << group;
            continue;
        }
        user_info.add_group_name(orig_user.group_name(i));
    }
    return true;
}

bool UserManager::SetUserInfo(const std::string& user_name, UserInfo& user_info) {
    MutexLock locker(&m_mutex);
    UserList::iterator it = m_all_users.find(user_name);
    if (it == m_all_users.end()) {
        LOG(ERROR) << "user:" << user_name << " not found";        
        return false;
    }
    it->second->GetUserInfo().CopyFrom(user_info);
    return true; 
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