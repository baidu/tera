// Copyright (c) 2015-2018, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "access/helpers/access_utils.h"
#include "access/access_entry.h"
#include "access/verification/ugi_verification.h"
#include "master/master_env.h"
#include <algorithm>
#include <memory>

namespace tera {
namespace auth {

using policy_type = std::underlying_type<AuthPolicyType>::type;

static const std::vector<std::string> msg = {
    "none", "ugi", "giano",
};

bool AccessUtils::GetAuthPolicy(const AuthPolicyType& auth_policy_type,
                                std::string* const auth_policy) {
  uint32_t msg_size = msg.size();
  uint32_t index = static_cast<policy_type>(auth_policy_type) -
                   static_cast<policy_type>(AuthPolicyType::kNoneAuthPolicy);
  if (index >= msg_size) {
    return false;
  }
  *auth_policy = std::string(msg[index]);
  return true;
}

bool AccessUtils::GetAuthPolicyType(const std::string& auth_policy,
                                    AuthPolicyType* const auth_policy_type) {
  uint32_t index = 0;
  std::find_if(msg.cbegin(), msg.cend(), [&index, &auth_policy](const std::string& s) {
    if (s != auth_policy) {
      ++index;
      return false;
    } else {
      return true;
    }
  });
  uint32_t msg_size = msg.size();
  if (index >= msg_size) {
    return false;
  }
  *auth_policy_type = static_cast<AuthPolicyType>(
      index + static_cast<policy_type>(AuthPolicyType::kNoneAuthPolicy));
  return true;
}

static void GetVerificationInfo(const std::shared_ptr<AccessEntry>& access_entry,
                                const std::string& user_name,
                                VerificationInfoPtr* verification_info_ptr) {
  VerificationPtr verification_ptr = access_entry->GetAccessUpdater().GetVerification();
  verification_ptr->Get(user_name, verification_info_ptr);
}

static void MergeVerificationInfoUgi(const UgiInfo& ugi_info,
                                     VerificationInfoPtr* verification_info_ptr) {
  (*verification_info_ptr)->first = ugi_info.passwd();
}

static void MergeVerificationInfoAuthority(const AuthorityInfo& authority_info,
                                           UpdateAuthType update_auth_type,
                                           VerificationInfoPtr* verification_info_ptr) {
  std::set<std::string>& roles = (*verification_info_ptr)->second;
  if (update_auth_type == kRevokeRole) {
    roles.erase(authority_info.role());
  } else {
    roles.emplace(authority_info.role());
  }
}

static bool GetMetaRecordKeyValue(const std::shared_ptr<AccessEntry>& access_entry,
                                  const UpdateAuthInfo& update_auth_info,
                                  master::MetaWriteRecord* meta_write_record) {
  VerificationInfoPtr verification_info_ptr(new VerificationInfo);
  std::string user_name;
  if (update_auth_info.update_type() == kUpdateUgi || update_auth_info.update_type() == kDelUgi) {
    const UgiInfo& ugi_info = update_auth_info.ugi_info();
    user_name = ugi_info.user_name();
    meta_write_record->key = std::string("|00") + user_name;
    if (update_auth_info.update_type() == kDelUgi) {
      return true;
    }

    GetVerificationInfo(access_entry, user_name, &verification_info_ptr);
    MergeVerificationInfoUgi(ugi_info, &verification_info_ptr);
  } else if (update_auth_info.update_type() == kGrantRole ||
             update_auth_info.update_type() == kRevokeRole) {
    const AuthorityInfo& authority_info = update_auth_info.authority_info();
    user_name = authority_info.user_name();
    meta_write_record->key = std::string("|00") + user_name;

    GetVerificationInfo(access_entry, user_name, &verification_info_ptr);
    MergeVerificationInfoAuthority(authority_info, update_auth_info.update_type(),
                                   &verification_info_ptr);
  } else {
    return false;
  }
  UgiMetaInfo ugi_meta_info;
  ugi_meta_info.set_user_name(user_name);
  ugi_meta_info.set_passwd(verification_info_ptr->first);
  const RoleList& roles = verification_info_ptr->second;
  for (auto& role : roles) {
    *ugi_meta_info.add_roles() = role;
  }
  if (!ugi_meta_info.SerializeToString(&meta_write_record->value)) {
    return false;
  }
  return true;
}

// |00User => Passwd,[Role1,Role2...](pb)
static master::MetaWriteRecord* NewMetaRecordFromUgiRole(
    const std::shared_ptr<AccessEntry>& access_entry, const UpdateAuthInfo& update_auth_info) {
  if (!update_auth_info.has_ugi_info() && !update_auth_info.has_authority_info()) {
    return nullptr;
  }
  std::unique_ptr<master::MetaWriteRecord> meta_write_record(new master::MetaWriteRecord);

  if (kDelUgi == update_auth_info.update_type()) {
    meta_write_record->is_delete = true;
  } else if (kUpdateUgi == update_auth_info.update_type()) {
    meta_write_record->is_delete = false;
  } else {
    return nullptr;
  }
  if (!GetMetaRecordKeyValue(access_entry, update_auth_info, meta_write_record.get())) {
    return nullptr;
  }
  return meta_write_record.release();
}

// |01Role => Permissions(pb)
static master::MetaWriteRecord* NewMetaRecordFromRole(const UpdateAuthInfo& update_auth_info) {
  if (!update_auth_info.has_role_info()) {
    return nullptr;
  }
  std::unique_ptr<master::MetaWriteRecord> meta_write_record(new master::MetaWriteRecord);
  if (kAddRole == update_auth_info.update_type()) {
    meta_write_record->is_delete = false;
  } else if (kDelRole == update_auth_info.update_type()) {
    meta_write_record->is_delete = true;
  } else {
    return nullptr;
  }
  const RoleInfo& role_info = update_auth_info.role_info();
  meta_write_record->key = std::string("|01") + role_info.role();
  meta_write_record->value = "";
  return meta_write_record.release();
}

master::MetaWriteRecord* AccessUtils::NewMetaRecord(
    const std::shared_ptr<AccessEntry>& access_entry, const UpdateAuthInfo& update_auth_info) {
  UpdateAuthType update_auth_type = update_auth_info.update_type();
  if (update_auth_type == kUpdateUgi || update_auth_type == kDelUgi ||
      update_auth_type == kGrantRole || update_auth_type == kRevokeRole) {
    return NewMetaRecordFromUgiRole(access_entry, update_auth_info);
  } else if (update_auth_type == kAddRole || update_auth_type == kDelRole) {
    return NewMetaRecordFromRole(update_auth_info);
  } else {
    return nullptr;
  }
}

std::string AccessUtils::GetNameFromMetaKey(const std::string& key) {
  if (key.length() <= 3 || key[1] != '0') {
    return "";
  }
  return key.substr(3);
}

VerificationInfoPtr AccessUtils::GetVerificationInfoFromMetaValue(const std::string& value) {
  if (value.size() <= 0) {
    return nullptr;
  }
  VerificationInfoPtr verification_info_ptr(new VerificationInfo);
  UgiMetaInfo ugi_meta_info;
  if (!ugi_meta_info.ParseFromString(value)) {
    return nullptr;
  }
  verification_info_ptr->first = ugi_meta_info.passwd();
  RoleList& roles = verification_info_ptr->second;
  int roles_size = ugi_meta_info.roles_size();
  for (int roles_index = 0; roles_index < roles_size; ++roles_index) {
    roles.emplace(ugi_meta_info.roles(roles_index));
  }
  return verification_info_ptr;
}

}  // namespace auth
}  // namespace tera
