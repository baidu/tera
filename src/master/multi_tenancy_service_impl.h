// Copyright (c) 2015-2018, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#pragma once
#include <memory>
#include "common/thread_pool.h"
#include "proto/master_rpc.pb.h"

namespace tera {

namespace auth {
class AccessEntry;
}

namespace quota {
class MasterQuotaEntry;
}

namespace master {

class MultiTenacyServiceImpl {
 public:
  explicit MultiTenacyServiceImpl(const std::shared_ptr<auth::AccessEntry>& access_entry,
                                  const std::shared_ptr<quota::MasterQuotaEntry>& quota_entry);
  virtual ~MultiTenacyServiceImpl() {}
  void UpdateUgi(const UpdateUgiRequest* request, UpdateUgiResponse* response,
                 google::protobuf::Closure* done);
  void ShowUgi(const ShowUgiRequest* request, ShowUgiResponse* response,
               google::protobuf::Closure* done);

  void UpdateAuth(const UpdateAuthRequest* request, UpdateAuthResponse* response,
                  google::protobuf::Closure* done);
  void ShowAuth(const ShowAuthRequest* request, ShowAuthResponse* response,
                google::protobuf::Closure* done);

  void SetAuthPolicy(const SetAuthPolicyRequest* request, SetAuthPolicyResponse* response,
                     google::protobuf::Closure* done);
  void ShowAuthPolicy(const ShowAuthPolicyRequest* request, ShowAuthPolicyResponse* response,
                      google::protobuf::Closure* done);

  void SetQuota(const SetQuotaRequest* request, SetQuotaResponse* response,
                google::protobuf::Closure* done);
  void ShowQuota(const ShowQuotaRequest* request, ShowQuotaResponse* response,
                 google::protobuf::Closure* done);

 private:
  std::shared_ptr<auth::AccessEntry> access_entry_;
  std::shared_ptr<quota::MasterQuotaEntry> quota_entry_;
  std::shared_ptr<ThreadPool> thread_pool_;
};
}
}
