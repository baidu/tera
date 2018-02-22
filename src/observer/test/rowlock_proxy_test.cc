// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include <atomic>
#include <string>
#include <vector>

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>
#include <sofa/pbrpc/pbrpc.h>

#include "observer/rowlockproxy/remote_rowlock_proxy.h"
#include "observer/rowlockproxy/rowlock_proxy_impl.h"
#include "proto/rpc_client.h"
#include "sdk/rowlock_client.h"
#include "utils/utils_cmd.h"

class TestClosure : public google::protobuf::Closure {
public:
	TestClosure() {}
	virtual void Run() {}
};

namespace tera {
namespace observer {

class TestClient : public RowlockStub {
public:
	TestClient() : RowlockStub("127.0.0.1:22222") {};
	~TestClient() {}

	virtual bool TryLock(const RowlockRequest* request,
            RowlockResponse* response,
            std::function<void (RowlockRequest*, RowlockResponse*, bool, int)> done = NULL) {
		response->set_lock_status(kLockSucc);
		return true;
	}

    virtual bool UnLock(const RowlockRequest* request,
            RowlockResponse* response,
            std::function<void (RowlockRequest*, RowlockResponse*, bool, int)> done = NULL) {
    	response->set_lock_status(kLockSucc);
		return true;
    }
};

TEST(RowlockProxyTest, ValueTest) {	
	RowlockProxyImpl rowlock_proxy_impl;

	rowlock_proxy_impl.SetServerNumber(100);
	EXPECT_EQ(100, rowlock_proxy_impl.server_number_);
	EXPECT_EQ(100, rowlock_proxy_impl.GetServerNumber());
	
	rowlock_proxy_impl.SetServerNumber(1000);
	EXPECT_EQ(1000, rowlock_proxy_impl.server_number_);
	EXPECT_EQ(1000, rowlock_proxy_impl.GetServerNumber());

	rowlock_proxy_impl.SetServerNumber(2);
	EXPECT_EQ(1000, rowlock_proxy_impl.server_addrs_->size());
	EXPECT_EQ(0, rowlock_proxy_impl.clients_->size());
	rowlock_proxy_impl.UpdateServers(0, "0.0.0.0:9999");

	EXPECT_EQ(1, rowlock_proxy_impl.clients_->size());
	rowlock_proxy_impl.UpdateServers(0, "0.0.1.1:9999");

	EXPECT_EQ(2, rowlock_proxy_impl.clients_->size());

	EXPECT_EQ(std::hash<std::string>()("tablerow"), 
		rowlock_proxy_impl.GetRowKey("table", "row"));

	EXPECT_EQ((*rowlock_proxy_impl.server_addrs_)[0], rowlock_proxy_impl.ScheduleRowKey(0));
	EXPECT_EQ((*rowlock_proxy_impl.server_addrs_)[1], rowlock_proxy_impl.ScheduleRowKey(1));
}

TEST(RowlockProxyTest, LockTest) {
	RowlockProxyImpl rowlock_proxy_impl;

	rowlock_proxy_impl.SetServerNumber(1);
	rowlock_proxy_impl.UpdateServers(0, "0.0.0.0:9999");
	EXPECT_EQ(1, rowlock_proxy_impl.server_addrs_->size());
	EXPECT_EQ(1, rowlock_proxy_impl.clients_->size());

	EXPECT_TRUE(rowlock_proxy_impl.clients_->find("0.0.0.0:9999") != 
		rowlock_proxy_impl.clients_->end());
	delete (*rowlock_proxy_impl.clients_)["0.0.0.0:9999"];
	(*rowlock_proxy_impl.clients_)["0.0.0.0:9999"] = new TestClient();

	RowlockRequest request;
	RowlockResponse response;
	request.set_table_name("table");
	request.set_row("row");

	google::protobuf::Closure* closure = new TestClosure();

	rowlock_proxy_impl.TryLock(&request, &response, closure);
	EXPECT_EQ(response.lock_status(), kLockSucc);

	google::protobuf::Closure* unlock_closure = new TestClosure();
	rowlock_proxy_impl.UnLock(&request, &response, unlock_closure);
	EXPECT_EQ(response.lock_status(), kLockSucc);
}

} // namespace observer
} // namespace tera

