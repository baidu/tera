// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef  TERA_ZK_ZK_UTIL_H_
#define  TERA_ZK_ZK_UTIL_H_

#include <stdint.h>

#include <string>
#include <vector>

namespace tera {
namespace zk {

std::string ZkErrnoToString(int err);
void SetZkAdapterCode(int code, int* ret_code);
std::string ZooStateToString(int state);
std::string ZooTypeToString(int type);

enum ZooKeeperErrno {
    ZE_OK = 0,
    ZE_ARG,
    ZE_SESSION,
    ZE_SYSTEM,
    ZE_INITED,
    ZE_NOT_INIT,
    ZE_EXIST,
    ZE_NOT_EXIST,
    ZE_NO_PARENT,
    ZE_ENTITY_PARENT,
    ZE_AUTH,
    ZE_HAS_CHILD,
    ZE_LOCK_TIMEOUT,
    ZE_LOCK_EXIST,
    ZE_LOCK_NOT_EXIST,
    ZE_LOCK_CANCELED,
    ZE_LOCK_ACQUIRED,
    ZE_LOCK_NOT_ACQUIRED,
    ZE_UNKNOWN
};

enum ZooKeeperState {
    ZS_DISCONN,
    ZS_CONNECTING,
    ZS_CONNECTED,
    ZS_AUTH,
    ZS_TIMEOUT
};

enum ZooKeeperWatchType {
    ZT_WATCH_VALUE = 1,
    ZT_WATCH_EXIST = 2,
    ZT_WATCH_CHILD = 4
};

class ZooKeeperUtil {
public:
    static bool IsChild(const char * child, const char * parent);
    static bool GetParentPath(const std::string& path, std::string* parent);
    static const char * GetNodeName(const char * path);
    static int32_t GetSequenceNo(const std::string& name);
    static bool IsValidPath(const std::string& path);
};

class FakeZkUtil {
public:
    static bool WriteNode(const std::string& name, const std::string& value);
    static bool ReadNode(const std::string& name, std::string* value);
    static bool ListNodes(const std::string& path, std::vector<std::string>* values);
};

class InsUtil {
public:
    static bool ReadNode(const std::string& name, std::string* value);
};

} // namespace zk
} // namespace tera

#endif  // TERA_ZK_ZK_UTIL_H_
