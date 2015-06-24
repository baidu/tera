// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "zk/zk_util.h"

#include <string.h>
#include <string>

#include <glog/logging.h>
#include <zookeeper/zookeeper.h>

#include "common/file/file_path.h"
#include "common/file/file_stream.h"

#include "ins_sdk.h"

DECLARE_string(tera_ins_addr_list);

namespace tera {
namespace zk {

std::string ZkErrnoToString(int err) {
    switch (err) {
    case ZE_OK:
        return "OK";
    case ZE_ARG:
        return "Bad argument";
    case ZE_SESSION:
        return "Broken session";
    case ZE_SYSTEM:
        return "System error";
    case ZE_INITED:
        return "Has init";
    case ZE_NOT_INIT:
        return "Not init";
    case ZE_EXIST:
        return "Node exist";
    case ZE_NOT_EXIST:
        return "Node not exist";
    case ZE_NO_PARENT:
        return "No parent node";
    case ZE_ENTITY_PARENT:
        return "Ephemeral parent node";
    case ZE_AUTH:
        return "Authorization error";
    case ZE_HAS_CHILD:
        return "Has child";
    case ZE_LOCK_TIMEOUT:
        return "Lock timeout";
    case ZE_LOCK_EXIST:
        return "Lock exist";
    case ZE_LOCK_NOT_EXIST:
        return "Lock not exist";
    case ZE_LOCK_CANCELED:
        return "Lock is canceled";
    case ZE_LOCK_ACQUIRED:
        return "Lock is acquired";
    case ZE_LOCK_NOT_ACQUIRED:
        return "Lock not acquired";
    case ZE_UNKNOWN:
        return "Unknown error";
    default:
        ;
    }
    return "";
}

void SetZkAdapterCode(int code, int* ret_code) {
    *ret_code = code;
}

std::string ZooStateToString(int state) {
    if (ZOO_EXPIRED_SESSION_STATE == state) {
        return "ZOO_EXPIRED_SESSION_STATE";
    } else if (ZOO_AUTH_FAILED_STATE == state) {
        return "ZOO_AUTH_FAILED_STATE";
    } else if (ZOO_CONNECTING_STATE == state) {
        return "ZOO_CONNECTING_STATE";
    } else if (ZOO_ASSOCIATING_STATE == state) {
        return "ZOO_ASSOCIATING_STATE";
    } else if (ZOO_CONNECTED_STATE == state) {
        return "ZOO_CONNECTED_STATE";
    } else {
        return "ZOO_UNKNOWN_STATE";
    }
}

std::string ZooTypeToString(int type) {
    if (ZOO_CREATED_EVENT == type) {
        return "ZOO_CREATED_EVENT";
    } else if (ZOO_DELETED_EVENT == type) {
        return "ZOO_DELETED_EVENT";
    } else if (ZOO_CHANGED_EVENT == type) {
        return "ZOO_CHANGED_EVENT";
    } else if (ZOO_CHILD_EVENT == type) {
        return "ZOO_CHILD_EVENT";
    } else if (ZOO_SESSION_EVENT == type) {
        return "ZOO_SESSION_EVENT";
    } else if (ZOO_NOTWATCHING_EVENT == type) {
        return "ZOO_NOTWATCHING_EVENT";
    } else {
        return "ZOO_UNKNOWN_EVENT";
    }
}

bool ZooKeeperUtil::IsChild(const char * child, const char * parent) {
    size_t child_len = strlen(child);
    size_t parent_len = strlen(parent);
    if (child[child_len - 1] == '/') {
        child_len--;
    }
    if (parent[parent_len - 1] == '/') {
        parent_len--;
    }
    if (child_len <= parent_len || 0 != strncmp(parent, child, parent_len)
        || child[parent_len] != '/' || child[parent_len + 1] == '\0') {
        return false;
    }

    const char * slash_ptr = strchr(child + parent_len + 1, '/');
    if (slash_ptr == NULL || slash_ptr == child + child_len) {
        return false;
    }

    return true;
}

bool ZooKeeperUtil::GetParentPath(const std::string& path, std::string* parent) {
    if (path[0] != '/') {
        return false;
    }
    size_t last_slash_pos = path.find_last_of('/');
    if (last_slash_pos > 0) {
        parent->assign(path, 0, last_slash_pos);
    } else {
        parent->assign("/");
    }
    return true;
}

const char * ZooKeeperUtil::GetNodeName(const char * path) {
    if (path[0] != '/') {
        return NULL;
    }
    const char * last_slash_ptr = rindex(path, '/');
    return last_slash_ptr + 1;
}

int32_t ZooKeeperUtil::GetSequenceNo(const std::string& name) {
    size_t name_len = name.size();
    if (name_len < 10) {
        LOG(ERROR) << "name [" << name << "] too short";
        return -1;
    }

    const char * seq_str = name.c_str() + name_len - 10;
    while (*seq_str == '0') { // skip '0'
        seq_str++;
    }

    int32_t seq_no;
    char * seq_end_ptr;
    if (*seq_str != '\0') {
        seq_no = strtol(seq_str, &seq_end_ptr, 10);
        if (*seq_end_ptr == '\0' && seq_no > 0) {
            return seq_no;
        } else {
            LOG(ERROR) << "name [" << name << "] not end in 10 digit";
            return -1;
        }
    } else {
        return 0;
    }
}

bool ZooKeeperUtil::IsValidPath(const std::string& path) {
    if (path.empty() || path[0] != '/'
        || (path.size() > 1 && *path.rbegin() == '/')) {
        return false;
    }
    return true;
}

bool FakeZkUtil::WriteNode(const std::string& name, const std::string& value) {
    FileStream node_file;
    if (!node_file.Open(name, FILE_WRITE)) {
        return false;
    }
    if (node_file.Write(value.c_str(), value.size()) != (int32_t)value.size()) {
        return false;
    }
    node_file.Close();
    return true;
}

bool FakeZkUtil::ReadNode(const std::string& name, std::string* value) {
    FileStream node_file;
    if (!node_file.Open(name, FILE_READ)) {
        LOG(ERROR) << "fail to open node file: " << name;
        return false;
    }
    if (node_file.ReadLine(value) < 0) {
        LOG(ERROR) << "fail to read node file: " << name;
        return false;
    }
    node_file.Close();
    return true;
}

bool FakeZkUtil::ListNodes(const std::string& path,
                          std::vector<std::string>* values) {
    if (!ListCurrentDir(path, values)) {
        return false;
    }
    return true;
}

bool InsUtil::ReadNode(const std::string& name, std::string* value) {
    galaxy::ins::sdk::InsSDK ins_sdk(FLAGS_tera_ins_addr_list);
    galaxy::ins::sdk::SDKError err;
    CHECK(ins_sdk.Get(name, value, &err)) << "sdk read ins fail";
    return true;
}

} // namespace zk
} // namespace tera
