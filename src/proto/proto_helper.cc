// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include <stdio.h>

#include "proto/proto_helper.h"
#include "proto/master_rpc.pb.h"

namespace tera {

std::string StatusCodeToString(int32_t status) {
    switch (status) {
    // master
    case kMasterNotInited:
        return "kMasterNotInited";
    case kMasterIsBusy:
        return "kMasterIsBusy";
    case kMasterIsSecondary:
        return "kMasterIsSecondary";
    case kMasterIsReadonly:
        return "kMasterIsReadonly";
    case kMasterOnRestore:
        return "kMasterOnRestore";
    case kMasterIsRunning:
        return "kMasterIsRunning";
    case kMasterOnWait:
        return "kMasterOnWait";

    // tablet node
    case kTabletNodeNotInited:
        return "kTabletNodeNotInited";
    case kTabletNodeIsBusy:
        return "kTabletNodeIsBusy";
    case kTabletNodeIsIniting:
        return "kTabletNodeIsIniting";
    case kTabletNodeIsReadonly:
        return "kTabletNodeIsReadonly";
    case kTabletNodeIsRunning:
        return "kTabletNodeIsRunning";

    // tablet node manage
    case kTabletNodeReady:
        return "kTabletNodeReady";
    case kTabletNodeOffLine:
        return "kTabletNodeOffLine";
    case kTabletNodeOnKick:
        return "kTabletNodeOnKick";
    case kTabletNodeWaitKick:
        return "kTabletNodeWaitKick";

    // table status
    case kTableOk:
        return "kTableOk";

    case kTableNotFound:
        return "kTableNotFound";
    case kTableCorrupt:
        return "kTableCorrupt";
    case kTableNotSupport:
        return "kTableNotSupport";
    case kTableInvalidArg:
        return "kTableInvalidArg";

    // ACL & system
    case kIllegalAccess:
        return "kIllegalAccess";
    case kNotPermission:
        return "kNotPermission";
    case kIOError:
        return "kIOError";

    //// master rpc ////
    // create table
    case kMasterOk:
        return "kMasterOk";
    case kTableExist:
        return "kTableExist";
    case kTableIsBusy:
        return "kTableIsBusy";
    case kTableMergeError:
        return "kTableMergeError";

    // register
    case kInvalidSequenceId:
        return "kInvalidSequenceId";
    case kInvalidTabletNodeInfo:
        return "kInvalidTabletNodeInfo";

    // report
    case kTabletNodeNotRegistered:
        return "kTabletNodeNotRegistered";

    // cmd ctrl
    case kInvalidArgument:
        return "kInvalidArgument";

    //// tablet node rpc ////
    // response
    // case kTabletNodeOk:
    //     return "kTabletNodeOk";

    // key
    case kKeyNotExist:
        return "kKeyNotExist";
    case kKeyNotInRange:
        return "kKeyNotInRange";

    // meta table
    case kMetaTabletError:
        return "kMetaTabletError";

    // RPC
    case kRPCError:
        return "kRPCError";
    case kServerError:
        return "kServerError";
    case kClientError:
        return "kClientError";
    case kConnectError:
        return "kConnectError";
    case kRPCTimeout:
        return "kRPCTimeout";

    // Async Writer
    case kAsyncNotRunning:
        return "kAsyncNotRunning";
    case kAsyncTooBusy:
        return "kAsyncTooBusy";
    case kAsyncFailure:
        return "kAsyncFailure";

    // zk
    case kZKError:
        return "kZKError";

    //// TabletStatus ////
    case kTableNotInit:
        return "kTableNotInit";
    case kTableOffLine:
        return "kTableOffLine";
    case kTableReady:
        return "kTableReady";
    case kTableUnLoad:
        return "kTableUnLoad";
    case kTableWaitLoad:
        return "kTableWaitLoad";
    case kTableOnLoad:
        return "kTableOnLoad";
    case kTableLoadFail:
        return "kTableLoadFail";
    case kTableWaitSplit:
        return "kTableWaitSplit";
    case kTableOnSplit:
        return "kTableOnSplit";
    case kTableSplitFail:
        return "kTableSplitFail";
    case kTableUnLoading:
        return "kTableUnLoading";
    case kTableUnLoadFail:
        return "kTableUnLoadFail";
    case kTableOnMerge:
        return "kTableOnMerge";
    case kTabletDisable:
        return "kTabletDisable";
    case kTabletPending:
        return "kTabletPending";
    case kTabletUnLoading2:
        return "kTabletUnLoading2";
    case kSnapshotNotExist:
        return "kSnapshotNotExist";

    //// TableStatus ////
    case kTableEnable:
        return "kTableEnable";
    case kTableDisable:
        return "kTableDisable";
    case kTableDeleting:
        return "kTableDeleting";

    //// CompactStatus ////
    case kTableNotCompact:
        return "kTableNotCompact";
    case kTableOnCompact:
        return "kTableOnCompact";
    case kTableCompacted:
        return "kTableCompacted";
    default:
        ;
    }
    char num[16];
    snprintf(num, 16, "%d", status);
    num[15] = '\0';
    return num;
}

std::string StatusCodeToString(TabletNodeStatus status) {
    switch (status) {
        case kTabletNodeInit:
            return "kTabletNodeInit";
        case kTabletNodeRegistered:
            return "kTabletNodeRegistered";
        case kTabletNodeRunning:
            return "kTabletNodeRunning";
        case kTabletNodeTimeout:
            return "kTabletNodeTimeout";
        case kTabletNodeFrozen:
            return "kTabletNodeFrozen";
        case kTabletNodeRestore:
            return "kTabletNodeRestore";
        case kTabletNodeOffline:
            return "kTabletNodeOffline";
        default:
            return "";
    }
}
} // namespace tera
