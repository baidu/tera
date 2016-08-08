// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef TERA_PROTO_PROTO_HELPER_H_
#define TERA_PROTO_PROTO_HELPER_H_

#include <string>

#include "proto/master_rpc.pb.h"
#include "proto/table_meta.pb.h"
#include "proto/tabletnode.pb.h"
#include "proto/tabletnode_rpc.pb.h"

namespace tera {

typedef ::google::protobuf::RepeatedPtrField< RowMutationSequence> RowMutationList;
typedef ::google::protobuf::RepeatedPtrField< KeyValuePair> KeyValueList;
typedef ::google::protobuf::RepeatedPtrField< ::std::string> KeyList;
typedef ::google::protobuf::RepeatedPtrField< ::tera::RowResult> RowResultList;
typedef ::google::protobuf::RepeatedPtrField< ::tera::RowReaderInfo> RowReaderList;

std::string StatusCodeToString(StatusCode status);
std::string StatusCodeToString(TabletStatus status);
std::string StatusCodeToString(TableStatus status);
std::string StatusCodeToString(CompactStatus status);
std::string StatusCodeToString(TabletNodeStatus status);
std::string StatusCodeToString(int32_t status);

} // namespace tera
#endif // TERA_PROTO_PROTO_HELPER_H_
