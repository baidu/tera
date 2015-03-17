// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef TERA_PROTO_PROTO_HELPER_H
#define TERA_PROTO_PROTO_HELPER_H

#include <string>

#include "proto/table_meta.pb.h"
#include "proto/tabletnode.pb.h"
#include "proto/tabletnode_rpc.pb.h"

namespace tera {

typedef ::google::protobuf::RepeatedPtrField< RowMutationSequence> RowMutationList;
typedef ::google::protobuf::RepeatedPtrField< KeyValuePair> KeyValueList;
typedef ::google::protobuf::RepeatedPtrField< ::std::string> KeyList;
typedef ::google::protobuf::RepeatedPtrField< ::tera::RowResult> RowResultList;
typedef ::google::protobuf::RepeatedPtrField< ::tera::RowReaderInfo> RowReaderList;

std::string StatusCodeToString(int32_t status);
std::string StatusCodeToString(TabletNodeStatus status);
} // namespace tera

#endif // TERA_PROTO_PROTO_HELPER_H
