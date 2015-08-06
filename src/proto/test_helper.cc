// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "proto/test_helper.h"

#include "common/base/string_number.h"

#include "proto/table_schema.pb.h"

namespace tera {

TableSchema DefaultTableSchema() {
    TableSchema schema;
    schema.set_id(0);
    schema.set_name("lg0");
    schema.set_owner(0);
    schema.add_acl(0777);

    ColumnFamilySchema* cf_schema = schema.add_column_families();
    cf_schema->set_id(0);
    cf_schema->set_name("lg0_cf0");
    cf_schema->set_locality_group("lg0");
    cf_schema->set_owner(0);
    cf_schema->add_acl(0777);

    LocalityGroupSchema* lg_schema = schema.add_locality_groups();
    lg_schema->set_id(0);
    lg_schema->set_name("lg0_name");

    return schema;
}

ColumnFamilySchema DefaultCFSchema(const std::string& lg_name,
                                   uint32_t id) {
    ColumnFamilySchema cf_schema;
    std::string cf_name("cf");
    cf_name += NumberToString(id);
    cf_schema.set_id(id);
    cf_schema.set_name(lg_name + "_" + cf_name);
    cf_schema.set_locality_group(lg_name);
    cf_schema.set_owner(0);
    cf_schema.add_acl(0777);

    return cf_schema;
}

LocalityGroupSchema DefaultLGSchema(uint32_t id) {
    LocalityGroupSchema lg_schema;
    std::string lg_name("lg");
    lg_name += NumberToString(id);
    lg_schema.set_id(id);
    lg_schema.set_name(lg_name);

    return lg_schema;
}

TableSchema DefaultTableSchema(uint32_t id, uint32_t lg_num,
                               uint32_t cf_num) {
    TableSchema schema;
    std::string name("table");
    name += NumberToString(id);
    schema.set_id(id);
    schema.set_name(name);
    schema.set_owner(0);
    schema.set_acl(0, 0777);

    for (uint32_t lg_id = 0; lg_id < lg_num; ++lg_id) {
        LocalityGroupSchema lg_schema = DefaultLGSchema(lg_id);
    }

    return schema;
}
} // namespace tera
