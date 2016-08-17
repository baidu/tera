// Copyright (c) 2016, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

/* Copyright (c) 2004, 2015, Oracle and/or its affiliates. All rights reserved.

  This program is free software; you can redistribute it and/or modify
  it under the terms of the GNU General Public License as published by
  the Free Software Foundation; version 2 of the License.

  This program is distributed in the hope that it will be useful,
  but WITHOUT ANY WARRANTY; without even the implied warranty of
  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
  GNU General Public License for more details.

  You should have received a copy of the GNU General Public License
  along with this program; if not, write to the Free Software
  Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA */

#include "ha_tera_format.h"
#include "key.h"
#include "sql_class.h"

ha_tera_format::ha_tera_format(TABLE* table)
    : table_(table), field_buf_(new uchar[64 << 20]) {
}

ha_tera_format::~ha_tera_format() {
    delete[] field_buf_;
}

void ha_tera_format::mysql_buf_to_primary_data(const uchar* buf, std::string* key,
                                               std::string* value) {
    pack_key(buf, key);
    pack_row(buf, value);
}

int ha_tera_format::primary_data_to_mysql_buf(const std::string& key, const std::string& value,
                                              uchar* buf) {
    return unpack_row(value, buf);
}

void ha_tera_format::pack_key(const uchar* buf, std::string* key) {
    DBUG_ENTER("ha_tera_format::pack_key");
    key->resize(table_->key_info[0].key_length);
    uchar* to = (uchar*)key->data();
    key_copy(to, (uchar*)buf, &table_->key_info[0], 0);
    DBUG_VOID_RETURN;
}

void ha_tera_format::pack_row(const uchar* buf, std::string* row) {
    DBUG_ENTER("ha_tera_format::pack_row");
    for (Field **field = table_->field; *field; field++) {
        uint32_t field_len = (*field)->pack_length();
        if ((*field)->type() == MYSQL_TYPE_VARCHAR) {
            field_len = (*field)->data_length();
        }
        if ((*field)->type() == MYSQL_TYPE_BLOB) {
            field_len = ((Field_blob *)(*field))->get_length();
        }

        uchar* data_ptr = (*field)->ptr;
        if ((*field)->type() == MYSQL_TYPE_VARCHAR) {
            int offset = (*field)->pack_length() - (*field)->field_length;
            if (offset > 0) {
                data_ptr += offset;
            }
        }
        if ((*field)->type() == MYSQL_TYPE_BLOB) {
            ((Field_blob *)(*field))->get_ptr(&data_ptr);
        }

        (*row) += std::string((char*)&field_len, sizeof(field_len));
        (*row) += std::string((char*)data_ptr, field_len);
    }

    DBUG_VOID_RETURN;
}

int ha_tera_format::unpack_row(const std::string& row, uchar* buf) {
    DBUG_ENTER("ha_tera_format::unpack_row");

    uchar* blob_ptr = field_buf_;
    char* field_ptr = (char*)row.data();
    char* end_ptr = field_ptr + row.size();
    for (Field** field = table_->field; *field; field++) {
        if (end_ptr - field_ptr < (long long)sizeof(uint32_t)) {
            DBUG_RETURN(-1);
        }
        uint32_t field_len = uint4korr(field_ptr);
        field_ptr += sizeof(uint32_t);

        if (field_len == 0) {
            (*field)->set_null();
            continue;
        }
        if (end_ptr - field_ptr < field_len) {
            DBUG_RETURN(-1);
        }
        (*field)->set_notnull();
        int offset = 0;
        if ((*field)->type() == MYSQL_TYPE_VARCHAR) {
            offset = (*field)->pack_length() - (*field)->field_length;
            DBUG_ASSERT(offset <= 2 && offset >= 1);
            DBUG_ASSERT(field_len < 65535);
            if (offset == 1) {
                *((*field)->ptr) = (uchar)field_len;
            } else {
                int2store((*field)->ptr, field_len);
            }
        }
        if ((*field)->type() == MYSQL_TYPE_BLOB) {
            memcpy(blob_ptr, field_ptr, field_len);
            ((Field_blob *)(*field))->set_ptr(field_len, blob_ptr);
            blob_ptr += field_len;
        } else {
            memcpy((*field)->ptr + offset, field_ptr, field_len);
        }
        field_ptr += field_len;
    }

    DBUG_RETURN(0);
}
