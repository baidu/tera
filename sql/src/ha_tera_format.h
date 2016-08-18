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

#include "handler.h"
#include <string>

class ha_tera_format {
public:
    ha_tera_format(TABLE* table);
    ~ha_tera_format();

    void mysql_buf_to_primary_data(const uchar* buf,
                                   std::string* key,
                                   std::string* value);
    int primary_data_to_mysql_buf(const std::string& key,
                                  const std::string& value,
                                  uchar* buf);

private:
    virtual void pack_key(const uchar* buf, std::string* key);
    virtual void pack_row(const uchar* buf, std::string* row);
    virtual int unpack_row(const std::string& row, uchar* buf);

    TABLE* table_;
    uchar* field_buf_;
};
