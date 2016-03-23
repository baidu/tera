// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

/* Copyright (c) 2004, 2014, Oracle and/or its affiliates. All rights reserved.

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

class ha_tera_util {
public:
  static void path_to_dbname(const char *path_name, char *dbname);
  static void path_to_tabname(const char *path_name, char *tabname);
  static void name_to_tera_tabname(const char* dbname, const char* tabname, char* tera_tabname);
  static bool tera_tabname_to_name(const char* tera_tabname, char* dbname, char* tabname);
};
