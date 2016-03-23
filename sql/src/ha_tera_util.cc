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

#include <string.h>
#include "ha_tera_util.h"
#include "sql_plugin.h"
#include "sql_table.h"

void ha_tera_util::path_to_dbname(const char *path_name, char *dbname)
{
  char *end, *ptr, *tmp_name;
  char tmp_buff[FN_REFLEN + 1];

  tmp_name= tmp_buff;
  /* Scan name from the end */
  ptr= strend(path_name)-1;
  while (ptr >= path_name && *ptr != '\\' && *ptr != '/') {
    ptr--;
  }
  ptr--;
  end= ptr;
  while (ptr >= path_name && *ptr != '\\' && *ptr != '/') {
    ptr--;
  }
  uint name_len= (uint)(end - ptr);
  memcpy(tmp_name, ptr + 1, name_len);
  tmp_name[name_len]= '\0';
  filename_to_tablename(tmp_name, dbname, sizeof(tmp_buff) - 1);
}

/**
  Set a given location from full pathname to table file.
*/

void ha_tera_util::path_to_tabname(const char *path_name, char * tabname)
{
  char *end, *ptr, *tmp_name;
  char tmp_buff[FN_REFLEN + 1];

  tmp_name= tmp_buff;
  /* Scan name from the end */
  end= strend(path_name)-1;
  ptr= end;
  while (ptr >= path_name && *ptr != '\\' && *ptr != '/') {
    ptr--;
  }
  uint name_len= (uint)(end - ptr);
  memcpy(tmp_name, ptr + 1, end - ptr);
  tmp_name[name_len]= '\0';
  filename_to_tablename(tmp_name, tabname, sizeof(tmp_buff) - 1);
}

void ha_tera_util::name_to_tera_tabname(const char* dbname, const char* tabname,
                                        char* tera_tabname) {
  strcpy(tera_tabname, dbname);
  strcat(tera_tabname, "_");
  strcat(tera_tabname, tabname);
}

bool ha_tera_util::tera_tabname_to_name(const char* tera_tabname, char* dbname, char* tabname) {
  const char* delim = strchr(tera_tabname, '_');
  if (delim == NULL || delim == tera_tabname || *(delim + 1) == '\0') {
    return false;
  }
  strncpy(dbname, tera_tabname, delim - tera_tabname);
  dbname[delim - tera_tabname] = '\0';
  strcpy(tabname, delim + 1);
  return true;
}

