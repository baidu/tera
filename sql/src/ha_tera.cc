// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
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

/**
  @file ha_tera.cc

  @brief
  The ha_example engine is a stubbed storage engine for example purposes only;
  it does nothing at this point. Its purpose is to provide a source
  code illustration of how to begin writing new storage engines; see also
  /storage/example/ha_example.h.

  @details
  ha_example will let you create/open/delete tables, but
  nothing further (for example, indexes are not supported nor can data
  be stored in the table). Use this example as a template for
  implementing the same functionality in your own storage engine. You
  can enable the example storage engine in your build by doing the
  following during your build process:<br> ./configure
  --with-example-storage-engine

  Once this is done, MySQL will let you create tables with:<br>
  CREATE TABLE <table name> (...) ENGINE=EXAMPLE;

  The example storage engine is set up to use table locks. It
  implements an example "SHARE" that is inserted into a hash by table
  name. You can use this to store information of state that any
  example handler object will be able to see when it is using that
  table.

  Please read the object definition in ha_example.h before reading the rest
  of this file.

  @note
  When you create an EXAMPLE table, the MySQL Server creates a table .frm
  (format) file in the database directory, using the table name as the file
  name as is customary with MySQL. No other files are created. To get an idea
  of what occurs, here is an example select that would do a scan of an entire
  table:

  @code
  ha_example::store_lock
  ha_example::external_lock
  ha_example::info
  ha_example::rnd_init
  ha_example::extra
  ENUM HA_EXTRA_CACHE        Cache record in HA_rrnd()
  ha_example::rnd_next
  ha_example::rnd_next
  ha_example::rnd_next
  ha_example::rnd_next
  ha_example::rnd_next
  ha_example::rnd_next
  ha_example::rnd_next
  ha_example::rnd_next
  ha_example::rnd_next
  ha_example::extra
  ENUM HA_EXTRA_NO_CACHE     End caching of records (def)
  ha_example::external_lock
  ha_example::extra
  ENUM HA_EXTRA_RESET        Reset database to after open
  @endcode

  Here you see that the example storage engine has 9 rows called before
  rnd_next signals that it has reached the end of its data. Also note that
  the table in question was already opened; had it not been open, a call to
  ha_example::open() would also have been necessary. Calls to
  ha_example::extra() are hints as to what will be occuring to the request.

  A Longer Example can be found called the "Skeleton Engine" which can be
  found on TangentOrg. It has both an engine and a full build environment
  for building a pluggable storage engine.

  Happy coding!<br>
    -Brian
*/

#include <pthread.h>
#include "discover.h"
#include "ha_tera.h"
#include "ha_tera_util.h"
#include "probes_mysql.h"
#include "sql_class.h"           // MYSQL_HANDLERTON_INTERFACE_VERSION
#include "sql_plugin.h"
#include "sql_table.h"
#include "tera.h"

handlerton *tera_hton;

static const uint32 tera_frm_magic_num = 0x66668888;
static const char* tera_frm_data_column = "frm";
static const char* tera_frm_flag_column = "flag";

/* must be protected by tera_mutex */
static mysql_mutex_t tera_mutex;
static std::map<std::string, std::map<std::string, uint32> > all_table_version;

/* donot need protection */
static tera::Client* tera_client = NULL;
static tera::Table* tera_frm_table = NULL;
static pthread_t tera_loop_thread_id = 0;

static std::map<std::string, Tera_share*> tera_open_tables;

/* Interface to mysqld, to check system tables supported by SE */
static const char* tera_system_database();
static bool tera_is_supported_system_table(const char *db,
                                           const char *table_name,
                                           bool is_sql_layer_system_table);

static handler* tera_create_handler(handlerton *hton,
                                    TABLE_SHARE *table,
                                    MEM_ROOT *mem_root)
{
  return new (mem_root) ha_tera(hton, table);
}

static void* tera_loop(void*);
static int tera_discover(handlerton *hton, THD* thd, const char *db,
                         const char *name, uchar **frmblob, size_t *frmlen);

static int tera_init(void *p)
{
  DBUG_ENTER("tera_init");

  mysql_mutex_init(0, &tera_mutex, MY_MUTEX_INIT_FAST);
  tera_hton= (handlerton *)p;
  tera_hton->state= SHOW_OPTION_YES;
  tera_hton->create= tera_create_handler;
  tera_hton->discover = tera_discover;
  tera_hton->flags= HTON_CAN_RECREATE;
  tera_hton->system_database=   tera_system_database;
  tera_hton->is_supported_system_table= tera_is_supported_system_table;

  tera::ErrorCode ec;
  tera_client = tera::Client::NewClient("/tmp/tera.flag", "../log/tera");
  if (tera_client == NULL)
  {
    DBUG_PRINT("TERA", ("client init fail %s", ec.GetReason().c_str()));
    DBUG_RETURN(1);
  }

  tera_frm_table = tera_client->OpenTable("mysql_frm", &ec);
  if (tera_frm_table == NULL)
  {
    DBUG_PRINT("TERA", ("open frm table fail %s", ec.GetReason().c_str()));
    delete tera_client;
    DBUG_RETURN(2);
  }

  if (pthread_create(&tera_loop_thread_id, NULL, tera_loop, NULL))
  {
    DBUG_PRINT("error", ("create tera loop thread fail"));
    delete tera_frm_table;
    delete tera_client;
    DBUG_RETURN(3);
  }

  DBUG_PRINT("info", ("tera init success"));
  DBUG_RETURN(0);
}

static void* tera_loop(void*)
{
  my_thread_init();
  pthread_detach_this_thread();

  DBUG_ENTER("tera_loop");
  THD* thd = new THD;
  THD_CHECK_SENTRY(thd);
  thd->thread_stack= (char*)&thd; /* remember where our stack is */
  if (thd->store_globals())
    DBUG_RETURN(NULL);
  lex_start(thd);
  thd->set_command(COM_DAEMON);
  thd->client_capabilities = 0;
  thd->security_ctx->skip_grants();
  my_net_init(&thd->net, 0);

  CHARSET_INFO *charset_connection;
  charset_connection= get_charset_by_csname("utf8",
                                            MY_CS_PRIMARY, MYF(MY_WME));
  thd->variables.character_set_client= charset_connection;
  thd->variables.character_set_results= charset_connection;
  thd->variables.collation_connection= charset_connection;
  thd->update_charset();

  /*
    wait for mysql server to start
  */
  struct timespec abstime;
  mysql_mutex_lock(&LOCK_server_started);
  while (!mysqld_server_started)
  {
    set_timespec(abstime, 1);
    mysql_cond_timedwait(&COND_server_started, &LOCK_server_started,
                         &abstime);
  }
  mysql_mutex_unlock(&LOCK_server_started);

  // Defer call of THD::init_for_query until after mysqld_server_started
  // to ensure that the parts of MySQL Server it uses has been created
  thd->init_for_queries();

  DBUG_PRINT("TERA", ("begin to loop"));
  for(;;)
  {
    // DBUG_PRINT("TERA", ("scan frm table"));
    tera::ErrorCode ec;
    tera::ScanDescriptor scan_desc("");
    scan_desc.AddColumnFamily(tera_frm_flag_column);
    tera::ResultStream* scan_stream = tera_frm_table->Scan(scan_desc, &ec);
    if (scan_stream == NULL)
    {
      DBUG_PRINT("TERA", ("scan frm table fail %s", ec.GetReason().c_str()));
    }
    for (; scan_stream != NULL && !scan_stream->Done(); scan_stream->Next())
    {
      const std::string& tera_tabname = scan_stream->RowName();
      char dbname[FN_HEADLEN];
      char tabname[FN_HEADLEN];
      if (!ha_tera_util::tera_tabname_to_name(tera_tabname.c_str(), dbname, tabname))
      {
        DBUG_PRINT("error", ("tera tabname %s invalid", tera_tabname.c_str()));
        continue;
      }
      const std::string& flag = scan_stream->Value();
      if (flag.size() != 8)
      {
        DBUG_PRINT("error", ("frm flag of %s %s is not 8 bytes", dbname, tabname));
        continue;
      }
      uint32 frm_magic = *(uint32*)flag.data();
      uint32 frm_version = *(uint32*)(flag.data() + 4);
      if (frm_magic != tera_frm_magic_num)
      {
        DBUG_PRINT("error", ("frm magic of %s %s is invalid %u",
                             dbname, tabname, frm_magic));
        continue;
      }

      uint32 old_version = 0;
      mysql_mutex_lock(&tera_mutex);
      if (all_table_version.find(dbname) != all_table_version.end()
          && all_table_version[dbname].find(tabname) != all_table_version[dbname].end())
      {
        old_version = all_table_version[dbname][tabname];
      }
      mysql_mutex_unlock(&tera_mutex);

      if (frm_version <= old_version)
      {

        DBUG_PRINT("info", ("frm version of %s %s too old %u, now %u",
                            dbname, tabname, frm_version, old_version));
        continue;
      }
      mysql_mutex_unlock(&tera_mutex);

      DBUG_PRINT("info", ("try create table %s %s from engine", dbname, tabname));
      if (ha_create_table_from_engine(thd, dbname, tabname))
      {
        DBUG_PRINT("error", ("create table %s %s from engine tail", dbname, tabname));
        continue;
      }

      mysql_mutex_lock(&tera_mutex);
      all_table_version[dbname][tabname] = frm_version;
      mysql_mutex_unlock(&tera_mutex);

      DBUG_PRINT("info", ("update table %s %s version from %u to %u",
                          dbname, tabname, old_version, frm_version));
    }
    delete scan_stream;
    sleep(1);
  }
  delete thd;
  DBUG_RETURN(NULL);
}

static int tera_discover(handlerton *hton, THD* thd, const char *db,
                         const char *name, uchar **frmblob, size_t *frmlen)
{
  char key[FN_REFLEN + 1];
  DBUG_ENTER("tera_discover");
  DBUG_PRINT("info", ("discover table %s %s", db, name));

  // Check if the database directory for the table to discover exists
  // as otherwise there is no place to put the discovered .frm file.
  build_table_filename(key, sizeof(key) - 1, db, "", "", 0);
  const int database_exists= !my_access(key, F_OK);
  if (!database_exists)
  {
    sql_print_information("TERA: Could not find database directory '%s' "
                          "while trying to discover table '%s'", db, name);
    // Can't discover table when database directory does not exist
    DBUG_RETURN(1);
  }

  char tera_tabname[FN_HEADLEN * 2];
  ha_tera_util::name_to_tera_tabname(db, name, tera_tabname);
  tera::ErrorCode ec;
  std::string table_frm_data;
  if (!tera_frm_table->Get(tera_tabname, tera_frm_data_column, "", &table_frm_data, &ec))
  {
    DBUG_PRINT("TERA", ("not found %s in frm table", tera_tabname));
    DBUG_RETURN(1);
  }
  DBUG_PRINT("TERA", ("found %s in frm table, value len %ld", tera_tabname,
                      table_frm_data.size()));

  size_t len = table_frm_data.size();
  if (len == 0)
  {
    DBUG_PRINT("error", ("frm data of table %s %s is empty", db, name));
    DBUG_RETURN(1);
  }

  uchar* data= NULL;
  if (unpackfrm(&data, &len, (uchar*)table_frm_data.data()))
  {
    DBUG_PRINT("error", ("Could not unpack frm data of table %s %s", db, name));
    DBUG_RETURN(1);
  }
  *frmlen= len;
  *frmblob= data;

  DBUG_RETURN(0);
}

/*
static int tera_find_files(handlerton *hton, THD *thd, const char *db,
                           const char *path, const char *wild, bool dir,
                           List<LEX_STRING> *files) {
  DBUG_ENTER("tera_find_files");
  DBUG_RETURN(1);
}

static int tera_table_exists_in_engine(handlerton *hton, THD* thd, const char *db,
                                       const char *name) {
  DBUG_ENTER("tera_table_exists_in_engine");
  DBUG_RETURN(1);
}
*/

Tera_share::Tera_share()
{
  thr_lock_init(&lock);
  table = NULL;
}

/**
  @brief
  Example of simple lock controls. The "share" it creates is a
  structure we will pass to each example handler. Do you have to have
  one of these? Well, you have pieces that are used for locking, and
  they are needed to function.
*/

Tera_share *ha_tera::get_share(const char* name)
{
  Tera_share *tmp_share= NULL;
  DBUG_ENTER("ha_tera::get_share()");

  char dbname[FN_HEADLEN];
  char tabname[FN_HEADLEN];
  char tera_tabname[FN_HEADLEN * 2];
  ha_tera_util::path_to_dbname(name, dbname);
  ha_tera_util::path_to_tabname(name, tabname);
  ha_tera_util::name_to_tera_tabname(dbname, tabname, tera_tabname);
  DBUG_PRINT("info", ("name: %s table: %s %s", name, dbname, tabname));

  mysql_mutex_lock(&tera_mutex);
  if (tera_open_tables.find(tera_tabname) != tera_open_tables.end())
  {
    tmp_share = tera_open_tables[tera_tabname];
  }
  else
  {
    tera::ErrorCode ec;
    tera::TableDescriptor table_desc(tera_tabname);
    table_desc.SetRawKey(tera::kGeneralKv);
    table_desc.AddLocalityGroup("default");
    if (!tera_client->CreateTable(table_desc, &ec))
    {
      // goto err;
    }

    tera::Table* tera_table = tera_client->OpenTable(tera_tabname, &ec);
    if (tera_table == NULL)
    {
      goto err;
    }

    tmp_share= new Tera_share;
    tmp_share->table = tera_table;
    tera_open_tables[tera_tabname] = tmp_share;

    lock_shared_ha_data();
    set_ha_share_ptr(static_cast<Handler_share*>(tmp_share));
    unlock_shared_ha_data();
  }

err:
  mysql_mutex_unlock(&tera_mutex);
  DBUG_RETURN(tmp_share);
}

ha_tera::ha_tera(handlerton *hton, TABLE_SHARE *table_arg)
  :handler(hton, table_arg),
   share_(NULL),
   result_stream_(NULL),
   field_buf_(new uchar[64 << 20])
{
  DBUG_ENTER("ha_tera::ha_tera");
  DBUG_PRINT("info", ("ha_tera handler %p", this));
  DBUG_VOID_RETURN;
}

ha_tera::~ha_tera()
{
  DBUG_ENTER("ha_tera::~ha_tera");
  DBUG_PRINT("info", ("ha_tera handler %p", this));
  delete[] field_buf_;
  delete result_stream_;
  DBUG_VOID_RETURN;
}

/**
  @brief
  If frm_error() is called then we will use this to determine
  the file extensions that exist for the storage engine. This is also
  used by the default rename_table and delete_table method in
  handler.cc.

  For engines that have two file name extentions (separate meta/index file
  and data file), the order of elements is relevant. First element of engine
  file name extentions array should be meta/index file extention. Second
  element - data file extention. This order is assumed by
  prepare_for_repair() when REPAIR TABLE ... USE_FRM is issued.

  @see
  rename_table method in handler.cc and
  delete_table method in handler.cc
*/

static const char *ha_tera_exts[] = {
  NullS
};

const char **ha_tera::bas_ext() const
{
  return ha_tera_exts;
}

/*
  Following handler function provides access to
  system database specific to SE. This interface
  is optional, so every SE need not implement it.
*/
const char* ha_tera_system_database= NULL;
const char* tera_system_database()
{
  return ha_tera_system_database;
}

/*
  List of all system tables specific to the SE.
  Array element would look like below,
     { "<database_name>", "<system table name>" },
  The last element MUST be,
     { (const char*)NULL, (const char*)NULL }

  This array is optional, so every SE need not implement it.
*/
static st_system_tablename ha_tera_system_tables[]= {
  {"meta_table", ""},
  {"stat_table", ""},
  {"user_table", ""},
  {(const char*)NULL, (const char*)NULL}
};

/**
  @brief Check if the given db.tablename is a system table for this SE.

  @param db                         Database name to check.
  @param table_name                 table name to check.
  @param is_sql_layer_system_table  if the supplied db.table_name is a SQL
                                    layer system table.

  @return
    @retval TRUE   Given db.table_name is supported system table.
    @retval FALSE  Given db.table_name is not a supported system table.
*/
static bool tera_is_supported_system_table(const char *db,
                                           const char *table_name,
                                           bool is_sql_layer_system_table)
{
  st_system_tablename *systab;

  // Does this SE support "ALL" SQL layer system tables ?
  if (is_sql_layer_system_table)
    return false;

  // Check if this is SE layer system tables
  systab= ha_tera_system_tables;
  while (systab && systab->db)
  {
    if (strcmp(systab->db, db) == 0 &&
        strcmp(systab->tablename, table_name) == 0)
      return true;
    systab++;
  }

  return false;
}


/**
  @brief
  Used for opening tables. The name will be the name of the file.

  @details
  A table is opened when it needs to be opened; e.g. when a request comes in
  for a SELECT on the table (tables are not open and closed for each request,
  they are cached).

  Called from handler.cc by handler::ha_open(). The server opens all tables by
  calling ha_open() which then calls the handler specific open().

  @see
  handler::ha_open() in handler.cc
*/

int ha_tera::open(const char *name, int mode, uint test_if_locked)
{
  DBUG_ENTER("ha_tera::open");
  DBUG_PRINT("info", ("ha_tera handler %p", this));
  if (!(share_ = get_share(name)))
    DBUG_RETURN(1);
  thr_lock_data_init(&share_->lock,&lock_,NULL);

  DBUG_RETURN(0);
}


/**
  @brief
  Closes a table.

  @details
  Called from sql_base.cc, sql_select.cc, and table.cc. In sql_select.cc it is
  only used to close up temporary tables or during the process where a
  temporary table is converted over to being a myisam table.

  For sql_base.cc look at close_data_tables().

  @see
  sql_base.cc, sql_select.cc and table.cc
*/

int ha_tera::close(void)
{
  DBUG_ENTER("ha_tera::close");
  delete result_stream_;
  result_stream_ = NULL;
  last_key_.clear();
  DBUG_RETURN(0);
}


/**
  @brief
  write_row() inserts a row. No extra() hint is given currently if a bulk load
  is happening. buf() is a byte array of data. You can use the field
  information to extract the data from the native byte array type.

  @details
  Example of this would be:
  @code
  for (Field **field=table->field ; *field ; field++)
  {
    ...
  }
  @endcode

  See ha_tina.cc for an example of extracting all of the data as strings.
  ha_berekly.cc has an example of how to store it intact by "packing" it
  for ha_berkeley's own native storage type.

  See the note for update_row() on auto_increments. This case also applies to
  write_row().

  Called from item_sum.cc, item_sum.cc, sql_acl.cc, sql_insert.cc,
  sql_insert.cc, sql_select.cc, sql_table.cc, sql_udf.cc, and sql_update.cc.

  @see
  item_sum.cc, item_sum.cc, sql_acl.cc, sql_insert.cc,
  sql_insert.cc, sql_select.cc, sql_table.cc, sql_udf.cc and sql_update.cc
*/

int ha_tera::write_row(uchar *buf)
{
  DBUG_ENTER("ha_tera::write_row");
  DBUG_PRINT("info", ("ha_tera handler %p", this));
  std::string key, value;
  mysql_buf_to_primary_data(buf, &key, &value);
  if (key == "")
  {
    DBUG_RETURN(1);
  }

  tera::RowMutation* mu = share_->table->NewRowMutation(key);
  mu->Put(value);
  share_->table->ApplyMutation(mu);
  tera::ErrorCode ec = mu->GetError();
  delete mu;

  if (ec.GetType() != tera::ErrorCode::kOK)
  {
    DBUG_RETURN(2);
  }
  DBUG_RETURN(0);
}

/**
  @brief
  Yes, update_row() does what you expect, it updates a row. old_data will have
  the previous row record in it, while new_data will have the newest data in it.
  Keep in mind that the server can do updates based on ordering if an ORDER BY
  clause was used. Consecutive ordering is not guaranteed.

  @details
  Currently new_data will not have an updated auto_increament record. You can
  do this for example by doing:

  @code

  if (table->next_number_field && record == table->record[0])
    update_auto_increment();

  @endcode

  Called from sql_select.cc, sql_acl.cc, sql_update.cc, and sql_insert.cc.

  @see
  sql_select.cc, sql_acl.cc, sql_update.cc and sql_insert.cc
*/
int ha_tera::update_row(const uchar *old_data, uchar *new_data)
{

  DBUG_ENTER("ha_tera::update_row");
  int r = write_row(new_data);
  DBUG_RETURN(r);
}


/**
  @brief
  This will delete a row. buf will contain a copy of the row to be deleted.
  The server will call this right after the current row has been called (from
  either a previous rnd_nexT() or index call).

  @details
  If you keep a pointer to the last row or can access a primary key it will
  make doing the deletion quite a bit easier. Keep in mind that the server does
  not guarantee consecutive deletions. ORDER BY clauses can be used.

  Called in sql_acl.cc and sql_udf.cc to manage internal table
  information.  Called in sql_delete.cc, sql_insert.cc, and
  sql_select.cc. In sql_select it is used for removing duplicates
  while in insert it is used for REPLACE calls.

  @see
  sql_acl.cc, sql_udf.cc, sql_delete.cc, sql_insert.cc and sql_select.cc
*/

int ha_tera::delete_row(const uchar *buf)
{
  DBUG_ENTER("ha_tera::delete_row");
  tera::RowMutation* mu = share_->table->NewRowMutation(last_key_);
  mu->DeleteRow();
  share_->table->ApplyMutation(mu);
  tera::ErrorCode ec = mu->GetError();
  delete mu;

  if (ec.GetType() != tera::ErrorCode::kOK)
  {
    DBUG_RETURN(1);
  }
  DBUG_RETURN(0);
}


/**
  @brief
  Positions an index cursor to the index specified in the handle. Fetches the
  row if available. If the key value is null, begin at the first key of the
  index.
*/

int ha_tera::index_read_map(uchar *buf, const uchar *key,
                               key_part_map keypart_map __attribute__((unused)),
                               enum ha_rkey_function find_flag
                               __attribute__((unused)))
{
  int rc;
  DBUG_ENTER("ha_tera::index_read");
  MYSQL_INDEX_READ_ROW_START(table_share->db.str, table_share->table_name.str);
  rc= HA_ERR_WRONG_COMMAND;
  MYSQL_INDEX_READ_ROW_DONE(rc);
  DBUG_RETURN(rc);
}


/**
  @brief
  Used to read forward through the index.
*/

int ha_tera::index_next(uchar *buf)
{
  int rc;
  DBUG_ENTER("ha_tera::index_next");
  MYSQL_INDEX_READ_ROW_START(table_share->db.str, table_share->table_name.str);
  rc= HA_ERR_WRONG_COMMAND;
  MYSQL_INDEX_READ_ROW_DONE(rc);
  DBUG_RETURN(rc);
}


/**
  @brief
  Used to read backwards through the index.
*/

int ha_tera::index_prev(uchar *buf)
{
  int rc;
  DBUG_ENTER("ha_tera::index_prev");
  MYSQL_INDEX_READ_ROW_START(table_share->db.str, table_share->table_name.str);
  rc= HA_ERR_WRONG_COMMAND;
  MYSQL_INDEX_READ_ROW_DONE(rc);
  DBUG_RETURN(rc);
}


/**
  @brief
  index_first() asks for the first key in the index.

  @details
  Called from opt_range.cc, opt_sum.cc, sql_handler.cc, and sql_select.cc.

  @see
  opt_range.cc, opt_sum.cc, sql_handler.cc and sql_select.cc
*/
int ha_tera::index_first(uchar *buf)
{
  int rc;
  DBUG_ENTER("ha_tera::index_first");
  MYSQL_INDEX_READ_ROW_START(table_share->db.str, table_share->table_name.str);
  rc= HA_ERR_WRONG_COMMAND;
  MYSQL_INDEX_READ_ROW_DONE(rc);
  DBUG_RETURN(rc);
}


/**
  @brief
  index_last() asks for the last key in the index.

  @details
  Called from opt_range.cc, opt_sum.cc, sql_handler.cc, and sql_select.cc.

  @see
  opt_range.cc, opt_sum.cc, sql_handler.cc and sql_select.cc
*/
int ha_tera::index_last(uchar *buf)
{
  int rc;
  DBUG_ENTER("ha_tera::index_last");
  MYSQL_INDEX_READ_ROW_START(table_share->db.str, table_share->table_name.str);
  rc= HA_ERR_WRONG_COMMAND;
  MYSQL_INDEX_READ_ROW_DONE(rc);
  DBUG_RETURN(rc);
}


/**
  @brief
  rnd_init() is called when the system wants the storage engine to do a table
  scan. See the example in the introduction at the top of this file to see when
  rnd_init() is called.

  @details
  Called from filesort.cc, records.cc, sql_handler.cc, sql_select.cc, sql_table.cc,
  and sql_update.cc.

  @see
  filesort.cc, records.cc, sql_handler.cc, sql_select.cc, sql_table.cc and sql_update.cc
*/
int ha_tera::rnd_init(bool scan)
{
  DBUG_ENTER("ha_tera::rnd_init");
  DBUG_PRINT("info", ("ha_tera handler %p", this));
  delete result_stream_;

  tera::ErrorCode ec;
  tera::ScanDescriptor scan_desc("");
  result_stream_ = share_->table->Scan(scan_desc, &ec);
  if (result_stream_ == NULL)
  {
    DBUG_RETURN(1);
  }
  DBUG_RETURN(0);
}

int ha_tera::rnd_end()
{
  DBUG_ENTER("ha_tera::rnd_end");
  DBUG_PRINT("info", ("ha_tera handler %p", this));
  DBUG_RETURN(0);
}


/**
  @brief
  This is called for each row of the table scan. When you run out of records
  you should return HA_ERR_END_OF_FILE. Fill buff up with the row information.
  The Field structure for the table is the key to getting data into buf
  in a manner that will allow the server to understand it.

  @details
  Called from filesort.cc, records.cc, sql_handler.cc, sql_select.cc, sql_table.cc,
  and sql_update.cc.

  @see
  filesort.cc, records.cc, sql_handler.cc, sql_select.cc, sql_table.cc and sql_update.cc
*/
int ha_tera::rnd_next(uchar *buf)
{
  int rc = 0;
  DBUG_ENTER("ha_tera::rnd_next");
  DBUG_PRINT("info", ("ha_tera handler %p", this));
  // MYSQL_READ_ROW_START(table_share->db.str, table_share->table_name.str, TRUE);

  std::string key, value;
  for (; !result_stream_->Done(); result_stream_->Next())
  {
    const std::string& key = result_stream_->RowName();
    const std::string& value = result_stream_->Value();
    if (key == last_key_)
    {
      continue;
    }
    if (result_stream_->Family() != "" || result_stream_->Qualifier() != "")
    {
      continue;
    }
    if (primary_data_to_mysql_buf(key, value, buf) != 0)
    {
      continue;
    }
    break;
  }
  if (result_stream_->Done())
  {
    rc = HA_ERR_END_OF_FILE;
  }
  else
  {
    last_key_ = key;
    result_stream_->Next();
  }

  // MYSQL_READ_ROW_DONE(rc);
  DBUG_RETURN(rc);
}


/**
  @brief
  position() is called after each call to rnd_next() if the data needs
  to be ordered. You can do something like the following to store
  the position:
  @code
  my_store_ptr(ref, ref_length, current_position);
  @endcode

  @details
  The server uses ref to store data. ref_length in the above case is
  the size needed to store current_position. ref is just a byte array
  that the server will maintain. If you are using offsets to mark rows, then
  current_position should be the offset. If it is a primary key like in
  BDB, then it needs to be a primary key.

  Called from filesort.cc, sql_select.cc, sql_delete.cc, and sql_update.cc.

  @see
  filesort.cc, sql_select.cc, sql_delete.cc and sql_update.cc
*/
void ha_tera::position(const uchar *record)
{
  DBUG_ENTER("ha_tera::position");
  memcpy(ref, last_key_.c_str(), last_key_.size());
  ref_length = last_key_.size();
  DBUG_VOID_RETURN;
}


/**
  @brief
  This is like rnd_next, but you are given a position to use
  to determine the row. The position will be of the type that you stored in
  ref. You can use ha_get_ptr(pos,ref_length) to retrieve whatever key
  or position you saved when position() was called.

  @details
  Called from filesort.cc, records.cc, sql_insert.cc, sql_select.cc, and sql_update.cc.

  @see
  filesort.cc, records.cc, sql_insert.cc, sql_select.cc and sql_update.cc
*/
int ha_tera::rnd_pos(uchar *buf, uchar *pos)
{
  int rc = 0;
  DBUG_ENTER("ha_tera::rnd_pos");
  DBUG_PRINT("info", ("ha_tera handler %p", this));
  MYSQL_READ_ROW_START(table_share->db.str, table_share->table_name.str, TRUE);
  last_key_.assign((char*)pos, ref_length);
  // get_row(last_key, buf);
  rc = HA_ERR_WRONG_COMMAND;
  MYSQL_READ_ROW_DONE(rc);
  DBUG_RETURN(rc);
}


/**
  @brief
  ::info() is used to return information to the optimizer. See my_base.h for
  the complete description.

  @details
  Currently this table handler doesn't implement most of the fields really needed.
  SHOW also makes use of this data.

  You will probably want to have the following in your code:
  @code
  if (records < 2)
    records = 2;
  @endcode
  The reason is that the server will optimize for cases of only a single
  record. If, in a table scan, you don't know the number of records, it
  will probably be better to set records to two so you can return as many
  records as you need. Along with records, a few more variables you may wish
  to set are:
    records
    deleted
    data_file_length
    index_file_length
    delete_length
    check_time
  Take a look at the public variables in handler.h for more information.

  Called in filesort.cc, ha_heap.cc, item_sum.cc, opt_sum.cc, sql_delete.cc,
  sql_delete.cc, sql_derived.cc, sql_select.cc, sql_select.cc, sql_select.cc,
  sql_select.cc, sql_select.cc, sql_show.cc, sql_show.cc, sql_show.cc, sql_show.cc,
  sql_table.cc, sql_union.cc, and sql_update.cc.

  @see
  filesort.cc, ha_heap.cc, item_sum.cc, opt_sum.cc, sql_delete.cc, sql_delete.cc,
  sql_derived.cc, sql_select.cc, sql_select.cc, sql_select.cc, sql_select.cc,
  sql_select.cc, sql_show.cc, sql_show.cc, sql_show.cc, sql_show.cc, sql_table.cc,
  sql_union.cc and sql_update.cc
*/
int ha_tera::info(uint flag)
{
  DBUG_ENTER("ha_tera::info");
  DBUG_PRINT("info", ("ha_tera handler %p", this));
  DBUG_RETURN(0);
}


/**
  @brief
  extra() is called whenever the server wishes to send a hint to
  the storage engine. The myisam engine implements the most hints.
  ha_innodb.cc has the most exhaustive list of these hints.

    @see
  ha_innodb.cc
*/
int ha_tera::extra(enum ha_extra_function operation)
{
  DBUG_ENTER("ha_tera::extra");
  DBUG_PRINT("info", ("ha_tera handler %p", this));
  DBUG_RETURN(0);
}


/**
  @brief
  Used to delete all rows in a table, including cases of truncate and cases where
  the optimizer realizes that all rows will be removed as a result of an SQL statement.

  @details
  Called from item_sum.cc by Item_func_group_concat::clear(),
  Item_sum_count_distinct::clear(), and Item_func_group_concat::clear().
  Called from sql_delete.cc by mysql_delete().
  Called from sql_select.cc by JOIN::reinit().
  Called from sql_union.cc by st_select_lex_unit::exec().

  @see
  Item_func_group_concat::clear(), Item_sum_count_distinct::clear() and
  Item_func_group_concat::clear() in item_sum.cc;
  mysql_delete() in sql_delete.cc;
  JOIN::reinit() in sql_select.cc and
  st_select_lex_unit::exec() in sql_union.cc.
*/
int ha_tera::delete_all_rows()
{
  DBUG_ENTER("ha_tera::delete_all_rows");
  DBUG_RETURN(HA_ERR_WRONG_COMMAND);
}


/**
  @brief
  Used for handler specific truncate table.  The table is locked in
  exclusive mode and handler is responsible for reseting the auto-
  increment counter.

  @details
  Called from Truncate_statement::handler_truncate.
  Not used if the handlerton supports HTON_CAN_RECREATE, unless this
  engine can be used as a partition. In this case, it is invoked when
  a particular partition is to be truncated.

  @see
  Truncate_statement in sql_truncate.cc
  Remarks in handler::truncate.
*/
int ha_tera::truncate()
{
  DBUG_ENTER("ha_tera::truncate");
  DBUG_RETURN(HA_ERR_WRONG_COMMAND);
}


/**
  @brief
  This create a lock on the table. If you are implementing a storage engine
  that can handle transacations look at ha_berkely.cc to see how you will
  want to go about doing this. Otherwise you should consider calling flock()
  here. Hint: Read the section "locking functions for mysql" in lock.cc to understand
  this.

  @details
  Called from lock.cc by lock_external() and unlock_external(). Also called
  from sql_table.cc by copy_data_between_tables().

  @see
  lock.cc by lock_external() and unlock_external() in lock.cc;
  the section "locking functions for mysql" in lock.cc;
  copy_data_between_tables() in sql_table.cc.
*/
int ha_tera::external_lock(THD *thd, int lock_type)
{
  DBUG_ENTER("ha_tera::external_lock");
  DBUG_PRINT("info", ("ha_tera handler %p", this));
  DBUG_RETURN(0);
}


/**
  @brief
  The idea with handler::store_lock() is: The statement decides which locks
  should be needed for the table. For updates/deletes/inserts we get WRITE
  locks, for SELECT... we get read locks.

  @details
  Before adding the lock into the table lock handler (see thr_lock.c),
  mysqld calls store lock with the requested locks. Store lock can now
  modify a write lock to a read lock (or some other lock), ignore the
  lock (if we don't want to use MySQL table locks at all), or add locks
  for many tables (like we do when we are using a MERGE handler).

  Berkeley DB, for example, changes all WRITE locks to TL_WRITE_ALLOW_WRITE
  (which signals that we are doing WRITES, but are still allowing other
  readers and writers).

  When releasing locks, store_lock() is also called. In this case one
  usually doesn't have to do anything.

  In some exceptional cases MySQL may send a request for a TL_IGNORE;
  This means that we are requesting the same lock as last time and this
  should also be ignored. (This may happen when someone does a flush
  table when we have opened a part of the tables, in which case mysqld
  closes and reopens the tables and tries to get the same locks at last
  time). In the future we will probably try to remove this.

  Called from lock.cc by get_lock_data().

  @note
  In this method one should NEVER rely on table->in_use, it may, in fact,
  refer to a different thread! (this happens if get_lock_data() is called
  from mysql_lock_abort_for_thread() function)

  @see
  get_lock_data() in lock.cc
*/
THR_LOCK_DATA **ha_tera::store_lock(THD *thd,
                                       THR_LOCK_DATA **to,
                                       enum thr_lock_type lock_type)
{
  if (lock_type != TL_IGNORE && lock_.type == TL_UNLOCK)
    lock_.type=lock_type;
  *to++= &lock_;
  return to;
}


/**
  @brief
  Used to delete a table. By the time delete_table() has been called all
  opened references to this table will have been closed (and your globally
  shared references released). The variable name will just be the name of
  the table. You will need to remove any files you have created at this point.

  @details
  If you do not implement this, the default delete_table() is called from
  handler.cc and it will delete all files with the file extensions returned
  by bas_ext().

  Called from handler.cc by delete_table and ha_create_table(). Only used
  during create if the table_flag HA_DROP_BEFORE_CREATE was specified for
  the storage engine.

  @see
  delete_table and ha_create_table() in handler.cc
*/
int ha_tera::delete_table(const char *name)
{
  DBUG_ENTER("ha_tera::delete_table");

  tera::ErrorCode ec;
  if (!tera_client->DisableTable(name, &ec))
  {
    DBUG_RETURN(-1);
  }
  sleep(1);
  if (!tera_client->DeleteTable(name, &ec))
  {
    DBUG_RETURN(-1);
  }
  DBUG_RETURN(0);
}


/**
  @brief
  Renames a table from one name to another via an alter table call.

  @details
  If you do not implement this, the default rename_table() is called from
  handler.cc and it will delete all files with the file extensions returned
  by bas_ext().

  Called from sql_table.cc by mysql_rename_table().

  @see
  mysql_rename_table() in sql_table.cc
*/
int ha_tera::rename_table(const char * from, const char * to)
{
  DBUG_ENTER("ha_tera::rename_table ");
  DBUG_RETURN(HA_ERR_WRONG_COMMAND);
}


/**
  @brief
  Given a starting key and an ending key, estimate the number of rows that
  will exist between the two keys.

  @details
  end_key may be empty, in which case determine if start_key matches any rows.

  Called from opt_range.cc by check_quick_keys().

  @see
  check_quick_keys() in opt_range.cc
*/
ha_rows ha_tera::records_in_range(uint inx, key_range *min_key,
                                     key_range *max_key)
{
  DBUG_ENTER("ha_tera::records_in_range");
  DBUG_RETURN(10);                         // low number to force index usage
}


/**
  @brief
  create() is called to create a database. The variable name will have the name
  of the table.

  @details
  When create() is called you do not need to worry about
  opening the table. Also, the .frm file will have already been
  created so adjusting create_info is not necessary. You can overwrite
  the .frm file at this point if you wish to change the table
  definition, but there are no methods currently provided for doing
  so.

  Called from handle.cc by ha_create_table().

  @see
  ha_create_table() in handle.cc
*/

int ha_tera::create(const char *name, TABLE *table_arg,
                    HA_CREATE_INFO *create_info)
{
  DBUG_ENTER("ha_tera::create");
  DBUG_PRINT("info", ("ha_tera handler %p", this));
  char dbname[FN_HEADLEN];
  //char m_schemaname[FN_HEADLEN];
  char tabname[FN_HEADLEN];
  char tera_tabname[FN_HEADLEN * 2];

  bool create_from_tera = (create_info->table_options & HA_OPTION_CREATE_FROM_ENGINE);
  ha_tera_util::path_to_dbname(name, dbname);
  ha_tera_util::path_to_tabname(name, tabname);
  ha_tera_util::name_to_tera_tabname(dbname, tabname, tera_tabname);
  DBUG_PRINT("info", ("create table: %s %s, from tera: %d", dbname, tabname, create_from_tera));

  if (create_from_tera)
  {
    DBUG_RETURN(0);
  }

  // Save frm data for this table
  uchar* data = NULL;
  uchar* pack_data = NULL;
  size_t pack_length = 0, length = 0;
  if (readfrm(name, &data, &length))
  {
    DBUG_RETURN(1);
  }
  if (packfrm(data, length, &pack_data, &pack_length))
  {
    my_free((char*)data);
    DBUG_RETURN(2);
  }
  DBUG_PRINT("info", ("setFrm table: %s %s  data: 0x%lx  len: %lu", dbname, tabname,
                      (long)pack_data, (ulong)pack_length));
  std::string frm_data((char*)pack_data, pack_length);
  my_free((char*)data);
  my_free((char*)pack_data);

  tera::ErrorCode ec;
  tera::TableDescriptor table_desc(tera_tabname);
  table_desc.SetRawKey(tera::kGeneralKv);
  table_desc.AddLocalityGroup("default");
  if (!tera_client->CreateTable(table_desc, &ec))
  {
    DBUG_PRINT("TERA", ("create table %s %s on tera fail: %s", dbname, tabname,
                        ec.GetReason().c_str()));
    DBUG_RETURN(3);
  }

  std::string frm_flag(8, '\0');
  uint32 frm_version = 1;
  memcpy((char*)frm_flag.data(), &tera_frm_magic_num, 4);
  memcpy((char*)frm_flag.data() + 4, &frm_version, 4);
  tera::RowMutation* frm_mu = tera_frm_table->NewRowMutation(tera_tabname);
  frm_mu->Put(tera_frm_data_column, "", frm_data);
  frm_mu->Put(tera_frm_flag_column, "", frm_flag);
  tera_frm_table->ApplyMutation(frm_mu);
  ec = frm_mu->GetError();
  delete frm_mu;

  if (ec.GetType() != tera::ErrorCode::kOK)
  {
    DBUG_PRINT("TERA", ("put table %s %s frm into tera fail: %s", dbname, tabname,
                        ec.GetReason().c_str()));
    DBUG_RETURN(4);
  }

  mysql_mutex_lock(&tera_mutex);
  all_table_version[dbname][tabname] = frm_version;
  mysql_mutex_unlock(&tera_mutex);

  DBUG_PRINT("info", ("create table %s %s on tera success", dbname, tabname));
  DBUG_RETURN(0);
}

void ha_tera::mysql_buf_to_primary_data(const uchar* buf,
                                        std::string* key,
                                        std::string* value)
{
  DBUG_ENTER("ha_tera::mysql_buf_to_primary_data");
  std::cerr << "ha_tera::mysql_buf_to_primary_data" << std::endl;
  for(Field **field = table->field; *field; field++)
  {
    uint32_t field_len = (*field)->pack_length();
    if((*field)->type() == MYSQL_TYPE_VARCHAR)  //对变长数据的特殊处理
    {
      field_len = (*field)->data_length();
    }
    if((*field)->type() == MYSQL_TYPE_BLOB)  // process blog type
    {
      field_len = ((Field_blob *)(*field))->get_length();
    }

    uchar* data_ptr = (*field)->ptr;
    if((*field)->type() == MYSQL_TYPE_VARCHAR)
    {
      int offset = (*field)->pack_length() - (*field)->field_length;
      if (offset > 0)
      {
        data_ptr += offset;
      }
    }
    if((*field)->type() == MYSQL_TYPE_BLOB)
    {
      ((Field_blob *)(*field))->get_ptr(&data_ptr);
    }

    (*value) += std::string((char*)&field_len, sizeof(field_len));
    (*value) += std::string((char*)data_ptr, field_len);

    // if((*field)->key_start.to_ulonglong() == 1)
    if (*key == "")
    {
      // int key_len = table->key_info->key_length;
      // key->assign((char*)((*field)->ptr), key_len);
      key->assign((char*)data_ptr, field_len);
    }
  }

  std::cerr << "key: " << *key << " value: " << *value << std::endl;
  DBUG_VOID_RETURN;
}

int ha_tera::primary_data_to_mysql_buf(const std::string& key,
                                       const std::string& value,
                                       uchar* buf)
{
  DBUG_ENTER("ha_tera::primary_data_to_mysql_buf");
  std::cerr << "ha_tera::primary_data_to_mysql_buf" << std::endl;
  std::cerr << "key: " << key << " value: " << value << std::endl;

  uchar* blob_ptr = field_buf_;
  char* field_ptr = (char*)value.data();
  char* end_ptr = field_ptr + value.size();
  for(Field **field = table->field; *field; field++)
  {
    std::cerr << "1" << std::endl;
    if (end_ptr - field_ptr < (long long)sizeof(uint32_t))
    {
      DBUG_RETURN(-1);
    }
    std::cerr << "2" << std::endl;
    uint32_t field_len = uint4korr(field_ptr);
    field_ptr += sizeof(uint32_t);

    if (field_len == 0)
    {
      (*field)->set_null();
      continue;
    }
    std::cerr << "3" << std::endl;
    if (end_ptr - field_ptr < field_len)
    {
      DBUG_RETURN(-1);
    }
    std::cerr << "4" << std::endl;
    (*field)->set_notnull();
    int offset = 0;
    if((*field)->type()  == MYSQL_TYPE_VARCHAR)
    {
      offset = (*field)->pack_length() - (*field)->field_length;
      DBUG_ASSERT(offset <= 2 && offset >= 1);
      DBUG_ASSERT(field_len < 65535);
      if (offset == 1 )
      {
        *((*field)->ptr) = (uchar)field_len;
      }
      else
      {
        int2store((*field)->ptr, field_len);
      }
    }
    if((*field)->type() == MYSQL_TYPE_BLOB)
    {
      memcpy(blob_ptr, field_ptr, field_len);
      ((Field_blob *)(*field))->set_ptr(field_len, blob_ptr);
      blob_ptr += field_len;
    }
    else
    {
      memcpy((*field)->ptr + offset, field_ptr, field_len);
    }
    field_ptr += field_len;
    std::cerr << "5" << std::endl;
  }

  DBUG_RETURN(0);
}

struct st_mysql_storage_engine tera_storage_engine=
{ MYSQL_HANDLERTON_INTERFACE_VERSION };

static ulong srv_enum_var= 0;
static ulong srv_ulong_var= 0;
static double srv_double_var= 0;

const char *enum_var_names[]=
{
  "e1", "e2", NullS
};

TYPELIB enum_var_typelib=
{
  array_elements(enum_var_names) - 1, "enum_var_typelib",
  enum_var_names, NULL
};

static MYSQL_SYSVAR_ENUM(
  enum_var,                       // name
  srv_enum_var,                   // varname
  PLUGIN_VAR_RQCMDARG,            // opt
  "Sample ENUM system variable.", // comment
  NULL,                           // check
  NULL,                           // update
  0,                              // def
  &enum_var_typelib);             // typelib

static MYSQL_SYSVAR_ULONG(
  ulong_var,
  srv_ulong_var,
  PLUGIN_VAR_RQCMDARG,
  "0..1000",
  NULL,
  NULL,
  8,
  0,
  1000,
  0);

static MYSQL_SYSVAR_DOUBLE(
  double_var,
  srv_double_var,
  PLUGIN_VAR_RQCMDARG,
  "0.500000..1000.500000",
  NULL,
  NULL,
  8.5,
  0.5,
  1000.5,
  0);                             // reserved always 0

static MYSQL_THDVAR_DOUBLE(
  double_thdvar,
  PLUGIN_VAR_RQCMDARG,
  "0.500000..1000.500000",
  NULL,
  NULL,
  8.5,
  0.5,
  1000.5,
  0);

static struct st_mysql_sys_var* tera_system_variables[]= {
  MYSQL_SYSVAR(enum_var),
  MYSQL_SYSVAR(ulong_var),
  MYSQL_SYSVAR(double_var),
  MYSQL_SYSVAR(double_thdvar),
  NULL
};

// this is an example of SHOW_FUNC and of my_snprintf() service
static int show_func_tera(MYSQL_THD thd, struct st_mysql_show_var *var,
                             char *buf)
{
  var->type= SHOW_CHAR;
  var->value= buf; // it's of SHOW_VAR_FUNC_BUFF_SIZE bytes
  my_snprintf(buf, SHOW_VAR_FUNC_BUFF_SIZE,
              "enum_var is %lu, ulong_var is %lu, "
              "double_var is %f, %.6b", // %b is a MySQL extension
              srv_enum_var, srv_ulong_var, srv_double_var, "really");
  return 0;
}

struct tera_vars_t
{
  ulong  var1;
  double var2;
  char   var3[64];
  bool   var4;
  bool   var5;
  ulong  var6;
};

tera_vars_t tera_vars= {100, 20.01, "three hundred", true, 0, 8250};

static st_mysql_show_var show_status_tera[]=
{
  {"var1", (char *)&tera_vars.var1, SHOW_LONG},
  {"var2", (char *)&tera_vars.var2, SHOW_DOUBLE},
  {0,0,SHOW_UNDEF} // null terminator required
};

static struct st_mysql_show_var show_array_tera[]=
{
  {"array", (char *)show_status_tera, SHOW_ARRAY},
  {"var3", (char *)&tera_vars.var3, SHOW_CHAR},
  {"var4", (char *)&tera_vars.var4, SHOW_BOOL},
  {0,0,SHOW_UNDEF}
};

static struct st_mysql_show_var func_status[]=
{
  {"tera_func_tera", (char *)show_func_tera, SHOW_FUNC},
  {"tera_status_var5", (char *)&tera_vars.var5, SHOW_BOOL},
  {"tera_status_var6", (char *)&tera_vars.var6, SHOW_LONG},
  {"tera_status",  (char *)show_array_tera, SHOW_ARRAY},
  {0,0,SHOW_UNDEF}
};

mysql_declare_plugin(tera)
{
  MYSQL_STORAGE_ENGINE_PLUGIN,
  &tera_storage_engine,
  "TERA",
  "Baidu Inc",
  "Tera storage engine",
  PLUGIN_LICENSE_GPL,
  tera_init,                            /* Plugin Init */
  NULL,                                         /* Plugin Deinit */
  0x0001 /* 0.1 */,
  func_status,                                  /* status variables */
  tera_system_variables,                     /* system variables */
  NULL,                                         /* config options */
  0,                                            /* flags */
}
mysql_declare_plugin_end;
