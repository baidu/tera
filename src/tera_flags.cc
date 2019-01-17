// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "common/base/stdint.h"
#include "gflags/gflags.h"

/////////  stat table   //////////
DEFINE_bool(tera_stat_table_enabled, true, "use stat_table record history or not");
DEFINE_int64(tera_stat_table_ttl, 8000000, "default ttl for stat table (s / 100d).");
DEFINE_int64(tera_stat_table_splitsize, 100, "(MB) default split size of stat table");

/////////  common /////////
DEFINE_string(tera_role, "",
              "the role of tera running binary, should be one of (master | tabletnode)");
DEFINE_string(tera_user_identity, "", "the identity of tera user");
DEFINE_string(tera_user_passcode, "", "the passcode of tera user");
DEFINE_bool(tera_acl_enabled, false, "enable access control");
DEFINE_bool(tera_only_root_create_table, false, "only the root user can create table");

DEFINE_string(tera_working_dir, "./", "the base dir for system data");

DEFINE_string(tera_coord_type, "",
              "the coordinator service type for tera cluster "
              "[zk,ins,mock_zk,mock_ins,fake_zk]");

DEFINE_bool(tera_zk_enabled, true,
            "[obsoleted replace by --tera_coord_type=zk] enable zk adapter to coord");
DEFINE_bool(tera_mock_zk_enabled, false,
            "[obsoleted replace by --tera_coord_type=mock_zk] enable mock zk "
            "adapter to coord");
DEFINE_string(tera_zk_addr_list, "localhost:2180", "zookeeper server list");
DEFINE_string(tera_zk_root_path, "/tera", "zookeeper root path");
DEFINE_string(tera_fake_zk_path_prefix, "../fakezk", "fake zk path prefix in onebox tera");
DEFINE_int32(tera_zk_timeout, 10000, "(ms) zookeeper session timeout");
DEFINE_int64(tera_zk_retry_period, 3000, "zookeeper operation retry period (in ms)");
DEFINE_int32(tera_zk_retry_max_times, 10, "zookeeper operation max retry times");
DEFINE_string(tera_zk_lib_log_path, "../log/zk.log", "zookeeper library log output file");
DEFINE_string(tera_log_prefix, "", "prefix of log file (INFO, WARNING)");
DEFINE_string(tera_local_addr, "", "local host's ip address");
DEFINE_bool(tera_online_schema_update_enabled, false, "enable online-schema-update");
DEFINE_bool(tera_info_log_clean_enable, true, "enable log cleaner task, enable as default");
DEFINE_int64(tera_info_log_clean_period_second, 2592000,
             "time period (in second) for log cleaner task, 30 days as default");
DEFINE_int64(tera_info_log_expire_second, 2592000,
             "expire time (in second) of log file, 30 days as default");

DEFINE_string(tera_tabletnode_path_prefix, "../data/", "the path prefix for table storage");
DEFINE_int32(tera_tabletnode_scan_pack_max_size, 10240,
             "the max size(KB) of the package for scan rpc");

DEFINE_string(tera_auth_policy, "none", "none/ugi/giano");
DEFINE_string(tera_auth_name, "",
              "if tera_auth_policy == default, name should be user_name; "
              "otherwise group_name");
DEFINE_string(tera_auth_token, "",
              "if tera_auth_policy == default, token should be passwd; "
              "otherwise credential");

DEFINE_bool(tera_quota_enabled, false, "quota enable or not");
DEFINE_string(tera_quota_limiter_type, "general_quota_limiter",
              "quota_limiter for generic purpose");
DEFINE_int64(tera_quota_normal_estimate_value, 1024,
             "default estimate value per read/scan request is 1KB");
DEFINE_double(tera_quota_adjust_estimate_ratio, 0.9,
              "quota adjust estimate ratio for read and scan");

/////////  io  /////////
DEFINE_int64(tera_tablet_write_block_size, 4, "the block size (in KB) for teblet write block");
DEFINE_int64(tera_tablet_memtable_ldb_block_size, 4,
             "the block size (in KB) for memtable on leveldb");
DEFINE_int64(tera_tablet_ldb_sst_size, 8, "the sstable file size (in MB) on leveldb");
DEFINE_string(tera_leveldb_env_type, "dfs",
              "the default type for leveldb IO environment, should be [local | dfs]");
DEFINE_string(tera_leveldb_log_path, "../log/leveldb.log", "the default path for leveldb logger");
DEFINE_int32(leveldb_max_log_size_MB, 1024,
             "create a new log file if the file size is larger than this value ");
DEFINE_int32(leveldb_log_flush_trigger_size_B, 1048576,
             "trigger force flush log to disk by either leveldb_log_flush_trigger_size_B or "
             "leveldb_log_flush_trigger_interval_ms");
DEFINE_int32(leveldb_log_flush_trigger_interval_ms, 1000,
             "trigger force flush log to disk by either leveldb_log_flush_trigger_size_B or "
             "leveldb_log_flush_trigger_interval_ms");

DEFINE_int32(tera_rpc_client_max_inflow, -1,
             "the max input flow (in MB/s) for rpc-client, -1 means no limit");
DEFINE_int32(tera_rpc_client_max_outflow, -1,
             "the max input flow (in MB/s) for rpc-client, -1 means no limit");
DEFINE_int32(tera_rpc_timeout_period, 60000, "the timeout period (in ms) for rpc");

// those flags prefixed with "tera_master" are shared by several modules which
// cannot be move to
// master/master_flags, so they are kept here for compatibility until all flags
// are moved to their own dir
DEFINE_string(tera_master_meta_table_name, "meta_table", "the meta table name");
DEFINE_string(tera_master_meta_table_path, "meta", "the path of meta table");
DEFINE_int64(tera_master_split_tablet_size, 512, "the size (in MB) of tablet to trigger split");
DEFINE_int64(tera_master_merge_tablet_size, 0, "the size (in MB) of tablet to trigger merge");
DEFINE_int64(tera_master_gc_trash_expire_time_s, 86400,
             "time (in second) for gc file keeped in trash");
DEFINE_int64(tera_master_ins_session_timeout, 10000000, "ins session timeout(us), default 10sec");

/////////  http /////////
DEFINE_string(tera_http_port, "8657", "the http proxy port of tera");
DEFINE_int32(tera_http_request_thread_num, 30,
             "the http proxy thread num for handle client request");
DEFINE_int32(tera_http_ctrl_thread_num, 10, "the http proxy thread num for it self");

/////////  timeoracle /////////
DEFINE_string(tera_timeoracle_port, "30000", "the timeoracle port of tera");
DEFINE_int32(tera_timeoracle_max_lease_second, 30, "(s) timeoracle work this seconds for a lease");
DEFINE_int32(tera_timeoracle_refresh_lease_second, 10,
             "(s) timeoracle refresh lease before this seconds");

// only used by timeoracle
DEFINE_bool(tera_timeoracle_mock_enabled, false, "used local filesystem replace zk and ins.");
DEFINE_string(tera_timeoracle_mock_root_path, "/tmp/", "the root path of local filesystem.");
DEFINE_int32(tera_timeoracle_work_thread_num, 16, "timeoracle sofarpc server work_thread_number");
DEFINE_int32(tera_timeoracle_io_service_pool_size, 4,
             "timeoracle sofarpc server io_service_pool_size");

//////// observer ///////
DEFINE_int32(observer_proc_thread_num, 20, "");
DEFINE_int64(observer_max_pending_limit, 10000, "");
DEFINE_int32(observer_scanner_thread_num, 20, "");
DEFINE_int32(observer_read_thread_num, 20, "observer read thread num");
DEFINE_int32(observer_ack_conflict_timeout, 3600, "(ms) timeout for ack column conflict check");
DEFINE_int32(observer_rowlock_client_thread_num, 20, "rowlock client thread number");
DEFINE_int32(observer_random_access_thread_num, 20, "async read and write thread number");
DEFINE_int64(observer_update_table_info_period_s, 60,
             "the period of update table info for select key to observe");

//////// rowlock server ////////
DEFINE_bool(rowlock_rpc_limit_enabled, false, "enable the rpc traffic limit in sdk");
DEFINE_int32(rowlock_rpc_limit_max_inflow, 10,
             "the max bandwidth (in MB/s) for sdk rpc traffic limitation on input flow");
DEFINE_int32(rowlock_rpc_limit_max_outflow, 10,
             "the max bandwidth (in MB/s) for sdk rpc traffic limitation on "
             "output flow");
DEFINE_int32(rowlock_rpc_max_pending_buffer_size, 200,
             "max pending buffer size (in MB) for sdk rpc");
DEFINE_int32(rowlock_rpc_work_thread_num, 2, "thread num of sdk rpc client");

DEFINE_string(rowlock_server_ip, "0.0.0.0", "rowlock server ip");
DEFINE_string(rowlock_server_port, "22222", "rowlock server port");
DEFINE_string(rowlock_zk_root_path, "/rowlock", "");
DEFINE_int32(rowlock_zk_timeout, 10000, "(ms) zk timeout");
DEFINE_string(rowlock_ins_root_path, "/rowlock", "ins rowlock root path");
DEFINE_int32(rowlock_server_node_num, 1, "number of rowlock servers in cluster");

DEFINE_int32(rowlock_db_ttl, 600000, "(ms) timeout for an unlocked lock, 10min");
DEFINE_int32(rowlock_timing_wheel_patch_num, 600,
             "the number of timing wheel, every patch_num step the oldest data "
             "will be cleared");
DEFINE_int32(rowlock_db_sharding_number, 1024, "sharding number, enhance concurrency");
DEFINE_string(rowlock_fake_root_path, "../fakezk/rowlock", "one box fake zk root path");
DEFINE_int32(rowlock_thread_max_num, 20, "the max thread number of rowlock server");
DEFINE_int32(rowlock_client_max_fail_times, 5, "client max failure times");

DEFINE_bool(rowlock_proxy_async_enable, false, "sync | async");
DEFINE_string(rowlock_proxy_port, "22223", "rowlock proxy port");

DEFINE_int32(rowlock_io_service_pool_size, 4, "rowlock server sofarpc server io_service_pool_size");

DEFINE_bool(mock_rowlock_enable, false, "test case switch");

////////// PROFILER ///////////
