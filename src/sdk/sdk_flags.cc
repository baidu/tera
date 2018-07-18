// Copyright (c) 2018, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "common/base/stdint.h"
#include "gflags/gflags.h"

/////////  global transaction  ////////
DEFINE_bool(tera_sdk_client_for_gtxn, false, "build thread_pool for global transaction");
DEFINE_bool(tera_sdk_tso_client_enabled, false, "get timestamp from timeoracle, default from local timestamp");
DEFINE_int32(tera_gtxn_thread_max_num, 20, "the max thread number for global transaction operations");
DEFINE_int32(tera_gtxn_commit_timeout_ms, 600000, "global transaction timeout limit (ms) default 10 minutes");
DEFINE_int32(tera_gtxn_get_waited_times_limit, 10, "global txn wait other locked times limit");
DEFINE_int32(tera_gtxn_all_puts_size_limit, 10000, "(B) global txn all puts data size limit");
DEFINE_int32(tera_gtxn_timeout_ms, 86400000, "global transaction timeout limit (ms) default 24 hours");

///////// SDK  /////////
DEFINE_string(tera_sdk_impl_type, "tera", "the activated type of SDK impl");
DEFINE_int32(tera_sdk_retry_times, 10, "the max retry times during sdk operation fail");
DEFINE_int32(tera_sdk_retry_period, 500, "the retry period (in ms) between two operations");
DEFINE_string(tera_sdk_conf_file, "", "the path of default flag file");
DEFINE_int32(tera_sdk_show_max_num, 20000, "the max fetch meta number for each rpc connection");
DEFINE_int32(tera_sdk_async_pending_limit, 2000, "the max number for pending task in async writer");
DEFINE_int32(tera_sdk_async_sync_task_threshold, 1000, "the sync task threshold to do sync operation");
DEFINE_int32(tera_sdk_async_sync_record_threshold, 1000, "the sync kv record threshold");
DEFINE_int32(tera_sdk_async_sync_interval, 15, "the interval (in ms) to sync write buffer to disk");
DEFINE_int32(tera_sdk_async_thread_min_num, 1, "the min thread number for tablet node impl operations");
DEFINE_int32(tera_sdk_async_thread_max_num, 200, "the max thread number for tablet node impl operations");
DEFINE_int32(tera_sdk_rpc_request_max_size, 30, "the max size(MB) for the request message of RPC");
DEFINE_string(tera_sdk_root_table_addr,"127.0.0.1:22000","the default table server has root_table");
DEFINE_int32(tera_sdk_thread_min_num, 1, "the min thread number for tablet node impl operations");
DEFINE_int32(tera_sdk_thread_max_num, 20, "the max thread number for tablet node impl operations");
DEFINE_bool(tera_sdk_rpc_limit_enabled, false, "enable the rpc traffic limit in sdk");
DEFINE_int32(tera_sdk_rpc_limit_max_inflow, 10, "the max bandwidth (in MB/s) for sdk rpc traffic limitation on input flow");
DEFINE_int32(tera_sdk_rpc_limit_max_outflow, 10, "the max bandwidth (in MB/s) for sdk rpc traffic limitation on output flow");
DEFINE_int32(tera_sdk_rpc_max_pending_buffer_size, 200, "max pending buffer size (in MB) for sdk rpc");
DEFINE_int32(tera_sdk_rpc_work_thread_num, 8, "thread num of sdk rpc client");
DEFINE_int32(tera_sdk_update_meta_internal, 10000, "the sdk update meta table internal time(ms)");
DEFINE_int32(tera_sdk_check_timer_internal, 100, "the sdk check the resend quest queue internal time");
DEFINE_int32(tera_sdk_timeout, 60000, "timeout of sdk read/write request (in ms)");
DEFINE_int32(tera_sdk_timeout_precision, 100, "precision of sdk read/write timeout detector (in ms)");
DEFINE_int32(tera_sdk_delay_send_internal, 2, "the sdk resend the request internal time(s)");
DEFINE_int32(tera_sdk_scan_buffer_limit, 2048000, "(B) the pack size limit for scan operation");
DEFINE_bool(tera_sdk_write_sync, false, "sync flag for write");
DEFINE_int32(tera_sdk_batch_size, 250, "batch_size (task number in task_batch)");
DEFINE_int32(tera_sdk_write_send_interval, 10, "(ms) write batch send interval time");
DEFINE_int32(tera_sdk_read_send_interval, 5, "(ms) read batch send interval time");
DEFINE_int64(tera_sdk_max_mutation_pending_num, INT64_MAX, "default number of pending mutations in async put op");
DEFINE_int64(tera_sdk_max_reader_pending_num, INT64_MAX, "default number of pending readers in async get op");
DEFINE_bool(tera_sdk_async_blocking_enabled, true, "enable blocking when async writing and reading");
DEFINE_int32(tera_sdk_update_meta_concurrency, 3, "the concurrency for updating meta");
DEFINE_int32(tera_sdk_update_meta_buffer_limit, 102400, "(B) the pack size limit for updating meta");
DEFINE_bool(tera_sdk_table_rename_enabled, false, "enable sdk table rename");

DEFINE_bool(tera_sdk_cookie_enabled, true, "enable sdk cookie");
DEFINE_string(tera_sdk_cookie_path, "/tmp/.tera_cookie", "the default path of sdk cookie");
DEFINE_int32(tera_sdk_cookie_update_interval, 600, "the interval of cookie updating(s)");

DEFINE_bool(tera_sdk_perf_counter_enabled, true, "enable performance counter log");
DEFINE_int64(tera_sdk_perf_counter_log_interval, 60, "the interval period (in sec) of performance counter log dumping");
DEFINE_bool(tera_sdk_perf_collect_enabled, false, "enable collect perf counter for metrics");
DEFINE_int32(tera_sdk_perf_collect_interval, 10000, "the interval of collect perf counter(ms)");

DEFINE_bool(tera_sdk_batch_scan_enabled, true, "enable batch scan");
DEFINE_int64(tera_sdk_scan_buffer_size, 65536, "(B) default buffer limit for scan");
DEFINE_int64(tera_sdk_scan_number_limit, 1000000000, "default number limit for scan");
DEFINE_int32(tera_sdk_max_batch_scan_req, 30, "the max number of concurrent scan req");
DEFINE_int64(tera_sdk_scan_timeout, 30000, "(ms) scan timeout");
DEFINE_int32(tera_sdk_batch_scan_max_retry, 60, "the max retry times for session scan");
DEFINE_int64(batch_scan_delay_retry_in_us, 1000000, "timewait in us before retry batch scan");
DEFINE_int32(tera_sdk_sync_scan_max_retry, 10, "the max retry times for sync scan");
DEFINE_int64(sync_scan_delay_retry_in_ms, 1000, "timewait in ms before retry sync scan");

DEFINE_string(tera_ins_addr_list, "", "the ins cluster addr. e.g. abc.com:1234,abb.com:1234");
DEFINE_string(tera_ins_root_path, "", "root path on ins. e.g /ps/sandbox");
DEFINE_bool(tera_ins_enabled, false, "[obsoleted replace by --tera_coord_type=ins] option to open ins naming");
DEFINE_bool(tera_mock_ins_enabled, false, "[obsoleted replace by --tera_coord_type=mock_ins] option to open mock ins naming");
DEFINE_int64(tera_ins_session_timeout, 600000000, "ins session timeout(us), default 10min");
DEFINE_int64(tera_sdk_ins_session_timeout, 10000000, "ins session timeout(us), default 10s");

DEFINE_int64(tera_sdk_status_timeout, 600, "(s) check tablet/tabletnode status timeout");
DEFINE_uint64(tera_sdk_read_max_qualifiers, 18446744073709551615U, "read qu limit of each cf, default value is the max of uint64");

DEFINE_bool(tera_sdk_mock_enable, false, "tera sdk mock enable");

// --- Only for online debug ---
// Batch Scan
DEFINE_bool(debug_tera_sdk_scan, false, "enable print detail info for debug online");
