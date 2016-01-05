# Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
# Use of this source code is governed by a BSD-style license that can be
# found in the LICENSE file.

#!/usr/bin/env pythons

class CONF:
    def __init__(self):
        self.TABLE_NAME = 'table_name'
        self.TABLET_NUM = 'tablet_number'
        self.KEY_SIZE = 'key_size(B)'  # Bytes
        self.VALUE_SIZE = 'value_sizeB(B)'  # Bytes
        self.KEY_SEED = 'key_seed'
        self.VALUE_SEED = 'value_seed'
        self.STEP = 'step'
        self.ENTRY_SIZE = 'entry_size(B)'  # Bytes
        self.ENTRY_NUM = 'entry_number(M)'  # MB
        self.LG_NUM = 'lg_number'
        self.CF_NUM = 'cf_number'
        self.CF = 'cf'
        self.KV = 'kv_mode'
        self.WRITE_SPEED_LIMIT = 'write_speed_limit(M)'
        self.READ_SPEED_LIMIT = 'read_speed_limit(Qps)'
        self.MODE_SEQ_WRITE = 'sw'
        self.MODE_RAND_WRITE = 'rw'
        self.MODE_READ = 'r'
        self.MODE_SCAN = 's'
        self.MODE = 'mode'
        self.SCAN_BUFFER = 'scan_buffer(M)'
        self.TERA_BENCH = ''
        self.SPLIT_SIZE = 'split_size'
        self.SCHEMA = 'table_schema'
        self.g_speed_limit = 0
        self.g_datasize = 0
        self.g_web_report = True
        self.g_web_report_type = ''

        self.TS_NUMBER = 'ts_number'

        self.g_test_conf = {self.TABLE_NAME: '', self.TABLET_NUM: 0, self.KEY_SIZE: 20, self.VALUE_SIZE: 1024,
                            self.KEY_SEED: '', self.VALUE_SEED: '', self.STEP: 'False',
                            self.ENTRY_SIZE: 20 + 1024, self.ENTRY_NUM: 0, self.LG_NUM: 0, self.CF_NUM: 1, self.CF: '',
                            self.KV: None, self.WRITE_SPEED_LIMIT: 0, self.READ_SPEED_LIMIT: 0, self.MODE: '',
                            self.SCAN_BUFFER: 0, self.TS_NUMBER: 0, self.SPLIT_SIZE: 0, self.SCHEMA: ''}
conf = CONF()


class Stat:
    def __init__(self):
        self.READY = 'kReady'
        self.OFFLINE = 'kOffLine'
        self.ONKICK = 'kOnKick'
        self.WAITKICK = 'kWaitKick'
        self.TS_STATUS_KEYS = [self.READY, self.OFFLINE, self.ONKICK, self.WAITKICK]

        self.T_TOTAL = 'tablet_total'
        self.T_BUSY = 'tablet_onbusy'
        self.T_SPLIT = 'tablet_onsplit'
        self.T_LOAD = 'tablet_onload'
        self.SCAN = 'scan_size'
        self.WRITE = 'write_size'
        self.READ_SIZE = 'read_size'
        self.READ_ROWS = 'read_rows'
        self.READ_DELAY = 'rand_read_delay'
        self.DFS_READ = 'dfs_io_r'
        self.DFS_WRITE = 'dfs_io_w'
        self.LOCAL_READ = 'local_io_r'
        self.LOCAL_WRITE = 'local_io_w'
        self.CPU = 'cpu_usage'
        self.MEM = 'mem_used'
        self.STAT_KEYS = [self.T_TOTAL, self.T_BUSY, self.T_SPLIT, self.T_LOAD, self.SCAN, self.WRITE, self.READ_SIZE,
                          self.READ_ROWS, self.DFS_READ, self.READ_DELAY, self.DFS_WRITE, self.LOCAL_READ,
                          self.LOCAL_WRITE, self.CPU, self.MEM]
        self.ACCUMULATE_PART = [self.SCAN, self.WRITE, self.READ_SIZE, self.READ_ROWS, self.DFS_READ, self.READ_DELAY,
                                self.DFS_WRITE, self.LOCAL_READ, self.LOCAL_WRITE, self.CPU, self.MEM]
        self.RATIO_PART = [self.T_BUSY, self.T_SPLIT, self.T_LOAD]

        self.WRITE_AMP = 'write_amplification'
        self.READ_AMP = 'read_amplification'
        self.SCAN_AMP = 'scan_amplification'

        self.g_ts_status = {}
        for item in self.TS_STATUS_KEYS:
            self.g_ts_status.update({item: 0})

        self.g_stat = {}
        for item in self.STAT_KEYS:
            self.g_stat.update({item: 0})


stat = Stat()


class Common:
    def __init__(self):
        self.TERACLI = './teracli --flagfile=../conf/tera.flag'
        self.TERAMO = './teramo'
        self.TMP_DIR = '../tmp/'
        self.CREATE = 'create'
        self.DROP = 'drop'
        self.QUERY_INTERVAL = 20
        self.REPORT_INTERVAL = 10 * 60 * 60
        self.MICRO = 1000000
        self.MEGA = 1024.0 * 1024.0
        self.RANDOM_MAX = 2147483254
        self.EMAIL_BLOCK_TITLE = ''
        self.SENDMAIL = '/usr/sbin/sendmail'
        self.MAIL_PATH = '../tmp/mail_report'
        self.WEB_PATH = '../tmp/web_report'
        self.MAIL_HEADER = 'Sender: tera_eva <tera_eva@baidu.com>\nTo: tera_dev <tera_dev@baidu.com>\n\
Content-type: text/html\nSubject: EVA report\n\n'

        self.g_query_thread = None
        self.g_query_event = None
        self.g_report_thread = None
        self.g_exit = False
        self.g_force_exit = False
        self.g_logger = None

        self.g_next_query_time = 0
        self.g_last_query_ts = 0
        self.g_query_times = 0
common = Common()

