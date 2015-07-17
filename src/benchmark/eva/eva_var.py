TABLET_NUM = 8
KEY_SIZE = 20  # Bytes
VALUE_SIZE = 1024  # Bytes
ENTRY_SIZE = KEY_SIZE + VALUE_SIZE  # Bytes
ENTRY_NUM = 4  # MB
DATA_PATH = ''
MODE_WRITE = 'w'
MODE_READ = 'r'
MODE_SCAN = 's'

TS_NUMBER = 1
READY = 'kReady'
OFFLINE = 'kOffLine'
ONKICK = 'kOnKick'
WAITKICK = 'kWaitKick'
TS_STATUS_KEYS = [READY, OFFLINE, ONKICK, WAITKICK]

T_TOTAL = 'tablet_total'
T_BUSY = 'tablet_onbusy'
T_SPLIT = 'tablet_onsplit'
T_LOAD = 'tablet_onload'
SCAN = 'scan_size'
WRITE = 'write_size'
READ = 'read_rows'
DFS_READ = 'dfs_io_r'
DFS_WRITE = 'dfs_io_w'
LOCAL_READ = 'local_io_r'
LOCAL_WRITE = 'local_io_w'
STAT_KEYS = [T_TOTAL, T_BUSY, T_SPLIT, T_LOAD, SCAN, WRITE, READ, DFS_READ, DFS_WRITE, LOCAL_READ, LOCAL_WRITE]
ACCUMULATE_PART = [SCAN, WRITE, READ, DFS_READ, DFS_WRITE, LOCAL_READ, LOCAL_WRITE]
RATIO_PART = [T_BUSY, T_SPLIT, T_LOAD]

WRITE_AMP = 'write_amplification'
READ_AMP = 'read_amplification'

TERACLI = '../bin/teracli'
TERAMO = '../bin/teramo'
CREATE = 'create'
DROP = 'drop'
QUERY_INTERVAL = 20
REPORT_INTERVAL = 10 * 60 * 60
MICRO = 1000000
MEGA = 1000000.0
EMAIL_BLOCK_TITLE = ''
SENDMAIL = '/usr/sbin/sendmail'
MAIL_PATH = '../tmp/mail_report'
MAIL_HEADER = 'Sender: leiliyuan <leiliyuan@baidu.com>\n\
To: leiliyuan <leiliyuan@baidu.com>\n\
Content-type: text/html\n\
Subject: EVA report\n\n'

SPLIT_SIZE = 512

g_query_thread = None
g_query_event = None
g_report_thread = None
g_exit = False
g_logger = None

g_mode = ' '

g_next_query_time = 0
g_last_query_ts = 0
g_query_times = 0

g_ts_status = {}
for item in TS_STATUS_KEYS:
    g_ts_status.update({item: 0})

g_stat = {}
for item in STAT_KEYS:
    g_stat.update({item: 0})
