import subprocess
import time
import os
import logging
from xml.dom.minidom import Document

from bin import eva_var


def write_deli():
    fp = open(eva_var.DATA_PATH + 't.deli', 'w')
    if eva_var.g_mode == eva_var.MODE_WRITE:
        if eva_var.DATA_PATH == '../seq.eva/':
            for i in range(1, eva_var.TABLET_NUM, 1):
                fp.write('%020d' % (i * eva_var.ENTRY_NUM / 2 * int(eva_var.MEGA)) + '\n')
        else:
            post_fix = '0' * 16 + '\n'
            for i in range(1, eva_var.TABLET_NUM, 1):
                fp.write('%04d' % i + post_fix)
    fp.close()

def table_manipulate(tablename, op, schema='', delimiter=''):
    if op == eva_var.CREATE:
        write_deli()
        cmd = ' '.join([eva_var.TERACLI, 'createbyfile', schema, delimiter])
        print cmd
        ret = subprocess.Popen(cmd, stdout=subprocess.PIPE, shell=True)
        print ret.stdout.read()
        eva_var.g_logger.info('created table ' + tablename)
    elif op == eva_var.DROP:
        cmd = ' '.join([eva_var.TERACLI, 'disable', tablename])
        ret = subprocess.Popen(cmd, stdout=subprocess.PIPE, shell=True)
        print cmd
        print ret.stdout.read()
        cmd = ' '.join([eva_var.TERACLI, 'drop', tablename])
        ret = subprocess.Popen(cmd, stdout=subprocess.PIPE, shell=True)
        print cmd
        print ret.stdout.read()
        eva_var.g_logger.info('droped table ' + tablename)
    else:
        pass
        eva_var.g_logger.warning('unrecognized opration: ' + op)

def init():
    logfile = '../log/eva/eva.' + time.strftime('%Y-%m-%d-%H-%M-%S')
    eva_var.g_logger = logging.getLogger('eva')
    eva_var.g_logger.setLevel(logging.DEBUG)
    fh = logging.FileHandler(logfile)
    formatter = logging.Formatter('%(asctime)s-%(name)s-%(levelname)s - %(message)s')
    fh.setFormatter(formatter)
    eva_var.g_logger.addHandler(fh)
    eva_var.g_logger.info('log init done!')
    if not os.path.exists(eva_var.MAIL_PATH):
        mail = open(eva_var.MAIL_PATH, 'w')
        mail.write(eva_var.MAIL_HEADER)
        mail.close()

    eva_var.g_last_query_ts = int(time.time() * eva_var.MICRO)

def cleanup():
    for item in eva_var.TS_STATUS_KEYS:
        eva_var.g_ts_status[item] = 0
    for item in eva_var.STAT_KEYS:
        eva_var.g_stat[item] = 0
    eva_var.g_query_event.Clear()
    eva_var.g_exit = False

    eva_var.g_last_query_ts = int(time.time() * eva_var.MICRO)
    eva_var.g_query_times = 0
    eva_var.g_first_run = True

def query():
    while not eva_var.g_exit:
        print 'next query in ', str(eva_var.g_next_query_time - time.time())
        eva_var.g_logger.info('next query in {s} seconds'.
                              format(s=str(int(eva_var.g_next_query_time - time.time()))))
        eva_var.g_query_event.wait(eva_var.g_next_query_time - time.time())

        eva_var.g_next_query_time = time.time() + eva_var.QUERY_INTERVAL
        eva_var.g_logger.info('start querying...')
        cmd = eva_var.TERACLI + ' showts'
        data = subprocess.Popen(cmd, stdout=subprocess.PIPE, shell=True)
        while True:
            line = data.stdout.readline()
            if line != '':
                if eva_var.READY in line:
                    eva_var.g_ts_status[eva_var.READY] += 1
                elif eva_var.OFFLINE in line:
                    eva_var.g_ts_status[eva_var.OFFLINE] += 1
                elif eva_var.ONKICK in line:
                    eva_var.g_ts_status[eva_var.ONKICK] += 1
                elif eva_var.WAITKICK in line:
                    eva_var.g_ts_status[eva_var.WAITKICK] += 1
            else:
                break
        eva_var.g_logger.info('tabletnode status: ' + str(eva_var.TS_NUMBER) + ' ' + str(eva_var.g_ts_status))

        '''
        time_stamp: 1436663159449671 load: 2194625611 tablet_total: 11 tablet_onbusy: 0 low_read_cell: 0 scan_rows: 0
        scan_size: 0 read_rows: 0 read_size: 0 write_rows: 0 write_size: 0 mem_used: 318562304 net_tx: 8435
        net_rx: 2892 dfs_io_r: 0 dfs_io_w: 0 local_io_r: 0 local_io_w: 0 status_m: "" tablet_onload: 0
        tablet_onsplit: 0 read_pending: 0 write_pending: 0 scan_pending: 0
        '''
        now = int(time.time() * eva_var.MICRO)
        cmd = ' '.join([eva_var.TERAMO, 'eva', str(eva_var.g_last_query_ts), str(now)])
        ret = subprocess.Popen(cmd, stdout=subprocess.PIPE, shell=True)
        ret.wait()
        data = subprocess.Popen(eva_var.TERAMO + ' print tera_monitor.response', stdout=subprocess.PIPE, shell=True)
        data = data.stdout.read()
        eva_var.g_last_query_ts = now
        data = data.split('\n')
        data = filter(None, data)
        try:
            for line in data:
                line = line.split(' ')
                line = filter(None, line)
                for item in eva_var.ACCUMULATE_PART:
                    index = line.index(item + ':')
                    eva_var.g_stat[item] += int(line[index + 1])

                index = line.index('tablet_total:')
                eva_var.g_stat[eva_var.T_TOTAL] = int(line[index + 1])
                for item in eva_var.RATIO_PART:
                    if float(eva_var.g_stat[eva_var.T_TOTAL]) - 2 != 0:
                        index = line.index(item + ':')
                        eva_var.g_stat[item] += float(line[index + 1]) / (float(eva_var.g_stat[eva_var.T_TOTAL]) - 2)
                    else:
                        print 'no tablets'

                eva_var.g_query_times += 1
                eva_var.g_logger.info('query times: ' + str(eva_var.g_query_times))
                eva_var.g_logger.info('tablet status: ' + str(eva_var.g_stat))
        except ValueError:
            pass

        eva_var.g_logger.info('done query')

def write_title(email, doc, level, content):
    title_node = doc.createElement(level)
    title_name = doc.createTextNode(content)
    title_node.appendChild(title_name)
    email.appendChild(title_node)

def write_table(email, doc, contents):
    table = doc.createElement('table border="1" width="800"')
    for title, content_dict in contents:
        title_node = doc.createElement('tr')
        title_line = doc.createElement('th align=left colspan="{n}"'.format(n=len(content_dict)))
        title_name = doc.createTextNode(title)
        title_line.appendChild(title_name)
        title_node.appendChild(title_line)
        table.appendChild(title_node)
        line = doc.createElement('tr')
        for k, v in content_dict:
            col_node = doc.createElement('th')
            col_name = doc.createTextNode(k + ': ' + str(v))
            col_node.appendChild(col_name)
            line.appendChild(col_node)
        table.appendChild(line)
    email.appendChild(table)

def write_email(fp, desp):
    doc = Document()
    email = doc.createElement('email')
    doc.appendChild(email)
    write_title(email, doc, 'h3', eva_var.EMAIL_BLOCK_TITLE)
    write_title(email, doc, 'h4', desp)
    rw_perf_tuple = (('write throughput(M)', '%6.3f' % eva_var.g_stat[eva_var.WRITE]),
                     ('write amplification', '%4.2f' % eva_var.g_stat[eva_var.WRITE_AMP]),
                     ('read throughput(M)', '%6.3f' % eva_var.g_stat[eva_var.READ]),
                     ('read amplification', '%4.2f' % eva_var.g_stat[eva_var.READ_AMP]))
    ts_tuple = (('ready(%)', '%06.3f' % (eva_var.g_ts_status[eva_var.READY] * 100)),
                ('wait kick(%)', '%6.3f' % (eva_var.g_ts_status[eva_var.WAITKICK] * 100)),
                ('on kick(%)', '%6.3f' % (eva_var.g_ts_status[eva_var.ONKICK] * 100)),
                ('off line(%)', '%6.3f' % (eva_var.g_ts_status[eva_var.OFFLINE] * 100)))
    tablets_tuple = (('total number', eva_var.g_stat[eva_var.T_TOTAL]),
                     ('busy(%)', '%6.3f' % (eva_var.g_stat[eva_var.T_BUSY] * 100)),
                     ('load(%)', '%6.3f' % (eva_var.g_stat[eva_var.T_LOAD] * 100)),
                     ('split(%)', '%6.3f' % (eva_var.g_stat[eva_var.T_SPLIT] * 100)))
    write_table(email, doc, (('Read/Write Performance', rw_perf_tuple),
                             ('Tablet Servers', ts_tuple),
                             ('Tablets', tablets_tuple)))
    fp.write(doc.toprettyxml(indent=' '))
    fp.close()

def send_email():
    ret = subprocess.Popen('cat ' + eva_var.MAIL_PATH + ' | ' + eva_var.SENDMAIL + ' -oi leiliyuan@baidu.com',
                           stdout=subprocess.PIPE, shell=True)
    ret.wait()

def comput_ts_stat():
    total = sum(eva_var.g_ts_status.values())
    if total == 0:
        return None
    total = float(total)
    eva_var.g_ts_status[eva_var.READY] /= total
    eva_var.g_ts_status[eva_var.OFFLINE] /= total
    eva_var.g_ts_status[eva_var.ONKICK] /= total
    eva_var.g_ts_status[eva_var.WAITKICK] /= total
    print eva_var.g_ts_status

def comput_stat():
    if eva_var.g_query_times == 0:
        return None
    times = float(eva_var.g_query_times)
    tablet_total = eva_var.g_stat[eva_var.T_TOTAL] - 2  # meta_table & stat_table
    for item in eva_var.ACCUMULATE_PART:
        eva_var.g_stat[item] /= eva_var.MEGA * times * eva_var.TS_NUMBER  # TS
    for item in eva_var.RATIO_PART:
        eva_var.g_stat[item] /= times
    eva_var.g_stat[eva_var.T_TOTAL] = tablet_total

    write_size = float(eva_var.g_stat[eva_var.WRITE])
    if write_size > 1:
        write_augment = (float(eva_var.g_stat[eva_var.DFS_WRITE]) + float(eva_var.g_stat[eva_var.LOCAL_WRITE])) / write_size
        eva_var.g_stat.update({eva_var.WRITE_AMP: write_augment})
    else:
        eva_var.g_stat.update({eva_var.WRITE_AMP: 0})

    read_size = float(eva_var.g_stat[eva_var.READ])
    if read_size > 1:
        read_augment = (float(eva_var.g_stat[eva_var.DFS_READ]) + float(eva_var.g_stat[eva_var.LOCAL_READ])) / read_size
        eva_var.g_stat.update({eva_var.READ_AMP: read_augment})
    else:
        eva_var.g_stat.update({eva_var.READ_AMP: 0})
