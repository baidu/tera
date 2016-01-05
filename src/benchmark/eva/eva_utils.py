# Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
# Use of this source code is governed by a BSD-style license that can be
# found in the LICENSE file.

#!/usr/bin/env pythons

import subprocess
import time
import os
import logging
from xml.dom.minidom import Document
import urllib

from bin import eva_var

conf = eva_var.conf
stat = eva_var.stat
common = eva_var.common


def write_deli():
    fp = open(common.TMP_DIR + 't.deli', 'w')
    if conf.g_test_conf[conf.MODE] == conf.MODE_SEQ_WRITE or conf.g_test_conf[conf.MODE] == conf.MODE_RAND_WRITE:
        post_fix = '0' * 16 + '\n'
        for i in range(1, conf.g_test_conf[conf.TABLET_NUM], 1):
            fp.write('%04d' % i + post_fix)
    fp.close()


def table_manipulate(tablename, op, schema):
    schema_file = common.TMP_DIR+'t.schema'
    fp = open(schema_file, 'w')
    if 'storage' in schema:
        schema_str = '{tablename} <splitsize={size},storage={storage}>'.\
            format(tablename=tablename, size=str(conf.g_test_conf[conf.SPLIT_SIZE]),
                   storage=schema['storage'])
        fp.write(schema_str)
        conf.g_test_conf[conf.KV] = True
        if op == conf.CF:
            return 1, ''
    else:
        fp.write('{tablename} <splitsize={size}> '.format(tablename=tablename, size=str(conf.g_test_conf[conf.SPLIT_SIZE])))
        fp.write('{\n')
        lg_schemas_list = []
        total_cf_number = 0
        total_cf_list = []
        for lg, lg_schema in schema.iteritems():
            # lg schema
            lg_str = lg + '<'
            temp = []
            if type(lg_schema) == dict:
                for k, v in lg_schema.iteritems():
                    if k != 'cf':
                        temp.append(k+'='+v)
                lg_str += ','.join(temp)
                lg_str += '>{\n'

                # cf schema
                cf_list = lg_schema['cf']
                total_cf_list.append(cf_list)
                comp = cf_list.split(',')
                total_cf_number += len(comp)
                cf_list = []
                for cf in comp:
                    try:
                        cf, qualifier = cf.split(':')
                    except:
                        pass
                    finally:
                        cf_list.append(cf)
                cf_list = list(set(cf_list))
                lg_str += ','.join(cf_list)
                lg_str += '}\n'
                lg_schemas_list.append(lg_str)
            else:
                cf_list = schema['cf']
                total_cf_list.append(cf_list)
                comp = cf_list.split(',')
                total_cf_number += len(comp)
                cf_list = []
                cf_str = ''
                for cf in comp:
                    try:
                        cf, qualifier = cf.split(':')
                    except:
                        pass
                    finally:
                        cf_list.append(cf)
                cf_list = list(set(cf_list))
                cf_str += ','.join(cf_list)
                lg_schemas_list.append(cf_str)
        fp.write(',\n'.join(lg_schemas_list) + '}\n')
        if op == conf.CF:
            return total_cf_number, ','.join(total_cf_list)
    fp.close()

    if op == common.CREATE:
        write_deli()
        cmd = ' '.join([common.TERACLI, 'createbyfile', schema_file, common.TMP_DIR + 't.deli'])
        print cmd
        ret = subprocess.Popen(cmd, stdout=subprocess.PIPE, shell=True)
        print ret.stdout.read()
        common.g_logger.info('created table ' + tablename)
    elif op == common.DROP:
        cmd = ' '.join([common.TERACLI, 'disable', tablename])
        ret = subprocess.Popen(cmd, stdout=subprocess.PIPE, shell=True)
        print cmd
        print ret.stdout.read()
        cmd = ' '.join([common.TERACLI, 'drop', tablename])
        ret = subprocess.Popen(cmd, stdout=subprocess.PIPE, shell=True)
        print cmd
        print ret.stdout.read()
        common.g_logger.info('droped table ' + tablename)
    else:
        pass
        common.g_logger.warning('unrecognized opration: ' + op)


def init():
    logfile = '../log/eva/eva.' + time.strftime('%Y-%m-%d-%H-%M-%S')
    common.g_logger = logging.getLogger('eva')
    common.g_logger.setLevel(logging.DEBUG)
    fh = logging.FileHandler(logfile)
    formatter = logging.Formatter('%(asctime)s-%(name)s-%(levelname)s - %(message)s')
    fh.setFormatter(formatter)
    common.g_logger.addHandler(fh)
    common.g_logger.info('log init done!')
    if not os.path.exists(common.MAIL_PATH):
        mail = open(common.MAIL_PATH, 'w')
        mail.write(common.MAIL_HEADER)
        mail.close()

    common.g_last_query_ts = int(time.time() * common.MICRO)


def cleanup():
    for item in conf.TS_STATUS_KEYS:
        stat.g_ts_status[item] = 0
    for item in stat.STAT_KEYS:
        stat.g_stat[item] = 0
    common.g_query_event.Clear()
    common.g_exit = False

    common.g_last_query_ts = int(time.time() * common.MICRO)
    common.g_query_times = 0


def query():
    while not common.g_exit:
        print 'next query in ', str(common.g_next_query_time - time.time())
        common.g_logger.info('next query in {s} seconds'.
                             format(s=str(int(common.g_next_query_time - time.time()))))
        common.g_query_event.wait(common.g_next_query_time - time.time())
        if common.g_exit is True:
            return

        common.g_next_query_time = time.time() + common.QUERY_INTERVAL
        common.g_logger.info('start querying...')
        cmd = common.TERACLI + ' showts'
        data = subprocess.Popen(cmd, stdout=subprocess.PIPE, shell=True)
        while True:
            line = data.stdout.readline()
            if line != '':
                if stat.READY in line:
                    stat.g_ts_status[stat.READY] += 1
                elif stat.OFFLINE in line:
                    stat.g_ts_status[stat.OFFLINE] += 1
                elif stat.ONKICK in line:
                    stat.g_ts_status[stat.ONKICK] += 1
                elif stat.WAITKICK in line:
                    stat.g_ts_status[stat.WAITKICK] += 1
            else:
                break
        common.g_logger.info('tabletnode status: ' + str(conf.TS_NUMBER) + ' ' + str(stat.g_ts_status))

        '''
        time_stamp: 1438761682963212 load: 55610651740 tablet_total: 21 tablet_onbusy: 0 low_read_cell: 0 scan_rows: 0
        scan_size: 0 read_rows: 0 read_size: 0 write_rows: 81674 write_size: 87640307 mem_used: 2645516288
        net_tx: 215516201 net_rx: 216682508 dfs_io_r: 0 dfs_io_w: 166861771 local_io_r: 40010453 local_io_w: 79213879
        status_m: "" tablet_onload: 0 tablet_onsplit: 0 extra_stat { name: "rand_read_delay" value: 0 }
        extra_stat { name: "row_read_delay" value: 0 } extra_stat { name: "range_error" value: 0 }
        extra_stat { name: "read_pending" value: 0 }extra_stat { name: "write_pending" value: 0 }
        extra_stat { name: "scan_pending" value: 0 } extra_stat { name: "compact_pending" value: 0 }
        read_pending: 0 write_pending: 0 scan_pending: 0 cpu_usage: 16.1 rand_read_delay: 0
        '''
        now = int(time.time() * common.MICRO)
        cmd = ' '.join([common.TERAMO, 'eva', str(common.g_last_query_ts), str(now)])
        ret = subprocess.Popen(cmd, stdout=subprocess.PIPE, shell=True)
        common.g_logger.info(cmd)
        ret.wait()
        data = subprocess.Popen(common.TERAMO + ' print tera_monitor.response', stdout=subprocess.PIPE, shell=True)
        data = data.stdout.read()
        common.g_last_query_ts = now
        data = data.split('\n')
        data = filter(None, data)
        try:
            for line in data:
                common.g_logger.info('debug: ' + line)
                line = line.split(' ')
                line = filter(None, line)
                for item in stat.ACCUMULATE_PART:
                    index = line.index(item + ':')
                    stat.g_stat[item] += float(line[index + 1])

                index = line.index('tablet_total:')
                stat.g_stat[stat.T_TOTAL] = float(line[index + 1])
                if stat.g_stat[stat.T_TOTAL] != 0:
                    for item in stat.RATIO_PART:
                        index = line.index(item + ':')
                        stat.g_stat[item] += float(line[index + 1]) / float(stat.g_stat[stat.T_TOTAL])

                common.g_query_times += 1
                common.g_logger.info('query times: ' + str(common.g_query_times))
                common.g_logger.info('tablet status: ' + str(stat.g_stat))
        except ValueError:
            pass

        common.g_logger.info('done query')


def write_title(email, doc, level, content):
    title_node = doc.createElement(level)
    title_name = doc.createTextNode(content)
    title_node.appendChild(title_name)
    email.appendChild(title_node)


def write_table(email, doc, contents):
    table = doc.createElement('table border="1" width="800"')
    for title, content_dict in contents:
        if conf.g_test_conf[conf.MODE] == conf.MODE_READ and title == 'Read/Write Performance':
            title += ' read_delay=' + str(stat.g_stat[stat.READ_DELAY])
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

performance = None
def write_email(mail_fp, web_fp, desp):
    doc = Document()
    email = doc.createElement('email')
    doc.appendChild(email)
    write_title(email, doc, 'h3', common.EMAIL_BLOCK_TITLE + ' ' + time.strftime('%Y-%m-%d-%H-%M-%S'))
    write_title(email, doc, 'h4', desp)
    write_title(email, doc, 'h4', 'force_exit=' + str(common.g_force_exit))
    print conf.g_test_conf
    global performance
    if conf.g_test_conf[conf.MODE] == conf.MODE_SEQ_WRITE or conf.g_test_conf[conf.MODE] == conf.MODE_RAND_WRITE:
        performance = (('write throughput(M)', '%6.3f' % stat.g_stat[stat.WRITE]),
                       ('write amplification', '%4.2f' % stat.g_stat[stat.WRITE_AMP]),
                       ('CPU usage', '%4.2f' % stat.g_stat[stat.CPU]),
                       ('Memory(G)', '%4.2f' % (stat.g_stat[stat.MEM]/1024.0)))
        s = urllib.urlencode({'ts': time.strftime('%Y-%m-%d'),
                              'speed': '%6.3f' % stat.g_stat[stat.WRITE],
                              'amp': '%4.2f' % stat.g_stat[stat.WRITE_AMP],
                              'op': 'rw', 'type': conf.g_web_report_type})
        if web_fp:
            web_fp.write(s + '\n')
    elif conf.g_test_conf[conf.MODE] == conf.MODE_READ:
        performance = (('read Qps(/s)', '%6.2f' % stat.g_stat[stat.READ_ROWS]),
                       ('read amplification', '%4.2f' % stat.g_stat[stat.READ_AMP]),
                       ('CPU usage', '%4.2f' % stat.g_stat[stat.CPU]),
                       ('Memory(G)', '%4.2f' % (stat.g_stat[stat.MEM]/1024.0)))
        s = urllib.urlencode({'ts': time.strftime('%Y-%m-%d'),
                              'speed': '%6.2f' % stat.g_stat[stat.READ_ROWS],
                              'amp': '%4.2f' % stat.g_stat[stat.READ_AMP],
                              'op': 'r', 'type': conf.g_web_report_type})
        if web_fp:
            web_fp.write(s + '\n')
    elif conf.g_test_conf[conf.MODE] == conf.MODE_SCAN:
        performance = (('scan throughput(M)', '%6.3f' % stat.g_stat[stat.SCAN]),
                       ('scan amplification', '%4.2f' % stat.g_stat[stat.SCAN_AMP]),
                       ('CPU usage', '%4.2f' % stat.g_stat[stat.CPU]),
                       ('Memory(G)', '%4.2f' % (stat.g_stat[stat.MEM]/1024.0)))
    ts_tuple = (('ready(%)', '%06.3f' % (stat.g_ts_status[stat.READY] * 100)),
                ('wait kick(%)', '%6.3f' % (stat.g_ts_status[stat.WAITKICK] * 100)),
                ('on kick(%)', '%6.3f' % (stat.g_ts_status[stat.ONKICK] * 100)),
                ('off line(%)', '%6.3f' % (stat.g_ts_status[stat.OFFLINE] * 100)))
    tablets_tuple = (('total number', stat.g_stat[stat.T_TOTAL]),
                     ('busy(%)', '%6.3f' % (stat.g_stat[stat.T_BUSY] * 100)),
                     ('load(%)', '%6.3f' % (stat.g_stat[stat.T_LOAD] * 100)),
                     ('split(%)', '%6.3f' % (stat.g_stat[stat.T_SPLIT] * 100)))
    write_table(email, doc, (('Read/Write Performance', performance),
                             ('Tablet Servers', ts_tuple),
                             ('Tablets/TS', tablets_tuple)))
    mail_fp.write(doc.toprettyxml(indent=' '))
    mail_fp.close()

    if web_fp:
        web_fp.close()


def send_email():
    ret = subprocess.Popen('cat ' + common.MAIL_PATH + ' | ' + common.SENDMAIL + ' -oi leiliyuan@baidu.com',
                           stdout=subprocess.PIPE, shell=True)
    ret.wait()


def compute_ts_stat():
    total = sum(stat.g_ts_status.values())
    if total == 0:
        return None
    total = float(total)
    stat.g_ts_status[stat.READY] /= total
    stat.g_ts_status[stat.OFFLINE] /= total
    stat.g_ts_status[stat.ONKICK] /= total
    stat.g_ts_status[stat.WAITKICK] /= total


def compute_stat():
    if common.g_query_times == 0:
        return None
    times = float(common.g_query_times)
    tablet_total = stat.g_stat[stat.T_TOTAL]
    stat.g_stat[stat.CPU] *= common.MEGA
    stat.g_stat[stat.READ_DELAY] *= common.MEGA
    for item in stat.ACCUMULATE_PART:
        stat.g_stat[item] /= common.MEGA * times  # TS
    for item in stat.RATIO_PART:
        stat.g_stat[item] /= times
    stat.g_stat[stat.T_TOTAL] = tablet_total
    stat.g_stat[stat.READ_ROWS] *= common.MEGA

    write_size = float(stat.g_stat[stat.WRITE])
    if write_size > 1:
        write_augment = float(stat.g_stat[stat.DFS_WRITE]) / write_size
        stat.g_stat.update({stat.WRITE_AMP: write_augment})
    else:
        stat.g_stat.update({stat.WRITE_AMP: 0})

    read_size = float(stat.g_stat[stat.READ_SIZE])
    if read_size > 1:
        read_augment = (float(stat.g_stat[stat.DFS_READ]) + float(stat.g_stat[stat.LOCAL_READ])) / read_size
        stat.g_stat.update({stat.READ_AMP: read_augment})
    else:
        stat.g_stat.update({stat.READ_AMP: 0})

    scan_size = float(stat.g_stat[stat.SCAN])
    if scan_size > 1:
        scan_augment = (float(stat.g_stat[stat.DFS_READ]) + float(stat.g_stat[stat.LOCAL_READ])) / scan_size
        stat.g_stat.update({stat.SCAN_AMP: scan_augment})
    else:
        stat.g_stat.update({stat.SCAN_AMP: 0})
