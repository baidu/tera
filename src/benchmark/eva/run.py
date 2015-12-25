# Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
# Use of this source code is governed by a BSD-style license that can be
# found in the LICENSE file.

#!/usr/bin/env pythons

import sys
import subprocess
import time
import threading
import traceback
import json
import signal

from bin import eva_var
from bin import eva_utils

conf = eva_var.conf
stat = eva_var.stat
common = eva_var.common


def parse_input():
    conf_file = sys.argv[1]
    if len(sys.argv) == 3:
        conf.g_web_report = False
    conf_dict = json.load(open(conf_file, 'r'))
    bench_cmd_prefix = './tera_bench --compression_ratio=1 '
    for pre, post in conf_dict.iteritems():
        if pre == conf.TABLE_NAME:
            conf.g_test_conf[conf.TABLE_NAME] = post
        if pre == conf.KEY_SIZE:
            conf.g_test_conf[conf.KEY_SIZE] = int(post)
            bench_cmd_prefix += '--key_size={ks} '.format(ks=post)
        if pre == conf.VALUE_SIZE:
            conf.g_test_conf[conf.VALUE_SIZE] = int(post)
            bench_cmd_prefix += '--value_size={vs} '.format(vs=post)
        if pre == conf.KEY_SEED:
            conf.g_test_conf[conf.KEY_SEED] = int(post)
            bench_cmd_prefix += '--key_seed={ks} '.format(ks=post)
        if pre == conf.VALUE_SEED:
            conf.g_test_conf[conf.VALUE_SEED] = int(post)
            bench_cmd_prefix += '--value_seed={vs} '.format(vs=post)
        if pre == conf.MODE:
            conf.g_test_conf[conf.MODE] = post
            if post == conf.MODE_SEQ_WRITE:
                bench_cmd_prefix += '--benchmarks=seq '
                common.EMAIL_BLOCK_TITLE = 'Sequential Write'
            elif post == conf.MODE_RAND_WRITE:
                bench_cmd_prefix += '--benchmarks=random '
                common.EMAIL_BLOCK_TITLE = 'Random Write'
            elif post == conf.MODE_SCAN:
                common.EMAIL_BLOCK_TITLE = 'Scan'
            elif post == conf.MODE_READ:
                bench_cmd_prefix += '--benchmarks=random '
                common.EMAIL_BLOCK_TITLE = 'Read'
        elif pre == conf.TABLET_NUM:
            conf.g_test_conf[conf.TABLET_NUM] = int(post)
        elif pre == conf.SPLIT_SIZE:
            conf.g_test_conf[conf.SPLIT_SIZE] = int(post)
        elif pre == conf.TS_NUMBER:
            conf.g_test_conf[conf.TS_NUMBER] = int(post)
        elif pre == conf.WRITE_SPEED_LIMIT:
            conf.g_speed_limit = post
        elif pre == conf.READ_SPEED_LIMIT:
            conf.g_test_conf[conf.READ_SPEED_LIMIT] = int(post)
        elif pre == 'random':
            bench_cmd_prefix += '--random=' + post + ' '
        elif pre == conf.ENTRY_NUM:
            num = float(post) * common.MEGA
            bench_cmd_prefix += '--num=' + str(int(num)) + ' '
            conf.g_test_conf[conf.ENTRY_NUM] = int(num)
        elif pre == conf.SCAN_BUFFER:
            buffer = float(post)
            buffer *= common.MEGA
            conf.g_test_conf[conf.SCAN_BUFFER] = str(int(buffer))
        elif pre == conf.SCHEMA:
            conf.g_test_conf[conf.SCHEMA] = post
            conf.g_test_conf[conf.LG_NUM] = len(post)
        elif pre == conf.STEP:
            conf.g_test_conf[conf.STEP] = post
        elif pre == 'web_report_type':
            conf.g_web_report_type = post


    conf.g_test_conf[conf.ENTRY_SIZE] = conf.g_test_conf[conf.KEY_SIZE] + conf.g_test_conf[conf.VALUE_SIZE]
    conf.g_test_conf[conf.WRITE_SPEED_LIMIT] = int(round(float(conf.g_speed_limit) / conf.g_test_conf[conf.TABLET_NUM] * common.MEGA / conf.g_test_conf[conf.ENTRY_SIZE]))
    conf.g_test_conf[conf.READ_SPEED_LIMIT] = int(float(conf.g_test_conf[conf.READ_SPEED_LIMIT]) / conf.g_test_conf[conf.TABLET_NUM])
    conf.g_test_conf[conf.CF_NUM], conf.g_test_conf[conf.CF] = \
        eva_utils.table_manipulate(conf.g_test_conf[conf.TABLE_NAME], conf.CF, conf.g_test_conf[conf.SCHEMA])
    if conf.g_test_conf[conf.CF] != '':
        bench_cmd_prefix += '--cf=' + conf.g_test_conf[conf.CF] + ' '
    if conf.g_test_conf[conf.STEP] == 'True':
        bench_cmd_prefix += '--key_step=' + str(common.RANDOM_MAX / conf.g_test_conf[conf.ENTRY_NUM]) + ' '
    conf.TERA_BENCH = bench_cmd_prefix
    conf.g_datasize = (conf.g_test_conf[conf.CF_NUM] * conf.g_test_conf[conf.TABLET_NUM] *
                          conf.g_test_conf[conf.ENTRY_NUM] * conf.g_test_conf[conf.ENTRY_SIZE])
    if conf.g_test_conf[conf.MODE] == conf.MODE_SEQ_WRITE or conf.g_test_conf[conf.MODE] == conf.MODE_RAND_WRITE:
        print '\t%-25s' % 'estimated running time:', get_time_form((conf.g_datasize >> 20) / float(conf.g_speed_limit))
    else:
        print '\t%-25s' % 'estimated running time:', get_time_form(conf.g_test_conf[conf.ENTRY_NUM] /
                                      conf.g_test_conf[conf.READ_SPEED_LIMIT])
    conf.g_datasize = get_data_size(conf.g_datasize)
    print '\t%-25s' % 'user data size:', conf.g_datasize
    if common.g_logger is not None:
        common.g_logger.info('running tera_mark: ' + str(conf.g_test_conf))

def work():
    try:
        common.g_next_query_time = time.time() + common.QUERY_INTERVAL
        common.g_query_thread = threading.Thread(target=eva_utils.query)
        common.g_query_event = threading.Event()
        run_test()
    except:
        common.g_logger.info(traceback.print_exc())


def run_test():
    common.g_query_thread.setDaemon(True)
    common.g_query_thread.start()
    common.g_logger.info('running tera_mark with {n} tablets'.format(n=conf.g_test_conf[conf.TABLET_NUM]))
    wait_list = []
    kill_list = []
    start_time = time.time()
    if conf.g_test_conf[conf.MODE] == conf.MODE_SEQ_WRITE or\
       conf.g_test_conf[conf.MODE] == conf.MODE_RAND_WRITE:
        wait_list, kill_list = run_write_test()
    elif conf.g_test_conf[conf.MODE] == conf.MODE_SCAN:
        wait_list, kill_list = run_scan_test()
    elif conf.g_test_conf[conf.MODE] == conf.MODE_READ:
        wait_list, kill_list = run_read_test()

    count = 0
    wait_num = conf.g_test_conf[conf.TABLET_NUM] * 2 / 3 + 1
    while count < wait_num:
        count = 0
        for ret in wait_list:
            if ret.poll() is not None:
                count += 1
        time.sleep(common.QUERY_INTERVAL)

    for ret in wait_list:
        try:
            ret.kill()
        except OSError:
            pass
    end_time = time.time()

    for ret in kill_list:
        ret.kill()

    total_time = get_time_form(end_time - start_time)

    common.g_logger.info('done running test: ' + total_time)
    common.g_exit = True
    common.g_query_event.set()
    common.g_query_thread.join()
    compute_write_main(total_time)


def compute_write_main(total_time):
    try:
        eva_utils.compute_ts_stat()
        eva_utils.compute_stat()
    except ZeroDivisionError:
        common.g_logger.error(traceback.print_exc())

    mail = open(common.MAIL_PATH, 'a')
    web = None
    if conf.g_web_report_type != '' and conf.g_web_report:
        web = open(common.WEB_PATH, 'a')
    desp = 'data={datasize} key={ks} value={vs} lg={lg} cf={cf} run_time={t}'.format(
        datasize=conf.g_datasize, ks=get_data_size(conf.g_test_conf[conf.KEY_SIZE]),
        vs=get_data_size(conf.g_test_conf[conf.VALUE_SIZE]), lg=conf.g_test_conf[conf.LG_NUM],
        cf=conf.g_test_conf[conf.CF_NUM], t=total_time, e=str(common.g_force_exit))
    if conf.g_test_conf[conf.MODE] == conf.MODE_SEQ_WRITE or conf.g_test_conf[conf.MODE] == conf.MODE_RAND_WRITE:
        desp += 'write_speed={ws}/TS*M schema={s}'.format(
            ws=int(conf.g_speed_limit) / int(conf.g_test_conf[conf.TS_NUMBER]),
            s=json.dumps(conf.g_test_conf[conf.SCHEMA], separators=(',', ':')))
    elif conf.g_test_conf[conf.MODE] == conf.MODE_READ:
        desp += 'write_speed={ws}/TS*M read_speed={rs}/TS*Qps schema={s}'.format(
            ws=int(conf.g_speed_limit) / int(conf.g_test_conf[conf.TS_NUMBER]),
            rs=int(int(conf.g_test_conf[conf.READ_SPEED_LIMIT]) * int(conf.g_test_conf[conf.TABLET_NUM]) /
                   int(conf.g_test_conf[conf.TS_NUMBER])),
            s=json.dumps(conf.g_test_conf[conf.SCHEMA], separators=(',', ':')))
    elif conf.g_test_conf[conf.MODE] == conf.MODE_SCAN:
        desp += 'write_speed={ws}/TS*M scan_buffer={buffer}/M'.format(
            ws=int(conf.g_speed_limit) / int(conf.g_test_conf[conf.TS_NUMBER]),
            buffer=float(conf.g_test_conf[conf.SCAN_BUFFER])/common.MEGA)
    eva_utils.write_email(mail, web, desp)


def run_write_test():
    eva_utils.table_manipulate(conf.g_test_conf[conf.TABLE_NAME], common.CREATE, conf.g_test_conf[conf.SCHEMA])
    wait_list = []
    for i in range(conf.g_test_conf[conf.TABLET_NUM]):
        prefix = '%04d' % i
        bench_cmd = conf.TERA_BENCH + " | awk -F '\t' '{print \"" + prefix + """\"$1"\t"$2"\t"$3"\t"$4}' """
        if conf.g_test_conf[conf.KV] is True:
            bench_cmd = conf.TERA_BENCH + " | awk -F '\t' '{print \"" + prefix + """\"$1"\t"$2}' """
        cmd = '{bench} | ./tera_mark --mode=w --tablename={name} --type=async --verify=false --entry_limit={limit}'.\
            format(bench=bench_cmd, name=conf.g_test_conf[conf.TABLE_NAME], limit=str(conf.g_test_conf[conf.WRITE_SPEED_LIMIT]))
        print cmd
        fout = open('../tmp/'+str(i)+'.w.out', 'w')
        ferr = open('../tmp/'+str(i)+'.w.err', 'w')
        ret = subprocess.Popen(cmd, stdout=fout, stderr=ferr, shell=True)
        wait_list.append(ret)
    return wait_list, []


def run_scan_test():
    wait_list = []
    fp = open(common.TMP_DIR + 't.deli', 'r')
    deli_str = fp.read()
    deli = deli_str.split('\n')
    deli = filter(None, deli)
    start_end_key = []
    key_pair = ["", ""]
    for tablet in deli:
        key_pair[1] = tablet
        start_end_key.append(key_pair)
        key_pair = [tablet, ""]
    start_end_key.append(key_pair)

    for i in range(len(start_end_key)):
        cmd = './tera_mark --mode=s --tablename={name} --type=async --verify=false --start_key={skey} --end_key={ekey} --buf_size={buffer}'.\
            format(name=conf.g_test_conf[conf.TABLE_NAME], skey=start_end_key[i][0], ekey=start_end_key[i][1], buffer=conf.g_test_conf[conf.SCAN_BUFFER])
        print cmd
        fout = open('../tmp/'+str(i)+'.r.out', 'w')
        ferr = open('../tmp/'+str(i)+'.r.err', 'w')
        ret = subprocess.Popen(cmd, stdout=fout, stderr=ferr, shell=True)
        wait_list.append(ret)
    return wait_list, []


bench_cmd = ''
def run_read_test():
    write_ret_list = []
    read_ret_list = []
    if conf.g_test_conf[conf.WRITE_SPEED_LIMIT] != 0:
        for i in range(conf.g_test_conf[conf.TABLET_NUM]):
            prefix = '%04d' % i

            global bench_cmd
            if conf.g_test_conf[conf.KV] is True:
                tera_bench = './tera_bench --value_size={vs} --compression_ratio=1 --key_seed=111 --value_seed=111 --key_size={ks} --benchmarks=random --num=10000000'.\
                    format(ks=conf.g_test_conf[conf.KEY_SIZE], vs=conf.g_test_conf[conf.VALUE_SIZE])
                bench_cmd = tera_bench + " | awk -F '\t' '{print \"" + prefix + """\"$1"\t"$2}' """
            else:
                tera_bench = './tera_bench --value_size={vs} --compression_ratio=1 --key_seed=111 --value_seed=111 --key_size={ks} --benchmarks=random --cf={cf} --num=10000000'.\
                    format(cf=conf.g_test_conf[conf.CF], ks=conf.g_test_conf[conf.KEY_SIZE], vs=conf.g_test_conf[conf.VALUE_SIZE])
                bench_cmd = tera_bench + " | awk -F '\t' '{print \"" + prefix + """\"$1"\t"$2"\t"$3"\t"$4}' """
            cmd = '{bench} | ./tera_mark --mode=w --tablename={name} --type=async --verify=false --entry_limit={limit}'.\
                format(bench=bench_cmd, name=conf.g_test_conf[conf.TABLE_NAME], limit=str(conf.g_test_conf[conf.WRITE_SPEED_LIMIT]))
            print cmd
            fout = open('../tmp/'+str(i)+'.w.out', 'w')
            ferr = open('../tmp/'+str(i)+'.w.err', 'w')
            ret = subprocess.Popen(cmd, stdout=fout, stderr=ferr, shell=True)
            write_ret_list.append(ret)

    for i in range(conf.g_test_conf[conf.TABLET_NUM]):
        prefix = '%04d' % i
        bench_cmd = conf.TERA_BENCH[0:conf.TERA_BENCH.rfind('--cf')] + " | awk -F '\t' '{print \"" + prefix + """\"$1}' """
        cmd = '{bench} | ./tera_mark --mode=r --tablename={name} --type=async --verify=false --entry_limit={limit}'.\
            format(bench=bench_cmd, name=conf.g_test_conf[conf.TABLE_NAME], limit=conf.g_test_conf[conf.READ_SPEED_LIMIT])
        print cmd
        fout = open('../tmp/'+str(i)+'.r.out', 'w')
        ferr = open('../tmp/'+str(i)+'.r.err', 'w')
        ret = subprocess.Popen(cmd, stdout=fout, stderr=ferr, shell=True)
        read_ret_list.append(ret)
    return read_ret_list, write_ret_list


def handler(signum, frame):
    common.g_force_exit = True
    common.g_exit = True


def get_data_size(size):
    size = int(size)
    post_fix = ['B', 'K', 'M', 'G', 'T']
    sizes = []
    tmp = size
    try:
        for i in range(len(post_fix)):
            sizes.append(tmp % 1024)
            tmp >>= 10
            if tmp == 0:
                break

        if len(sizes) <= 1:
            return str(sizes[0]) + post_fix[0]
        else:
            largest = len(sizes) - 1
            size = sizes[largest] + float(sizes[largest - 1]) / 1024
            return '%03.2f' %  size + post_fix[largest]
    except:
        common.g_logger.info(traceback.print_exc())

def get_time_form(total_time):
    total_time = int(total_time)
    hours = total_time / 3600
    mins = (total_time % 3600) / 60
    return str(hours) + 'h' + str(mins) + 'm'


def main():
    signal.signal(signal.SIGINT, handler)
    signal.signal(signal.SIGTERM, handler)
    eva_utils.init()
    parse_input()
    work()

if __name__ == '__main__':
    main()
