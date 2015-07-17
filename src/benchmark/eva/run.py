import sys
import subprocess
import time
import threading
import traceback

from bin import eva_var
from bin import eva_utils


def parse_input():
    if sys.argv[1] == 'seq_write':
        eva_var.DATA_PATH = '../seq.eva/'
        eva_var.g_mode = eva_var.MODE_WRITE
        eva_var.EMAIL_BLOCK_TITLE = 'Sequential Write'
    if sys.argv[1] == 'ran_write':
        eva_var.DATA_PATH = '../ran.eva/'
        eva_var.g_mode = eva_var.MODE_WRITE
        eva_var.EMAIL_BLOCK_TITLE = 'Random Write'
    if sys.argv[1] == 'ran_read':
        eva_var.DATA_PATH = '../ran.read/'
        eva_var.g_mode = eva_var.MODE_READ
        eva_var.EMAIL_BLOCK_TITLE = 'Random Read'
    if sys.argv[1] == 'scan':
        eva_var.g_mode = eva_var.MODE_SCAN
        eva_var.EMAIL_BLOCK_TITLE = 'Scan'

    eva_var.TABLET_NUM = int(sys.argv[2])
    eva_var.SPLIT_SIZE = int(sys.argv[3])
    eva_var.g_logger.info('running tera_mark in ' + sys.argv[1] + ' mode, split size = ' + sys.argv[3])

def work():
    eva_var.g_next_query_time = time.time() + eva_var.QUERY_INTERVAL
    eva_var.g_query_thread = threading.Thread(target=eva_utils.query)
    eva_var.g_query_event = threading.Event()

    if eva_var.g_mode == eva_var.MODE_WRITE:
        eva_utils.table_manipulate('test', eva_var.CREATE,
                                   schema=eva_var.DATA_PATH+'t.schema.'+str(eva_var.SPLIT_SIZE),
                                   delimiter=eva_var.DATA_PATH+'t.deli')
    run_test()

    eva_var.g_query_thread.join()
    # eva_utils.table_manipulate('test', eva_var.DROP)

def run_test():
    eva_var.g_query_thread.start()
    eva_var.g_logger.info('running tera_mark with {n} tablets'.format(n=eva_var.TABLET_NUM))
    if eva_var.g_mode == eva_var.MODE_SCAN:
        cmd = './tera_mark --mode=s --tablename=test --type=async --verify=false'
        fout = open('../tmp/scan.out', 'w')
        ret = subprocess.Popen(cmd, stdout=fout, shell=True)
        ret.wait()
    else:
        wait_list = []
        for i in range(eva_var.TABLET_NUM):
            f = str(i) + '.data'
            cmd = 'cat {data} | ./tera_mark --mode={m} --tablename=test --type=async --verify=false'.\
                format(m=eva_var.g_mode, data=eva_var.DATA_PATH+f)
            print cmd
            fout = open('../tmp/'+f+'.out', 'w')
            ferr = open('../tmp/'+f+'.err', 'w')
            ret = subprocess.Popen(cmd, stdout=fout, stderr=ferr, shell=True)
            wait_list.append(ret)

        for ret in wait_list:
            ret.wait()

    eva_var.g_logger.info('done running test')
    eva_var.g_exit = True
    eva_var.g_query_event.set()

    try:
        eva_utils.comput_ts_stat()
        eva_utils.comput_stat()
    except ZeroDivisionError:
        eva_var.g_logger.error(traceback.print_exc())

    mail = open(eva_var.MAIL_PATH, 'a')
    desp = 'key={ks}B value={vs}B entry_number={e}M size={s}G split_size={sp}G ts_number={ts}'.\
        format(ks=eva_var.KEY_SIZE, vs=eva_var.VALUE_SIZE, e=eva_var.ENTRY_NUM*eva_var.TABLET_NUM,
               s=eva_var.ENTRY_NUM*eva_var.TABLET_NUM*eva_var.ENTRY_SIZE/1000,
               sp=eva_var.SPLIT_SIZE, ts = eva_var.TABLET_NUM)
    eva_utils.write_email(mail, desp)
    mail.close()

def main():
    eva_utils.init()
    parse_input()
    work()

if __name__ == '__main__':
    main()
