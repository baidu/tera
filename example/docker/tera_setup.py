import argparse
import subprocess
import os
import time
import hdfs_setup

def parse_input():
	parser = argparse.ArgumentParser()
	parser.add_argument('--ip', required=True, type=str, help='Localhost ip address')
	parser.add_argument('--port', required=True, type=str, help='A file describes the zk cluster')
	parser.add_argument('--mode', required=True, type=str, choices=['master', 'tabletnode'], help='tera instnace mode')
	parser.add_argument('--zk', required=True, type=str, help='zk list, ip:port,ip:port')
	parser.add_argument('--hdfs_master', required=True, type=str, help='hdfs master ip')
	parser.add_argument('--hdfs_slaves', required=True, type=str, help='hdfs slaves ip')
	args = parser.parse_args()
	return args

def write_config(args):
	port_op = {'master': '--tera_master_port=', 'tabletnode': '--tera_tabletnode_port='}
	fp = open('/opt/tera/conf/tera.flag', 'a')
	fp.write(port_op[args.mode] + str(args.port) + '\n')
	fp.write('--tera_zk_addr_list=' + args.zk + '\n')
	fp.close()

	os.mkdir('/opt/share/log')
	os.mkdir('/opt/share/teracache')

	cmd = 'python hdfs_setup.py --masters {mip} --slaves {sip} --mode master --tera'.\
		format(mip=args.hdfs_master, sip=args.hdfs_slaves.replace(':', ' '))
	p = subprocess.Popen(cmd, stdout=subprocess.PIPE, shell=True)

def start_tera(args):
	if args.mode == 'tabletnode':
		p = subprocess.Popen('/opt/tera/bin/tabletnode ' + args.ip, stdout=subprocess.PIPE, shell=True)
	else:
		p = subprocess.Popen('/opt/tera/bin/master ' + args.ip, stdout=subprocess.PIPE, shell=True)

def doing_nothing():
	while True:
		time.sleep(1000)

def main():
	args = parse_input()
	write_config(args)
	start_tera(args)
	doing_nothing()

if __name__ == '__main__':
	main()
