import argparse
import subprocess
import os
import time

def parse_input():
	parser = argparse.ArgumentParser()
	parser.add_argument('--port', required=True, type=str, help='A file describes the zk cluster')
	parser.add_argument('--mode', required=True, type=str, choices=['master', 'tabletnode'], help='tera instnace mode')
	parser.add_argument('--zk', required=True, type=str, help='zk list, ip:port,ip:port')
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

def start_tera(args):
	if args.mode == 'tabletnode':
		p = subprocess.Popen('/opt/tera/bin/tabletnode', stdout=subprocess.PIPE, shell=True)
	else:
		p = subprocess.Popen('/opt/tera/bin/master', stdout=subprocess.PIPE, shell=True)

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
