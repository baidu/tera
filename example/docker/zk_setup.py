import argparse
import os
import subprocess
import zookeeper
import socket
import time
import traceback

def parse_input():
	parser = argparse.ArgumentParser()
	parser.add_argument('--servers', required=True, nargs='+', type=str, help='server list, must be in order')
	parser.add_argument('--port', required=True, type=int, help='client port')
	parser.add_argument('--myid', required=True, type=int, help='myid for zk')
	parser.add_argument('--cluster', type=str, default='tera',help='tera cluster name, default is "tera"')
	args = parser.parse_args()
	return args

def config(args):
	path = os.path.abspath('.')
	zk_path = path + '/zookeeper'
	log_path = path + '/share/log'
	data_path = path + '/share/data'
	os.mkdir(log_path)
	os.mkdir(data_path)

	conf_path = zk_path + '/conf/zoo.cfg'
	fp = open(conf_path, 'wb')
	fp.write('tickTime=2000\ninitLimit=10\nsyncLimit=5\n')
	start_port = 2888
	end_port = 3888
	if args.servers is not None:
		for i in range(len(args.servers)):
			server_name = "server.{id}={server}:{start_port}:{end_port}\n".format(id=str(i + 1), server=args.servers[i], start_port=start_port, end_port=end_port)
			fp.write(server_name)
			start_port += 1
			end_port += 1
	fp.write('dataDir=' + data_path + '\n')
	fp.write('dataLogDir=' + log_path + '\n')
	fp.write('clientPort=' + str(args.port) + '\n')
	fp.close()

	myid_path = data_path + '/myid'
	fp = open(myid_path, 'wb')
	fp.write(str(args.myid))
	fp.close()
	return zk_path

def start_zk(args, zk_path):
	print zk_path + '/bin/zkServer.sh start'
	subprocess.Popen(zk_path + '/bin/zkServer.sh start', shell=True)
	hostname = socket.gethostname()
        ip = socket.gethostbyname(hostname)
        print ip + ':' + str(args.port)
	if args.myid == 1:
		while True:
			time.sleep(2)
			handler = zookeeper.init(ip + ':' + str(args.port))
			try:
				zookeeper.create(handler,"/tera","",[{"perms":0x7f,"scheme":"world","id":"anyone"}],0)
				zookeeper.create(handler,"/tera/master-lock","",[{"perms":0x7f,"scheme":"world","id":"anyone"}],0)
				zookeeper.create(handler,"/tera/zk","",[{"perms":0x7f,"scheme":"world","id":"anyone"}],0)
				zookeeper.create(handler,"/tera/kick","",[{"perms":0x7f,"scheme":"world","id":"anyone"}],0)
				zookeeper.close(handler)
				break
			except:
				print '#######################\nkeep trying!!\n##########################\n'
				traceback.print_exc()
				continue

def doing_nothing():
	while True:
		time.sleep(100000)

def main():
	args = parse_input()
	zk_path = config(args)
	start_zk(args, zk_path)
	doing_nothing()

if __name__ == '__main__':
	main()
