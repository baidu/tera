import paramiko
import argparse
import traceback

ZK = 'zk'
HDFS = 'hdfs'

def parse_input():
        parser = argparse.ArgumentParser()
        parser.add_argument('file', type=str, help='A file describes the zk cluster')
        args = parser.parse_args()
        return args

def read_ip_list(args):
	try:
		fp = open(args.file, 'r')
	except:
		traceback.print_exc()
	
	zk_ip_dicts = []
	hdfs_ip_dicts = {'master':[], 'slave':[]}
	start_port = 2888
	end_port = 3888
	client_port = 2181
	myid = 1
	while True:
		try:
			comp = fp.readline().split(' ')
			if comp[0].startswith(ZK):
				zk_ip_dicts.append([comp[1], {'start_port': str(start_port), 'end_port': str(end_port), 'client_port': str(client_port), 'myid': str(myid), 'path': comp[2][:-1]}])
				start_port += 1
				end_port += 1
				client_port += 1
				myid += 1
			elif comp[0].startswith(HDFS):
				if comp[3].startswith('master'):
					hdfs_ip_dicts['master'].append([comp[1], {'path': comp[2]}]) 
				else:
					hdfs_ip_dicts['slave'].append([comp[1], {'path': comp[2]}])
			else:
				break
		except:
			break
	if hdfs_ip_dicts['slave'] != [] and hdfs_ip_dicts == []:
		print 'must have a master!'
		return
	return zk_ip_dicts, hdfs_ip_dicts

def start_zks(ip_dicts):
	if ip_dicts == []:
		return
	s=paramiko.SSHClient()
	s.load_system_host_keys()
	s.set_missing_host_key_policy(paramiko.AutoAddPolicy())
	ips = []
	for item in ip_dicts:
		ips.append(item[0])
	ips = ' '.join(ips)
	for details in ip_dicts:
		ip = details[0]
		details = details[1]
		try:
			s.connect(ip)
			cmd = 'docker run -t -d -v {dir}:/opt/share -p {cport}:{cport} -p {sport}:{sport} -p {eport}:{eport} --net=host d8 /usr/bin/python /opt/zk_setup.py --servers {ip} --port {cport} --myid {myid}'.\
				format(dir=details['path'], cport=details['client_port'], sport=details['start_port'], eport=details['end_port'], ip=ips, myid=details['myid'])
			stdin, stdout, stderr = s.exec_command(cmd)
			print cmd
			print stdout.read()
			print '\n', stderr.read()
			s.close()
		except:
			traceback.print_exc()
	
def start_hdfs(ip_dicts):
	if ip_dicts == {}:
		return
	s = paramiko.SSHClient()
	s.load_system_host_keys()
	s.set_missing_host_key_policy(paramiko.AutoAddPolicy())
	ips = []
	master = ip_dicts['master'][0]
	master_ip = master[0]
	master_details = master[1]
	slave_list = ip_dicts['slave']
	for item in slave_list:
		ips.append(item[0])
	ips = ' '.join(ips)
	
	cmd = 'docker run -t -d -v {dir}:/opt/share -p 9000:9000 -p 9001:9001 --net=host 67 /usr/bin/python /opt/hdfs_setup.py --masters {master} --slaves {slaves} --mode master'.\
		format(dir=master_details['path'], master=master_ip, slaves=ips)
	print cmd
	s.connect(master_ip)
	#stdin, stdout, stderr = s.exec_command(cmd)
	#print stdout.read()
	#print '\n', stderr.read()
	s.close()

	for slave in slave_list:
		slave_ip = slave[0]
		slave_details = slave[1]
		cmd = 'docker run -t -d -v {dir}:/opt/share -p 9000:9000 -p 9001:9001 --net=host 67 /usr/bin/python /opt/hdfs_setup.py --masters {master} --slaves {slaves} --mode slave'.\
			format(dir=slave_details['path'], master=master_ip, slaves=ips)
		print cmd
		s.connect(slave_ip)
		#stdin, stdout, stderr = s.exec_command(cmd)
		#print stdout.read()
		#print '\n', stderr.read()
		s.close()

def main():
	args = parse_input()
	zk_ip_dicts, hdfs_ip_dicts = read_ip_list(args)
	start_zks(zk_ip_dicts)
	start_hdfs(hdfs_ip_dicts)

if __name__ == '__main__':
	main()
