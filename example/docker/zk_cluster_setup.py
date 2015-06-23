import paramiko
import argparse
import traceback

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
	
	ip_dicts = []
	start_port = 2888
	end_port = 3888
	client_port = 2181
	myid = 1
	while True:
		try:
			ip, user, path = fp.readline().split(' ')
			ip_dicts.append([ip, {'user': user, 'start_port': str(start_port), 'end_port': str(end_port), 'client_port': str(client_port), 'myid': str(myid), 'path': path[:-1]}])
			start_port += 1
			end_port += 1
			client_port += 1
			myid += 1
		except:
			break
	return ip_dicts

def start_dockers(ip_dicts):
	login_port = 22
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
		print details
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
	
def main():
	args = parse_input()
	ip_dicts = read_ip_list(args)
	start_dockers(ip_dicts)

if __name__ == '__main__':
	main()

