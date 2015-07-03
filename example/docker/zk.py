import time

class Zookeeper:
	def __init__(self, ip, start_port, end_port, client_port, myid, log_prefix):
		self.ip = ip
		self.start_port = start_port
		self.end_port = end_port
		self.client_port = client_port
		self.myid = myid
		self.path = self.get_log_path(log_prefix)

	def get_log_path(self, log_prefix):
		path = '{pre}/zk/{ip}-{port}-{time}'.format(pre=log_prefix, ip=self.ip,
																								port=self.client_port, time=time.strftime('%Y%m%d%H%M%S'))
		return path

	def to_string(self):
		info = 'zk\t{ip}:{sport}:{eport}\tclient port:{cport}\tmyid:{myid}\tlog:{log}'.\
			format(ip=self.ip, sport=self.start_port, eport=self.end_port, cport=self.client_port, myid=self.myid, log=self.path)
		return info

	def to_cmd(self, ip_str, docker):
		cmd = 'docker run -t -d -v {dir}:/opt/share -p {cport}:{cport} -p {sport}:{sport} -p {eport}:{eport} --net=host {docker} /usr/bin/python /opt/zk_setup.py --servers {ip} --port {cport} --myid {myid}'.\
						format(dir=self.path, cport=self.client_port, sport=self.start_port,
									 eport=self.end_port, docker=docker, ip=ip_str, myid=self.myid)
		return cmd

class ZkCluster:
	def __init__(self, ip_list, num_of_zk, log_prefix):
		self.ip_list = ip_list
		self.ip_index = 0
		self.start_port = 2888
		self.end_port = 3888
		self.client_port = 2181
		self.myid = 1
		self.num_of_zk = num_of_zk
		self.cluster = []
		self.ip_zk = []
		self.ip_tera = []
		self.log_prefix = log_prefix

	def add_zk(self):
		zk = Zookeeper(self.ip_list[self.ip_index % len(self.ip_list)], str(self.start_port),
									 str(self.end_port), str(self.client_port), str(self.myid), self.log_prefix)
		self.cluster.append(zk)
		self.ip_index += 1
		self.start_port += 1
		self.end_port += 1
		self.client_port += 1
		self.myid += 1
		self.ip_zk.append(zk.ip)
		self.ip_tera.append(zk.ip + ':' + str(zk.client_port))

	def populate_zk_cluster(self):
		for i in range(self.num_of_zk):
			self.add_zk()

