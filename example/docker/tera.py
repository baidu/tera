import time
import os

class Tera:
	def __init__(self, ip, port, mode, log_prefix):
		self.ip = ip
		self.port = port
		self.mode = mode
		self.path = self.get_log_path(log_prefix)

	def get_log_path(self, log_prefix):
		path = '{pre}/tera/{ip}-{port}-{mode}-{time}'.format(pre=log_prefix, ip=self.ip, port=self.port,
																												 mode=self.mode, time=time.strftime('%Y%m%d%H%M%S'))
		os.makedirs(path)
		return path

	def to_string(self):
		info = 'tera\t{ip}:{port}\t{mode}\tlog:{log}'.format(ip=self.ip, port=self.port, mode=self.mode, log=self.path)
		return info

	def to_cmd(self, docker, zk):
		cmd = 'docker run -t -d -v {dir}:/opt/share -p {port}:{port} --net=host {docker} /usr/bin/python /opt/tera_setup.py --ip {ip} --zk {zk} --port {port} --mode {mode}'.\
			format(dir=self.path, port=self.port, ip=self.ip, zk=zk, docker=docker, mode=self.mode)
		return cmd

class TeraCluster():
	def __init__(self, ip_list, num_of_tera, log_prefix):
		self.ip_list = ip_list
		self.ip_index = 0
		self.port = 2200
		self.num_of_tera = num_of_tera
		self.log_prefix = log_prefix
		self.cluster = []

	def add_tera(self):
		tera = Tera(self.ip_list[self.ip_index % len(self.ip_list)], str(self.port), 'tabletnode', self.log_prefix)
		self.cluster.append(tera)
		self.ip_index += 1
		self.port += 1

	def populate_tera_cluster(self):
		for i in range(self.num_of_tera):
			self.add_tera()
		master = Tera(self.ip_list[0], '1100', 'master', self.log_prefix)
		self.cluster.append(master)
