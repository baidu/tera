import time

class Hdfs:
	def __init__(self, ip, mode, log_prefix):
		self.ip = ip
		self.mode = mode
		self.path = self.get_log_path(log_prefix)

	def get_log_path(self, log_prefix):
		path = '{pre}/hdfs/{ip}-{mode}-{time}'.format(pre=log_prefix, ip=self.ip, mode=self.mode, time=time.strftime('%Y%m%d%H%M%S'))
		return path

	def to_string(self):
		info = 'hdfs\t{ip}:\t{mode}\tlog:{log}'.format(ip=self.ip, mode=self.mode, log=self.path)
		return info

	def to_cmd(self, docker, masters, slaves):
		cmd = 'docker run -t -d -v {dir}:/opt/share -p 9000:9000 -p 9001:9001 --net=host {docker} /usr/bin/python /opt/hdfs_setup.py --masters {master} --slaves {slaves} --mode {mode}'.\
			format(dir=self.path, docker=docker, master=masters, slaves=slaves, mode=self.mode)
		return cmd

class HdfsCluster:
	def __init__(self, ip_list, num_of_hdfs, log_prefix):
		self.ip_list = ip_list
		self.ip_index = 0
		self.num_of_hdfs = num_of_hdfs
		self.cluster = []
		self.log_prefix = log_prefix
		self.master_ip = self.ip_list[0]
		self.slave_ip = []

	def add_hdfs(self):
		hdfs = Hdfs(self.ip_list[self.ip_index], 'slave', self.log_prefix)
		self.cluster.append(hdfs)
		self.slave_ip.append(hdfs.ip)
		self.ip_index += 1

	def populate_hdfs_cluster(self):
		if self.num_of_hdfs > len(self.ip_list):
			print 'not enough ip address for hdfs!!'
			return False
		master = Hdfs(self.ip_list[0], 'master', self.log_prefix)
		self.cluster.append(master)
		for i in range(self.num_of_hdfs):
			self.add_hdfs()
