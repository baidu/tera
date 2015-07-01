import json
import argparse
import paramiko
import traceback
import zk
import hdfs
import tera

class SSH():
	def __init__(self):
		self.s = paramiko.SSHClient()
		self.s.load_system_host_keys()
		self.s.set_missing_host_key_policy(paramiko.AutoAddPolicy())

	def run_cmd(self, ip, cmd):
		try:
			self.s.connect(ip)
			stdin, stdout, stderr = self.s.exec_command(cmd)
			self.s.close()
		except:
			traceback.print_exc()
		return stdin, stdout, stderr

def parse_input():
	parser = argparse.ArgumentParser()
	parser.add_argument('file', type=str, help='A file describes the zk cluster')
	parser.add_argument('--docker', type=str, required=True, help='ID of the docker image')
	parser.add_argument('--zk', action='store_true', help='Launch zk')
	parser.add_argument('--hdfs', action='store_true', help='Launch hdfs')
	parser.add_argument('--tera', action='store_true', help='Launch tera')
	args = parser.parse_args()
	return args

def config(args):
	config = json.load(open(args.file, 'r'))
	ip_list = config['ip'].split(':')
	log_prefix = config['log_prefix']
	zk_cluster = zk.ZkCluster(ip_list, config['zk'], log_prefix)
	zk_cluster.populate_zk_cluster()
	for z in zk_cluster.cluster:
		print z.to_string()

	hdfs_cluster = hdfs.HdfsCluster(ip_list, config['hdfs'], log_prefix)
	ret = hdfs_cluster.populate_hdfs_cluster()
	if ret is False:
		exit(1)
	for h in hdfs_cluster.cluster:
		print h.to_string()

	tera_cluster = tera.TeraCluster(ip_list, config['tera'], log_prefix)
	tera_cluster.populate_tera_cluster()
	for t in tera_cluster.cluster:
		print t.to_string()

	return zk_cluster, hdfs_cluster, tera_cluster

def start_zk(args, zk_cluster, s):
	if (args.hdfs or args.tera) and not args.zk:
		return
	for zk_instance in zk_cluster.cluster:
		#print zk_instance.to_string()
		cmd = zk_instance.to_cmd(' '.join(zk_cluster.ip_zk), args.docker)
		print cmd
		s.run_cmd(zk_instance.ip, cmd)

def start_hdfs(args, hdfs_cluster, s):
	if (args.zk or args.tera) and not args.hdfs:
		return
	for hdfs_instance in hdfs_cluster.cluster:
		#print hdfs_instance.to_string()
		cmd = hdfs_instance.to_cmd(args.docker, hdfs_cluster.master_ip, ' '.join(hdfs_cluster.slave_ip))
		print cmd
		s.run_cmd(hdfs_instance.ip, cmd)

def start_tera(args, tera_cluster, zk_cluster, s):
	if (args.zk or args.hdfs) and not args.tera:
		return
	for tera_instance in tera_cluster.cluster:
		#print tera_instance.to_string()
		cmd = tera_instance.to_cmd(args.docker, ','.join(zk_cluster.ip_tera))
		print cmd
		s.run_cmd(tera_instance.ip, cmd)

def main():
	args = parse_input()
	zk_cluster, hdfs_cluster, tera_cluster = config(args)
	s = SSH()
	start_zk(args, zk_cluster, s)
	start_hdfs(args, hdfs_cluster, s)
	start_tera(args, tera_cluster, zk_cluster, s)

if __name__ == '__main__':
	main()
