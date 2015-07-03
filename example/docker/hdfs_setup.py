import subprocess
import argparse
import time
from xml.dom.minidom import Document

conf_path_prefix = '/opt/hadoop-1.2.1/conf/'
share_path_prefix = '/opt/share/'

def parse_input():
	parser = argparse.ArgumentParser()
	parser.add_argument('--masters', required=True, type=str, help='Name node ip')
	parser.add_argument('--slaves', nargs='+', required=True, type=str, help='data node ip')
	parser.add_argument('--mode', required=True, type=str, choices=['master', 'slave'], help='start hdfs as master/slave')
	parser.add_argument('--tera', action='store_true')
	args = parser.parse_args()
	return args

def write_property(doc, name, value, final):
	prop = doc.createElement('property')
	
	name_node = doc.createElement('name')
	name_text = doc.createTextNode(name)
	name_node.appendChild(name_text)
	value_node = doc.createElement('value')
	value_text = doc.createTextNode(value)
	value_node.appendChild(value_text)
	
	prop.appendChild(name_node)
	prop.appendChild(value_node)
	
	if final is True:
		final_node = doc.createElement('final')
		final_text = doc.createTextNode('true')
		final_node.appendChild(final_text)
		prop.appendChild(final_node)
	
	return prop

def get_doc():
	doc = Document()
	site = doc.createElement('configuration')
	doc.appendChild(site)
	return doc, site

def write_core_site_xml(args):
	doc, coresite = get_doc()
	ip = 'hdfs://{name_node}:9000'.format(name_node=args.masters)	
	prop = write_property(doc, 'fs.default.name', ip, True)
	coresite.appendChild(prop)
	prop = write_property(doc, 'hadoop.tmp.dir', share_path_prefix + 'tmp', False)
	coresite.appendChild(prop)

	f = open(conf_path_prefix + 'core-site.xml', 'wb')
	f.write(doc.toprettyxml(indent = ''))
	f.close()

def write_hdfs_site_xml():
	doc, hdfssite = get_doc()	
	prop = write_property(doc, 'dfs.name.dir', share_path_prefix + 'name', True)
	hdfssite.appendChild(prop)
	prop = write_property(doc, 'dfs.data.dir', share_path_prefix + 'data', True)
	hdfssite.appendChild(prop)
	prop = write_property(doc, 'dfs.replication', '3', True)
	hdfssite.appendChild(prop)

	f = open(conf_path_prefix + 'hdfs-site.xml', 'wb')
	f.write(doc.toprettyxml(indent = ''))
	f.close()

def write_mapred_site_xml(args):
	doc, mapredsite = get_doc()
	ip = 'hdfs://{name_node}:9001'.format(name_node=args.masters)
	prop = write_property(doc, 'mapred.job.tracker', ip, False)
	mapredsite.appendChild(prop)

	f = open(conf_path_prefix + 'mapred-site.xml', 'wb')
	f.write(doc.toprettyxml(indent = ''))
	f.close()

def write_hadoop_env():
	f = open(conf_path_prefix + 'hadoop-env.sh', 'a')
	f.write('\nexport JAVA_HOME="/usr/lib/jvm/java-1.7.0-openjdk-amd64"\n')
	f.close()

def write_maters_slaves(args):
	f = open(conf_path_prefix + 'masters', 'wb')
	f.write(args.masters)
	f.close()

	f = open(conf_path_prefix + 'slaves', 'wb')
	slaves = '\n'.join(args.slaves)
	f.write(slaves)
	f.close()

def start_hdfs(args):
	cmd_prefix = '/opt/hadoop-1.2.1/bin/hadoop-daemon.sh --config /opt/hadoop-1.2.1/'
	if args.mode == 'master':
		p = subprocess.Popen('/opt/hadoop-1.2.1/bin/hadoop namenode -format', stdout=subprocess.PIPE, shell=True)
		print p.stdout.read()
		p = subprocess.Popen(cmd_prefix + 'conf start namenode', stdout=subprocess.PIPE, shell=True)
		print p.stdout.read()
		p = subprocess.Popen(cmd_prefix + 'conf --hosts masters start secondarynamenode', stdout=subprocess.PIPE, shell=True)
		print p.stdout.read()
	else:
		p = subprocess.Popen(cmd_prefix + 'conf start datanode', stdout=subprocess.PIPE, shell=True)
		print p.stdout.read()

def doing_nothing():
	while True:
		time.sleep(1000)

def main():
	args = parse_input()
	write_core_site_xml(args)
	write_hdfs_site_xml()
	#write_mapred_site_xml(args)
	write_hadoop_env()
	write_maters_slaves(args)
	if args.tera is False:
		start_hdfs(args)
		doing_nothing()

if __name__ == '__main__':
	main()
