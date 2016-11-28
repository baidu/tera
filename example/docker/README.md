Use Docker to deploy Tera
===============
##Prerequisites
###Install Docker
* For docker installation, please refer to [Docker](https://docs.docker.com/). Ubuntu's Docker installation:

  ```
    wget -qO- https://get.docker.com/ | sh
  ```

* Manage Docker as a non-root user
 
  To create the docker group and add your user:

	1. Log into Ubuntu as a user with sudo privileges.
	2. Create the docker group. bash $ sudo groupadd docker
	3. Add your user to the docker group.
		
		```$ sudo usermod -aG docker $USER```
	
	4. Log out and log back in so that your group membership is re-evaluated.


###Enable Password-less SSH
* Generate the SSH keys: ssh-keygen -t rsa
	
		Generating public/private key pair.
		Enter file in which to save the key (/xxx/.ssh/id_rsa):
		Enter passphrase (empty for no passphrase):
		Enter same passphrase again:
		Your identification has been saved in /xxx/.ssh/id_rsa.
		Your public key has been saved in /xxx/.ssh/id_rsa.pub.
	
* Copy the key to each Node:

		ssh-copy-id {username}@node1
		ssh-copy-id {username}@node2
		ssh-copy-id {username}@node3

###Install Python and Tera Image
* Use exsample/docker/install.sh to install python package and download Tera docker image. For example:
 
  ```
     //Cluster has 3 nodes: 
     //		deplying node is 192.168.100.2; 
     //		other two slave node is 192.168.100.3 and 192.168.100.4
     //
     // exec installation like (no need to setup installation in deplying node):
    sh install.sh 192.168.100.3 192.168.100.4
  ```


##Running
###Configuration


* Use example/docker/conf as a template to configure your Tera cluster, for example:

  ```
  ip   	: ip address, use the colon as a separator
  hdfs  ：configure the number of hdfs's datanode
  zk    ：configure the number of zookeeper's node
  tera  ：configure the number of Tera's tablet servers
  log_prefix：log file's dir ($HOME by default)
  
  conf template:
  {	
  	"ip":"192.168.100.2:192.168.100.3:192.168.100.4", 
  	"hdfs":3, 
  	"zk":3, 
  	"tera":3
  }
  ```

###Execution

* Fast startup

  use /example/docker/cluster_setup.py, it will create a Tera cluster automatically：
  
  ```
    hdfs：namenode*1，datanode*1
    zk：  standalone
    tera：master*1， tabletnode*1
    log： $HOME
  ```
  
* Self-configure Tera cluster

  cluster_setup.py will start zk, hdfs and tera together by default; User can use flags to setup one or more cluster respectively。


      --help：  show help manual
      --conf：  cluster's configuration information, see below
      --docker：Tera's docker image ID
      --zk：    startup zookeeper cluster
      --hdfs：  startup hdfs cluster
      --tera：  startup tera cluster

      
  for example：

  ```
  // use TeraImage and my_config file to startup Tera cluster
  python cluster_setup.py --conf my_config --docker TeraImage  
  
  // startup zk and hdfs
  python cluster_setup.py --zk --hdfs
  ```

