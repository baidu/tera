FROM ubuntu:latest  

RUN apt-get update && apt-get install -y openjdk-7-jre-headless wget
RUN wget -q -O - http://apache.mirrors.pair.com/zookeeper/zookeeper-3.4.6/zookeeper-3.4.6.tar.gz | tar -xzf - -C /opt \
    && mv /opt/zookeeper-3.4.6 /opt/zookeeper \
    && cp /opt/zookeeper/conf/zoo_sample.cfg /opt/zookeeper/conf/zoo.cfg \
    && mkdir -p /tmp/zookeeper

ENV JAVA_HOME /usr/lib/jvm/java-7-openjdk-amd64

RUN apt-get -y install python
RUN apt-get -y install python-dev
RUN apt-get -y install gcc
RUN apt-get -y install make
RUN cd /opt/zookeeper/src/c && ./configure && make && make install

ADD workspace /opt/workspace

RUN wget https://bootstrap.pypa.io/ez_setup.py -O - | python
RUN cd /opt/workspace/argparse-1.3.0/ && python setup.py install
RUN cd /opt/workspace/zkpython-0.4/ && python setup.py install
RUN cp /opt/workspace/zk_setup.py /opt/
RUN mkdir /opt/share

ENV LD_LIBRARY_PATH /usr/local/lib/

WORKDIR /opt/

ADD hadoop-1.2.1 /opt/hadoop-1.2.1
RUN cp /opt/hadoop-1.2.1/hdfs_setup.py /opt
RUN /opt/hadoop-1.2.1/bin/hadoop-config.sh

RUN apt-get -y install build-essential
RUN cd /opt/workspace/zlib-1.2.8 && ./configure && make && make install
RUN apt-get -y install libssl-dev
ENV PATH /usr/bin:/bin:/opt/hadoop-1.2.1/bin:/usr/local/sbin:/usr/sbin:/sbin
ENV LD_LIBRARY_PATH $JAVA_HOME/jre/lib/amd64/server:$JAVA_HOME/jre/lib/amd64/:/opt/hadoop-1.2.1/c++/Linux-amd64-64/lib:/usr/local/lib/
RUN cp /opt/workspace/ifconfig /sbin/

ADD share/tera /opt/tera
RUN mv /opt/tera/bin/tera_setup.py /opt
RUN apt-get -y install vim
