FROM debian:jessie  

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

