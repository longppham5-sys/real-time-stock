FROM openjdk:8-jdk

ENV HADOOP_VERSION=3.4.1
ENV HADOOP_URL=https://downloads.apache.org/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz
ENV HADOOP_HOME=/opt/hadoop

ENV HADOOP_COMMON_HOME=${HADOOP_HOME}
ENV HADOOP_HDFS_HOME=${HADOOP_HOME}
ENV HADOOP_MAPRED_HOME=${HADOOP_HOME}
ENV HADOOP_YARN_HOME=${HADOOP_HOME}
ENV HADOOP_CONF_DIR=${HADOOP_HOME}/etc/hadoop
ENV PATH=${PATH}:${HADOOP_HOME}/bin

RUN curl --silent --output /tmp/hadoop.tgz ${HADOOP_URL} && tar --directory /opt -xzf /tmp/hadoop.tgz && rm /tmp/hadoop.tgz
RUN ln -s /opt/hadoop-${HADOOP_VERSION} ${HADOOP_HOME}

WORKDIR $HADOOP_HOME

# Hdfs ports
EXPOSE 50010 50020 50070 50075 50090 8020 9000

# Mapred ports
EXPOSE 19888

#Yarn ports
EXPOSE 8030 8031 8032 8033 8040 8042 8088

#Other ports
EXPOSE 49707 2122
