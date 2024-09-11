FROM openjdk:11-jdk

ENV SPARK_VERSION=3.5.2
ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin

ENV HADOOP_VERSION=3.4.0
ENV HADOOP_HOME=/opt/hadoop
ENV PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin

RUN apt-get update && \ 
    apt-get install -y \
    curl bash procps python3 python3-pip ssh pdsh && \
    rm -rf /var/lib/apt/lists/*

RUN mkdir -p $SPARK_HOME && \
    HADOOP_MAJOR_VERSION=$(echo $HADOOP_VERSION | cut -d'.' -f1) && \
    curl -L https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_MAJOR_VERSION}.tgz -o spark.tgz && \
    tar -xzf spark.tgz --strip-components=1 -C $SPARK_HOME && \
    rm spark.tgz

RUN mkdir -p $HADOOP_HOME && \
    curl -O https://downloads.apache.org/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz && \
    tar -xzf hadoop-${HADOOP_VERSION}.tar.gz --strip-components=1 -C ${HADOOP_HOME} && \
    rm hadoop-${HADOOP_VERSION}.tar.gz

COPY ./config/core-site.xml ./config/hdfs-site.xml $HADOOP_HOME/etc/hadoop/

RUN ssh-keygen -t rsa -f ~/.ssh/id_rsa -N '' && \
    cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys && \
    chmod 0600 ~/.ssh/authorized_keys && \
    echo "Host *\n   StrictHostKeyChecking no" > /root/.ssh/config

EXPOSE 4040 5000 6000 7077 8080 9000 9864 9870 18080

COPY entrypoint.sh /entrypoint.sh

RUN chmod +x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]