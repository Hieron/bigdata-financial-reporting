#!/bin/bash

start_ssh() {
    echo "Starting $NODE_TYPE ssh..."
    service ssh start
}

if [ "$NODE_TYPE" = "coordinator" ]; then
    start_ssh

    if [ ! -d "/opt/hadoop_data/namenode/current" ]; then
        echo "Formatting $NODE_TYPE hdfs..."
        $HADOOP_HOME/bin/hdfs namenode -format -force -nonInteractive
    fi

    echo "Starting $NODE_TYPE hdfs..."
    $HADOOP_HOME/bin/hdfs --daemon start namenode

    echo "Starting $NODE_TYPE spark..."
    $SPARK_HOME/sbin/start-master.sh

    tail -f /dev/null

elif [ "$NODE_TYPE" = "executor" ]; then
    start_ssh

    if [ -z "$COORDINATOR_URL" ]; then
        echo "Erro: COORDINATOR_URL not defined."
        exit 1
    fi

    echo "Starting $NODE_TYPE hdfs..."
    $HADOOP_HOME/bin/hdfs --daemon start datanode

    echo "Starting $NODE_TYPE spark..."
    $SPARK_HOME/sbin/start-worker.sh $COORDINATOR_URL

    tail -f /dev/null

elif [ "$NODE_TYPE" = "controller" ]; then
    start_ssh
    tail -f /dev/null

else
    echo "Erro: NODE_TYPE must be coordinator, executor or controller."
    exit 1
fi
