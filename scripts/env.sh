#!/usr/bin/env bash
# 航班延误平台 — 统一环境变量。所有脚本 source 本文件。
export PROJECT_DIR=/home/alone/projects/flights/flight-delay-platform
export FLIGHT_CSV=/home/alone/projects/flights/flights_sample_3m.csv

export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
export HADOOP_HOME=/home/alone/apps/hadoop-3.3.6
export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
export YARN_CONF_DIR=$HADOOP_CONF_DIR
export HADOOP_COMMON_HOME=$HADOOP_HOME
export HADOOP_HDFS_HOME=$HADOOP_HOME
export HADOOP_MAPRED_HOME=$HADOOP_HOME
export HADOOP_YARN_HOME=$HADOOP_HOME

export HBASE_HOME=/home/alone/apps/hbase-2.5.8-hadoop3
export KAFKA_HOME=/home/alone/apps/kafka_2.13-3.6.1
export FLUME_HOME=/home/alone/apps/apache-flume-1.11.0-bin
export SQOOP_HOME=/home/alone/apps/sqoop-1.4.7
export SPARK_HOME=/home/alone/apps/spark-3.5.1-bin-hadoop3

export PYSPARK_PYTHON=$PROJECT_DIR/.venv/bin/python
export PYSPARK_DRIVER_PYTHON=$PYSPARK_PYTHON
export SPARK_LOCAL_IP=127.0.0.1
export LANG=C.UTF-8
export LC_ALL=C.UTF-8
export PYTHONIOENCODING=utf-8
export UV_HTTP_TIMEOUT=180

# 直接 python 启动时可导入 Spark 自带的 pyspark（不安装重复的 wheel）
export PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/pyspark.zip:$SPARK_HOME/python/lib/py4j-0.10.9.7-src.zip:${PYTHONPATH:-}

export PATH=$JAVA_HOME/bin:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$HBASE_HOME/bin:$KAFKA_HOME/bin:$FLUME_HOME/bin:$SQOOP_HOME/bin:$SPARK_HOME/bin:$PATH

# 下载代理；本地流量必须绕过
export http_proxy=http://192.168.0.100:20808
export https_proxy=$http_proxy
export HTTP_PROXY=$http_proxy
export HTTPS_PROXY=$http_proxy
export no_proxy=localhost,127.0.0.1,::1,ubuntu-dev
export NO_PROXY=$no_proxy

[ -f "$PROJECT_DIR/scripts/env.local.sh" ] && . "$PROJECT_DIR/scripts/env.local.sh"
