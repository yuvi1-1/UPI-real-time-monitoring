# Source this on the MASTER PC:   source scripts/native/env.sh
# Change MASTER if your master PC has a different hostname (must match /etc/hosts on all 3 PCs).
export MASTER=${MASTER:-master}

export JAVA_HOME=${JAVA_HOME:-/usr/lib/jvm/java-11-openjdk-amd64}
export HADOOP_HOME=/opt/hadoop
export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
export SPARK_HOME=/opt/spark
export KAFKA_HOME=/opt/kafka
export HBASE_HOME=/opt/hbase
export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$SPARK_HOME/bin:$SPARK_HOME/sbin:$KAFKA_HOME/bin:$HBASE_HOME/bin

# read by our Python code (common/config.py)
export KAFKA_BOOTSTRAP=$MASTER:9092
export HBASE_HOST=$MASTER
export HBASE_PORT=9090
export HDFS_URL=hdfs://$MASTER:9820   # 9820: port 9000 is often taken on Windows/WSL
# NOTE: not named SPARK_MASTER - Spark's own scripts treat that variable as an rsync source
export SPARK_MASTER_URL=spark://$MASTER:7077
unset SPARK_MASTER
# driver listens on all interfaces (needed when the master runs in WSL with mirrored networking)
export SPARK_DRIVER_OPTS="--conf spark.driver.bindAddress=0.0.0.0 --conf spark.driver.host=$MASTER"
export PYTHONPATH=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
