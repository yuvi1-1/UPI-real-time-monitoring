# Real cluster setup: 1 master PC + 2 worker PCs

This is the setup the course asks for. It takes about **1.5–2 hours** the first time. Do it together; one person types on the master, the other on the workers.

| PC | Hostname | Runs |
|----|----------|------|
| PC 1 | `master`  | HDFS NameNode, Spark Master, Kafka broker, HBase + Thrift, generator, Spark driver, dashboard |
| PC 2 | `worker1` | HDFS DataNode, Spark Worker |
| PC 3 | `worker2` | HDFS DataNode, Spark Worker |

All three: **Ubuntu 22.04/24.04** (a VM in VirtualBox with *Bridged* networking also works), same Wi‑Fi/LAN, ≥ 4 GB RAM (8 GB for master).

Versions used: Java 11, Hadoop 3.3.6, Spark 3.5.1, Kafka 3.7.0, HBase 2.5.10.

### Only two of you / only two laptops?
The brief counts **machines**, not people, so a 2-member group still needs 3 nodes. Options, best first:
1. **Use lab PCs** (or borrow one laptop from a friend for the setup + demo day).
2. **Two laptops + one VM:** install VirtualBox on the laptop with more RAM (16 GB ideal), create an Ubuntu VM with
   *Bridged Adapter* networking and 3 GB RAM, and use it as `worker2`. The VM gets its own IP on the Wi‑Fi, so it is a
   separate node to Hadoop and Spark. Give the master role to the *other* laptop.
3. Phone hotspot as the network if campus Wi‑Fi blocks device‑to‑device traffic (very common — test this first).

Suggested split for 2 people: **Member 1** sets up master (Kafka, HBase, runs the jobs), **Member 2** sets up both workers
and explains HDFS/Spark distribution in the viva.

---

## Step 1 — On ALL 3 PCs

```bash
sudo apt update && sudo apt install -y openjdk-11-jdk openssh-server python3 python3-pip curl rsync
ip -4 addr | grep inet          # note each PC's IP
```

Create the same user on all three (makes SSH simple), e.g. `hadoop`:
```bash
sudo adduser hadoop && sudo usermod -aG sudo hadoop
su - hadoop
```

Edit `/etc/hosts` on **all three** (use your real IPs, and remove the `127.0.1.1 <hostname>` line):
```
192.168.1.10  master
192.168.1.11  worker1
192.168.1.12  worker2
```
Set each machine's hostname: `sudo hostnamectl set-hostname master` (or `worker1`, `worker2`).

## Step 2 — Passwordless SSH (on master)

```bash
ssh-keygen -t rsa -P "" -f ~/.ssh/id_rsa
for h in master worker1 worker2; do ssh-copy-id hadoop@$h; done
ssh worker1 hostname   # must print worker1 without asking a password
```

## Step 3 — Download everything on master

```bash
cd /tmp
wget https://archive.apache.org/dist/hadoop/common/hadoop-3.3.6/hadoop-3.3.6.tar.gz
wget https://archive.apache.org/dist/spark/spark-3.5.1/spark-3.5.1-bin-hadoop3.tgz
wget https://archive.apache.org/dist/kafka/3.7.0/kafka_2.13-3.7.0.tgz
wget https://archive.apache.org/dist/hbase/2.5.10/hbase-2.5.10-hadoop3-bin.tar.gz

sudo tar -xzf hadoop-3.3.6.tar.gz -C /opt && sudo mv /opt/hadoop-3.3.6 /opt/hadoop
sudo tar -xzf spark-3.5.1-bin-hadoop3.tgz -C /opt && sudo mv /opt/spark-3.5.1-bin-hadoop3 /opt/spark
sudo tar -xzf kafka_2.13-3.7.0.tgz -C /opt && sudo mv /opt/kafka_2.13-3.7.0 /opt/kafka
sudo tar -xzf hbase-2.5.10-hadoop3-bin.tar.gz -C /opt && sudo mv /opt/hbase-2.5.10-hadoop3 /opt/hbase
sudo chown -R hadoop:hadoop /opt/hadoop /opt/spark /opt/kafka /opt/hbase
```

Add to `~/.bashrc` on **all three** PCs, then `source ~/.bashrc`:
```bash
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
export HADOOP_HOME=/opt/hadoop
export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
export SPARK_HOME=/opt/spark
export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$SPARK_HOME/bin:$SPARK_HOME/sbin
```

## Step 4 — Configure Hadoop (on master, in `/opt/hadoop/etc/hadoop/`)

`hadoop-env.sh` — add:
```bash
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
```

`core-site.xml`:
```xml
<configuration>
  <property><name>fs.defaultFS</name><value>hdfs://master:9820</value></property>
  <property><name>hadoop.tmp.dir</name><value>/home/hadoop/hadoop_tmp</value></property>
</configuration>
```

`hdfs-site.xml`:
```xml
<configuration>
  <property><name>dfs.replication</name><value>2</value></property>
  <property><name>dfs.namenode.name.dir</name><value>file:///home/hadoop/hdfs/namenode</value></property>
  <property><name>dfs.datanode.data.dir</name><value>file:///home/hadoop/hdfs/datanode</value></property>
  <property><name>dfs.permissions.enabled</name><value>false</value></property>
</configuration>
```

`workers` (replace `localhost`):
```
worker1
worker2
```

## Step 5 — Configure Spark (on master, in `/opt/spark/conf/`)

```bash
cd /opt/spark/conf
cp spark-env.sh.template spark-env.sh
echo 'export SPARK_MASTER_HOST=master'                          >> spark-env.sh
echo 'export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64'      >> spark-env.sh
echo 'export SPARK_WORKER_MEMORY=2g'                            >> spark-env.sh
printf "worker1\nworker2\n" > workers
```

## Step 6 — Copy Hadoop + Spark to the workers (from master)

```bash
for h in worker1 worker2; do
  ssh $h "sudo mkdir -p /opt/hadoop /opt/spark && sudo chown hadoop:hadoop /opt/hadoop /opt/spark"
  rsync -a /opt/hadoop/ $h:/opt/hadoop/
  rsync -a /opt/spark/  $h:/opt/spark/
done
```

## Step 7 — Configure Kafka (master only)

Edit `/opt/kafka/config/kraft/server.properties`:
```
listeners=PLAINTEXT://:9092,CONTROLLER://:9093
advertised.listeners=PLAINTEXT://master:9092
num.partitions=3
```
Format the storage once:
```bash
cd /opt/kafka
bin/kafka-storage.sh format -t $(bin/kafka-storage.sh random-uuid) -c config/kraft/server.properties
```

## Step 8 — Configure HBase (master only, data stored in HDFS)

`/opt/hbase/conf/hbase-env.sh`:
```bash
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
export HBASE_MANAGES_ZK=true
```
`/opt/hbase/conf/hbase-site.xml`:
```xml
<configuration>
  <property><name>hbase.rootdir</name><value>hdfs://master:9820/hbase</value></property>
  <property><name>hbase.cluster.distributed</name><value>false</value></property>
  <property><name>hbase.zookeeper.property.dataDir</name><value>/home/hadoop/zookeeper</value></property>
  <property><name>hbase.unsafe.stream.capability.enforce</name><value>false</value></property>
</configuration>
```

## Step 9 — Format HDFS once and start everything (master)

```bash
hdfs namenode -format          # ONLY the first time
git clone https://github.com/<your-username>/bigdata-realtime-analytics.git ~/project
cd ~/project
pip3 install -r requirements.txt
./scripts/native/start_cluster.sh
```

`check_cluster.sh` at the end should show 2 live datanodes, 2 Spark workers, both Kafka topics and the HBase tables.

Web UIs (open from any PC): HDFS `http://master:9870` · Spark `http://master:8080` · HBase `http://master:16010`

## Step 10 — Run the project (3 terminals on master)

```bash
./scripts/native/run_stream.sh            # terminal 1: Spark job on the cluster
./scripts/native/run_generator.sh 20      # terminal 2: live data into Kafka
./scripts/native/run_dashboard.sh         # terminal 3: open http://master:5000
```
After a few minutes, run the batch job: `./scripts/native/run_batch.sh`

## Using WSL on Windows as a node (what we did for the master)

WSL works as a cluster node if it uses **mirrored networking** (WSL then shares the laptop's real Wi-Fi IP):

1. `%USERPROFILE%\.wslconfig`:
   ```
   [wsl2]
   networkingMode=mirrored
   memory=8GB
   processors=4
   ```
2. Admin PowerShell: `Set-NetFirewallHyperVVMSetting -Name '{40E0AC32-46A5-438A-A0B2-2B479E8F2E90}' -DefaultInboundAction Allow` then `wsl --shutdown`
3. `/etc/wsl.conf`: `[boot] systemd=true` and `[network] hostname=master` + `generateHosts=false`

In mirrored mode a program **cannot connect to its own Wi‑Fi IP** if the server listens only on that IP, so every
server on the WSL node must listen on `0.0.0.0`:

- `hdfs-site.xml`: `dfs.namenode.rpc-bind-host`, `dfs.namenode.servicerpc-bind-host`, `dfs.namenode.http-bind-host` = `0.0.0.0`
- `hbase-site.xml`: `hbase.master.ipc.address`, `hbase.regionserver.ipc.address` = `0.0.0.0`
- Spark: `start-master.sh --host 0.0.0.0`; spark-submit with `--conf spark.driver.bindAddress=0.0.0.0 --conf spark.driver.host=master`
  (already in `scripts/native/`)
- Port 9000 is often already used by Windows, so HDFS uses **9820** here.

If the Wi‑Fi changes, every IP changes: update `/etc/hosts` on all three nodes.

---

## Common problems

| Symptom | Fix |
|--------|-----|
| `Live datanodes (0)` | `/etc/hosts` wrong or `127.0.1.1` line still there; firewall: `sudo ufw disable` on all PCs. Check `$HADOOP_HOME/logs/*datanode*.log` on the worker. |
| DataNode dies with "Incompatible clusterIDs" | You formatted the namenode twice. On workers: `rm -rf ~/hdfs/datanode/*`, restart. |
| Spark workers not on `:8080` | `SPARK_MASTER_HOST=master` missing, or hostnames don't resolve on workers. |
| Generator can't reach Kafka | `advertised.listeners` must be `PLAINTEXT://master:9092`, not localhost. |
| Dashboard: "Cannot read from HBase" | Thrift not running: `hbase-daemon.sh start thrift`; check port: `ss -ltnp | grep 9090`. |
| `spark-submit` hangs downloading the Kafka package | Master needs internet the first time (Maven). It is cached in `~/.ivy2` afterwards. |
