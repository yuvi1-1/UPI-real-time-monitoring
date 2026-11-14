# Demo script and viva notes

## 10-minute demo order
1. **Show the cluster** (master terminal): `./scripts/native/check_cluster.sh`
   - `jps` on master → NameNode, SecondaryNameNode, Master, Kafka, HMaster, ThriftServer
   - `ssh worker1 jps` → DataNode, Worker
   - HDFS UI `:9870` → *Datanodes* tab shows 2 live nodes. Spark UI `:8080` → 2 workers ALIVE.
2. **Show the data going into Kafka**:
   `kafka-console-consumer.sh --bootstrap-server master:9092 --topic upi_transactions --max-messages 5`
3. **Start the Spark job** (`run_stream.sh`) — point out the `[batch N] events=… alerts=…` lines every 5 s, and the job UI on `:4040` (Streaming tab: input rate, processing time).
4. **Dashboard** `:5000` — KPIs, live chart, alerts flashing in red as they arrive.
5. **HDFS**: `hdfs dfs -ls -R /bigdata/upi/raw | head` and `hdfs fsck /bigdata/upi/raw -files -blocks -locations | head -30` (blocks stored on both workers).
6. **HBase shell**: `hbase shell` → `list`, `scan 'upi_metrics', {ROWPREFIXFILTER => 'bank|', LIMIT => 5}`, `get_counter 'upi_metrics', 'total|all', 'm:count'`, `scan 'upi_alerts', {LIMIT => 3}`
7. **Batch job**: `run_batch.sh` → tables printed, then the "Batch analytics" section of the dashboard fills in.
8. **Fault tolerance (optional, impressive)**: stop the streaming job with Ctrl+C, wait 20 s, start it again — it continues from the checkpointed Kafka offsets.

## Basic commands to know (they will ask)

**HDFS**
```
hdfs dfs -ls /            hdfs dfs -mkdir -p /test       hdfs dfs -put file.txt /test
hdfs dfs -cat /test/file.txt                             hdfs dfs -get /test/file.txt .
hdfs dfs -du -h /bigdata  hdfs dfsadmin -report          hdfs fsck / -files -blocks
start-dfs.sh / stop-dfs.sh
```
**Spark**
```
start-master.sh   start-workers.sh   spark-submit --master spark://master:7077 app.py
pyspark --master spark://master:7077        spark-shell
```
**Kafka**
```
kafka-topics.sh --bootstrap-server master:9092 --list
kafka-topics.sh --bootstrap-server master:9092 --describe --topic upi_transactions
kafka-console-producer.sh --bootstrap-server master:9092 --topic test
kafka-console-consumer.sh --bootstrap-server master:9092 --topic test --from-beginning
kafka-consumer-groups.sh --bootstrap-server master:9092 --list
```
**HBase shell**
```
list   create 't1','cf'   put 't1','r1','cf:a','v1'   get 't1','r1'   scan 't1'
count 't1'   describe 't1'   disable 't1'   drop 't1'   status
```

## Likely questions

**Why Kafka and not send data straight to Spark?** Kafka is a durable, replayable buffer. If Spark is down or slow, events wait in Kafka; Spark resumes from its last committed offset. It also lets several consumers read the same stream.

**What is a micro-batch?** Structured Streaming collects the events that arrived in each trigger interval (5 s here) into a small DataFrame and runs normal Spark SQL on it. `foreachBatch` gives us that DataFrame so we can write it to HBase.

**What is the checkpoint for?** It stores the Kafka offsets and progress in HDFS, so after a restart the job continues exactly where it stopped.

**Why HBase for the dashboard and HDFS for raw data?** HDFS is great for large sequential scans (batch jobs) but not for small random reads. HBase gives millisecond reads/writes by row key, which the dashboard needs every 3 s. (In our setup HBase runs standalone on the master with its files on local disk; in production it would store its files on HDFS.)

**Explain your row key design.** Metrics: `dimension|value` so one prefix scan returns a whole chart (e.g. all banks). Alerts: reverse timestamp first, so the newest alerts are at the top of the table and `scan LIMIT 25` gives the latest ones without sorting.

**Why counters?** Each micro-batch only knows its own totals. `counter_inc` adds them atomically on the HBase server, so totals stay correct even with concurrent writers and we never read-modify-write.

**How is work distributed?** The Kafka topic has 3 partitions → Spark creates tasks per partition and the group-by shuffle runs as tasks on the executors on worker1 and worker2. The driver (on master) only collects the small aggregated results.

**What does replication factor 2 mean?** Every HDFS block is stored on both DataNodes, so if one worker dies no data is lost.

**What is the role of the NameNode / DataNode?** NameNode keeps metadata (which file = which blocks on which nodes). DataNodes store the actual blocks.

**Limitations / future work.** Rules are per micro-batch (a burst split across two batches may be missed → use a sliding window with watermark); rule-based detection could be replaced by an MLlib model trained on the archived data; single Kafka broker and HBase in standalone mode (would be a 3-broker Kafka and distributed HBase in production).

**Is the data real?** No — it is simulated, with realistic distributions (bank/app market share, log-normal amounts, rush-hour patterns). Real UPI data is not public; the pipeline would work unchanged on a real feed with the same JSON schema.
