# 单机环境（Step 12）

| 组件 | 版本 | 路径 / 端口 |
|---|---|---|
| JDK | OpenJDK 8u514 | `/usr/lib/jvm/java-8-openjdk-amd64` |
| Hadoop | 3.3.6 | `~/apps/hadoop-3.3.6`；HDFS 9000/9870/9864/9868；YARN 8088 |
| HBase | 2.5.8 (hadoop3) | `~/apps/hbase-2.5.8-hadoop3`；ZK 2181，Master UI 16010，RS UI 16030，Thrift 9090，REST 8085（info 8086） |
| Spring Boot API | 2.7.18 | `api/`，8080，`/swagger-ui.html` |
| Vue 3 + Vite | Vue 3.5 / Vite 5 | `web/`，5173，`/api` 代理到 8080 |
| Kafka | 3.6.1 | `~/apps/kafka_2.13-3.6.1`；ZK 2182，broker 9092 |
| Flume | 1.11.0 | `~/apps/apache-flume-1.11.0-bin` |
| Sqoop | 1.4.7 | `~/apps/sqoop-1.4.7` |
| Spark | 3.5.1 | `~/apps/spark-3.5.1-bin-hadoop3`；UI 4040 |
| MySQL | 8.0.46 | 127.0.0.1:3306，库 `flight_delay`，用户 `flight` |
| Python | CPython 3.11.16 | 项目 `.venv` |
| 数据集 | 3,000,000 行 | `~/projects/flights/flights_sample_3m.csv` |

## 常用命令

```bash
source ~/projects/flights/flight-delay-platform/scripts/env.sh
start-all.sh                 # 不含 YARN；默认启动 Flume
start-all.sh --with-yarn     # 课程演示需要 YARN 时
start-streaming.sh           # HBase Thrift + Spark KPI + hbase_sink
start-app.sh                 # HBase REST + API + 前端
stop-app.sh
stop-streaming.sh
stop-all.sh
jps -l
hdfs dfsadmin -report
hbase shell
kafka-topics.sh --bootstrap-server localhost:9092 --list
mysql -h127.0.0.1 -uflight -p"$MYSQL_FLIGHT_PASSWORD" flight_delay
```

## 约定

- 主机名统一 `localhost` / `127.0.0.1`，未修改 `/etc/hosts`。
- **Flume 双 channel**：一个 memory channel 接多个 sink 是竞争消费（事件被分流），不是广播；`flight-agent.conf` / `weather-agent.conf` 为 Kafka 和 HDFS 各配一个 channel，source 同时写入两个 channel。已用 `event_type=test` 事件验证 Kafka 与 HDFS 都收到。
- HBase ZK 2181 与 Kafka ZK 2182 独立；Kafka ZK 管理端口关闭，8080 留给 Spring Boot。
- HDFS replication=1，`dfs.datanode.du.reserved=2 GiB` 防止写满根分区。
- 全机唯一 JDK 为 8（Sqoop 1.4.7 限制）；不要安装 Java 11/17/21。
- 下载代理 `192.168.0.100:20808` 仅用于外网；`no_proxy` 已覆盖 localhost。
- YARN 默认不启动；需要 MapReduce/Spark-on-YARN 时用 `start-all.sh --with-yarn`，演示完 `stop-yarn.sh` 释放约 2 GB。
- HBase 自带 ZooKeeper 的管理端口已禁用（`zookeeper.admin.enableServer=false`），8080 专供 Spring Boot。
- **Sqoop 已知问题**：本构建的 `org.apache.sqoop.manager.MySQLManager` 构造函数与 `ConnFactory` 不匹配，`sqoop list-databases` 不可用。统一加 `--driver com.mysql.cj.jdbc.Driver` 走 `GenericJdbcManager`，用 `sqoop eval` / `import` / `export`。已实测 `sqoop import`（MySQL → HDFS）和 `sqoop export`（HDFS → MySQL，见 `scripts/sqoop-export-demo.sh`）成功。Sqoop 1.4.7 的 TextExportMapper 需要表对应的 `SqoopRecord` 类，export 前需 `sqoop codegen` 生成并通过 `-libjars` 传入。
- 启动脚本会等待 HDFS 退出 safemode 与 HBase active master，再返回；`verify_stack.sh --with-spark` 可做完整验收。
- HBase REST 的 info 端口默认也是 8085，会与 REST 端口冲突；启动时必须 `-p 8085 -i 8086`（hbase-site.xml 已写入 `hbase.rest.port/info.port`）。REST 不支持 `info:*` 通配，API 使用行级 GET 并解码 Base64 Cell。
- 前端 `Flights` 页的分页数据来自 MySQL `ads_flight_list`（回放窗口 2019-01，19,568 行），HBase `flight_detail` 已回填 19,568 行，点击任意行均有明细。
- 实时趋势 `/api/realtime/trend` 以 **最新 KPI 的 stat_time** 为锚点取最近 N 分钟，而不是墙钟 `NOW()`；这样回放暂停/跨天后大屏仍显示最近一段回放趋势，右上角显示“数据时间”。
- 完整 10 天回放写入了 19,218 条事件；HDFS ODS 另存 `flight-full-replay.jsonl` 作为完整原始日志。
- 浏览器验收：`PATH=~/apps/node20/bin:$PATH node web/scripts/screenshot.mjs`，四页 0 console error，输出 `/tmp/shot-*.png`。
- ML 当前特征包含航司/机场/航线/月度目标编码、机场小时计划量、起降机场天气（历史 Open-Meteo）与距离等；GBT 2023 测试集 AUC **0.6790**，验证集选模，验证集 F1 阈值调优（0.17）。详见 `docs/MODEL.md`。
