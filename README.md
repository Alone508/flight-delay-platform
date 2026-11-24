# 航班延误监测与预测平台

单机伪分布式全栈大数据课程项目：Flume 采集 → Kafka/HDFS → Spark 批流处理 → MySQL/HBase → Spring Boot API → Vue3 + ECharts。

## 架构

```
Kaggle CSV ─► flight_replay.py ─► data/logs/*.log ─► Flume taildir ─┬─► Kafka flight-events
                                                                   └─► HDFS /flight/ods/events
AWC / Open-Meteo ─► weather_collector.py ─► data/logs/weather_events.log ─► Flume ─┬─► Kafka weather-events
                                                                                  └─► HDFS /flight/ods/weather
CSV ─► Spark clean_dwd.py ─► DWD Parquet ─► ads_stats.py ─► MySQL ADS / HDFS DWS
DWD + 历史天气 ─► train_model.py ─► MLlib 模型 / ads_predictions / ads_model_metrics
Kafka ─► realtime_kpi.py ─► ads_realtime_kpi + ads_alerts + HDFS DWS
Kafka ─► hbase_sink.py ─► HBase flight_detail / airport_realtime
MySQL / HBase REST 8085 ─► Spring Boot 8080 ─► Vue 5173
```

## 组件与端口

| 组件 | 端口 | 路径 |
|---|---|---|
| HDFS / YARN | 9000/9870；8088 | `~/apps/hadoop-3.3.6` |
| HBase Master / RS / Thrift / REST | 16010/16030；9090；8085(+8086 info) | `~/apps/hbase-2.5.8-hadoop3` |
| Kafka / Kafka ZK / HBase ZK | 9092 / 2182 / 2181 | `~/apps/kafka_2.13-3.6.1` |
| MySQL | 3306（库 `flight_delay`，用户 `flight`） | apt |
| Spring Boot | 8080 | `api/` |
| Vue dev | 5173 | `web/` |
| Spark UI | 4040 | `~/apps/spark-3.5.1-bin-hadoop3` |

## 一键运行

```bash
source scripts/env.sh

# 1) 基础设施（HDFS/HBase/Kafka/MySQL，默认含 Flume）
scripts/start-all.sh                 # 需要 YARN 时加 --with-yarn

# 2) 实时链路（HBase Thrift + Spark KPI + HBase sink）
scripts/start-streaming.sh

# 3) 展示层（HBase REST + API + 前端）
scripts/start-app.sh

# 4) 回放 10 个历史日（约 10 分钟；speedup=1440）
python -I collector/flight_replay.py --start-date 2019-01-01 --end-date 2019-01-10 --speedup 1440

# 5) 天气（可 --once，或常驻每 300 秒）
python -I collector/weather_collector.py --once

# 停止
scripts/stop-app.sh
scripts/stop-streaming.sh
scripts/stop-all.sh
```

浏览器打开 `http://localhost:5173/`；Swagger：`http://localhost:8080/swagger-ui.html`。

## 离线批处理

```bash
scripts/run-batch.sh        # DWD -> ADS -> 航班索引 -> 模型训练
```

- `spark/batch/clean_dwd.py`：ODS CSV → DWD Parquet（`/flight/dwd/flight/dt=YYYYMMDD`），2,999,864 行。
- `spark/batch/ads_stats.py`：航司/机场准点率、五类延误原因、航线、热力图 → MySQL + HDFS DWS。
- `spark/batch/ads_flight_list.py`：回放窗口 2019-01 的航班索引 → `ads_flight_list`（前端分页，与 HBase 明细对齐）。
- `spark/batch/train_model.py`：时间切分 2019-2021 训练 / 2022 验证 / 2023 测试；LR/RF/GBT；目标编码只用训练集；天气与拥堵特征；验证集按 F1 调阈值；指标写 `ads_model_metrics`，Top 5000 风险航班写 `ads_predictions`。GBT 测试 AUC **0.6790**（超过基线 0.65；0.72 目标未达，原因见 `docs/MODEL.md`）。
- **完整训练报告**：[docs/训练报告.md](docs/训练报告.md)（数据、特征、超参数、各版本指标、混淆矩阵、特征重要性、局限与优化方向）。

## 目录

```
collector/    回放器、天气采集、历史天气、ODS 引导
flume/        flight-agent / weather-agent 配置
spark/batch/  DWD、ADS、航班索引、MLlib
spark/streaming/  realtime_kpi、hbase_sink
sql/          MySQL DDL、HBase 建表
api/          Spring Boot 2.7 + JdbcTemplate + HBase REST
web/          Vue3 + Vite + ECharts（4 个页面）
scripts/      env/启停/验收/批处理/演示
docs/         实施计划、环境说明
```

## 验收

```bash
scripts/verify_stack.sh --with-spark
scripts/verify_api.sh
scripts/sqoop-export-demo.sh          # HDFS -> MySQL 的 Sqoop export 演示
curl --noproxy '*' localhost:8080/api/realtime/kpi
curl --noproxy '*' 'localhost:8080/api/airlines/rank?limit=3'
curl --noproxy '*' 'localhost:8080/api/flights?page=1&size=5'
```

浏览器视觉验收（Playwright + Chromium；Node 20 在 `~/apps/node20`）：

```bash
export PATH=$HOME/apps/node20/bin:$PATH
node web/scripts/screenshot.mjs      # 输出 /tmp/shot-*.png，四页 + 明细弹窗
```

## 已验证结果

- 10 天完整回放：**19,218** 条事件；Kafka/HDFS/Spark KPI/HBase sink 全链路正常。
- HBase `flight_detail` 已回填 **19,568** 行（覆盖 `ads_flight_list` 全部航班）。
- Sqoop import（MySQL→HDFS）与 export（HDFS→MySQL，`scripts/sqoop-export-demo.sh`）均成功。
- Playwright + Chromium 真实浏览器打开四个页面：**0 控制台错误**，四页截图 + 明细弹窗正常。

## 页面截图

- [实时监控大屏](docs/screenshots/shot-dashboard.png)
- [延误分析](docs/screenshots/shot-analysis.png)
- [预测与模型](docs/screenshots/shot-prediction.png)
- [航班明细](docs/screenshots/shot-flights.png)
- [HBase 明细弹窗](docs/screenshots/shot-flights-detail.png)

## 已知说明

- 全机仅 JDK 8（Sqoop 1.4.7 限制）；Spring Boot 固定 2.7.x。
- **Flume 广播**：一个 memory channel 接多个 sink 是竞争消费（事件被分流）；本项目为 Kafka 和 HDFS 各配一个独立 channel，source 同时写入两个 channel。已用测试事件验证两边都能收到。
- YARN 默认不常驻；`start-all.sh --with-yarn`。
- Sqoop 1.4.7 的 `MySQLManager` 构造器缺陷导致 `list-databases` 不可用；统一 `--driver com.mysql.cj.jdbc.Driver` 走 GenericJdbcManager，`eval/import/export` 正常。
- HBase REST 使用 `info:*` 通配会 404，API 改为行级读取后解码 Base64 Cell。
- 15 GiB 单机内存：HBase RS 1 GB、Kafka 0.5–0.75 GB、Spark driver 2 GB；不要同时跑 YARN 与 2 GB local 作业。
