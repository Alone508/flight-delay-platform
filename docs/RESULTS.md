# 验收结果汇总（2026-11-23）

## 环境

- 根分区扩容至 **60 GB**（可用约 36 GB），15 GiB RAM，6 vCPU。
- 全栈运行：HDFS、HBase、Kafka、Flume、Spark、MySQL、Spring Boot、Vue。
- `scripts/verify_stack.sh`：**33/33 PASS**（含 Flume 双 agent、API、HBase REST）。
- `scripts/verify_api.sh`：**15/15 PASS**。

## 数据链路

- 完整 10 天回放：**19,218** 条事件（2019-01-01 ~ 2019-01-10，speedup=1440）。
- HDFS ODS：Flume sink 文件 + `flight-full-replay.jsonl`（19,568 行）。
- Kafka `flight-events`：消费正常；Spark Streaming KPI 写入 2,212 个机场×分钟窗口。
- HBase `flight_detail`：**19,568** 行（由 `collector/backfill_hbase_detail.py` 幂等回填）。
- HBase `airport_realtime`：实时聚合计数可用。
- 发现并修复：Flume 单 channel 多 sink 是竞争消费，事件被 Kafka/HDFS 分流（16,601 + 2,966）；改为每 sink 独立 channel，测试事件验证两边都收到。

## Sqoop

- `sqoop import`：MySQL `smoke_sqoop` → HDFS，3 条记录成功。
- `sqoop export`：HDFS → MySQL `sqoop_export_demo`，4 条记录成功。
- 注意：Sqoop 1.4.7 的 TextExportMapper 需要表对应 `SqoopRecord`，export 前 `sqoop codegen` 生成类并通过 `-libjars` 传入；脚本见 `scripts/sqoop-export-demo.sh`。

## 展示层

- Spring Boot 13 个接口 + Swagger（`/swagger-ui.html`），全部返回 `code=0`。
- Vue 四页面：实时监控大屏、延误分析、预测与模型、航班明细。
- Playwright + Chromium 真实浏览器：四页 **0 console error**；Flights 行点击弹窗正常。
- 截图：`docs/screenshots/`。

### 修复的 UI 问题

- 机场排名被航班量个位数的小机场刷成 100% → API 过滤 `flights >= 100`。
- 热力图所有格子红色 → `visualMap` 显式 `dimension: 2`（平均值），不再误用第 4 维航班量。
- 航司/特征重要性横向图标签被裁切 → 增大左侧 grid、按长度截断。
- 告警时间显示 `T` → 前端替换为空格。

## 模型

- 最新模型：`gbt-202611231630`。
- 验证 AUC **0.6779**，测试 AUC **0.6790**，阈值 0.17，Accuracy 0.7753，F1 0.7100，Precision 0.5656，Recall 0.1038。
- 较上一版（0.6563）提升 +0.0227；新增的机场×时段、航司×航线、前一日延误率、前 2–4 小时同机场起飞延误占据特征重要性前四。
- 目标 0.72 仍未达到；原因与后续特征方向见 `docs/MODEL.md`。
