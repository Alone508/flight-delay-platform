## 一、近 60 分钟趋势的数据来源

链路如下：

```text
flight_replay.py
  → data/logs/flight_events.log
  → Flume taildir (flightAgent)
  → Kafka flight-events
  → Spark Structured Streaming: spark/streaming/realtime_kpi.py
  → MySQL ads_realtime_kpi
  → Spring Boot /api/realtime/trend?minutes=60
  → Vue Dashboard 趋势图
```

关键处理在 [spark/streaming/realtime_kpi.py](spark/streaming/realtime_kpi.py)：

- Kafka 读取 `flight-events`，解析 JSON；
- `withWatermark("event_time", "5 minutes")`；
- **1 分钟滚动窗口**，按 `origin` 聚合：
  - `departures = count(*)`
  - `delayed_flights = sum(dep_delay >= 15)`
  - `cancelled = sum(cancelled)`
  - `avg_dep_delay = avg(dep_delay)`
  - `ontime_rate = 1 - delayed/departures`
- `foreachBatch` 写入 MySQL `ads_realtime_kpi`，同时写 HDFS `/flight/dws/realtime_kpi`。

趋势接口在 [api/src/main/java/com/flight/api/service/QueryService.java](api/src/main/java/com/flight/api/service/QueryService.java)：

```sql
SELECT stat_time, SUM(departures), SUM(delayed_flights), ...
FROM ads_realtime_kpi
WHERE stat_time >= DATE_SUB(
  (SELECT MAX(stat_time) FROM ads_realtime_kpi),
  INTERVAL ? MINUTE
)
GROUP BY stat_time
ORDER BY stat_time
```

所以它不是“当前墙钟前 60 分钟”，而是**最新一批回放 KPI 之前 60 分钟**。当前 KPI 数据：

- 2,212 行，时间范围 **2026-11-23 14:21 ~ 16:24**
- 因此趋势图显示 **16:14–16:24 的 11 个点**
- 这不是空数据，而是上一轮完整回放留下的数据

## 二、实时告警的数据来源

告警和 KPI 在**同一个流式批处理**里产生，规则在 [realtime_kpi.py](spark/streaming/realtime_kpi.py)：

```text
avg_dep_delay > 30 分钟  或  delayed_ratio > 0.30
```

- `delayed_ratio = delayed_flights / departures`
- 级别：
  - `CRITICAL`：`avg_dep_delay > 60` 或 `delayed_ratio > 0.5`
  - 其他满足条件的为 `WARN`
- 每个批次按平均延误排序，最多写 20 条到 MySQL `ads_alerts`
- 前端 `/api/alerts` 从 `ads_alerts` 按 `alert_time` 倒序读取

当前告警表：

- **876 行**告警，时间范围同为 14:21 ~ 16:24
- `CRITICAL` 410 条，`WARN` 466 条
- 最新告警时间 16:24

## 三、是否做到了“实时预警”？

要分两层看：

### 1. 实时监控 + 阈值告警：做到了

- 链路是真正的流式链路：Kafka → Spark Structured Streaming → MySQL；
- 微批触发间隔 **10 秒**，窗口 **1 分钟**；
- 窗口数据正常会在下一个微批写入 MySQL，端到端大约 **10–20 秒**；
- 5 分钟 watermark 允许迟到事件继续修正窗口结果；
- HBase sink 也是独立消费 Kafka，实时写 `flight_detail` / `airport_realtime`。

在回放运行时，大屏数字和告警确实会随事件增长。

### 2. 预测式“提前预警”：没有做到

当前告警是**基于已经发生的起飞延误**：

- 事件里的 `dep_delay` 是起飞后的实际值；
- 告警含义是“某机场最近一分钟已经出现较多延误”，属于**事后/准实时告警**；
- 不是“某航班将在 2 小时后延误”的提前预警。

当前 ML 模型是**批处理离线打分**：

- `train_model.py` 对 2023 测试集生成 `ads_predictions`；
- 没有把模型加载进 Streaming 或在线服务；
- 因此预测页是历史评分结果，不会随实时事件变化。

另外，当前 KPI 会从 `weather_latest` 关联天气快照，但**告警规则没有使用天气**，也没有接入预报天气/NAS 流控。

## 四、当前“实时”状态

目前：

- Spark Streaming、HBase sink 进程都在运行；
- 但 Kafka `flight-events` 的 offset 已停止增长，回放已结束；
- 所以 MySQL 不再产生新的 KPI/告警，大屏数据时间停在 **2026-11-23 16:24**。

要看到数字和告警继续变化，需要启动回放：

```bash
cd ~/projects/flights/flight-delay-platform
source scripts/env.sh
python -I collector/flight_replay.py --start-date 2019-01-01 --end-date 2019-01-10 --speedup 1440
```

## 五、当前实现的主要限制

1. **告警是反应式的**：依赖已起飞航班的 `dep_delay`，不是预测。
2. **无告警去重**：`ads_alerts` 没有唯一键，窗口在 update 模式或 checkpoint 重启后可能重复插入；876 条里包含重复/多次更新。
3. **告警时间语义**：`alert_time` 是窗口起点，不是检测时刻；真正检测时间可看 `created_at`。
4. **告警无冷却/升级机制**：同一机场可能连续多分钟重复告警。
5. **天气未参与告警**：只有 KPI 关联了天气字段，规则未使用能见度/风速/降水。
6. **预测未在线化**：ML 模型没有接入流式链路。

## 六、要变成真正的“提前预警”，建议这样改

1. **在线打分**：
   - 用保存的 MLlib/CatBoost/LightGBM 模型，对“未来 2–6 小时计划起飞”的航班实时打分；
   - 写入 `ads_predictions` 或 Kafka `delay-alerts`；
   - 大屏新增“高风险航班预警”，显示预测分和提前量。
2. **告警规则升级**：
   - 当前延误 + 预报天气 + NAS/ground stop + 预测分联合判定；
   - 加唯一键 `(airport, window_start, level)` 和冷却时间，避免重复。
3. **时间语义修正**：
   - `alert_time` 用检测时间，另存 `window_start`；
   - 前端展示“检测于 … / 窗口 …”。
4. **评估指标**：
   - 用提前量（lead time）、Precision@K、Recall@K 衡量预警效果，而不是只看告警条数。

当前版本可以准确描述为：**准实时监控 + 基于实际起飞延误的阈值告警**；还不能称为**预测式提前预警**。