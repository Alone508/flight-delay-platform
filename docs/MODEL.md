# 延误预测模型报告

> 更完整的训练细节、各版本对比、混淆矩阵、特征重要性与优化方向见 [训练报告.md](训练报告.md)。

## 数据与切分

- 数据源：`flights_sample_3m.csv`（3,000,000 行）；DWD 清洗后 **2,999,864** 行。
- 标签：`ARR_DELAY >= 15` 分钟 → `label=1`；仅保留 `CANCELLED=0` 且 `ARR_DELAY` 非空。
- 时间切分（避免随机切分泄漏）：
  - 训练：2019-01-01 ~ 2021-12-31（1,791,569 行）
  - 验证：2022 全年（667,726 行）
  - 测试：2023-01-01 ~ 2023-08-31（454,373 行）
- 模型选择只看**验证集 AUC**，测试集用于最终报告。

## 特征（全部起飞前可得）

| 类别 | 特征 |
|---|---|
| 类别 | `airline_code`, `origin`, `dest`, `month`, `day_of_week` |
| 数值 | `dep_hour`, `distance`, `crs_elapsed_time`, `is_weekend`, `is_holiday`（美国主要节假日） |
| 拥堵 | `origin_hour_traffic`, `dest_hour_traffic`（同机场同小时计划航班数） |
| 目标编码（仅训练集） | `airline_rate`, `origin_rate`, `dest_rate`, `route_rate`, `airline_origin_rate`, `airline_month_rate`, `origin_month_rate`, `airline_route_rate`, `origin_dow_hour_rate`（alpha=20~50 平滑） |
| 时间序列（仅历史信息） | `origin_prev_day_rate`, `airline_prev_day_rate`（前一日的延误率）；`origin_prev_dep_delay`（同机场前 2–4 小时航班的平均起飞延误） |
| 天气（Open-Meteo 历史，起降机场） | `visib`, `wind`, `precip`, `flight_category`（VFR/MVFR/IFR/LIFR → 0-3） |

**严格排除**：`DEP_DELAY, ARR_DELAY, DEP_TIME, ARR_TIME, TAXI_OUT, TAXI_IN, WHEELS_OFF, WHEELS_ON, AIR_TIME, ELAPSED_TIME, DELAY_DUE_*`。
目标编码只在训练集计算，验证/测试使用训练集统计量；未见类别回退到全局延误率。
时间序列特征只使用预测时刻之前已完成的航班：前一日延误率用前一天标签；前序延误只用同机场 2–4 小时前计划起飞的航班，避免同时刻信息泄漏。

## 模型与结果

Pipeline：`StringIndexer → OneHotEncoder → VectorAssembler`；LR 额外 `StandardScaler`。
RF/GBT 在训练集 40% 样本（718,556 行）上训练以控制单机耗时；LR 使用全量训练集。

| 模型 | 验证 AUC | 测试 AUC | Accuracy | F1 | Precision | Recall | 阈值 |
|---|---:|---:|---:|---:|---:|---:|---:|
| LogisticRegression | 0.6386 | 0.6359 | 0.7711 | 0.6724 | — | — | 0.50 |
| RandomForest | 0.6075 | 0.6054 | 0.7698 | 0.6696 | — | — | 0.50 |
| GBT（基线阈值） | 0.6499 | 0.6516 | 0.7711 | 0.6776 | 0.6057 | 0.0174 | 0.50 |
| GBT（阈值调优 + 起降天气/月度编码） | 0.6535 | 0.6563 | 0.7716 | 0.6810 | 0.5943 | 0.0253 | 0.14 |
| **GBT（+时序/交互编码，最新）** | **0.6779** | **0.6790** | 0.7753 | 0.7100 | 0.5656 | 0.1038 | 0.17 |

> GBT 默认 0.5 阈值在类别不平衡下 recall 极低；训练脚本在 2022 验证集上按 F1 扫描 0.05–0.95 选择阈值（0.14，验证 F1 0.4007），并写回模型与 `ads_model_metrics.threshold`。
> 注意 2023 测试集分数分布发生偏移，0.14 阈值下测试 recall 仅 0.025；因此实际展示更依赖 `predict_score` 排序（Top 5000 风险航班，分数 0.49–0.83，其中 89% 标为延误），而不是单点阈值分类。这是本项目观察到的 val→test 迁移差距。

## 特征重要性（最新 GBT，Top 10）

1. `origin_dow_hour_rate`（机场×星期×小时历史延误率，0.198）
2. `airline_route_rate`（航司×航线历史延误率，0.121）
3. `airline_prev_day_rate`（航司前一日延误率，0.104）
4. `origin_prev_dep_delay`（同机场前 2–4 小时平均起飞延误，0.076）
5. `dest_precip`（到达机场降水，0.074）
6. `weather_precip`（起飞机场降水，0.072）
7. `origin_hour_traffic`（机场小时计划量，0.067）
8. `origin_prev_day_rate`（机场前一日延误率，0.065）
9. `origin_rate`（机场历史延误率，0.053）
10. `origin_month_rate`（机场×月，0.046）

> 新增的时序/交互编码占据了重要性前四名，是 AUC 从 0.6563 提升到 0.6790 的主要原因。

## 结论与局限

- 仅用起飞前可得信息时，测试 AUC **0.6790**，超过计划的基线目标 0.65；加入时序/交互特征后仍未达到 0.72。
- 主要瓶颈：最有力的信号（实际起飞延误、前序航班周转、NAS 流量控制）在计划起飞时不可得；数据集缺少尾号，无法构造“同一架飞机前一段延误”。
- 后续可增强：引入 FAA/NAS 流量通告、机场跑道构型、天气预报而非实况、同日同航司前序航班延误等。
- 复现：`scripts/run-batch.sh`，或
  `spark-submit --master 'local[4]' --driver-memory 2g spark/batch/train_model.py --models lr,rf,gbt --tree-sample 0.4`
