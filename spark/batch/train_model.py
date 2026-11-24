#!/usr/bin/env python3
"""MLlib 延误预测：时间切分 + 目标编码 + LR/RF/GBT。

用法：
  spark-submit --master 'local[4]' --driver-memory 2g spark/batch/train_model.py
  spark-submit ... spark/batch/train_model.py --models lr,gbt --tree-sample 0.4

切分：2019-2021 训练，2022 验证，2023 测试（时间切分，避免随机泄漏）。
标签：arr_delay >= 15（仅未取消且到达延误非空）。
所有特征均为起飞前可得；DELAY_DUE_* / DEP_DELAY / ARR_DELAY 等事后字段不进入特征。
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import os

import mysql.connector
from pyspark import StorageLevel
from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.classification import GBTClassifier, LogisticRegression, RandomForestClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator
from pyspark.ml.feature import (
    OneHotEncoder,
    OneHotEncoderModel,
    StandardScaler,
    StringIndexer,
    StringIndexerModel,
    VectorAssembler,
)
from pyspark.ml.functions import vector_to_array
from pyspark.sql import DataFrame, SparkSession, Window, functions as F

DWD = "hdfs://localhost:9000/flight/dwd/flight"
MODEL_ROOT = "hdfs://localhost:9000/flight/ml/model"
PRED_ROOT = "hdfs://localhost:9000/flight/dws/predictions"
MYSQL_DB = os.environ.get("MYSQL_DATABASE", "flight_delay")
MYSQL_URL = (
    f"jdbc:mysql://127.0.0.1:3306/{MYSQL_DB}"
    "?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC"
    "&rewriteBatchedStatements=true&characterEncoding=utf8"
)
CAT_COLS = ["airline_code", "origin", "dest", "month_s", "day_of_week_s"]
NUM_COLS = [
    "dep_hour", "distance", "crs_elapsed_time", "is_weekend", "is_holiday",
    "origin_hour_traffic", "dest_hour_traffic",
    "airline_rate", "origin_rate", "dest_rate", "route_rate", "airline_origin_rate",
    "airline_month_rate", "origin_month_rate", "airline_route_rate", "origin_dow_hour_rate",
    "origin_prev_day_rate", "airline_prev_day_rate", "origin_prev_dep_delay",
    "weather_visib", "weather_wind", "weather_precip", "weather_cat",
    "dest_visib", "dest_wind", "dest_precip", "dest_cat",
]
WEATHER_PATH = "hdfs://localhost:9000/flight/dwd/weather_hourly"
US_HOLIDAYS = [
    "2019-01-01", "2019-01-21", "2019-02-18", "2019-05-27", "2019-07-04", "2019-09-02", "2019-11-28", "2019-12-25",
    "2020-01-01", "2020-01-20", "2020-02-17", "2020-05-25", "2020-07-04", "2020-09-07", "2020-11-26", "2020-12-25",
    "2021-01-01", "2021-01-18", "2021-02-15", "2021-05-31", "2021-07-04", "2021-09-06", "2021-11-25", "2021-12-25",
    "2022-01-01", "2022-01-17", "2022-02-21", "2022-05-30", "2022-07-04", "2022-09-05", "2022-11-24", "2022-12-25",
    "2023-01-01", "2023-01-16", "2023-02-20", "2023-05-29", "2023-07-04", "2023-09-04", "2023-11-23", "2023-12-25",
]


def mysql_conn():
    return mysql.connector.connect(
        host="127.0.0.1", port=3306,
        user=os.environ.get("MYSQL_FLIGHT_USER", "flight"),
        password=os.environ["MYSQL_FLIGHT_PASSWORD"],
        database=MYSQL_DB,
    )


def smoothed_rate(df: DataFrame, keys: list[str], name: str, global_rate: float, alpha: float = 20.0) -> DataFrame:
    agg = df.groupBy(*keys).agg(F.sum("label").alias("_s"), F.count("*").alias("_n"))
    return agg.withColumn(
        name,
        (F.col("_s") + F.lit(alpha * global_rate)) / (F.col("_n") + F.lit(alpha)),
    ).select(*keys, name)


def build_rates(train: DataFrame, global_rate: float) -> dict:
    return {
        "airline_rate": smoothed_rate(train, ["airline_code"], "airline_rate", global_rate),
        "origin_rate": smoothed_rate(train, ["origin"], "origin_rate", global_rate),
        "dest_rate": smoothed_rate(train, ["dest"], "dest_rate", global_rate),
        "route_rate": smoothed_rate(train, ["origin", "dest"], "route_rate", global_rate),
        "airline_origin_rate": smoothed_rate(train, ["airline_code", "origin"], "airline_origin_rate", global_rate),
        "airline_month_rate": smoothed_rate(train, ["airline_code", "month_s"], "airline_month_rate", global_rate),
        "origin_month_rate": smoothed_rate(train, ["origin", "month_s"], "origin_month_rate", global_rate),
        "airline_route_rate": smoothed_rate(train, ["airline_code", "origin", "dest"], "airline_route_rate", global_rate, alpha=30.0),
        "origin_dow_hour_rate": smoothed_rate(train, ["origin", "day_of_week", "dep_hour"], "origin_dow_hour_rate", global_rate, alpha=50.0),
    }


def apply_rates(df: DataFrame, rates: dict, global_rate: float) -> DataFrame:
    out = df
    out = out.join(rates["airline_rate"], "airline_code", "left")
    out = out.join(rates["origin_rate"], "origin", "left")
    out = out.join(rates["dest_rate"], "dest", "left")
    out = out.join(rates["route_rate"], ["origin", "dest"], "left")
    out = out.join(rates["airline_origin_rate"], ["airline_code", "origin"], "left")
    out = out.join(rates["airline_month_rate"], ["airline_code", "month_s"], "left")
    out = out.join(rates["origin_month_rate"], ["origin", "month_s"], "left")
    out = out.join(rates["airline_route_rate"], ["airline_code", "origin", "dest"], "left")
    out = out.join(rates["origin_dow_hour_rate"], ["origin", "day_of_week", "dep_hour"], "left")
    for c in ("airline_rate", "origin_rate", "dest_rate", "route_rate", "airline_origin_rate",
              "airline_month_rate", "origin_month_rate", "airline_route_rate", "origin_dow_hour_rate"):
        out = out.withColumn(c, F.coalesce(F.col(c), F.lit(global_rate)))
    return out


def build_prep_stages() -> list:
    stages = []
    for c in CAT_COLS:
        stages.append(StringIndexer(inputCol=c, outputCol=c + "_idx", handleInvalid="keep"))
    for c in CAT_COLS:
        stages.append(OneHotEncoder(inputCol=c + "_idx", outputCol=c + "_oh", handleInvalid="keep"))
    stages.append(
        VectorAssembler(
            inputCols=[c + "_oh" for c in CAT_COLS] + NUM_COLS,
            outputCol="features",
            handleInvalid="keep",
        )
    )
    return stages


def feature_names(model: PipelineModel, n_features: int) -> list[str]:
    idx_labels: dict[str, list] = {}
    names: list[str] = []
    for stage in model.stages:
        if isinstance(stage, StringIndexerModel):
            idx_labels[stage.getOutputCol()] = list(stage.labels)
        elif isinstance(stage, OneHotEncoderModel):
            in_col = stage.getInputCol()
            cat = in_col[:-4] if in_col.endswith("_idx") else in_col
            labels = idx_labels.get(in_col, [])
            sizes = list(stage.categorySizes)
            size = int(sizes[0]) if sizes else 0
            drop_last = bool(stage.getDropLast())
            # handleInvalid=keep 会额外占一个维度（invalid bucket），dropLast 不再减少维度
            n_dims = size if stage.getHandleInvalid() == "keep" else size - (1 if drop_last and size > 1 else 0)
            for i in range(max(0, n_dims)):
                lab = labels[i] if i < len(labels) else "__invalid__"
                names.append(f"{cat}={lab}")
    names.extend(NUM_COLS)
    if len(names) != n_features:
        names = [f"f{i}" for i in range(n_features)]
    return names


def top_importance(model: PipelineModel) -> list[dict]:
    final = model.stages[-1]
    if hasattr(final, "featureImportances"):
        vals = list(final.featureImportances.toArray())
    elif hasattr(final, "coefficients"):
        vals = list(abs(float(x)) for x in final.coefficients.toArray())
    else:
        return []
    names = feature_names(model, len(vals))
    pairs = sorted(zip(names, vals), key=lambda kv: kv[1], reverse=True)[:15]
    return [{"feature": k, "importance": round(float(v), 6)} for k, v in pairs]


def evaluate(pred: DataFrame) -> dict:
    auc = BinaryClassificationEvaluator(
        labelCol="label", rawPredictionCol="rawPrediction", metricName="areaUnderROC"
    ).evaluate(pred)
    acc = MulticlassClassificationEvaluator(
        labelCol="label", predictionCol="prediction", metricName="accuracy"
    ).evaluate(pred)
    f1 = MulticlassClassificationEvaluator(
        labelCol="label", predictionCol="prediction", metricName="f1"
    ).evaluate(pred)
    prec = MulticlassClassificationEvaluator(
        labelCol="label", predictionCol="prediction", metricName="precisionByLabel", metricLabel=1.0
    ).evaluate(pred)
    rec = MulticlassClassificationEvaluator(
        labelCol="label", predictionCol="prediction", metricName="recallByLabel", metricLabel=1.0
    ).evaluate(pred)
    cm = {f"{int(r['label'])}->{int(r['prediction'])}": int(r["count"])
          for r in pred.groupBy("label", "prediction").count().collect()}
    return {"auc": auc, "accuracy": acc, "f1": f1, "precision": prec, "recall": rec, "cm": cm}


def tune_threshold(model: PipelineModel, val_df: DataFrame, sample_fraction: float = 0.2) -> float:
    """在验证集上按 F1(label=1) 选择阈值，缓解类别不平衡导致的极低 recall。"""
    sample = val_df.sample(False, sample_fraction, seed=7) if sample_fraction < 1 else val_df
    rows = model.transform(sample).select(
        vector_to_array("probability")[1].alias("p"), "label"
    ).collect()
    best_t, best_f1 = 0.5, -1.0
    for i in range(5, 96):
        t = i / 100.0
        tp = fp = fn = 0
        for r in rows:
            p = float(r["p"])
            y = float(r["label"])
            if p >= t:
                if y == 1.0:
                    tp += 1
                else:
                    fp += 1
            elif y == 1.0:
                fn += 1
        if tp == 0:
            continue
        f1 = 2.0 * tp / (2.0 * tp + fp + fn)
        if f1 > best_f1:
            best_t, best_f1 = t, f1
    print(f"threshold tuned: {best_t:.2f} (val F1={best_f1:.4f}, sample={len(rows)})")
    return best_t


def write_metrics(model_name: str, version: str, val: dict, test: dict,
                  importance: list[dict], threshold: float) -> None:
    conn = mysql_conn()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO ads_model_metrics
          (model_name, model_version, auc, val_auc, threshold, accuracy, f1, precision_score, recall_score,
           confusion_matrix, feature_importance, train_time)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON DUPLICATE KEY UPDATE auc=VALUES(auc), val_auc=VALUES(val_auc), threshold=VALUES(threshold),
          accuracy=VALUES(accuracy), f1=VALUES(f1), precision_score=VALUES(precision_score),
          recall_score=VALUES(recall_score), confusion_matrix=VALUES(confusion_matrix),
          feature_importance=VALUES(feature_importance), train_time=VALUES(train_time)
        """,
        (
            model_name, version,
            float(test["auc"]), float(val["auc"]), float(threshold),
            float(test["accuracy"]), float(test["f1"]),
            float(test["precision"]), float(test["recall"]),
            json.dumps(test["cm"], ensure_ascii=False),
            json.dumps(importance, ensure_ascii=False),
            dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        ),
    )
    conn.commit()
    cur.close()
    conn.close()
    print(f"metrics {model_name}: val_auc={val['auc']:.4f} test_auc={test['auc']:.4f} "
          f"threshold={threshold:.2f} f1={test['f1']:.4f} precision={test['precision']:.3f} recall={test['recall']:.3f}")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--models", default="lr,rf,gbt")
    ap.add_argument("--tree-sample", type=float, default=0.4)
    ap.add_argument("--top-predictions", type=int, default=5000)
    args = ap.parse_args()
    wanted = [m.strip().lower() for m in args.models.split(",") if m.strip()]

    spark = SparkSession.builder.appName("train_model").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    base = (
        spark.read.parquet(DWD)
        .filter((F.col("cancelled") == 0) & F.col("arr_delay").isNotNull())
        .filter(F.col("distance").isNotNull())
        .withColumn("label", (F.col("arr_delay") >= 15).cast("double"))
        .withColumn("month_s", F.lpad(F.col("month").cast("string"), 2, "0"))
        .withColumn("day_of_week_s", F.col("day_of_week").cast("string"))
        .withColumn("is_weekend", F.when(F.col("day_of_week") >= 6, 1).otherwise(0))
        .withColumn("is_holiday", F.when(F.col("fl_date").cast("string").isin(*US_HOLIDAYS), 1).otherwise(0))
    )
    traffic_o = base.groupBy("origin", "dep_hour").agg(F.count("*").alias("origin_hour_traffic"))
    traffic_d = base.groupBy("dest", "dep_hour").agg(F.count("*").alias("dest_hour_traffic"))
    base = base.join(traffic_o, ["origin", "dep_hour"], "left")
    base = base.join(traffic_d, ["dest", "dep_hour"], "left")

    try:
        weather = (
            spark.read.parquet(WEATHER_PATH)
            .withColumn("fl_date", F.to_date("fl_date"))
            .withColumn(
                "weather_cat",
                F.when(F.col("flight_category") == "VFR", 3)
                .when(F.col("flight_category") == "MVFR", 2)
                .when(F.col("flight_category") == "IFR", 1)
                .when(F.col("flight_category") == "LIFR", 0)
                .otherwise(-1),
            )
        )
        w_origin = weather.select(
            "origin", "fl_date", "dep_hour",
            F.col("visib").alias("weather_visib"),
            F.col("wind").alias("weather_wind"),
            F.col("precip").alias("weather_precip"),
            "weather_cat",
        )
        w_dest = weather.select(
            F.col("origin").alias("dest"), "fl_date", "dep_hour",
            F.col("visib").alias("dest_visib"),
            F.col("wind").alias("dest_wind"),
            F.col("precip").alias("dest_precip"),
            F.col("weather_cat").alias("dest_cat"),
        )
        base = base.join(w_origin, ["origin", "fl_date", "dep_hour"], "left")
        base = base.join(w_dest, ["dest", "fl_date", "dep_hour"], "left")
        print(f"weather history (origin+dest) joined from {WEATHER_PATH}")
    except Exception as exc:
        print(f"weather history unavailable: {exc}")
        for c in ("weather_visib", "weather_wind", "weather_precip", "dest_visib", "dest_wind", "dest_precip"):
            base = base.withColumn(c, F.lit(-1.0))
        base = base.withColumn("weather_cat", F.lit(-1))
        base = base.withColumn("dest_cat", F.lit(-1))
    for c in ("crs_elapsed_time", "weather_visib", "weather_wind", "weather_precip",
              "dest_visib", "dest_wind", "dest_precip"):
        base = base.withColumn(c, F.coalesce(F.col(c), F.lit(-1.0)))
    base = base.withColumn("weather_cat", F.coalesce(F.col("weather_cat"), F.lit(-1)))
    base = base.withColumn("dest_cat", F.coalesce(F.col("dest_cat"), F.lit(-1)))

    # 前一日延误率（只用前一天已完成的航班标签，预测时已知）
    def add_prev_day_rate(df: DataFrame, keys: list[str], name: str) -> DataFrame:
        daily = df.groupBy(*keys, "fl_date").agg(F.avg("label").alias(name))
        daily = daily.withColumn("fl_date", F.date_add("fl_date", 1))
        return df.join(daily, keys + ["fl_date"], "left")

    base = add_prev_day_rate(base, ["origin"], "origin_prev_day_rate")
    base = add_prev_day_rate(base, ["airline_code"], "airline_prev_day_rate")
    for c in ("origin_prev_day_rate", "airline_prev_day_rate"):
        base = base.withColumn(c, F.coalesce(F.col(c), F.lit(0.18)))

    # 同机场前 2-4 小时的平均起飞延误（前序航班已完成，避免同时刻信息泄漏）
    hourly = base.groupBy("origin", "fl_date", "dep_hour").agg(F.avg("dep_delay").alias("_hour_dep"))
    w = Window.partitionBy("origin", "fl_date").orderBy("dep_hour").rangeBetween(-4, -2)
    hourly = hourly.withColumn("origin_prev_dep_delay", F.avg("_hour_dep").over(w))
    base = base.join(
        hourly.select("origin", "fl_date", "dep_hour", "origin_prev_dep_delay"),
        ["origin", "fl_date", "dep_hour"], "left",
    )
    base = base.withColumn("origin_prev_dep_delay", F.coalesce(F.col("origin_prev_dep_delay"), F.lit(0.0)))

    base = base.persist(StorageLevel.MEMORY_AND_DISK)
    base.count()
    train_raw = base.filter(F.col("fl_date") < F.lit("2022-01-01"))
    val_raw = base.filter((F.col("fl_date") >= F.lit("2022-01-01")) & (F.col("fl_date") < F.lit("2023-01-01")))
    test_raw = base.filter(F.col("fl_date") >= F.lit("2023-01-01"))

    global_rate = train_raw.agg(F.avg("label")).first()[0] or 0.0
    rates = {k: v.cache() for k, v in build_rates(train_raw, global_rate).items()}
    for v in rates.values():
        v.count()
    train = apply_rates(train_raw, rates, global_rate).cache()
    val = apply_rates(val_raw, rates, global_rate).cache()
    test = apply_rates(test_raw, rates, global_rate).cache()
    print(f"rows train={train.count()} val={val.count()} test={test.count()}")
    base.unpersist()
    for v in rates.values():
        v.unpersist()

    tree_train = train.sample(False, args.tree_sample, seed=42).cache() if args.tree_sample < 1 else train
    print(f"tree_train={tree_train.count()} (sample={args.tree_sample})")

    prep = build_prep_stages()
    pipelines = {
        "lr": Pipeline(stages=prep + [
            StandardScaler(inputCol="features", outputCol="scaled", withMean=False, withStd=True),
            LogisticRegression(featuresCol="scaled", labelCol="label", maxIter=50, regParam=0.01),
        ]),
        "rf": Pipeline(stages=prep + [
            RandomForestClassifier(labelCol="label", featuresCol="features", numTrees=50,
                                   maxDepth=7, maxBins=64, subsamplingRate=0.8, seed=42),
        ]),
        "gbt": Pipeline(stages=prep + [
            GBTClassifier(labelCol="label", featuresCol="features", maxIter=40,
                          maxDepth=5, stepSize=0.1, maxBins=64, seed=42),
        ]),
    }
    ts = dt.datetime.now().strftime("%Y%m%d%H%M")
    results = {}
    for name in wanted:
        if name not in pipelines:
            print(f"skip unknown model {name}")
            continue
        data = tree_train if name in ("rf", "gbt") else train
        print(f"=== training {name} on {data.count()} rows ===")
        model = pipelines[name].fit(data)
        threshold = tune_threshold(model, val)
        final = model.stages[-1]
        if hasattr(final, "setThreshold"):
            final.setThreshold(threshold)
        val_pred = model.transform(val).cache()
        test_pred = model.transform(test).cache()
        val_metrics = evaluate(val_pred)
        test_metrics = evaluate(test_pred)
        version = f"{name}-{ts}"
        importance = top_importance(model)
        write_metrics(name, version, val_metrics, test_metrics, importance, threshold)
        results[name] = {
            "model": model, "version": version, "val": val_metrics, "test": test_metrics,
            "test_pred": test_pred, "importance": importance,
        }
        val_pred.unpersist()

    best_name = max(results, key=lambda k: results[k]["val"]["auc"])
    best = results[best_name]
    for k, v in results.items():
        if k != best_name:
            v["test_pred"].unpersist()
    print(f"best model by val AUC: {best_name} ({best['val']['auc']:.4f})")
    best["model"].write().overwrite().save(f"{MODEL_ROOT}/{best['version']}")

    pred = (
        best["test_pred"]
        .withColumn("predict_score", vector_to_array("probability")[1])
        .withColumn("predict_label", F.col("prediction").cast("int"))
        .withColumn("model_version", F.lit(best["version"]))
        .select("fl_date", "airline_code", "fl_number", "origin", "dest", "crs_dep_time",
                "predict_score", "predict_label", "model_version")
    )
    pred.write.mode("overwrite").option("compression", "snappy").parquet(
        f"{PRED_ROOT}/{best['version']}"
    )
    top = pred.orderBy(F.desc("predict_score")).limit(args.top_predictions)
    (
        top.write.format("jdbc")
        .option("url", MYSQL_URL)
        .option("dbtable", "ads_predictions")
        .option("user", os.environ.get("MYSQL_FLIGHT_USER", "flight"))
        .option("password", os.environ["MYSQL_FLIGHT_PASSWORD"])
        .option("driver", "com.mysql.cj.jdbc.Driver")
        .option("batchsize", "1000")
        .option("truncate", "true")
        .mode("overwrite")
        .save()
    )
    print(f"predictions written: top={args.top_predictions} model={best['version']}")
    print("feature importance top:", json.dumps(best["importance"], ensure_ascii=False))
    spark.stop()


if __name__ == "__main__":
    main()
