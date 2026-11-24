-- 航班延误监测与预测平台 — MySQL 8.0 表结构
-- 维度表由 collector/bootstrap_ods.py 填充；ADS 表由 Spark 作业写入。
-- 所有写入统一使用 INSERT ... ON DUPLICATE KEY UPDATE，保证任务重跑幂等。

CREATE DATABASE IF NOT EXISTS flight_delay
  DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci;
USE flight_delay;

-- ============ 维度表 ============
CREATE TABLE IF NOT EXISTS dim_airline (
  airline_code VARCHAR(8)  NOT NULL COMMENT 'IATA 航司代码，如 WN',
  airline_name VARCHAR(160) NOT NULL,
  flights      INT NOT NULL DEFAULT 0 COMMENT '样本内航班量',
  PRIMARY KEY (airline_code)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS dim_airport (
  airport_code VARCHAR(8)  NOT NULL COMMENT 'IATA 机场代码，如 JFK',
  name         VARCHAR(200) NULL,
  city         VARCHAR(120) NULL,
  country      VARCHAR(8)   NULL,
  latitude     DECIMAL(10,6) NULL,
  longitude    DECIMAL(10,6) NULL,
  flights      INT NOT NULL DEFAULT 0 COMMENT '作为出发或到达的样本航班量',
  PRIMARY KEY (airport_code),
  KEY idx_dim_airport_city (city)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS dim_date (
  date_key    DATE NOT NULL,
  year        SMALLINT NOT NULL,
  quarter     TINYINT NOT NULL,
  month       TINYINT NOT NULL,
  day         TINYINT NOT NULL,
  day_of_week TINYINT NOT NULL COMMENT '1=周一 ... 7=周日',
  day_name    VARCHAR(12) NOT NULL,
  is_weekend  TINYINT NOT NULL DEFAULT 0,
  PRIMARY KEY (date_key),
  KEY idx_dim_date_ym (year, month)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 最新天气快照（天气采集器写入，Spark Streaming 关联用）
CREATE TABLE IF NOT EXISTS weather_latest (
  airport         VARCHAR(8) NOT NULL,
  icao            VARCHAR(8) NULL,
  wind            DOUBLE NULL,
  visib           DOUBLE NULL,
  flight_category VARCHAR(16) NULL,
  temperature     DOUBLE NULL,
  source          VARCHAR(16) NULL,
  is_stale        TINYINT NOT NULL DEFAULT 0,
  obs_time        DATETIME NULL,
  updated_at      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (airport)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ============ ADS 结果表 ============
CREATE TABLE IF NOT EXISTS ads_realtime_kpi (
  stat_time       DATETIME NOT NULL COMMENT '1 分钟窗口起点',
  airport         VARCHAR(8) NOT NULL,
  departures      INT NOT NULL DEFAULT 0,
  delayed_flights         INT NOT NULL DEFAULT 0,
  cancelled       INT NOT NULL DEFAULT 0,
  avg_dep_delay   DOUBLE NOT NULL DEFAULT 0,
  ontime_rate     DOUBLE NOT NULL DEFAULT 0,
  visib           DOUBLE NULL,
  wind            DOUBLE NULL,
  flight_category VARCHAR(16) NULL,
  updated_at      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (stat_time, airport),
  KEY idx_realtime_airport (airport, stat_time)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS ads_ontime_airline (
  stat_month   CHAR(7) NOT NULL COMMENT 'YYYY-MM',
  airline_code VARCHAR(8) NOT NULL,
  airline_name VARCHAR(160) NULL,
  flights      INT NOT NULL DEFAULT 0,
  delayed_flights      INT NOT NULL DEFAULT 0,
  ontime_rate  DOUBLE NOT NULL DEFAULT 0,
  avg_arr_delay DOUBLE NOT NULL DEFAULT 0,
  PRIMARY KEY (stat_month, airline_code),
  KEY idx_airline_month (stat_month, ontime_rate)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS ads_ontime_airport (
  stat_month   CHAR(7) NOT NULL,
  airport      VARCHAR(8) NOT NULL,
  flights      INT NOT NULL DEFAULT 0,
  delayed_flights      INT NOT NULL DEFAULT 0,
  ontime_rate  DOUBLE NOT NULL DEFAULT 0,
  avg_arr_delay DOUBLE NOT NULL DEFAULT 0,
  PRIMARY KEY (stat_month, airport),
  KEY idx_airport_month (stat_month, ontime_rate)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS ads_delay_cause (
  stat_month    CHAR(7) NOT NULL,
  cause         VARCHAR(24) NOT NULL COMMENT 'CARRIER/WEATHER/NAS/SECURITY/LATE_AIRCRAFT',
  delay_minutes DOUBLE NOT NULL DEFAULT 0,
  share         DOUBLE NOT NULL DEFAULT 0,
  PRIMARY KEY (stat_month, cause)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS ads_route_stats (
  origin        VARCHAR(8) NOT NULL,
  dest          VARCHAR(8) NOT NULL,
  flights       INT NOT NULL DEFAULT 0,
  avg_arr_delay DOUBLE NOT NULL DEFAULT 0,
  ontime_rate   DOUBLE NOT NULL DEFAULT 0,
  PRIMARY KEY (origin, dest),
  KEY idx_route_flights (flights)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS ads_delay_heatmap (
  day_of_week   TINYINT NOT NULL COMMENT '1=周一 ... 7=周日',
  dep_hour      TINYINT NOT NULL COMMENT '0-23',
  flights       INT NOT NULL DEFAULT 0,
  delayed_flights       INT NOT NULL DEFAULT 0,
  avg_arr_delay DOUBLE NOT NULL DEFAULT 0,
  PRIMARY KEY (day_of_week, dep_hour)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS ads_alerts (
  id            BIGINT NOT NULL AUTO_INCREMENT,
  alert_time    DATETIME NOT NULL,
  airport       VARCHAR(8) NOT NULL,
  level         VARCHAR(16) NOT NULL,
  reason        VARCHAR(160) NOT NULL,
  avg_delay     DOUBLE NOT NULL DEFAULT 0,
  delayed_ratio DOUBLE NOT NULL DEFAULT 0,
  created_at    TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (id),
  KEY idx_alerts_time (alert_time),
  KEY idx_alerts_airport (airport, alert_time)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS ads_predictions (
  fl_date       DATE NOT NULL,
  airline_code  VARCHAR(8) NOT NULL,
  fl_number     INT NOT NULL,
  origin        VARCHAR(8) NOT NULL,
  dest          VARCHAR(8) NOT NULL,
  crs_dep_time  CHAR(4) NOT NULL,
  predict_score DOUBLE NOT NULL,
  predict_label TINYINT NOT NULL,
  model_version VARCHAR(64) NOT NULL,
  PRIMARY KEY (fl_date, airline_code, fl_number, origin, crs_dep_time),
  KEY idx_pred_score (predict_score)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 航班明细索引（供前端列表/筛选；完整字段在 HBase flight_detail）
CREATE TABLE IF NOT EXISTS ads_flight_list (
  rowkey        VARCHAR(80) NOT NULL,
  fl_date       DATE NOT NULL,
  airline_code  VARCHAR(8) NULL,
  airline_name  VARCHAR(160) NULL,
  fl_number     INT NULL,
  origin        VARCHAR(8) NULL,
  dest          VARCHAR(8) NULL,
  crs_dep_time  CHAR(4) NULL,
  dep_delay     DOUBLE NULL,
  arr_delay     DOUBLE NULL,
  cancelled     TINYINT NULL,
  status        VARCHAR(16) NULL,
  distance      DOUBLE NULL,
  PRIMARY KEY (rowkey),
  KEY idx_fl_date (fl_date),
  KEY idx_fl_airline (airline_code),
  KEY idx_fl_origin (origin),
  KEY idx_fl_dest (dest)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS ads_model_metrics (
  model_name         VARCHAR(32) NOT NULL,
  model_version      VARCHAR(64) NOT NULL,
  auc                DOUBLE NULL,
  val_auc            DOUBLE NULL,
  threshold          DOUBLE NULL,
  accuracy           DOUBLE NULL,
  f1                 DOUBLE NULL,
  precision_score    DOUBLE NULL,
  recall_score       DOUBLE NULL,
  confusion_matrix   TEXT NULL,
  feature_importance TEXT NULL,
  train_time         DATETIME NOT NULL,
  PRIMARY KEY (model_version)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
