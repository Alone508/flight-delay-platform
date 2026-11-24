package com.flight.api.service;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

@Service
public class QueryService {
    private final JdbcTemplate jdbc;

    public QueryService(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    public List<Map<String, Object>> realtimeKpi() {
        return jdbc.queryForList(
                "SELECT k.stat_time, k.airport, a.name AS airport_name, a.city, a.latitude, a.longitude, " +
                "k.departures, k.delayed_flights, k.cancelled, k.avg_dep_delay, k.ontime_rate, " +
                "k.visib, k.wind, k.flight_category " +
                "FROM ads_realtime_kpi k LEFT JOIN dim_airport a ON a.airport_code = k.airport " +
                "WHERE k.stat_time = (SELECT MAX(stat_time) FROM ads_realtime_kpi) " +
                "ORDER BY k.departures DESC");
    }

    public List<Map<String, Object>> realtimeTrend(int minutes) {
        return jdbc.queryForList(
                "SELECT stat_time, SUM(departures) AS departures, " +
                "SUM(delayed_flights) AS delayed_flights, SUM(cancelled) AS cancelled, " +
                "AVG(avg_dep_delay) AS avg_dep_delay, " +
                "1 - SUM(delayed_flights) / NULLIF(SUM(departures), 0) AS ontime_rate " +
                "FROM ads_realtime_kpi " +
                "WHERE stat_time >= DATE_SUB((SELECT MAX(stat_time) FROM ads_realtime_kpi), INTERVAL ? MINUTE) " +
                "GROUP BY stat_time ORDER BY stat_time", minutes);
    }

    public List<Map<String, Object>> airportsRank(int limit) {
        return jdbc.queryForList(
                "SELECT o.stat_month, o.airport, a.name AS airport_name, a.city, a.latitude, a.longitude, " +
                "o.flights, o.delayed_flights, o.ontime_rate, o.avg_arr_delay " +
                "FROM ads_ontime_airport o LEFT JOIN dim_airport a ON a.airport_code = o.airport " +
                "WHERE o.stat_month = (SELECT MAX(stat_month) FROM ads_ontime_airport) " +
                "AND o.flights >= 100 " +
                "ORDER BY o.ontime_rate DESC LIMIT ?", limit);
    }

    public List<Map<String, Object>> airlinesRank(int limit) {
        return jdbc.queryForList(
                "SELECT stat_month, airline_code, airline_name, flights, delayed_flights, " +
                "ontime_rate, avg_arr_delay FROM ads_ontime_airline " +
                "WHERE stat_month = (SELECT MAX(stat_month) FROM ads_ontime_airline) " +
                "ORDER BY ontime_rate DESC LIMIT ?", limit);
    }

    public List<Map<String, Object>> delayCauses(String month) {
        if (month == null || month.trim().isEmpty()) {
            return jdbc.queryForList(
                    "SELECT stat_month, cause, delay_minutes, share FROM ads_delay_cause " +
                    "WHERE stat_month = (SELECT MAX(stat_month) FROM ads_delay_cause) " +
                    "ORDER BY delay_minutes DESC");
        }
        return jdbc.queryForList(
                "SELECT stat_month, cause, delay_minutes, share FROM ads_delay_cause " +
                "WHERE stat_month = ? ORDER BY delay_minutes DESC", month);
    }

    public List<Map<String, Object>> delayHeatmap() {
        return jdbc.queryForList(
                "SELECT day_of_week, dep_hour, flights, delayed_flights, avg_arr_delay " +
                "FROM ads_delay_heatmap ORDER BY day_of_week, dep_hour");
    }

    public List<Map<String, Object>> routesTop(int limit) {
        return jdbc.queryForList(
                "SELECT r.origin, r.dest, ao.city AS origin_city, ad.city AS dest_city, " +
                "ao.latitude AS origin_lat, ao.longitude AS origin_lon, " +
                "ad.latitude AS dest_lat, ad.longitude AS dest_lon, " +
                "r.flights, r.avg_arr_delay, r.ontime_rate " +
                "FROM ads_route_stats r " +
                "LEFT JOIN dim_airport ao ON ao.airport_code = r.origin " +
                "LEFT JOIN dim_airport ad ON ad.airport_code = r.dest " +
                "WHERE ao.latitude IS NOT NULL AND ad.latitude IS NOT NULL " +
                "ORDER BY r.flights DESC LIMIT ?", limit);
    }

    public List<Map<String, Object>> alerts(int limit) {
        return jdbc.queryForList(
                "SELECT id, alert_time, airport, level, reason, avg_delay, delayed_ratio " +
                "FROM ads_alerts ORDER BY alert_time DESC, id DESC LIMIT ?", limit);
    }

    public List<Map<String, Object>> predictionsTop(int limit) {
        return jdbc.queryForList(
                "SELECT fl_date, airline_code, fl_number, origin, dest, crs_dep_time, " +
                "predict_score, predict_label, model_version FROM ads_predictions " +
                "ORDER BY predict_score DESC LIMIT ?", limit);
    }

    public List<Map<String, Object>> modelMetrics() {
        return jdbc.queryForList(
                "SELECT model_name, model_version, auc, val_auc, threshold, accuracy, f1, " +
                "precision_score, recall_score, confusion_matrix, feature_importance, train_time " +
                "FROM ads_model_metrics ORDER BY train_time DESC");
    }

    public List<Map<String, Object>> airports() {
        return jdbc.queryForList(
                "SELECT airport_code, name, city, country, latitude, longitude, flights " +
                "FROM dim_airport WHERE latitude IS NOT NULL ORDER BY flights DESC");
    }

    public Map<String, Object> flightList(String date, String airline, String origin, String dest,
                                          Double minDelay, Double maxDelay, int page, int size) {
        StringBuilder where = new StringBuilder(" WHERE 1=1");
        List<Object> args = new ArrayList<>();
        if (date != null && !date.trim().isEmpty()) {
            where.append(" AND fl_date = ?");
            args.add(date.trim());
        }
        if (airline != null && !airline.trim().isEmpty()) {
            where.append(" AND airline_code = ?");
            args.add(airline.trim().toUpperCase());
        }
        if (origin != null && !origin.trim().isEmpty()) {
            where.append(" AND origin = ?");
            args.add(origin.trim().toUpperCase());
        }
        if (dest != null && !dest.trim().isEmpty()) {
            where.append(" AND dest = ?");
            args.add(dest.trim().toUpperCase());
        }
        if (minDelay != null) {
            where.append(" AND arr_delay >= ?");
            args.add(minDelay);
        }
        if (maxDelay != null) {
            where.append(" AND arr_delay <= ?");
            args.add(maxDelay);
        }
        Long total = jdbc.queryForObject("SELECT COUNT(*) FROM ads_flight_list" + where, Long.class, args.toArray());
        List<Object> pageArgs = new ArrayList<>(args);
        pageArgs.add(size);
        pageArgs.add(Math.max(0, (page - 1) * size));
        List<Map<String, Object>> rows = jdbc.queryForList(
                "SELECT rowkey, fl_date, airline_code, airline_name, fl_number, origin, dest, " +
                "crs_dep_time, dep_delay, arr_delay, cancelled, status, distance " +
                "FROM ads_flight_list" + where +
                " ORDER BY fl_date DESC, crs_dep_time LIMIT ? OFFSET ?",
                pageArgs.toArray());
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("total", total == null ? 0 : total);
        result.put("page", page);
        result.put("size", size);
        result.put("rows", rows);
        return result;
    }
}
