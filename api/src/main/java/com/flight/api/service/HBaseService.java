package com.flight.api.service;

import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.stereotype.Service;
import org.springframework.web.client.HttpStatusCodeException;
import org.springframework.web.client.RestTemplate;

@Service
public class HBaseService {
    private final RestTemplate rest;
    private final String baseUrl;

    public HBaseService(@Value("${hbase.rest-url}") String baseUrl) {
        this.baseUrl = baseUrl.replaceAll("/+$", "");
        SimpleClientHttpRequestFactory factory = new SimpleClientHttpRequestFactory();
        factory.setConnectTimeout(3000);
        factory.setReadTimeout(5000);
        this.rest = new RestTemplate(factory);
    }

    public Map<String, Object> getFlight(String rowkey) {
        // Spring 可能把 %7C 原样传入；先解码再编码一次，避免双重编码
        String raw = rowkey;
        if (rowkey.indexOf('%') >= 0) {
            try {
                raw = URLDecoder.decode(rowkey, "UTF-8");
            } catch (Exception ignored) {
                raw = rowkey;
            }
        }
        String url = baseUrl + "/flight_detail/" + urlEncode(raw);
        HttpHeaders headers = new HttpHeaders();
        headers.setAccept(Collections.singletonList(MediaType.APPLICATION_JSON));
        try {
            // 传 URI（而非 String），避免 RestTemplate 对 %7C 二次编码
            URI uri;
            try {
                uri = new URI(url);
            } catch (URISyntaxException e) {
                throw new IllegalArgumentException("bad hbase url: " + url, e);
            }
            ResponseEntity<Map> response = rest.exchange(uri, HttpMethod.GET, new HttpEntity<>(headers), Map.class);
            return decodeRow(response.getBody(), rowkey);
        } catch (HttpStatusCodeException ex) {
            if (ex.getStatusCode() == HttpStatus.NOT_FOUND) {
                return Collections.emptyMap();
            }
            throw ex;
        }
    }

    private String urlEncode(String value) {
        try {
            return URLEncoder.encode(value, "UTF-8").replace("+", "%20");
        } catch (Exception ex) {
            throw new IllegalArgumentException("rowkey encode failed", ex);
        }
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> decodeRow(Map body, String rowkey) {
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("rowkey", rowkey);
        if (body == null) {
            return out;
        }
        List<Map<String, Object>> rows = (List<Map<String, Object>>) body.get("Row");
        if (rows == null || rows.isEmpty()) {
            return out;
        }
        List<Map<String, Object>> cells = (List<Map<String, Object>>) rows.get(0).get("Cell");
        if (cells == null) {
            return out;
        }
        Base64.Decoder decoder = Base64.getDecoder();
        for (Map<String, Object> cell : cells) {
            String column = new String(decoder.decode((String) cell.get("column")), StandardCharsets.UTF_8);
            String value = new String(decoder.decode((String) cell.get("$")), StandardCharsets.UTF_8);
            out.put(column, value);
            out.put(column + "_timestamp", cell.get("timestamp"));
        }
        return out;
    }
}
