package com.flight.api.controller;

import java.util.List;
import java.util.Map;

import com.flight.api.common.ApiResponse;
import com.flight.api.service.QueryService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api")
public class AlertController {
    private final QueryService queryService;

    public AlertController(QueryService queryService) {
        this.queryService = queryService;
    }

    @GetMapping("/alerts")
    public ApiResponse<List<Map<String, Object>>> alerts(@RequestParam(defaultValue = "20") int limit) {
        return ApiResponse.ok(queryService.alerts(limit));
    }
}
