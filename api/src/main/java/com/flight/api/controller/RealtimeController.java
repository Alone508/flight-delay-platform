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
@RequestMapping("/api/realtime")
public class RealtimeController {
    private final QueryService queryService;

    public RealtimeController(QueryService queryService) {
        this.queryService = queryService;
    }

    @GetMapping("/kpi")
    public ApiResponse<List<Map<String, Object>>> kpi() {
        return ApiResponse.ok(queryService.realtimeKpi());
    }

    @GetMapping("/trend")
    public ApiResponse<List<Map<String, Object>>> trend(@RequestParam(defaultValue = "60") int minutes) {
        return ApiResponse.ok(queryService.realtimeTrend(minutes));
    }
}
