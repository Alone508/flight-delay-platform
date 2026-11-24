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
public class AnalysisController {
    private final QueryService queryService;

    public AnalysisController(QueryService queryService) {
        this.queryService = queryService;
    }

    @GetMapping("/airports/rank")
    public ApiResponse<List<Map<String, Object>>> airportsRank(@RequestParam(defaultValue = "10") int limit) {
        return ApiResponse.ok(queryService.airportsRank(limit));
    }

    @GetMapping("/airlines/rank")
    public ApiResponse<List<Map<String, Object>>> airlinesRank(@RequestParam(defaultValue = "10") int limit) {
        return ApiResponse.ok(queryService.airlinesRank(limit));
    }

    @GetMapping("/delay/causes")
    public ApiResponse<List<Map<String, Object>>> delayCauses(
            @RequestParam(required = false) String date) {
        return ApiResponse.ok(queryService.delayCauses(date));
    }

    @GetMapping("/delay/heatmap")
    public ApiResponse<List<Map<String, Object>>> delayHeatmap() {
        return ApiResponse.ok(queryService.delayHeatmap());
    }

    @GetMapping("/routes/top")
    public ApiResponse<List<Map<String, Object>>> routesTop(@RequestParam(defaultValue = "20") int limit) {
        return ApiResponse.ok(queryService.routesTop(limit));
    }

    @GetMapping("/airports")
    public ApiResponse<List<Map<String, Object>>> airports() {
        return ApiResponse.ok(queryService.airports());
    }
}
