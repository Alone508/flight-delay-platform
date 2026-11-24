package com.flight.api.controller;

import java.util.Map;

import com.flight.api.common.ApiResponse;
import com.flight.api.service.HBaseService;
import com.flight.api.service.QueryService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api")
public class FlightController {
    private final HBaseService hbaseService;
    private final QueryService queryService;

    public FlightController(HBaseService hbaseService, QueryService queryService) {
        this.hbaseService = hbaseService;
        this.queryService = queryService;
    }

    @GetMapping("/flights")
    public ApiResponse<Map<String, Object>> flights(
            @RequestParam(required = false) String date,
            @RequestParam(required = false) String airline,
            @RequestParam(required = false) String origin,
            @RequestParam(required = false) String dest,
            @RequestParam(required = false) Double minDelay,
            @RequestParam(required = false) Double maxDelay,
            @RequestParam(defaultValue = "1") int page,
            @RequestParam(defaultValue = "20") int size) {
        return ApiResponse.ok(queryService.flightList(date, airline, origin, dest, minDelay, maxDelay, page, size));
    }

    @GetMapping("/flights/{rowkey}")
    public ApiResponse<Map<String, Object>> flight(@PathVariable String rowkey) {
        return ApiResponse.ok(hbaseService.getFlight(rowkey));
    }
}
