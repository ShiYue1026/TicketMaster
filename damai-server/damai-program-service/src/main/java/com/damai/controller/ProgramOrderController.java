package com.damai.controller;

import com.damai.common.ApiResponse;
import com.damai.dto.ProgramOrderCreateDto;
import com.damai.service.ProgramOrderService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/program/order")
@Tag(name = "program-order", description = "节目订单")
public class ProgramOrderController {

    @Autowired
    private ProgramOrderService programOrderService;

    @Operation(summary = "购票V1")
    @PostMapping(value = "/create/v1")
    public ApiResponse<String> createV1(@Valid @RequestBody ProgramOrderCreateDto programOrderCreateDto) {
        return ApiResponse.ok(programOrderService.createV1(programOrderCreateDto));
    }

    @Operation(summary = "购票V2")
    @PostMapping(value = "/create/v2")
    public ApiResponse<String> createV2(@Valid @RequestBody ProgramOrderCreateDto programOrderCreateDto) {
        return ApiResponse.ok(programOrderService.createV1(programOrderCreateDto));
    }

    @Operation(summary = "购票V3")
    @PostMapping(value = "/create/v3")
    public ApiResponse<String> createV3(@Valid @RequestBody ProgramOrderCreateDto programOrderCreateDto) {
        return ApiResponse.ok(programOrderService.createV1(programOrderCreateDto));
    }

    @Operation(summary = "购票V4")
    @PostMapping(value = "/create/v4")
    public ApiResponse<String> createV4(@Valid @RequestBody ProgramOrderCreateDto programOrderCreateDto) {
        return ApiResponse.ok(programOrderService.createV1(programOrderCreateDto));
    }


}
