package com.damai.dto;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotBlank;
import lombok.Data;

@Data
public class TestSendDto {

    private Long count;

    @Schema(name ="message", type ="String", description ="消息",requiredMode= Schema.RequiredMode.REQUIRED)
    @NotBlank
    private String message;

    private Long time;
}