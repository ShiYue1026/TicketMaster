package com.damai.dto;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.Data;

@Data
@Schema(title="ProgramInvalidDto", description ="节目失效")
public class ProgramInvalidDto {

    @Schema(name ="id", type ="Long", description ="id",requiredMode= Schema.RequiredMode.REQUIRED)
    @NotNull
    private Long id;
}
