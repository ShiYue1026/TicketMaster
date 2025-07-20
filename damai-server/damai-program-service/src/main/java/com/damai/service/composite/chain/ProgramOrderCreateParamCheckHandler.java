package com.damai.service.composite.chain;

import cn.hutool.core.collection.CollectionUtil;
import com.damai.dto.ProgramOrderCreateDto;
import com.damai.dto.SeatDto;
import com.damai.enums.BaseCode;
import com.damai.exception.DaMaiFrameException;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

@Component
public class ProgramOrderCreateParamCheckHandler extends ProgramOrderCheckHandler {
    @Override
    protected void check(ProgramOrderCreateDto programOrderCreateDto) {
        List<SeatDto> seatDtoList = programOrderCreateDto.getSeatDtoList();
        List<Long> ticketUserIdList = programOrderCreateDto.getTicketUserIdList();
        Map<Long, List<Long>> ticketUserIdMap =
                ticketUserIdList.stream().collect(Collectors.groupingBy(ticketUserId -> ticketUserId));
        for (List<Long> value : ticketUserIdMap.values()) {
            if(value.size() > 1) {
                throw new DaMaiFrameException(BaseCode.TICKET_USER_ID_REPEAT);
            }
        }

        // 手动选座
        if(CollectionUtil.isNotEmpty(seatDtoList)){
            if(seatDtoList.size() != programOrderCreateDto.getTicketUserIdList().size()) {
                throw new DaMaiFrameException(BaseCode.TICKET_USER_COUNT_UNEQUAL_SEAT_COUNT);
            }
            for (SeatDto seatDto : seatDtoList) {
                if(Objects.isNull(seatDto.getId())) {
                    throw new DaMaiFrameException(BaseCode.SEAT_ID_EMPTY);
                }
                if(Objects.isNull(seatDto.getTicketCategoryId())){
                    throw new DaMaiFrameException(BaseCode.SEAT_TICKET_CATEGORY_ID_EMPTY);
                }
                if(Objects.isNull(seatDto.getRowCode())){
                    throw new DaMaiFrameException(BaseCode.SEAT_COL_CODE_EMPTY);
                }
                if(Objects.isNull(seatDto.getColCode())){
                    throw new DaMaiFrameException(BaseCode.SEAT_COL_CODE_EMPTY);
                }
                if(Objects.isNull(seatDto.getPrice())){
                    throw new DaMaiFrameException(BaseCode.SEAT_PRICE_EMPTY);
                }
            }
        } else{  // 自动选座
            if(Objects.isNull(programOrderCreateDto.getTicketCategoryId())){
                throw new DaMaiFrameException(BaseCode.TICKET_CATEGORY_NOT_EXIST);
            }
            if(Objects.isNull(programOrderCreateDto.getTicketCount())){
                throw new DaMaiFrameException(BaseCode.TICKET_COUNT_NOT_EXIST);
            }
            if(programOrderCreateDto.getTicketCount() <= 0){
                throw new DaMaiFrameException(BaseCode.TICKET_COUNT_ERROR);
            }
        }
    }
}
