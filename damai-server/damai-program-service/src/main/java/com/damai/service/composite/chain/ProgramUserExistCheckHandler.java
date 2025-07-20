package com.damai.service.composite.chain;

import cn.hutool.core.collection.CollectionUtil;
import com.alibaba.fastjson.JSON;
import com.damai.client.OrderClient;
import com.damai.client.UserClient;
import com.damai.common.ApiResponse;
import com.damai.core.RedisKeyManage;
import com.damai.dto.AccountOrderCountDto;
import com.damai.dto.ProgramGetDto;
import com.damai.dto.ProgramOrderCreateDto;
import com.damai.dto.TicketUserListDto;
import com.damai.enums.BaseCode;
import com.damai.exception.DaMaiFrameException;
import com.damai.redis.RedisCache;
import com.damai.redis.RedisKeyBuild;
import com.damai.service.ProgramService;
import com.damai.service.tool.TokenExpireManager;
import com.damai.vo.AccountOrderCountVo;
import com.damai.vo.ProgramVo;
import com.damai.vo.TicketUserVo;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@Slf4j
@Component
public class ProgramUserExistCheckHandler extends ProgramOrderCheckHandler {
    @Autowired
    private RedisCache redisCache;

    @Autowired
    private UserClient userClient;

    @Autowired
    private ProgramService programService;

    @Autowired
    private OrderClient orderClient;

    @Autowired
    private TokenExpireManager tokenExpireManager;

    @Override
    protected void check(ProgramOrderCreateDto programOrderCreateDto) {
        // 先从缓存中查询当前用户的购票人信息
        List<TicketUserVo> ticketUserVoList = redisCache.getValueIsList(
                RedisKeyBuild.createRedisKey(RedisKeyManage.TICKET_USER_LIST, programOrderCreateDto.getUserId()),
                TicketUserVo.class
        );
        // 缓存中没有，从数据库中查
        if(CollectionUtil.isEmpty(ticketUserVoList)){
            TicketUserListDto ticketUserListDto = new TicketUserListDto();
            ticketUserListDto.setUserId(programOrderCreateDto.getUserId());
            ApiResponse<List<TicketUserVo>> apiResponse = userClient.list(ticketUserListDto);
            if(Objects.equals(apiResponse.getCode(), BaseCode.SUCCESS.getCode())) {
                ticketUserVoList = apiResponse.getData();
            } else {
                log.error("user client rpc getUserAndTicketUserList select response : {}", JSON.toJSONString(apiResponse));
                throw new DaMaiFrameException(apiResponse);
            }
        }
        if(CollectionUtil.isEmpty(ticketUserVoList)){
            throw new DaMaiFrameException(BaseCode.TICKET_USER_EMPTY);
        }

        Map<Long, TicketUserVo> ticketUserVoMap = ticketUserVoList.stream()
                .collect(Collectors.toMap(TicketUserVo::getId, ticketUserVo -> ticketUserVo));
        for (Long ticketUserId : programOrderCreateDto.getTicketUserIdList()) {
            if(Objects.isNull(ticketUserVoMap.get(ticketUserId))){
                throw new DaMaiFrameException(BaseCode.TICKET_USER_EMPTY);
            }
        }

        // 统计购买数量是否超过票数限制
//        ProgramGetDto programGetDto = new ProgramGetDto();
//        programGetDto.setId(programOrderCreateDto.getProgramId());
//        ProgramVo programVo = programService.detailV2(programGetDto);
//        if(Objects.isNull(programVo)){
//            throw new DaMaiFrameException(BaseCode.PROGRAM_NOT_EXIST);
//        }
//        Integer count = 0;
//        if(redisCache.hasKey(RedisKeyBuild.createRedisKey(RedisKeyManage.ACCOUNT_ORDER_COUNT, programOrderCreateDto.getUserId(), programOrderCreateDto.getProgramId()))) {
//            count = redisCache.get(RedisKeyBuild.createRedisKey(RedisKeyManage.ACCOUNT_ORDER_COUNT, programOrderCreateDto.getUserId(), programOrderCreateDto.getProgramId()), Integer.class);
//        } else{
//            AccountOrderCountDto accountOrderCountDto = new AccountOrderCountDto();
//            accountOrderCountDto.setUserId(programOrderCreateDto.getUserId());
//            accountOrderCountDto.setProgramId(programOrderCreateDto.getProgramId());
//            ApiResponse<AccountOrderCountVo> apiResponse = orderClient.accountOrderCount(accountOrderCountDto);
//            if(Objects.equals(apiResponse.getCode(), BaseCode.SUCCESS.getCode())) {
//                count = Optional.ofNullable(apiResponse.getData()).map(AccountOrderCountVo::getCount).orElse(0);
//                redisCache.set(
//                        RedisKeyBuild.createRedisKey(RedisKeyManage.ACCOUNT_ORDER_COUNT, programOrderCreateDto.getUserId(), programOrderCreateDto.getProgramId()),
//                        count,
//                        tokenExpireManager.getTokenExpireTime() + 1,
//                        TimeUnit.MINUTES);
//            }
//        }
//
//        Integer seatCount = Optional.ofNullable(programOrderCreateDto.getSeatDtoList()).map(List::size).orElse(0);
//        Integer ticketCount = Optional.ofNullable(programOrderCreateDto.getTicketCount()).orElse(0);
//
//        if(seatCount != 0){
//            count += seatCount;
//        }
//        else if(ticketCount != 0){
//            count += ticketCount;
//        }
//
//        if(count > programVo.getPerAccountLimitPurchaseCount()) {
//            throw new DaMaiFrameException(BaseCode.PER_ACCOUNT_PURCHASE_COUNT_OVER_LIMIT);
//        }
    }
}
