package com.damai.service;

import cn.hutool.core.collection.CollectionUtil;
import cn.hutool.core.util.StrUtil;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson2.JSON;
import com.baidu.fsg.uid.UidGenerator;
import com.damai.client.OrderClient;
import com.damai.common.ApiResponse;
import com.damai.core.RedisKeyManage;
import com.damai.core.RepeatExecuteLimitConstants;
import com.damai.dto.*;
import com.damai.entity.ProgramShowTime;
import com.damai.enums.BaseCode;
import com.damai.enums.OrderStatus;
import com.damai.enums.SellStatus;
import com.damai.exception.DaMaiFrameException;
import com.damai.initialize.impl.composite.CompositeContainer;
import com.damai.locallock.LocalLockCache;
import com.damai.lua.ProgramCacheCreateOrderData;
import com.damai.lua.ProgramCacheCreateOrderResolutionOperate;
import com.damai.lua.ProgramCacheResolutionOperate;
import com.damai.redis.RedisKeyBuild;
import com.damai.repeatexecutelimit.annotation.RepeatExecuteLimit;
import com.damai.service.composite.chain.ProgramDetailCheckHandler;
import com.damai.service.composite.chain.ProgramOrderCreateParamCheckHandler;
import com.damai.service.composite.chain.ProgramUserExistCheckHandler;
import com.damai.service.delaysend.DelayOrderCancelSend;
import com.damai.service.kafka.CreateOrderMqDomain;
import com.damai.service.kafka.CreateOrderSend;
import com.damai.service.strategy.BaseProgramOrder;
import com.damai.service.tool.SeatMatch;
import com.damai.servicelock.LockType;
import com.damai.servicelock.annotation.ServiceLock;
import com.damai.util.DateUtils;
import com.damai.util.ServiceLockTool;
import com.damai.vo.ProgramVo;
import com.damai.vo.SeatVo;
import com.damai.vo.TicketCategoryVo;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

import static com.damai.core.DistributedLockConstants.*;
import static com.damai.service.constant.ProgramOrderConstant.ORDER_TABLE_COUNT;

@Slf4j
@Service
public class ProgramOrderService {

    @Autowired
    private ProgramShowTimeService programShowTimeService;

    @Autowired
    private TicketCategoryService ticketCategoryService;

    @Autowired
    private  SeatService seatService;

    @Autowired
    private ProgramCacheResolutionOperate programCacheResolutionOperate;

    @Autowired
    private ProgramService programService;

    @Autowired
    private DelayOrderCancelSend delayOrderCancelSend;

    @Autowired
    private UidGenerator uidGenerator;

    @Qualifier("com.damai.client.OrderClient")
    @Autowired
    private OrderClient orderClient;

    @Autowired
    private CompositeContainer compositeContainer;

    @Autowired
    private LocalLockCache localLockCache;

    @Autowired
    private ServiceLockTool serviceLockTool;

    @Autowired
    private BaseProgramOrder baseProgramOrder;

    @Autowired
    private ProgramCacheCreateOrderResolutionOperate programCacheCreateOrderResolutionOperate;

    @Autowired
    private CreateOrderSend createOrderSend;

    @Autowired
    private ProgramOrderCreateParamCheckHandler programOrderCreateParamCheckHandler;

    @Autowired
    private ProgramDetailCheckHandler programDetailCheckHandler;

    @Autowired
    private ProgramUserExistCheckHandler programUserExistCheckHandler;

    @RepeatExecuteLimit(
            name = RepeatExecuteLimitConstants.CREATE_PROGRAM_ORDER,
            keys = {"#programOrderCreateDto.userId","#programOrderCreateDto.programId"})
    @ServiceLock(name = PROGRAM_ORDER_CREATE_V1,keys = {"#programOrderCreateDto.programId"})
    public String createV1(ProgramOrderCreateDto programOrderCreateDto) {
        log.info("生成订单版本: V1");
//        compositeContainer.execute(CompositeCheckType.PROGRAM_ORDER_CREATE_CHECK.getValue(),programOrderCreateDto);

        // 责任链模式进行验证
        programOrderCreateParamCheckHandler.setNextHandler(programDetailCheckHandler);
        programDetailCheckHandler.setNextHandler(programUserExistCheckHandler);
        programOrderCreateParamCheckHandler.execute(programOrderCreateDto);

        return create(programOrderCreateDto);
    }

    @RepeatExecuteLimit(
            name = RepeatExecuteLimitConstants.CREATE_PROGRAM_ORDER,
            keys = {"#programOrderCreateDto.userId","#programOrderCreateDto.programId"})
    public String createV2(ProgramOrderCreateDto programOrderCreateDto) {
        log.info("生成订单版本: V2");

        // 责任链模式进行验证
        programOrderCreateParamCheckHandler.setNextHandler(programDetailCheckHandler);
        programDetailCheckHandler.setNextHandler(programUserExistCheckHandler);
        programOrderCreateParamCheckHandler.execute(programOrderCreateDto);

        List<SeatDto> seatDtoList = programOrderCreateDto.getSeatDtoList();
        List<Long> ticketCategoryIdList = new ArrayList<>();
        if(CollectionUtil.isNotEmpty(seatDtoList)){  // 手动选座
            ticketCategoryIdList =
                    seatDtoList.stream().map(SeatDto::getTicketCategoryId).distinct().collect(Collectors.toList());
        } else {  // 自动选座
            ticketCategoryIdList.add(programOrderCreateDto.getTicketCategoryId());
        }
        List<ReentrantLock> localLockList = new ArrayList<>(ticketCategoryIdList.size());  // 本地锁集合
        List<RLock> serviceLockList = new ArrayList<>(ticketCategoryIdList.size());  // 分布式锁集合
        List<ReentrantLock> localLockSuccessList = new ArrayList<>(ticketCategoryIdList.size()); // 加锁成功过的本地锁集合
        List<RLock> serviceLockSuccessList = new ArrayList<>(ticketCategoryIdList.size());  // 加锁成功的分布式锁集合
        for (Long ticketCategoryId : ticketCategoryIdList) {
            String lockKey = StrUtil.join("-", PROGRAM_ORDER_CREATE_V2,
                    programOrderCreateDto.getProgramId(), ticketCategoryId);
            ReentrantLock localLock = localLockCache.getLock(lockKey, false);  // 非公平锁效率更高
            RLock serviceLock = serviceLockTool.getLock(LockType.Reentrant, lockKey);
            localLockList.add(localLock);
            serviceLockList.add(serviceLock);
        }
        for (ReentrantLock reentrantLock : localLockList) {
            try{
                reentrantLock.lock();
            } catch(Throwable t){
                break;
            }
            localLockSuccessList.add(reentrantLock);
        }
        for (RLock rLock : serviceLockList) {
            try{
                rLock.lock();
            } catch(Throwable t){
                break;
            }
            serviceLockSuccessList.add(rLock);
        }
        try{
            return create(programOrderCreateDto);
        } finally {  // 解锁顺序与加锁顺序相反
            for(int i=serviceLockSuccessList.size() - 1; i>=0; i--){
                RLock rLock = serviceLockSuccessList.get(i);
                try{
                    rLock.unlock();
                } catch(Throwable t){
                    log.error("service lock unlock error",t);
                }
            }

            for(int i=localLockSuccessList.size() - 1; i>=0; i--){
                ReentrantLock reentrantLock = localLockSuccessList.get(i);
                try{
                    reentrantLock.unlock();
                } catch(Throwable t){
                    log.error("local lock unlock error",t);
                }
            }
        }
    }

    @RepeatExecuteLimit(
            name = RepeatExecuteLimitConstants.CREATE_PROGRAM_ORDER,
            keys = {"#programOrderCreateDto.userId","#programOrderCreateDto.programId"})
    public String createV3(ProgramOrderCreateDto programOrderCreateDto) {
        log.info("生成订单版本: V3");

        // 责任链模式进行验证
        programOrderCreateParamCheckHandler.setNextHandler(programDetailCheckHandler);
        programDetailCheckHandler.setNextHandler(programUserExistCheckHandler);
        programOrderCreateParamCheckHandler.execute(programOrderCreateDto);

        return baseProgramOrder.localLockCreateOrder(PROGRAM_ORDER_CREATE_V3, programOrderCreateDto, () -> createNew(programOrderCreateDto));
    }

    @RepeatExecuteLimit(
            name = RepeatExecuteLimitConstants.CREATE_PROGRAM_ORDER,
            keys = {"#programOrderCreateDto.userId","#programOrderCreateDto.programId"})
    public String createV4(ProgramOrderCreateDto programOrderCreateDto) {
        log.info("生成订单版本: V4");

        // 责任链模式进行验证
        programOrderCreateParamCheckHandler.setNextHandler(programDetailCheckHandler);
        programDetailCheckHandler.setNextHandler(programUserExistCheckHandler);
        programOrderCreateParamCheckHandler.execute(programOrderCreateDto);

        return baseProgramOrder.localLockCreateOrder(PROGRAM_ORDER_CREATE_V4, programOrderCreateDto, () -> createNewAsync(programOrderCreateDto));
    }

    private String createNewAsync(ProgramOrderCreateDto programOrderCreateDto) {
        List<SeatVo> purchaseSeatList = createOrderOperateProgramCacheResolution(programOrderCreateDto);
        return doCreateV2(programOrderCreateDto, purchaseSeatList);
    }

    private String createNew(ProgramOrderCreateDto programOrderCreateDto) {
        List<SeatVo> purchaseSeatList = createOrderOperateProgramCacheResolution(programOrderCreateDto);
        return doCreate(programOrderCreateDto, purchaseSeatList);
    }

    private String doCreateV2(ProgramOrderCreateDto programOrderCreateDto, List<SeatVo> purchaseSeatList) {
        OrderCreateDto orderCreateDto = buildCreateOrderParam(programOrderCreateDto, purchaseSeatList);
        String orderNumber = createOrderByMq(orderCreateDto, purchaseSeatList);

        DelayOrderCancelDto delayOrderCancelDto = new DelayOrderCancelDto();
        delayOrderCancelDto.setOrderNumber(orderCreateDto.getOrderNumber());
        delayOrderCancelSend.sendMessage(JSON.toJSONString(delayOrderCancelDto));

        return orderNumber;
    }

    private String createOrderByMq(OrderCreateDto orderCreateDto,List<SeatVo> purchaseSeatList){
        CreateOrderMqDomain createOrderMqDomain = new CreateOrderMqDomain();
        CountDownLatch latch = new CountDownLatch(1);
        createOrderSend.sendMessage(com.alibaba.fastjson.JSON.toJSONString(orderCreateDto), sendResult -> {
            createOrderMqDomain.orderNumber = String.valueOf(orderCreateDto.getOrderNumber());
            assert sendResult != null;
            log.info("创建订单kafka发送消息成功 topic : {}",sendResult.getRecordMetadata().topic());
            latch.countDown();
        },ex -> {
            log.error("创建订单kafka发送消息失败 error",ex);
            log.error("创建订单失败 需人工处理 orderCreateDto : {}", com.alibaba.fastjson.JSON.toJSONString(orderCreateDto));
            updateProgramCacheDataResolution(orderCreateDto.getProgramId(),purchaseSeatList,OrderStatus.CANCEL);
            createOrderMqDomain.daMaiFrameException = new DaMaiFrameException(ex);
            latch.countDown();
        });
        try {
            latch.await();
        } catch (InterruptedException e) {
            log.error("createOrderByMq InterruptedException",e);
            throw new DaMaiFrameException(e);
        }
        if (Objects.nonNull(createOrderMqDomain.daMaiFrameException)) {
            throw createOrderMqDomain.daMaiFrameException;
        }
        return createOrderMqDomain.orderNumber;
    }

    private List<SeatVo> createOrderOperateProgramCacheResolution(ProgramOrderCreateDto programOrderCreateDto) {
        // 从本地缓存中获取节目演出时间（之前已经放入本地缓存）
        ProgramShowTime programShowTime =
                programShowTimeService.selectProgramShowTimeByProgramIdMultipleCache(programOrderCreateDto.getProgramId());

        // 从本地缓存中获取用户选择的节目票档（之前已经放入本地缓存）
        List<TicketCategoryVo> getTicketCategoryList =
                getTicketCategoryList(programOrderCreateDto, programShowTime.getShowTime());

        for (TicketCategoryVo ticketCategory : getTicketCategoryList) {  // 将座位信息和余票信息加载到redis中
            seatService.selectSeatResolution(programOrderCreateDto.getProgramId(), ticketCategory.getId(),
                    DateUtils.countBetweenSecond(DateUtils.now(), programShowTime.getShowTime()), TimeUnit.SECONDS);
            ticketCategoryService.getRedisRemainNumberResolution(
                    programOrderCreateDto.getProgramId(), ticketCategory.getId());
        }

        Long programId = programOrderCreateDto.getProgramId();
        List<SeatDto> seatDtoList = programOrderCreateDto.getSeatDtoList();
        List<String> keys = new ArrayList<>();
        String[] data = new String[2];
        JSONArray jsonArray = new JSONArray();
        JSONArray addSeatDataJsonArray = new JSONArray();
        if(CollectionUtil.isNotEmpty(seatDtoList)){  // 手动选择座位
            keys.add("1");
            Map<Long, List<SeatDto>> seatTicketCategoryDtoCount = seatDtoList.stream()
                    .collect(Collectors.groupingBy(SeatDto::getTicketCategoryId));
            for (Entry<Long, List<SeatDto>> entry : seatTicketCategoryDtoCount.entrySet()) {
                Long ticketCategoryId = entry.getKey();
                int ticketCount = entry.getValue().size();  // 用户要购买的数量
                JSONObject jsonObject = new JSONObject();
                jsonObject.put("programTicketRemainNumberHashKey", RedisKeyBuild.createRedisKey(RedisKeyManage.PROGRAM_TICKET_REMAIN_NUMBER_HASH_RESOLUTION, programId, ticketCategoryId).getRelKey());
                jsonObject.put("ticketCategoryId", ticketCategoryId);
                jsonObject.put("ticketCount", ticketCount);
                jsonArray.add(jsonObject);

                JSONObject seatDataJsonObject = new JSONObject();
                seatDataJsonObject.put("seatNoSoldHashKey", RedisKeyBuild.createRedisKey(RedisKeyManage.PROGRAM_SEAT_NO_SOLD_RESOLUTION_HASH, programId, ticketCategoryId).getRelKey());
                seatDataJsonObject.put("seatDataList", JSON.toJSONString(seatDtoList));
                addSeatDataJsonArray.add(seatDataJsonObject);
            }
        }
        else{
            keys.add("2");
            Long ticketCategoryId = programOrderCreateDto.getTicketCategoryId();
            Integer ticketCount = programOrderCreateDto.getTicketCount();
            JSONObject jsonObject = new JSONObject();
            jsonObject.put("programTicketRemainNumberHashKey", RedisKeyBuild.createRedisKey(RedisKeyManage.PROGRAM_TICKET_REMAIN_NUMBER_HASH_RESOLUTION, programId, ticketCategoryId).getRelKey());
            jsonObject.put("ticketCategoryId", ticketCategoryId);
            jsonObject.put("ticketCount", ticketCount);
            jsonObject.put("seatNoSoldHashKey", RedisKeyBuild.createRedisKey(RedisKeyManage.PROGRAM_SEAT_NO_SOLD_RESOLUTION_HASH, programId, ticketCategoryId).getRelKey());
            jsonArray.add(jsonObject);
        }
        keys.add(RedisKeyBuild.getRedisKey(RedisKeyManage.PROGRAM_SEAT_NO_SOLD_RESOLUTION_HASH));
        keys.add(RedisKeyBuild.getRedisKey(RedisKeyManage.PROGRAM_SEAT_LOCK_RESOLUTION_HASH));
        keys.add(String.valueOf(programOrderCreateDto.getProgramId()));
        data[0] = JSON.toJSONString(jsonArray);
        data[1] = JSON.toJSONString(addSeatDataJsonArray);
        ProgramCacheCreateOrderData programCacheCreateOrderData =
                programCacheCreateOrderResolutionOperate.programCacheOperate(keys, data);
        if(!Objects.equals(programCacheCreateOrderData.getCode(), BaseCode.SUCCESS.getCode())) {
            throw new DaMaiFrameException(Objects.requireNonNull(BaseCode.getRc(programCacheCreateOrderData.getCode())));
        }
        return programCacheCreateOrderData.getPurchaseSeatList();
    }

    public String create(ProgramOrderCreateDto programOrderCreateDto) {
        // 从多级缓存中查询节目演出时间
        ProgramShowTime programShowTime = programShowTimeService.selectProgramShowTimeByProgramIdMultipleCache(programOrderCreateDto.getProgramId());

        // 查询对应的票档类型
        List<TicketCategoryVo> getTicketCategoryList = getTicketCategoryList(programOrderCreateDto, programShowTime.getShowTime());

        // 计算传入的座位的总价格
        BigDecimal parameterOrderPrice = new BigDecimal(0);
        //库中座位的总价格
        BigDecimal databaseOrderPrice = new BigDecimal(0);

        // 要购买的座位
        List<SeatVo> purchaseSeatList = new ArrayList<>();
        // 入参的座位
        List<SeatDto> seatDtoList = programOrderCreateDto.getSeatDtoList();
        // 当前节目下所有未售卖的座位
        List<SeatVo> seatVoList = new ArrayList<>();
        // 该节目下的余票数量
        Map<String, Long> ticketCategoryRemainNumber = new HashMap<>(16);

        for (TicketCategoryVo ticketCategory : getTicketCategoryList) {  // 遍历用户选择的所有票档
            // 查询该节目该票档下的所有座位
            List<SeatVo> allSeatVoList = seatService.selectSeatResolution(
                    programOrderCreateDto.getProgramId(),
                    programOrderCreateDto.getTicketCategoryId(),
                    DateUtils.countBetweenSecond(DateUtils.now(), programShowTime.getShowTime()),
                    TimeUnit.SECONDS
            );
            // 将所有未售卖的座位放入seatVoList
            seatVoList.addAll(allSeatVoList.stream().filter(seatVo -> seatVo.getSellStatus().equals(SellStatus.NO_SOLD.getCode())).toList());
            // 将查询到的余票数量放入ticketCategoryRemainNumber (票档id, 余票数量)
            ticketCategoryRemainNumber.putAll(ticketCategoryService.getRedisRemainNumberResolution(
                    programOrderCreateDto.getProgramId(),
                    ticketCategory.getId()
            ));
        }

        // 手动选座
        if(CollectionUtil.isNotEmpty(seatDtoList)) {
            // 余票数量检测
            Map<Long, Long> seatTicketCategoryDtoCount = seatDtoList.stream()
                    .collect(Collectors.groupingBy(SeatDto::getTicketCategoryId, Collectors.counting()));
            for(Entry<Long, Long> entry: seatTicketCategoryDtoCount.entrySet()) {
                Long ticketCategoryId = entry.getKey();
                Long purchaseCount = entry.getValue();
                // 余票数量
                Long remainNumber = Optional.ofNullable(ticketCategoryRemainNumber.get(String.valueOf(ticketCategoryId)))
                        .orElseThrow(() -> new DaMaiFrameException(BaseCode.TICKET_CATEGORY_NOT_EXIST_V2));
                if(purchaseCount > remainNumber) {
                    throw new DaMaiFrameException(BaseCode.TICKET_REMAIN_NUMBER_NOT_SUFFICIENT);
                }
            }

            for (SeatDto seatDto : seatDtoList) {
                Map<String, SeatVo> seatVoMap = seatVoList.stream()
                        .collect(Collectors.toMap(seat -> seat.getRowCode() + "-" + seat.getColCode(), seat -> seat, (v1, v2) -> v2));
                SeatVo seatVo = seatVoMap.get(seatDto.getRowCode() + "-" + seatDto.getColCode());
                if(Objects.isNull(seatVo)) {
                    throw new DaMaiFrameException(BaseCode.SEAT_IS_NOT_NOT_SOLD);
                }
                purchaseSeatList.add(seatVo);
                parameterOrderPrice = parameterOrderPrice.add(seatDto.getPrice());
                databaseOrderPrice = databaseOrderPrice.add(seatVo.getPrice());
            }
            if(parameterOrderPrice.compareTo(databaseOrderPrice) > 0) {
                throw new DaMaiFrameException(BaseCode.PRICE_ERROR);
            }
        }
        // 自动选座
        else{
            Long ticketCategoryId = programOrderCreateDto.getTicketCategoryId();
            Integer ticketCount = programOrderCreateDto.getTicketCount();
            Long remainNumber = Optional.ofNullable(ticketCategoryRemainNumber.get(String.valueOf(ticketCategoryId)))
                    .orElseThrow(() -> new DaMaiFrameException(BaseCode.TICKET_CATEGORY_NOT_EXIST_V2));
            if(ticketCount > remainNumber) {
                throw new DaMaiFrameException(BaseCode.TICKET_REMAIN_NUMBER_NOT_SUFFICIENT);
            }
            purchaseSeatList = SeatMatch.findAdjacentSeatVos(seatVoList.stream()
                    .filter(seatVo -> Objects.equals(seatVo.getTicketCategoryId(), ticketCategoryId)).collect(Collectors.toList()), ticketCount);
            if(purchaseSeatList.size() < ticketCount) {
                throw new DaMaiFrameException(BaseCode.SEAT_OCCUPY);
            }
        }
        updateProgramCacheDataResolution(programOrderCreateDto.getProgramId(), purchaseSeatList, OrderStatus.NO_PAY);
        return doCreate(programOrderCreateDto, purchaseSeatList);
    }

    private String doCreate(ProgramOrderCreateDto programOrderCreateDto, List<SeatVo> purchaseSeatList) {
        OrderCreateDto orderCreateDto = buildCreateOrderParam(programOrderCreateDto, purchaseSeatList);

        String orderNumber = createOrderByRpc(orderCreateDto, purchaseSeatList);

        DelayOrderCancelDto delayOrderCancelDto = new DelayOrderCancelDto();
        delayOrderCancelDto.setOrderNumber(orderCreateDto.getOrderNumber());
        delayOrderCancelSend.sendMessage(JSON.toJSONString(delayOrderCancelDto));
        return orderNumber;
    }

    private String createOrderByRpc(OrderCreateDto orderCreateDto, List<SeatVo> purchaseSeatList) {
        ApiResponse<String> createOrderResponse = orderClient.create(orderCreateDto);
        if(!Objects.equals(createOrderResponse.getCode(), BaseCode.SUCCESS.getCode())) {
            log.error("创建订单失败 需人工处理 orderCreateDto : {}", com.alibaba.fastjson.JSON.toJSONString(orderCreateDto));
            updateProgramCacheDataResolution(orderCreateDto.getProgramId(),purchaseSeatList,OrderStatus.CANCEL);
            throw new DaMaiFrameException(createOrderResponse);
        }
        return createOrderResponse.getData();
    }

    private OrderCreateDto buildCreateOrderParam(ProgramOrderCreateDto programOrderCreateDto, List<SeatVo> purchaseSeatList) {
        ProgramVo programVo = programService.simpleGetProgramAndShowMultipleCache(programOrderCreateDto.getProgramId());
        // 生成主订单
        OrderCreateDto orderCreateDto = new OrderCreateDto();
        orderCreateDto.setOrderNumber(uidGenerator.getOrderNumber(programOrderCreateDto.getUserId(), ORDER_TABLE_COUNT));  // 基因法生成订单id
        orderCreateDto.setProgramId(programOrderCreateDto.getProgramId());
        orderCreateDto.setProgramItemPicture(programVo.getItemPicture());
        orderCreateDto.setUserId(programOrderCreateDto.getUserId());
        orderCreateDto.setProgramTitle(programVo.getTitle());
        orderCreateDto.setProgramPlace(programVo.getPlace());
        orderCreateDto.setProgramShowTime(programVo.getShowTime());
        orderCreateDto.setProgramPermitChooseSeat(programVo.getPermitChooseSeat());
        BigDecimal databaseOrderPrice =
                purchaseSeatList.stream().map(SeatVo::getPrice).reduce(BigDecimal.ZERO, BigDecimal::add);
        orderCreateDto.setOrderPrice(databaseOrderPrice);
        orderCreateDto.setCreateOrderTime(DateUtils.now());

        // 生成每个购票人订单
        List<Long> ticketUserIdList = programOrderCreateDto.getTicketUserIdList();
        List<OrderTicketUserCreateDto> orderTicketUserCreateDtoList = new ArrayList<>();
        for (int i = 0; i < ticketUserIdList.size(); i++) {
            Long ticketUserId = ticketUserIdList.get(i);
            OrderTicketUserCreateDto orderTicketUserCreateDto = new OrderTicketUserCreateDto();
            orderTicketUserCreateDto.setOrderNumber(orderCreateDto.getOrderNumber());
            orderTicketUserCreateDto.setProgramId(programOrderCreateDto.getProgramId());
            orderTicketUserCreateDto.setUserId(programOrderCreateDto.getUserId());
            orderTicketUserCreateDto.setTicketUserId(ticketUserId);
            SeatVo seatVo =
                    Optional.ofNullable(purchaseSeatList.get(i))
                            .orElseThrow(() -> new DaMaiFrameException(BaseCode.SEAT_NOT_EXIST));
            orderTicketUserCreateDto.setSeatId(seatVo.getId());
            orderTicketUserCreateDto.setSeatInfo(seatVo.getRowCode() + "排" + seatVo.getColCode() + "列");
            orderTicketUserCreateDto.setTicketCategoryId(seatVo.getTicketCategoryId());
            orderTicketUserCreateDto.setOrderPrice(seatVo.getPrice());
            orderTicketUserCreateDto.setCreateOrderTime(DateUtils.now());
            orderTicketUserCreateDtoList.add(orderTicketUserCreateDto);
        }

        orderCreateDto.setOrderTicketUserCreateDtoList(orderTicketUserCreateDtoList);

        return orderCreateDto;
    }

    private void updateProgramCacheDataResolution(Long programId, List<SeatVo> seatVoList, OrderStatus orderStatus) {
        // 入参seatVoList是要进行锁定的座位
        if(!(Objects.equals(orderStatus.getCode(), OrderStatus.NO_PAY.getCode())
        || Objects.equals(orderStatus.getCode(), OrderStatus.CANCEL.getCode()))) {
            throw new DaMaiFrameException(BaseCode.OPERATE_ORDER_STATUS_NOT_PERMIT);
        }
        List<String> keys = new ArrayList<>();
        keys.add("#");  // 没用，占位的

        // 扣减库存的准备工作
        Map<Long, Long> ticketCategoryCountMap =
                seatVoList.stream().collect(Collectors.groupingBy(SeatVo::getTicketCategoryId, Collectors.counting()));
        JSONArray jsonArray = new JSONArray();  // 用于扣减库存需要的参数
        ticketCategoryCountMap.forEach((k, v) -> {
            JSONObject jsonObject = new JSONObject();
            jsonObject.put("programTicketRemainNumberHashKey", RedisKeyBuild.createRedisKey(
                    RedisKeyManage.PROGRAM_TICKET_REMAIN_NUMBER_HASH_RESOLUTION, programId, k).getRelKey());
            jsonObject.put("ticketCategoryId", String.valueOf(k));
            if(Objects.equals(orderStatus.getCode(), OrderStatus.NO_PAY.getCode())) {
                jsonObject.put("count", "-" + v);
            } else if(Objects.equals(orderStatus.getCode(), OrderStatus.CANCEL.getCode())) {
                jsonObject.put("count", v);
            }
            jsonArray.add(jsonObject);
        });

        // 从nosold中移除要下单的座位，并将这些座位添加到锁定中
        Map<Long, List<SeatVo>> seatVoMap =
                seatVoList.stream().collect(Collectors.groupingBy(SeatVo::getTicketCategoryId));
        JSONArray delSeatIdjsonArray = new JSONArray();  // 用于移除要下单的座位需要的参数
        JSONArray addSeatDatajsonArray = new JSONArray();  // 用于将这些座位添加到锁定中需要的参数
        seatVoMap.forEach((k, v) -> {
            JSONObject delSeatIdjsonObject = new JSONObject();
            JSONObject seatDatajsonObject = new JSONObject();
            String seatHashKeyDel = "";
            String seatHashKeyAdd = "";
            if(Objects.equals(orderStatus.getCode(), OrderStatus.NO_PAY.getCode())) {
                seatHashKeyDel = RedisKeyBuild.createRedisKey(RedisKeyManage.PROGRAM_SEAT_NO_SOLD_RESOLUTION_HASH, programId, k).getRelKey();
                seatHashKeyAdd = RedisKeyBuild.createRedisKey(RedisKeyManage.PROGRAM_SEAT_LOCK_RESOLUTION_HASH, programId, k).getRelKey();
                // 修改sellStatus
                for (SeatVo seatVo : v) {
                    seatVo.setSellStatus(SellStatus.LOCK.getCode());
                }
            } else if(Objects.equals(orderStatus.getCode(), OrderStatus.CANCEL.getCode())) {
                // 与未支付状态是相反的
                seatHashKeyDel = RedisKeyBuild.createRedisKey(RedisKeyManage.PROGRAM_SEAT_LOCK_RESOLUTION_HASH, programId, k).getRelKey();
                seatHashKeyAdd = RedisKeyBuild.createRedisKey(RedisKeyManage.PROGRAM_SEAT_NO_SOLD_RESOLUTION_HASH, programId, k).getRelKey();
                // 修改sellStatus
                for (SeatVo seatVo : v) {
                    seatVo.setSellStatus(SellStatus.NO_SOLD.getCode());
                }
            }

            delSeatIdjsonObject.put("seatHashKeyDel", seatHashKeyDel);
            delSeatIdjsonObject.put("seatIdList", v.stream().map(SeatVo::getId).map(String::valueOf).collect(Collectors.toList()));
            delSeatIdjsonArray.add(delSeatIdjsonObject);

            seatDatajsonObject.put("seatHashKeyAdd", seatHashKeyAdd);
            List<String> seatDataList = new ArrayList<>();  // 更新后的座位信息
            for (SeatVo seatVo : v) {
                seatDataList.add(String.valueOf(seatVo.getId()));
                seatDataList.add(JSON.toJSONString(seatVo));
            }
            seatDatajsonObject.put("seatDataList", seatDataList);
            addSeatDatajsonArray.add(seatDatajsonObject);
        });

        String[] data = new String[3];
        data[0] = JSON.toJSONString(jsonArray);
        data[1] = JSON.toJSONString(delSeatIdjsonArray);
        data[2] = JSON.toJSONString(addSeatDatajsonArray);
        programCacheResolutionOperate.programCacheOperate(keys, data);
    }

    // getTicketCategoryList 的作用是验证传入的手动座位中的票档id或者自动选座的票档id是否正确，如果正确的话则将这些传入的票档数据返回
    private List<TicketCategoryVo> getTicketCategoryList(ProgramOrderCreateDto programOrderCreateDto, Date showTime) {
        List<TicketCategoryVo> getTicketCategoryVoList = new ArrayList<>();
        // 查询该节目下的所有票档
        List<TicketCategoryVo> ticketCategoryVoList = ticketCategoryService.selectTicketCategoryListByProgramIdMultipleCache(programOrderCreateDto.getProgramId(), showTime);
        Map<Long, TicketCategoryVo> ticketCategoryVoMap = ticketCategoryVoList.stream()
                .collect(Collectors.toMap(TicketCategoryVo::getId, ticketCategoryVo -> ticketCategoryVo));

        // 获取前端传来的用户选择的票档id
        List<SeatDto> seatDtoList = programOrderCreateDto.getSeatDtoList();

        // 手动选择座位
        if(CollectionUtil.isNotEmpty(seatDtoList)) {
            for (SeatDto seatDto : seatDtoList) {
                    TicketCategoryVo ticketCategoryVo = ticketCategoryVoMap.get(seatDto.getId());
                if(Objects.nonNull(ticketCategoryVo)){
                    getTicketCategoryVoList.add(ticketCategoryVo);
                } else{
                    throw new DaMaiFrameException(BaseCode.TICKET_CATEGORY_NOT_EXIST_V2);
                }
            }
        }
        // 自动分配座位
        else{
            TicketCategoryVo ticketCategoryVo = ticketCategoryVoMap.get(programOrderCreateDto.getTicketCategoryId());
            if(Objects.nonNull(ticketCategoryVo)){
                getTicketCategoryVoList.add(ticketCategoryVo);
            } else{
                throw new DaMaiFrameException(BaseCode.TICKET_CATEGORY_NOT_EXIST_V2);
            }
        }

        return getTicketCategoryVoList;
    }
}
