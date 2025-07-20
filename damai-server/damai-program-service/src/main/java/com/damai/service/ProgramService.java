package com.damai.service;


import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.collection.CollectionUtil;
import com.alibaba.fastjson2.JSON;
import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.core.conditions.update.LambdaUpdateWrapper;
import com.baomidou.mybatisplus.core.metadata.IPage;
import com.baomidou.mybatisplus.core.toolkit.Wrappers;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.damai.BusinessThreadPool;
import com.damai.RedisStreamPushHandler;
import com.damai.client.BaseDataClient;
import com.damai.client.OrderClient;
import com.damai.client.UserClient;
import com.damai.common.ApiResponse;
import com.damai.core.RedisKeyManage;
import com.damai.dto.*;
import com.damai.entity.*;
import com.damai.enums.BaseCode;
import com.damai.enums.BusinessStatus;
import com.damai.enums.CompositeCheckType;
import com.damai.enums.SellStatus;
import com.damai.exception.DaMaiFrameException;
import com.damai.initialize.impl.composite.CompositeContainer;
import com.damai.lua.ProgramDelCacheData;
import com.damai.mapper.*;
import com.damai.page.PageUtil;
import com.damai.page.PageVo;
import com.damai.redis.RedisCache;
import com.damai.redis.RedisKeyBuild;
import com.damai.repeatexecutelimit.annotation.RepeatExecuteLimit;
import com.damai.service.cache.local.*;
import com.damai.service.es.ProgramEs;
import com.damai.service.tool.TokenExpireManager;
import com.damai.servicelock.LockType;
import com.damai.servicelock.annotation.ServiceLock;
import com.damai.threadlocal.BaseParameterHolder;
import com.damai.util.DateUtils;
import com.damai.util.ServiceLockTool;
import com.damai.util.StringUtil;
import com.damai.vo.*;
import com.damai.service.constant.ProgramTimeType;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.damai.constant.Constant.CODE;
import static com.damai.constant.Constant.USER_ID;
import static com.damai.core.DistributedLockConstants.*;
import static com.damai.core.RepeatExecuteLimitConstants.CANCEL_PROGRAM_ORDER;
import static com.damai.util.DateUtils.FORMAT_DATE;

@Slf4j
@Service
public class ProgramService extends ServiceImpl<ProgramMapper, Program> {

    @Autowired
    private ProgramMapper programMapper;

    @Autowired
    private ProgramCategoryMapper programCategoryMapper;

    @Autowired
    private ProgramShowTimeMapper programShowTimeMapper;

    @Autowired
    private TicketCategoryMapper ticketCategoryMapper;

    @Autowired
    private BaseDataClient baseDataClient;

    @Autowired
    private ProgramCategoryService programCategoryService;

    @Autowired
    private ProgramEs programEs;

    @Autowired
    private RedisCache redisCache;

    @Autowired
    private ProgramDelCacheData programDelCacheData;

    @Autowired
    private CompositeContainer compositeContainer;

    @Autowired
    private ProgramShowTimeService programShowTimeService;

    @Lazy
    @Autowired
    private ProgramService programService;  // Spring 的 AOP 是基于代理对象实现的。如果直接通过 this 调用类中的方法，不会经过代理对象，也就无法触发 AOP 逻辑。

    @Autowired
    private ServiceLockTool serviceLockTool;

    @Autowired
    private ProgramGroupMapper programGroupMapper;

    @Autowired
    private TicketCategoryService ticketCategoryService;

    @Autowired
    private UserClient userClient;

    @Autowired
    private LocalCacheProgram localCacheProgram;

    @Autowired
    private LocalCacheProgramGroup localCacheProgramGroup;

    @Autowired
    private LocalCacheProgramCategory localCacheProgramCategory;

    @Autowired
    private SeatMapper seatMapper;

    @Autowired
    private OrderClient orderClient;

    @Autowired
    private TokenExpireManager tokenExpireManager;

    @Autowired
    private RedisStreamPushHandler redisStreamPushHandler;

    @Autowired
    private LocalCacheProgramShowTime localCacheProgramShowTime;

    @Autowired
    private LocalCacheTicketCategory localCacheTicketCategory;

    /**
     * 查询主页信息
     * @param programListDto 查询节目数据的入参
     * @return 执行后的结果
     * */
    public List<ProgramHomeVo> selectHomeList(ProgramListDto programListDto) {
        List<ProgramHomeVo> programHomeVoList = programEs.selectHomeList(programListDto);
        if(CollectionUtil.isNotEmpty(programHomeVoList)){
            log.info("从Es中查到了主页列表的节目数据，无需查询数据库");
            return programHomeVoList;
        }
        log.info("需要查询数据库获取主页节目列表");
        return dbSelectHomeList(programListDto);
    }

    /**
     * 查询主页信息（数据库查询）
     * @param programPageListDto 查询节目数据的入参
     * @return 执行后的结果
     * */
     private List<ProgramHomeVo> dbSelectHomeList(ProgramListDto programPageListDto) {
         List<ProgramHomeVo> programHomeVoList = new ArrayList<>();

         // 获取所有父类型id对应的节目类型名称
         Map<Long, String> programCategoryMap = selectProgramCategoryMap(programPageListDto.getParentProgramCategoryIds());

        // 根据area_id和parent_id去表中查找符合条件的节目
         List<Program> programList = programMapper.selectHomeList(programPageListDto);

         if (CollectionUtil.isEmpty(programList)) {
             return programHomeVoList;
         }

         // 符合条件的节目的id
         List<Long> programIdList = programList.stream().map(Program::getId).collect(Collectors.toList());

         // 根据节目id获取演出时间信息
         LambdaQueryWrapper<ProgramShowTime> psLambdaQueryWrapper = Wrappers.lambdaQuery(ProgramShowTime.class)
                 .in(ProgramShowTime::getProgramId, programIdList);
         List<ProgramShowTime> programShowTimeList = programShowTimeMapper.selectList(psLambdaQueryWrapper);

         // 将List转为map<节目id,ProgramShowTime>的格式
         Map<Long, List<ProgramShowTime>> programShowTimeMap = programShowTimeList.stream().collect(Collectors.groupingBy(ProgramShowTime::getId));

         // 根据节目id获取价格信息（每个节目的最高票价和最低票价）
         Map<Long, TicketCategoryAggregate> ticketCategoryMap = selectTicketCategoryMap(programIdList);

         // 将符合条件的节目按父类型id分组
         Map<Long, List<Program>> programMap = programList.stream().collect(Collectors.groupingBy(Program::getParentProgramCategoryId));

         for (Map.Entry<Long, List<Program>> programEntry : programMap.entrySet()) {
             Long key = programEntry.getKey();  // 每个父类型id
             List<Program> value = programEntry.getValue();  // 每个父类型id下的7个节目
             List<ProgramListVo> programListVoList = new ArrayList<>();
             for (Program program : value) {
                 ProgramListVo programListVo = new ProgramListVo();
                 BeanUtils.copyProperties(program, programListVo);

                 programListVo.setShowTime(Optional.ofNullable(programShowTimeMap.get(program.getId()))
                         .filter(list -> !list.isEmpty())
                         .map(list -> list.get(0))
                         .map(ProgramShowTime::getShowTime)
                         .orElse(null));
                 programListVo.setShowDayTime(Optional.ofNullable(programShowTimeMap.get(program.getId()))
                         .filter(list -> !list.isEmpty())
                         .map(list -> list.get(0))
                         .map(ProgramShowTime::getShowDayTime)
                         .orElse(null));
                 programListVo.setShowWeekTime(Optional.ofNullable(programShowTimeMap.get(program.getId()))
                         .filter(list -> !list.isEmpty())
                         .map(list -> list.get(0))
                         .map(ProgramShowTime::getShowWeekTime)
                         .orElse(null));

                 programListVo.setMaxPrice(Optional.ofNullable(ticketCategoryMap.get(program.getId()))
                         .map(TicketCategoryAggregate::getMaxPrice).orElse(null));
                 programListVo.setMinPrice(Optional.ofNullable(ticketCategoryMap.get(program.getId()))
                         .map(TicketCategoryAggregate::getMinPrice).orElse(null));
                 programListVoList.add(programListVo);
             }
             ProgramHomeVo programHomeVo = new ProgramHomeVo();
             programHomeVo.setCategoryName(programCategoryMap.get(key));
             programHomeVo.setCategoryId(key);
             programHomeVo.setProgramListVoList(programListVoList);
             programHomeVoList.add(programHomeVo);
         }
         return programHomeVoList;
     }

    /**
     * 根据传过来的id获取对应每个节目的最高票价和最低票价，返回Map
     * */
    public Map<Long, TicketCategoryAggregate> selectTicketCategoryMap(List<Long> programIdList){
        List<TicketCategoryAggregate> ticketCategoryAggregateList = ticketCategoryMapper.selectAggregateList(programIdList);
        return ticketCategoryAggregateList.stream().collect(Collectors.toMap(TicketCategoryAggregate::getProgramId,
                ticketCategory -> ticketCategory, (v1, v2) -> v2));
    }

    /**
     * 根据传过来的id获取对应的节目类型，返回Map
     * */
     public Map<Long, String> selectProgramCategoryMap(Collection<Long> programCategoryIdList){
         // 构造条件
         LambdaQueryWrapper<ProgramCategory> pcLambdaQueryWrapper = Wrappers.lambdaQuery(ProgramCategory.class)
                 .in(ProgramCategory::getId, programCategoryIdList);
         // 进行查询
         List<ProgramCategory> programCategoryList = programCategoryMapper.selectList(pcLambdaQueryWrapper);

         // 返回结果
         return programCategoryList.stream().collect(Collectors.toMap(ProgramCategory::getId, ProgramCategory::getName, (v1, v2) -> v2));
     }


    /**
     * 获取所有当前有效的节目id
     * */
    public List<Long> getAllProgramIdList() {
        LambdaQueryWrapper<Program> programLambdaQueryWrapper = Wrappers.lambdaQuery(Program.class)
                .select(Program::getId);

        List<Program> programList = programMapper.selectList(programLambdaQueryWrapper);

        return programList.stream().map(Program::getId).collect(Collectors.toList());
    }

    /**
     * 从不同的表中查询节目的详细信息，填充ProgramVo的各个属性
     * */
    public ProgramVo getDetailFromDb(Long programId) {
        ProgramVo programVo = createProgramVo(programId);

        ProgramCategory programCategory = getProgramCategory(programVo.getProgramCategoryId());
        if(Objects.nonNull(programCategory)) {
            programVo.setProgramCategoryName(programCategory.getName());
        }
        ProgramCategory parentProgramCategory = getProgramCategory(programVo.getParentProgramCategoryId());
        if(Objects.nonNull(parentProgramCategory)) {
            programVo.setParentProgramCategoryName(parentProgramCategory.getName());
        }

        LambdaQueryWrapper<ProgramShowTime> programShowTimeLambdaQueryWrapper =
                Wrappers.lambdaQuery(ProgramShowTime.class).eq(ProgramShowTime::getProgramId, programId);
        ProgramShowTime programShowTime = Optional.ofNullable(programShowTimeMapper.selectOne(programShowTimeLambdaQueryWrapper))
                .orElseThrow(() -> new DaMaiFrameException(BaseCode.PROGRAM_SHOW_TIME_NOT_EXIST));

        programVo.setShowTime(programShowTime.getShowTime());
        programVo.setShowDayTime(programShowTime.getShowDayTime());
        programVo.setShowWeekTime(programShowTime.getShowWeekTime());

        return programVo;
    }

    /**
     * 用program表中的属性，填充ProgramVo的部分属性
     * */
    private ProgramVo createProgramVo(Long programId){
        ProgramVo programVo = new ProgramVo();
        Program program = Optional.ofNullable(programMapper.selectById(programId))
                .orElseThrow(() -> new DaMaiFrameException(BaseCode.PROGRAM_NOT_EXIST));

        BeanUtils.copyProperties(program, programVo);
        AreaGetDto areaGetDto = new AreaGetDto();
        areaGetDto.setId(program.getAreaId());
        ApiResponse<AreaVo> areaResponse = baseDataClient.getById(areaGetDto);
        if(Objects.equals(areaResponse.getCode(), ApiResponse.ok().getCode())){
            if(Objects.nonNull(areaResponse.getData())){
                programVo.setAreaName(areaResponse.getData().getName());
            }
        }else{
            log.error("base-data rpc getById error areaResponse:{}", JSON.toJSONString(areaResponse));
        }
        return programVo;
    }

    /**
     * 根据类型id查询节目类型表
     * */
    public ProgramCategory getProgramCategory(Long programCategoryId){
        return programCategoryService.getProgramCategory(programCategoryId);
    }

    /**
     * 查询分类列表
     * @param programPageListDto 查询节目数据的入参
     * @return 执行后的结果
     * */
    public PageVo<ProgramListVo> selectPage(ProgramPageListDto programPageListDto) {
        setQueryTime(programPageListDto);
        PageVo<ProgramListVo> pageVo = programEs.selectPage(programPageListDto);
        if(CollectionUtil.isNotEmpty(pageVo.getList())) {
            log.info("从Es中查到了分类列表的节目数据，无需查询数据库");
            return pageVo;
        }
        return dbSelectPage(programPageListDto);
    }


    /**
     * 查询分类信息（数据库查询）
     * @param programPageListDto 查询节目数据的入参
     * @return 执行后的结果
     * */
    private PageVo<ProgramListVo> dbSelectPage(ProgramPageListDto programPageListDto) {
        IPage<ProgramJoinShowTime> iPage = programMapper.selectPage(PageUtil.getPageParams(programPageListDto), programPageListDto);
        if(CollectionUtil.isEmpty(iPage.getRecords())) {
            return new PageVo<>(iPage.getCurrent(), iPage.getSize(), iPage.getTotal(), new ArrayList<>());
        }

        Set<Long> programCategoryIdSet = iPage.getRecords().stream().map(Program::getProgramCategoryId).collect(Collectors.toSet());
        Map<Long, String> programCategoryMap = selectProgramCategoryMap(programCategoryIdSet);

        List<Long> programIdList = iPage.getRecords().stream().map(Program::getId).collect(Collectors.toList());
        Map<Long, TicketCategoryAggregate> ticketCategorieMap = selectTicketCategoryMap(programIdList);

        Map<Long, String> tempAreaMap = new HashMap<>(64);
        AreaSelectDto areaSelectDto = new AreaSelectDto();
        areaSelectDto.setIdList(iPage.getRecords().stream().map(Program::getAreaId).distinct().collect(Collectors.toList()));
        ApiResponse<List<AreaVo>> areaResponse = baseDataClient.selectByIdList(areaSelectDto);

        if(Objects.equals(areaResponse.getCode(), ApiResponse.ok().getCode())){
            if(CollectionUtil.isNotEmpty(areaResponse.getData())) {
                tempAreaMap = areaResponse.getData()
                        .stream()
                        .collect(Collectors.toMap(AreaVo::getId, AreaVo::getName, (v1, v2) -> v2));
            }
        } else{
            log.error("base-data selectByIdList rpc error areaResponse:{}", com.alibaba.fastjson.JSON.toJSONString(areaResponse));
        }
        Map<Long,String> areaMap = tempAreaMap;

        return PageUtil.convertPage(iPage, programJoinShowTime -> {
           ProgramListVo programListVo = new ProgramListVo();
           BeanUtil.copyProperties(programJoinShowTime, programListVo);

           programListVo.setAreaName(areaMap.get(programJoinShowTime.getAreaId()));
           programListVo.setProgramCategoryName(programCategoryMap.get(programJoinShowTime.getProgramCategoryId()));
            programListVo.setMinPrice(Optional.ofNullable(ticketCategorieMap.get(programJoinShowTime.getId()))
                    .map(TicketCategoryAggregate::getMinPrice).orElse(null));
            programListVo.setMaxPrice(Optional.ofNullable(ticketCategorieMap.get(programJoinShowTime.getId()))
                    .map(TicketCategoryAggregate::getMaxPrice).orElse(null));
           return programListVo;
        });
    }


    /**
     * 根据用户选中的时间类型设置查询的时间范围
     * */
    private void setQueryTime(ProgramPageListDto programPageListDto) {
        switch (programPageListDto.getTimeType()) {
            case ProgramTimeType.TODAY:
                programPageListDto.setStartDateTime(DateUtils.now(FORMAT_DATE));
                programPageListDto.setEndDateTime(DateUtils.now(FORMAT_DATE));
                break;
            case ProgramTimeType.TOMORROW:
                programPageListDto.setStartDateTime(DateUtils.addDay(DateUtils.now(FORMAT_DATE), 1));
                programPageListDto.setEndDateTime(DateUtils.addDay(DateUtils.now(FORMAT_DATE), 1));
                break;
            case ProgramTimeType.WEEK:
                programPageListDto.setStartDateTime(DateUtils.now(FORMAT_DATE));
                programPageListDto.setEndDateTime(DateUtils.addWeek(DateUtils.now(FORMAT_DATE),1));
                break;
            case ProgramTimeType.MONTH:
                programPageListDto.setStartDateTime(DateUtils.now(FORMAT_DATE));
                programPageListDto.setEndDateTime(DateUtils.addMonth(DateUtils.now(FORMAT_DATE),1));
                break;
            case ProgramTimeType.CALENDAR:
                if (Objects.isNull(programPageListDto.getStartDateTime())) {
                    throw new DaMaiFrameException(BaseCode.START_DATE_TIME_NOT_EXIST);
                }
                if (Objects.isNull(programPageListDto.getEndDateTime())) {
                    throw new DaMaiFrameException(BaseCode.END_DATE_TIME_NOT_EXIST);
                }
                break;
            default:
                programPageListDto.setStartDateTime(null);
                programPageListDto.setEndDateTime(null);
        }
    }

    /**
     * 搜索
     * @param programSearchDto 搜索节目数据的入参
     * @return 执行后的结果
     * */
    public PageVo<ProgramListVo> search(ProgramSearchDto programSearchDto) {
        setQueryTime(programSearchDto);
        return programEs.search(programSearchDto);
    }

    /**
     * 查看节目详情
     * @param programGetDto 节目详情数据的入参
     * @return 执行后的结果
     * */
    public ProgramVo detail(ProgramGetDto programGetDto) {
        compositeContainer.execute(CompositeCheckType.PROGRAM_DETAIL_CHECK.getValue(), programGetDto);
        return getDetail(programGetDto);
    }

    public ProgramVo getDetail(ProgramGetDto programGetDto) {
        // 获取节目演出时间信息
        ProgramShowTime programShowTime = programShowTimeService.selectProgramShowTimeByProgramId(programGetDto.getId());

        // 查询节目部分信息和地区信息
        ProgramVo programVo = programService.getById(programGetDto.getId(), DateUtils.countBetweenSecond(DateUtils.now(), programShowTime.getShowTime()), TimeUnit.SECONDS);
        programVo.setShowTime(programShowTime.getShowTime());
        programVo.setShowDayTime(programShowTime.getShowDayTime());
        programVo.setShowWeekTime(programShowTime.getShowWeekTime());

        // 查询节目分组信息
        ProgramGroupVo programGroupVo = programService.getProgramGroup(programVo.getProgramGroupId());
        programVo.setProgramGroupVo(programGroupVo);

        // 预先加载用户购票人(用户已登录 && 当前节目是热门节目的情况下)
        preloadTicketUserList(programVo.getHighHeat());

        // 预先加载用户下的节目订单数量
        preloadAccountOrderCount(programVo.getId());

        // 查询节目类型信息
        ProgramCategory programCategory = getProgramCategory(programVo.getProgramCategoryId());
        if(Objects.nonNull(programCategory)) {
            programVo.setProgramCategoryName(programCategory.getName());
        }
        ProgramCategory parentProgramCategory = getProgramCategory(programVo.getParentProgramCategoryId());
        if(Objects.nonNull(parentProgramCategory)) {
            programVo.setParentProgramCategoryName(parentProgramCategory.getName());
        }

        // 查询节目票档信息
        List<TicketCategoryVo> ticketCategoryVoList = ticketCategoryService.selectTicketCategoryListByProgramId(programGetDto.getId(),
                DateUtils.countBetweenSecond(DateUtils.now(), programShowTime.getShowTime()), TimeUnit.SECONDS);
        programVo.setTicketCategoryVoList(ticketCategoryVoList);

        return programVo;
    }

    /**
     * 查看节目详情V2(使用本地缓存)
     * @param programGetDto 节目详情数据的入参
     * @return 执行后的结果
     * */
    public ProgramVo detailV2(ProgramGetDto programGetDto) {
        compositeContainer.execute(CompositeCheckType.PROGRAM_DETAIL_CHECK.getValue(),programGetDto);
        return getDetailV2(programGetDto);
    }

    public ProgramVo getDetailV2(ProgramGetDto programGetDto) {
        // 获取节目演出时间信息
        ProgramShowTime programShowTime =
                programShowTimeService.selectProgramShowTimeByProgramIdMultipleCache(programGetDto.getId());

        // 查询节目部分信息和地区信息
        ProgramVo programVo = programService.getByIdMultipleCache(programGetDto.getId(),programShowTime.getShowTime());
        programVo.setShowTime(programShowTime.getShowTime());
        programVo.setShowDayTime(programShowTime.getShowDayTime());
        programVo.setShowWeekTime(programShowTime.getShowWeekTime());

        // 查询节目分组信息
        ProgramGroupVo programGroupVo = programService.getProgramGroupMultipleCache(programVo.getProgramGroupId());
        programVo.setProgramGroupVo(programGroupVo);

        // 预先加载用户购票人(用户已登录 && 当前节目是热门节目的情况下)
        preloadTicketUserList(programVo.getHighHeat());

        // 预先加载用户下的节目订单数量
        preloadAccountOrderCount(programVo.getId());

        // 查询节目类型信息
        ProgramCategory programCategory = getProgramCategoryMultipleCache(programVo.getProgramCategoryId());
        if (Objects.nonNull(programCategory)) {
            programVo.setProgramCategoryName(programCategory.getName());
        }
        ProgramCategory parentProgramCategory = getProgramCategoryMultipleCache(programVo.getParentProgramCategoryId());
        if (Objects.nonNull(parentProgramCategory)) {
            programVo.setParentProgramCategoryName(parentProgramCategory.getName());
        }

        // 查询节目票档信息
        List<TicketCategoryVo> ticketCategoryVoList = ticketCategoryService
                .selectTicketCategoryListByProgramIdMultipleCache(programVo.getId(),programShowTime.getShowTime());
        programVo.setTicketCategoryVoList(ticketCategoryVoList);

        return programVo;
    }

    public ProgramCategory getProgramCategoryMultipleCache(Long programCategoryId) {
        return localCacheProgramCategory.get(String.valueOf(programCategoryId),
                key -> getProgramCategory(programCategoryId));
    }

    public ProgramGroupVo getProgramGroupMultipleCache(Long programGroupId) {
        return localCacheProgramGroup.getCache(
                RedisKeyBuild.createRedisKey(RedisKeyManage.PROGRAM_GROUP, programGroupId).getRelKey(),
                key -> getProgramGroup(programGroupId));
    }

    public ProgramVo getByIdMultipleCache(Long programId, Date showTime){
        log.info("从本地缓存查询节目详情");
        return localCacheProgram.getCache(RedisKeyBuild.createRedisKey(RedisKeyManage.PROGRAM, programId).getRelKey(),
                key -> {
                    log.info("查询节目详情 从本地缓存没有查询到 节目id : {}",programId);
                    ProgramVo programVo = getById(programId,DateUtils.countBetweenSecond(DateUtils.now(),showTime),
                            TimeUnit.SECONDS);
                    programVo.setShowTime(showTime);
                    return programVo;
                });
    }


    private void preloadTicketUserList(Integer highHeat) {
        // 判断当前节目是否是热门节目
        if(Objects.equals(highHeat, BusinessStatus.NO.getCode())) {
            log.info("当前节目不是热门节目，不预先加载用户购票人信息");
            return;
        }

        // 判断用户是否登录
        String userId = BaseParameterHolder.getParameter(USER_ID);
        log.info("userId: " + userId);
        String code = BaseParameterHolder.getParameter(CODE);

        if(StringUtil.isEmpty(userId) || StringUtil.isEmpty(code)){
            log.info("当前用户未登录，不预先加载用户购票人信息");
            return;
        }

        Boolean userLogin = redisCache.hasKey(RedisKeyBuild.createRedisKey(RedisKeyManage.USER_LOGIN, code, userId));
        if(!userLogin) {
            log.info("当前用户未登录，不预先加载用户购票人信息");
            return;
        }

        // 异步加载当前用户的购票人信息到redis中
        BusinessThreadPool.execute(() -> {
            try{
                log.info("预先加载用户购票人信息");
                TicketUserListDto ticketUserListDto = new TicketUserListDto();
                ticketUserListDto.setUserId(Long.parseLong(userId));
                ApiResponse<List<TicketUserVo>> apiResponse = userClient.list(ticketUserListDto);
                if(Objects.equals(apiResponse.getCode(), BaseCode.SUCCESS.getCode())) {
                    Optional.ofNullable(apiResponse.getData()).filter(CollectionUtil::isNotEmpty)
                            .ifPresent(ticketUserVoList -> redisCache.set(
                                    RedisKeyBuild.createRedisKey(RedisKeyManage.TICKET_USER_LIST, userId),
                                    ticketUserVoList
                            ));
                } else{
                    log.warn("userClient.select 调用失败 apiResponse : {}", com.alibaba.fastjson.JSON.toJSONString(apiResponse));
                }

            } catch (Exception e){
                log.error("预热加载购票人列表失败",e);
            }
        });
    }

    private void preloadAccountOrderCount(Long programId) {
        String userId = BaseParameterHolder.getParameter(USER_ID);
        String code = BaseParameterHolder.getParameter(CODE);
        if(StringUtil.isEmpty(userId) || StringUtil.isEmpty(code)){  // 未登录
            return;
        }

        Boolean userLogin = redisCache.hasKey(RedisKeyBuild.createRedisKey(RedisKeyManage.USER_LOGIN, code, userId));
        if(!userLogin) {  // 登录过期
            return;
        }

        BusinessThreadPool.execute(() -> {
            try{
                if(!redisCache.hasKey(RedisKeyBuild.createRedisKey(RedisKeyManage.ACCOUNT_ORDER_COUNT, userId, programId))) {
                    AccountOrderCountDto accountOrderCountDto = new AccountOrderCountDto();
                    accountOrderCountDto.setUserId(Long.parseLong(userId));
                    accountOrderCountDto.setProgramId(programId);
                    ApiResponse<AccountOrderCountVo> apiResponse = orderClient.accountOrderCount(accountOrderCountDto);
                    if(Objects.equals(apiResponse.getCode(), BaseCode.SUCCESS.getCode())) {
                        Optional.ofNullable(apiResponse.getData())
                                .ifPresent(accountOrderCountVo -> redisCache.set(
                                        RedisKeyBuild.createRedisKey(RedisKeyManage.ACCOUNT_ORDER_COUNT, userId, programId),
                                        accountOrderCountVo.getCount(),
                                        tokenExpireManager.getTokenExpireTime() + 1,
                                        TimeUnit.MINUTES
                                ));
                    }
                    else {
                        log.warn("orderClient.accountOrderCount 调用失败 apiResponse : {}", com.alibaba.fastjson.JSON.toJSONString(apiResponse));
                    }
                }
            } catch (Exception e){
                log.error("预热加载账户订单数量失败",e);
            }
        });
    }

    @ServiceLock(lockType= LockType.Read,name = PROGRAM_GROUP_LOCK,keys = {"#programGroupId"})
    public ProgramGroupVo getProgramGroup(Long programGroupId) {
        ProgramGroupVo programGroupVo =
                redisCache.get(RedisKeyBuild.createRedisKey(RedisKeyManage.PROGRAM_GROUP, programGroupId), ProgramGroupVo.class);
        if (Objects.nonNull(programGroupVo)) {
            return programGroupVo;
        }
        RLock lock = serviceLockTool.getLock(LockType.Reentrant, GET_PROGRAM_LOCK, new String[]{String.valueOf(programGroupId)});
        lock.lock();
        try {
            programGroupVo = redisCache.get(RedisKeyBuild.createRedisKey(RedisKeyManage.PROGRAM_GROUP, programGroupId),
                    ProgramGroupVo.class);
            if (Objects.isNull(programGroupVo)) {
                programGroupVo = createProgramGroupVo(programGroupId);
                redisCache.set(RedisKeyBuild.createRedisKey(RedisKeyManage.PROGRAM_GROUP, programGroupId),programGroupVo,
                        DateUtils.countBetweenSecond(DateUtils.now(),programGroupVo.getRecentShowTime()),TimeUnit.SECONDS);
            }
            return programGroupVo;
        }finally {
            lock.unlock();
        }
    }

    private ProgramGroupVo createProgramGroupVo(Long programGroupId) {
        ProgramGroupVo programGroupVo = new ProgramGroupVo();
        ProgramGroup programGroup = Optional.ofNullable(programGroupMapper.selectById(programGroupId))
                .orElseThrow(() -> new DaMaiFrameException(BaseCode.PROGRAM_GROUP_NOT_EXIST));
        programGroupVo.setId(programGroup.getId());
        programGroupVo.setProgramSimpleInfoVoList(JSON.parseArray(programGroup.getProgramJson(), ProgramSimpleInfoVo.class));
        programGroupVo.setRecentShowTime(programGroup.getRecentShowTime());
        return programGroupVo;
    }

    @ServiceLock(lockType= LockType.Read, name = PROGRAM_LOCK, keys = {"#programId"})
    public ProgramVo getById(Long programId, Long expireTime, TimeUnit timeUnit) {
        ProgramVo programVo = redisCache.get(RedisKeyBuild.createRedisKey(RedisKeyManage.PROGRAM, programId), ProgramVo.class);
        if(Objects.nonNull(programVo)){
            return programVo;
        }
        log.info("查询节目详情 从Redis缓存中没有查询到 节目id:{}", programId);
        RLock lock = serviceLockTool.getLock(LockType.Reentrant, GET_PROGRAM_LOCK, new String[]{String.valueOf(programId)});
        lock.lock();
        try{
            return redisCache.get(RedisKeyBuild.createRedisKey(RedisKeyManage.PROGRAM, programId),
                    ProgramVo.class,
                    () -> createProgramVo(programId),
                    expireTime,
                    timeUnit
            );
        } finally {
            lock.unlock();
        }
    }

    public ProgramVo simpleGetByIdMultipleCache(Long programId) {
        ProgramVo programVo = localCacheProgram.getCache(
          RedisKeyBuild.createRedisKey(RedisKeyManage.PROGRAM, programId).getRelKey());
        if(Objects.nonNull(programVo)){
            return programVo;
        }
        return redisCache.get(RedisKeyBuild.createRedisKey(RedisKeyManage.PROGRAM, programId), ProgramVo.class);
    }

    public ProgramVo simpleGetProgramAndShowMultipleCache(Long programId) {
        ProgramShowTime programShowTime =
                programShowTimeService.simpleSelectProgramShowTimeByProgramIdMultipleCache(programId);

        if(Objects.isNull(programShowTime)) {
            throw new DaMaiFrameException(BaseCode.PROGRAM_SHOW_TIME_NOT_EXIST);
        }

        ProgramVo programVo = simpleGetByIdMultipleCache(programId);
        if(Objects.isNull(programVo)) {
            throw new DaMaiFrameException(BaseCode.PROGRAM_NOT_EXIST);
        }

        programVo.setShowTime(programShowTime.getShowTime());
        programVo.setShowDayTime(programShowTime.getShowDayTime());
        programVo.setShowWeekTime(programShowTime.getShowWeekTime());

        return programVo;
    }

    public void delRedisData(Long programId) {
        Program program = Optional.ofNullable(programMapper.selectById(programId))
                .orElseThrow(() -> new DaMaiFrameException(BaseCode.PROGRAM_NOT_EXIST));
        List<String> keys = new ArrayList<>();
        keys.add(RedisKeyBuild.createRedisKey(RedisKeyManage.PROGRAM, programId).getRelKey());
        keys.add(RedisKeyBuild.createRedisKey(RedisKeyManage.PROGRAM_GROUP,program.getProgramGroupId()).getRelKey());
        keys.add(RedisKeyBuild.createRedisKey(RedisKeyManage.PROGRAM_SHOW_TIME,programId).getRelKey());
        keys.add(RedisKeyBuild.createRedisKey(RedisKeyManage.PROGRAM_SEAT_NO_SOLD_RESOLUTION_HASH, programId,"*").getRelKey());
        keys.add(RedisKeyBuild.createRedisKey(RedisKeyManage.PROGRAM_SEAT_LOCK_RESOLUTION_HASH, programId,"*").getRelKey());
        keys.add(RedisKeyBuild.createRedisKey(RedisKeyManage.PROGRAM_SEAT_SOLD_RESOLUTION_HASH, programId,"*").getRelKey());
        keys.add(RedisKeyBuild.createRedisKey(RedisKeyManage.PROGRAM_TICKET_CATEGORY_LIST, programId).getRelKey());
        keys.add(RedisKeyBuild.createRedisKey(RedisKeyManage.PROGRAM_TICKET_REMAIN_NUMBER_HASH_RESOLUTION, programId,"*").getRelKey());
        programDelCacheData.del(keys, new String[]{});
    }

    public List<ProgramListVo> recommendList(ProgramRecommendListDto programRecommendListDto) {
        compositeContainer.execute(CompositeCheckType.PROGRAM_RECOMMEND_CHECK.getValue(), programRecommendListDto);
        return programEs.recommendList(programRecommendListDto);
    }

    // 更新数据库中的座位，保持数据与缓存的一致性
    @RepeatExecuteLimit(name = CANCEL_PROGRAM_ORDER, keys = {"#programOperateDataDto.programId","#programOperateDataDto.seatIdList"})
    @Transactional(rollbackFor = Exception.class)
    public void operateProgramData(ProgramOperateDataDto programOperateDataDto) {
        List<TicketCategoryCountDto> ticketCategoryCountDtoList = programOperateDataDto.getTicketCategoryCountDtoList();
        List<Long> seatIdList = programOperateDataDto.getSeatIdList();
        LambdaQueryWrapper<Seat> seatLambdaQueryWrapper = Wrappers.lambdaQuery(Seat.class)
                .eq(Seat::getProgramId, programOperateDataDto.getProgramId())  // 要带上节目id，因为节目id是分片键
                .in(Seat::getId, seatIdList);
        List<Seat> seatList = seatMapper.selectList(seatLambdaQueryWrapper);
        if (CollectionUtil.isEmpty(seatList)){
            throw new DaMaiFrameException(BaseCode.SEAT_NOT_EXIST);
        }
        if(seatList.size() != seatIdList.size()){
            throw new DaMaiFrameException(BaseCode.SEAT_UPDATE_REL_COUNT_NOT_EQUAL_PRESET_COUNT);
        }
        for (Seat seat : seatList) {
            if(Objects.equals(seat.getSellStatus(), SellStatus.SOLD.getCode())) {
                throw new DaMaiFrameException(BaseCode.SEAT_SOLD);
            }
        }

        // 更新座位状态为已售卖
        LambdaUpdateWrapper<Seat> seatLambdaUpdateWrapper = Wrappers.lambdaUpdate(Seat.class)
                .eq(Seat::getProgramId, programOperateDataDto.getProgramId())
                .in(Seat::getId, seatIdList);
        Seat updateSeat = new Seat();
        updateSeat.setSellStatus(SellStatus.SOLD.getCode());
        seatMapper.update(updateSeat, seatLambdaUpdateWrapper);

        // 更新票档下的余票数量
        int updateRemainNumberCount =
                ticketCategoryMapper.batchUpdateRemainNumber(ticketCategoryCountDtoList, programOperateDataDto.getProgramId());
        if(updateRemainNumberCount != ticketCategoryCountDtoList.size()){
            throw new DaMaiFrameException(BaseCode.SEAT_UPDATE_REL_COUNT_NOT_EQUAL_PRESET_COUNT);
        }
    }

    public Boolean invalid(ProgramInvalidDto programInvalidDto) {
        Program updateProgram = new Program();
        updateProgram.setId(programInvalidDto.getId());
        updateProgram.setProgramStatus(BusinessStatus.NO.getCode());
        int result = programMapper.updateById(updateProgram);
        if(result > 0){
            delRedisData(programInvalidDto.getId());
            redisStreamPushHandler.push(String.valueOf(programInvalidDto.getId()));
            programEs.deleteByProgramId(programInvalidDto.getId());
            return true;
        } else{
            return false;
        }
    }

    public ProgramVo localDetail(ProgramGetDto programGetDto) {
        return localCacheProgram.getCache(RedisKeyBuild.createRedisKey(RedisKeyManage.PROGRAM, programGetDto.getId()).getRelKey());
    }

    public void delLocalCache(Long programId) {
        log.info("删除本地缓存 programId: {}", programId);
        localCacheProgram.del(RedisKeyBuild.createRedisKey(RedisKeyManage.PROGRAM, programId).getRelKey());
        localCacheProgramGroup.del(RedisKeyBuild.createRedisKey(RedisKeyManage.PROGRAM_GROUP, programId).getRelKey());
        localCacheProgramShowTime.del(RedisKeyBuild.createRedisKey(RedisKeyManage.PROGRAM_SHOW_TIME, programId).getRelKey());
        localCacheTicketCategory.del(programId);
    }
}
