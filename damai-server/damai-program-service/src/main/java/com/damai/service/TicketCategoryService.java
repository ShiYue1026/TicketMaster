package com.damai.service;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.collection.CollectionUtil;
import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.core.toolkit.Wrappers;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.damai.core.RedisKeyManage;
import com.damai.entity.TicketCategory;
import com.damai.enums.BaseCode;
import com.damai.exception.DaMaiFrameException;
import com.damai.mapper.TicketCategoryMapper;
import com.damai.redis.RedisCache;
import com.damai.redis.RedisKeyBuild;
import com.damai.service.cache.local.LocalCacheTicketCategory;
import com.damai.servicelock.LockType;
import com.damai.servicelock.annotation.ServiceLock;
import com.damai.util.DateUtils;
import com.damai.util.ServiceLockTool;
import com.damai.vo.TicketCategoryVo;
import org.redisson.api.RLock;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.damai.core.DistributedLockConstants.*;

@Service
public class TicketCategoryService extends ServiceImpl<TicketCategoryMapper, TicketCategory> {

    @Autowired
    private RedisCache redisCache;

    @Autowired
    private ServiceLockTool serviceLockTool;

    @Autowired
    private TicketCategoryMapper ticketCategoryMapper;

    @Autowired
    private LocalCacheTicketCategory localCacheTicketCategory;

    @ServiceLock(lockType = LockType.Read, name = TICKET_CATEGORY_LOCK, keys = {"#programId"})
    public List<TicketCategoryVo> selectTicketCategoryListByProgramId(Long programId, Long expireTime, TimeUnit timeUnit) {
        List<TicketCategoryVo> ticketCategoryVoList =
                redisCache.getValueIsList(RedisKeyBuild.createRedisKey(RedisKeyManage.PROGRAM_TICKET_CATEGORY_LIST, programId), TicketCategoryVo.class);
        if(CollectionUtil.isNotEmpty(ticketCategoryVoList)) {
            return ticketCategoryVoList;
        }

        RLock lock = serviceLockTool.getLock(LockType.Reentrant, GET_TICKET_CATEGORY_LOCK, new String[]{String.valueOf(programId)});
        lock.lock();
        try{
            ticketCategoryVoList =
                    redisCache.getValueIsList(RedisKeyBuild.createRedisKey(RedisKeyManage.PROGRAM_TICKET_CATEGORY_LIST, programId), TicketCategoryVo.class);
            if(CollectionUtil.isNotEmpty(ticketCategoryVoList)) {
                return ticketCategoryVoList;
            }
            // redis中没有，从数据库里查
            LambdaQueryWrapper<TicketCategory> ticketCategoryLambdaQueryWrapper =
                    Wrappers.lambdaQuery(TicketCategory.class).eq(TicketCategory::getProgramId, programId);
            List<TicketCategory> ticketCategoryList = Optional.ofNullable(ticketCategoryMapper.selectList(ticketCategoryLambdaQueryWrapper))
                    .orElseThrow(() -> new DaMaiFrameException(BaseCode.TICKET_CATEGORY_NOT_EXIST));
            ticketCategoryVoList = ticketCategoryList.stream().map(ticketCategory -> {
                ticketCategory.setRemainNumber(null);
                TicketCategoryVo ticketCategoryVo = new TicketCategoryVo();
                BeanUtil.copyProperties(ticketCategory, ticketCategoryVo);
                return ticketCategoryVo;
            }).collect(Collectors.toList());

            // 存到redis中
            redisCache.set(RedisKeyBuild.createRedisKey(RedisKeyManage.PROGRAM_TICKET_CATEGORY_LIST, programId),
                    ticketCategoryVoList, expireTime, timeUnit);

            return ticketCategoryVoList;
        } finally {
            lock.unlock();
        }
    }

    public List<TicketCategoryVo> selectTicketCategoryListByProgramIdMultipleCache(Long programId, Date showTime) {
        return localCacheTicketCategory.getCache(programId,
                key -> selectTicketCategoryListByProgramId(programId, DateUtils.countBetweenSecond(DateUtils.now(), showTime), TimeUnit.SECONDS));
    }

    @ServiceLock(lockType = LockType.Read, name = REMAIN_NUMBER_LOCK, keys = {"#programId", "#ticketCategoryId"})
    public Map<String, Long> getRedisRemainNumberResolution(Long programId, Long ticketCategoryId) {
        Map<String, Long> ticketCategoryRemainNumber =
                redisCache.getAllMapForHash(RedisKeyBuild.createRedisKey(RedisKeyManage.PROGRAM_TICKET_REMAIN_NUMBER_HASH_RESOLUTION, programId, ticketCategoryId), Long.class);

        if(CollectionUtil.isNotEmpty(ticketCategoryRemainNumber)) {
            return ticketCategoryRemainNumber;
        }
        RLock lock = serviceLockTool.getLock(LockType.Reentrant, GET_REMAIN_NUMBER_LOCK, new String[]{String.valueOf(programId), String.valueOf(ticketCategoryId)});
        lock.lock();
        try{
            ticketCategoryRemainNumber = redisCache.getAllMapForHash(
                    RedisKeyBuild.createRedisKey(RedisKeyManage.PROGRAM_TICKET_REMAIN_NUMBER_HASH_RESOLUTION, programId, ticketCategoryId),
                    Long.class);
            if(CollectionUtil.isNotEmpty(ticketCategoryRemainNumber)) {
                return ticketCategoryRemainNumber;
            }
            LambdaQueryWrapper<TicketCategory> ticketCategoryLambdaQueryWrapper = Wrappers.lambdaQuery(TicketCategory.class)
                    .eq(TicketCategory::getProgramId, programId)
                    .eq(TicketCategory::getId, ticketCategoryId);
            List<TicketCategory> ticketCategoryList = ticketCategoryMapper.selectList(ticketCategoryLambdaQueryWrapper);
            Map<String, Long> map = ticketCategoryList.stream().collect(Collectors.toMap(t -> String.valueOf(t.getId()), TicketCategory::getRemainNumber));
            redisCache.putHash(RedisKeyBuild.createRedisKey(RedisKeyManage.PROGRAM_TICKET_REMAIN_NUMBER_HASH_RESOLUTION, programId, ticketCategoryId), map);
            return map;
        } finally{
            lock.unlock();
        }
    }
}
