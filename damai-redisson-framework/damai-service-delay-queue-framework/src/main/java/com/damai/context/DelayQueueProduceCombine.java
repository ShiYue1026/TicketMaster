package com.damai.context;

import com.damai.core.DelayProduceQueue;
import com.damai.core.IsolationRegionSelector;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

// 消息发送处理器
@Slf4j
public class DelayQueueProduceCombine {

    private final IsolationRegionSelector isolationRegionSelector;

    private final List<DelayProduceQueue> delayProduceQueueList = new ArrayList<>();

    public DelayQueueProduceCombine(DelayQueueBasePart delayQueueBasePart, String topic) {
        Integer isolationRegionCount = delayQueueBasePart.getDelayQueueProperties().getIsolationRegionCount();
        isolationRegionSelector = new IsolationRegionSelector(isolationRegionCount);
        // 进行分片
        for(int i=0; i<isolationRegionCount; i++) {
            delayProduceQueueList.add(new DelayProduceQueue(delayQueueBasePart.getRedissonClient(), topic + "-" + i));
        }
    }

    public void offer(String content, long delayTime, TimeUnit timeUnit) {
        // 选择一个分区发送消息
        int index = isolationRegionSelector.getIndex();
//        log.info("延迟订单取消消息进行发送 当前分区: {}, 消息体: {}", index, content);
        delayProduceQueueList.get(index).offer(content, delayTime, timeUnit);
    }

}
