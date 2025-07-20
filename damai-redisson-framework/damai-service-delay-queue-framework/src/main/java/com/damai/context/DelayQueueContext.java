package com.damai.context;

import com.damai.core.DelayProduceQueue;

import java.util.Comparator;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

public class DelayQueueContext {

    private final DelayQueueBasePart delayQueueBasePart;

    private final Map<String, DelayQueueProduceCombine> delayQueueProduceCombineMap = new ConcurrentHashMap<>();  // 每个topic对应一个delayQueueProduceCombine, 在delayQueueProduceCombine中进行分区，每个分区对应各自的delayProduceQueue

    public DelayQueueContext(DelayQueueBasePart delayQueueBasePart){
        this.delayQueueBasePart = delayQueueBasePart;
    }

    public void sendMessage(String topic, String content, long delayTime, TimeUnit timeUnit){
        // 获取当前topic的DelayQueueProduceCombine发送消息
        DelayQueueProduceCombine delayQueueProduceCombine =
                delayQueueProduceCombineMap.computeIfAbsent(topic, k -> new DelayQueueProduceCombine(delayQueueBasePart, topic));
        delayQueueProduceCombine.offer(content, delayTime, timeUnit);
    }
}
