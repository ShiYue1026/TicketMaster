package com.damai.event;

import cn.hutool.core.collection.CollectionUtil;
import com.damai.context.DelayQueueBasePart;
import com.damai.context.DelayQueuePart;
import com.damai.core.ConsumerTask;
import com.damai.core.DelayConsumerQueue;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.event.ApplicationStartedEvent;
import org.springframework.context.ApplicationListener;

import java.util.Map;

@Slf4j
@AllArgsConstructor
public class DelayQueueInitHandler implements ApplicationListener<ApplicationStartedEvent> {

    private final DelayQueueBasePart delayQueueBasePart;

    @Override
    public void onApplicationEvent(ApplicationStartedEvent event) {

        Map<String, ConsumerTask> consumerTaskMap = event.getApplicationContext().getBeansOfType(ConsumerTask.class);
        if (CollectionUtil.isEmpty(consumerTaskMap)) {
            return;
        }
        for (ConsumerTask consumerTask : consumerTaskMap.values()) {
            log.info("初始化延迟消费队列: {}", consumerTask.topic());
            DelayQueuePart delayQueuePart = new DelayQueuePart(delayQueueBasePart,consumerTask);
            Integer isolationRegionCount = delayQueuePart.getDelayQueueBasePart().getDelayQueueProperties()
                    .getIsolationRegionCount();

            for(int i = 0; i < isolationRegionCount; i++) {
                DelayConsumerQueue delayConsumerQueue = new DelayConsumerQueue(delayQueuePart,
                        delayQueuePart.getConsumerTask().topic() + "-" + i);
                delayConsumerQueue.listenStart();
            }
        }
    }
}
