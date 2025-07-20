package com.damai.service.delayconsumer;

import com.alibaba.fastjson2.JSON;
import com.damai.context.DelayQueueContext;
import com.damai.core.ConsumerTask;
import com.damai.dto.TestSendDto;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class TestConsumer implements ConsumerTask {

    @Autowired
    private DelayQueueContext delayQueueContext;


    @Override
    public void execute(String content) {
        TestSendDto testSendDto = JSON.parseObject(content, TestSendDto.class);
        log.info("收到消息 : {} 延时: {} 毫秒" ,content, System.currentTimeMillis() - testSendDto.getTime() - 5000);
    }

    @Override
    public String topic() {
        return "test-topic";
    }
}