package com.damai.service;

import com.alibaba.fastjson2.JSON;
import com.baidu.fsg.uid.utils.PaddedAtomicLong;
import com.damai.context.DelayQueueContext;
import com.damai.dto.TestSendDto;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
@Service
public class TestService {

    @Autowired
    private DelayQueueContext delayQueueContext;

    AtomicLong count = new PaddedAtomicLong(0);

    public boolean testSend(TestSendDto testSendDto) {
        try {
            testSendDto.setTime(System.currentTimeMillis());
            testSendDto.setCount(count.incrementAndGet());
            String message = JSON.toJSONString(testSendDto);
            delayQueueContext.sendMessage("test-topic",
                    message, 5000, TimeUnit.MILLISECONDS);
            log.info("发送消息 : {}",message);
        }catch (Exception e) {
            log.error("test send message error message : {}",JSON.toJSONString(testSendDto),e);
            return false;
        }
        return true;
    }

    public Boolean reset(final TestSendDto testSendDto) {
        count.set(0);
        return true;
    }
}