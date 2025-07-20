package com.damai.service.delaysend;

import com.damai.context.DelayQueueContext;
import com.damai.core.SpringUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import static com.damai.service.constant.ProgramOrderConstant.*;

@Slf4j
@Component
public class DelayOrderCancelSend {

    @Autowired
    private DelayQueueContext delayQueueContext;

    public void sendMessage(String message){
        try {
            delayQueueContext.sendMessage(SpringUtil.getPrefixDistinctionName() + "-" + DELAY_ORDER_CANCEL_TOPIC,
                    message, DELAY_ORDER_CANCEL_TIME, DELAY_ORDER_CANCEL_TIME_UNIT);
        }catch (Exception e) {
            log.error("send message error message : {}", message, e);
        }

    }
}
