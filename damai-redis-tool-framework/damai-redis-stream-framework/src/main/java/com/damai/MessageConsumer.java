package com.damai;

import org.springframework.data.redis.connection.stream.ObjectRecord;

@FunctionalInterface
public interface MessageConsumer {

    /**
     * 消息处理
     * @param message 消息
     *
     * */
    void accept(ObjectRecord<String, String> message);

}
