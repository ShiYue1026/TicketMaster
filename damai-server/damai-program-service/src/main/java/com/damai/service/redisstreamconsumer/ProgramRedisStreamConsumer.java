package com.damai.service.redisstreamconsumer;

import com.damai.MessageConsumer;
import com.damai.service.ProgramService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.connection.stream.ObjectRecord;
import org.springframework.stereotype.Component;

@Component
public class ProgramRedisStreamConsumer implements MessageConsumer {

    @Autowired
    private ProgramService programService;

    @Override
    public void accept(ObjectRecord<String, String> message) {
        Long programId = Long.parseLong(message.getValue());
        programService.delLocalCache(programId);
    }

}
