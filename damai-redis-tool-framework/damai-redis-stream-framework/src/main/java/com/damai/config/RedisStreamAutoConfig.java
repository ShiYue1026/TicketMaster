package com.damai.config;

import com.damai.*;
import com.damai.constant.RedisStreamConstant;
import com.damai.enums.BaseCode;
import com.damai.exception.DaMaiFrameException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.stream.Consumer;
import org.springframework.data.redis.connection.stream.ObjectRecord;
import org.springframework.data.redis.connection.stream.ReadOffset;
import org.springframework.data.redis.connection.stream.StreamOffset;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.stream.StreamMessageListenerContainer;

import java.time.Duration;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
@EnableConfigurationProperties(RedisStreamConfigProperties.class)
public class RedisStreamAutoConfig {

    @Bean
    public RedisStreamPushHandler redisStreamPushHandler(StringRedisTemplate stringRedisTemplate ,
                                                         RedisStreamConfigProperties redisStreamConfigProperties) {
        return new RedisStreamPushHandler(stringRedisTemplate, redisStreamConfigProperties);
    }

    @Bean
    public RedisStreamHandler redisStreamHandler(RedisStreamPushHandler redisStreamPushHandler,
                                                 StringRedisTemplate stringRedisTemplate, RedisStreamConfigProperties redisStreamConfigProperties) {
        return new RedisStreamHandler(redisStreamPushHandler, stringRedisTemplate);
    }

    @Bean
    @ConditionalOnBean(MessageConsumer.class)
    public StreamMessageListenerContainer<String, ObjectRecord<String, String>> streamMessageListenerContainer(
            RedisConnectionFactory redisConnectionFactory,
            RedisStreamConfigProperties redisStreamConfigProperties,
            RedisStreamHandler redisStreamHandler,
            MessageConsumer messageConsumer
    ) {
        StreamMessageListenerContainer.StreamMessageListenerContainerOptions<String, ObjectRecord<String, String>>  options =
                StreamMessageListenerContainer.StreamMessageListenerContainerOptions.builder()
                .pollTimeout(Duration.ofSeconds(5))
                .batchSize(10)
                .targetType(String.class)
                .errorHandler(t -> log.error("出现异常", t))
                .executor(createThreadPool()).build();

        StreamMessageListenerContainer<String, ObjectRecord<String, String>> container =
                StreamMessageListenerContainer.create(redisConnectionFactory, options);

        checkConsumerType(redisStreamConfigProperties.getConsumerType());
        RedisStreamListener redisStreamListener = new RedisStreamListener(messageConsumer);

        if (RedisStreamConstant.GROUP.equals(redisStreamConfigProperties.getConsumerType())) {
            redisStreamHandler.streamBindingGroup(redisStreamConfigProperties.getStreamName(),
                    redisStreamConfigProperties.getConsumerGroup());
            container.receiveAutoAck(Consumer.from(redisStreamConfigProperties.getConsumerGroup(),
                            redisStreamConfigProperties.getConsumerName()),
                    StreamOffset.create(redisStreamConfigProperties.getStreamName(), ReadOffset.lastConsumed()),
                    redisStreamListener);
        } else {
            container.receive(StreamOffset.fromStart(redisStreamConfigProperties.getStreamName()), redisStreamListener);
        }
        container.start();
        return container;
    }

    public ThreadPoolExecutor createThreadPool(){
        int coreThreadCount = Runtime.getRuntime().availableProcessors();
        AtomicInteger threadCount = new AtomicInteger(1);
        return new ThreadPoolExecutor(
                coreThreadCount,
                2 * coreThreadCount,
                30,
                TimeUnit.SECONDS,
                new ArrayBlockingQueue<>(100),
                r -> {
                    Thread thread = new Thread(r);
                    thread.setName("thread-consumer-stream-task-" + threadCount.getAndIncrement());
                    return thread;
                });
    }

    public void checkConsumerType(String consumerType){
        if ((!RedisStreamConstant.GROUP.equals(consumerType)) && (!RedisStreamConstant.BROADCAST.equals(consumerType))) {
            throw new DaMaiFrameException(BaseCode.REDIS_STREAM_CONSUMER_TYPE_NOT_EXIST);
        }
    }
}
