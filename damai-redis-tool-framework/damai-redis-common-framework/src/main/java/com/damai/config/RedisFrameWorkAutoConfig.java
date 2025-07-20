package com.damai.config;

import io.lettuce.core.ReadFrom;
import org.springframework.boot.autoconfigure.data.redis.LettuceClientConfigurationBuilderCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.RedisSentinelConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceClientConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.serializer.StringRedisSerializer;

import org.springframework.data.redis.connection.lettuce.LettucePoolingClientConfiguration;
import org.springframework.boot.autoconfigure.data.redis.RedisProperties;

import java.util.HashSet;

public class RedisFrameWorkAutoConfig {
    @Bean("redisToolRedisTemplate")
    public RedisTemplate redisTemplate(RedisConnectionFactory redisConnectionFactory){
        RedisTemplate redisTemplate = new RedisTemplate();
        redisTemplate.setDefaultSerializer(new StringRedisSerializer());
        redisTemplate.setConnectionFactory(redisConnectionFactory);
        return redisTemplate;
    }

    @Primary
    @Bean("redisToolStringRedisTemplate")
    public StringRedisTemplate stringRedisTemplate(RedisConnectionFactory redisConnectionFactory){
        StringRedisTemplate myStringredisTemplate = new StringRedisTemplate();
        myStringredisTemplate.setDefaultSerializer(new StringRedisSerializer());
        myStringredisTemplate.setConnectionFactory(redisConnectionFactory);
        return myStringredisTemplate;
    }

//    @Bean
//    public LettuceClientConfigurationBuilderCustomizer clientConfigurationBuilderCustomizer() {
//        return new LettuceClientConfigurationBuilderCustomizer() {
//            @Override
//            public void customize(LettuceClientConfiguration.LettuceClientConfigurationBuilder clientConfigurationBuilder) {
//                clientConfigurationBuilder.readFrom(ReadFrom.REPLICA_PREFERRED);
//            }
//        };
//    }

}
