package com.hmdp.config;

import org.redisson.Redisson;

import org.redisson.api.RedissonClient;
import org.redisson.config.Config;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * @author Kiro
 * @project hm-dianping
 * @time 20240203 18:59
 */
@Configuration
public class RedisConfig {
    @Bean
    public RedissonClient redissonClient() {
        // 配置类
        Config config = new Config();
        // 添加redis地址，此处为单点地址；也可使用config.useClusterServers()添加集群地址
        config.useSingleServer().setAddress("redis://192.168.117.128:6379").setPassword("12321");
        // 创建客户端
        return Redisson.create(config);
    }
}
