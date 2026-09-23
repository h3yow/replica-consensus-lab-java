package com.github.h3yow.replicaconsensuslabjava.consensus;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/** 将进程内模拟集群装配为 Spring Bean。 */
@Configuration
@EnableConfigurationProperties(ConsensusProperties.class)
public class ConsensusConfig {

    @Bean
    public Cluster cluster(ConsensusProperties properties) {
        return new Cluster(properties.getNodeIds(), properties.getDataDir(), properties);
    }
}
