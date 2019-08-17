package com.bingo.kafka.listen;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * @program: springboot-kafka->DemoListener
 * @description: 监听
 * @author: zhangzb10
 * @create: 2019-08-16 09:55
 **/

@Component
public class DemoListener {
    private static final Logger log= LoggerFactory.getLogger(DemoListener.class);

    //声明consumerID为demo，监听topicName为topic.quick.demo的Topic
    @KafkaListener(id = "demo", topics = "topic.quick.demo")
    public void listen(String msgData) {
        log.info("demo receive : "+msgData);
    }
}
