package com.bingo.kafka.config;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.*;

import java.util.HashMap;
import java.util.Map;

@Configuration
@EnableKafka
public class KafkaConfiguration {
    /**
     * 1.使用@Bean注解创建Topic
     */

    //创建TopicName为topic.quick.initial的Topic并设置分区数为8以及副本数为1
   /* @Bean
    public NewTopic initialTopic() {
        return new NewTopic("topic.quick.initial",8, (short) 1 );
    }*/

    /**
     * 2.手动编码创建Topic，创建方法在DemoTest
     */

    @Bean
    public KafkaAdmin kafkaAdmin() {
        Map<String, Object> props = new HashMap<>();
        //配置Kafka实例的连接地址
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.183.128:9092");
        KafkaAdmin admin = new KafkaAdmin(props);
        return admin;
    }

    @Bean
    public AdminClient adminClient() {
        return AdminClient.create(kafkaAdmin().getConfig());
    }


    //这个是我们之前编写的KafkaTemplate代码，加入@Primary注解
    @Bean
    @Primary
    public KafkaTemplate<Integer, String> kafkaTemplate() {
        KafkaTemplate template = new KafkaTemplate<Integer, String>(producerFactory());
        return template;
    }

    @Bean("defaultKafkaTemplate")
    public KafkaTemplate<Integer, String> defaultKafkaTemplate() {
        KafkaTemplate template = new KafkaTemplate<Integer, String>(producerFactory());
        template.setDefaultTopic("topic.quick.default");
        return template;
    }

    /**
     * ---------------------------------------------------------------
     */

    //ConcurrentKafkaListenerContainerFactory为创建Kafka监听器的工程类，这里只配置了消费者
    @Bean
    public ConcurrentKafkaListenerContainerFactory<Integer, String> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<Integer, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
        return factory;
    }

    //根据consumerProps填写的参数创建消费者工厂
    @Bean
    public ConsumerFactory<Integer, String> consumerFactory() {
        return new DefaultKafkaConsumerFactory<>(consumerProps());
    }

    //根据senderProps填写的参数创建生产者工厂
    @Bean
    public ProducerFactory<Integer, String> producerFactory() {
        return new DefaultKafkaProducerFactory<>(senderProps());
    }

    //kafkaTemplate实现了Kafka发送接收等功能
//    @Bean
//    public KafkaTemplate<Integer, String> kafkaTemplate() {
//        KafkaTemplate template = new KafkaTemplate<Integer, String>(producerFactory());
//        return template;
//    }

    //消费者配置参数
    private Map<String, Object> consumerProps() {
        Map<String, Object> props = new HashMap<>();
        //连接地址
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.183.128:9092");
        //GroupID
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "bootKafka");
        //是否自动提交
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        //自动提交的频率
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "100");
        //Session超时设置
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "15000");
        //键的反序列化方式
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class);
        //值的反序列化方式
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        return props;
    }

    //生产者配置
    private Map<String, Object> senderProps (){
        Map<String, Object> props = new HashMap<>();
        //连接地址
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.183.128:9092");
        //重试，0为不启用重试机制
        props.put(ProducerConfig.RETRIES_CONFIG, 1);
        //控制批处理大小，单位为字节
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        //批量发送，延迟为1毫秒，启用该功能能有效减少生产者发送消息次数，从而提高并发量
        props.put(ProducerConfig.LINGER_MS_CONFIG, 1);
        //生产者可以使用的总内存字节来缓冲等待发送到服务器的记录
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 1024000);
        //键的序列化方式
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
        //值的序列化方式
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        return props;
    }
}
