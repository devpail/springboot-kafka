package com.bingo.kafka.test;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.support.GenericMessage;
import org.springframework.test.context.junit4.SpringRunner;

import javax.annotation.Resource;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

/**
 * @program: springboot-kafka->DemoTest
 * @description: 测试类
 * @author: zhangzb10
 * @create: 2019-08-16 09:58
 **/
@SpringBootTest
@RunWith(SpringRunner.class)
public class DemoTest {

    @Autowired
    private KafkaTemplate kafkaTemplate;

    /** 
    * @Description:
    * @Param: [] 
    * @return: void 
    * @Author: zhangzb10
    * @Date: 2019/8/16 
    */
    @Test
    public void testDemo() throws InterruptedException {
        kafkaTemplate.send("topic.quick.demo","this is my first demo");
        Thread.sleep(5000);
    }

    @Resource
    private KafkaTemplate defaultKafkaTemplate;

    /** 
    * @Description: 测试默认发送方法 
    * @Param: [] 
    * @return: void 
    * @Author: zhangzb10
    * @Date: 2019/8/16 
    */
    @Test
    public void testDefaultKafkaTemplate() {
        defaultKafkaTemplate.sendDefault("I`m send msg to default topic");
    }

    /** 
    * @Description: 测试带参数发送方法 
    * @Param: [] 
    * @return: void 
    * @Author: zhangzb10
    * @Date: 2019/8/16 
    */
    @Test
    public void testTemplateSend() {
        //发送带有时间戳的消息
        kafkaTemplate.send("topic.quick.demo", 0, System.currentTimeMillis(), 0, "send message with timestamp");

        //使用ProducerRecord发送消息
        ProducerRecord record = new ProducerRecord("topic.quick.demo", "use ProducerRecord to send message");
        kafkaTemplate.send(record);

        //使用Message发送消息
        Map map = new HashMap();
        map.put(KafkaHeaders.TOPIC, "topic.quick.demo");
        map.put(KafkaHeaders.PARTITION_ID, 0);
        map.put(KafkaHeaders.MESSAGE_KEY, 0);
        GenericMessage message = new GenericMessage("use Message to send message",new MessageHeaders(map));
        kafkaTemplate.send(message);
    }

    @Autowired
    private AdminClient adminClient;

    /** 
    * @Description: 创建topic 
    * @Param: [] 
    * @return: void 
    * @Author: zhangzb10
    * @Date: 2019/8/16 
    */
   /* @Test
    public void testCreateTopic() throws InterruptedException {
        NewTopic topic = new NewTopic("topic.quick.initial2", 1, (short) 1);
        adminClient.createTopics(Arrays.asList(topic));
        Thread.sleep(1000);
    }*/

    /** 
    * @Description: 查看topic信息 
    * @Param: [] 
    * @return: void 
    * @Author: zhangzb10
    * @Date: 2019/8/16 
    */
    @Test
    public void testSelectTopicInfo() throws ExecutionException, InterruptedException {
        DescribeTopicsResult result = adminClient.describeTopics(Arrays.asList("topic.quick.demo"));
        result.all().get().forEach((k,v)->System.out.println("k: "+k+" ,v: "+v.toString()+"\n"));
        System.out.println("-----------------");
    }


}
