package com.ft.demo.consumer;

import com.ft.demo.consitant.RocketMQConstant;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.*;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

public class SimpleConsumer {

    private DefaultMQPushConsumer consumer = null;

    public void initConsumer(String consumerGroup) {
        DefaultMQPushConsumer defaultMQPushConsumer = new DefaultMQPushConsumer(consumerGroup);
        // 设置NameServer的地址
        defaultMQPushConsumer.setNamesrvAddr(RocketMQConstant.TEST_NAMESERVER);
        consumer = defaultMQPushConsumer;
    }

    /**
     * 消费消息
     */
    public void consumeMessage() {
        try {
            // 订阅一个或者多个Topic，以及Tag来过滤需要消费的消息
            consumer.subscribe(RocketMQConstant.TEST_SIMPLE_MESSAGE_TOPIC_NAME, "*");
            // 注册回调实现类来处理从broker拉取回来的消息
            consumer.registerMessageListener(new MessageListenerConcurrently() {
                @Override
                public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
                    System.out.printf("%s Receive New Messages: %s %n", Thread.currentThread().getName(), msgs);
                    // 标记该消息已经被成功消费
                    return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
                }
            });
            // 启动消费者实例
            consumer.start();
            System.out.printf("Consumer Started.%n");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 顺序消费消息
     */
    public void consumeOrderlyMessage() {
        try {
            /**
             * 设置Consumer第一次启动是从队列头部开始消费还是队列尾部开始消费<br>
             * 如果非第一次启动，那么按照上次消费的位置继续消费
             */
            consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);

            consumer.subscribe(RocketMQConstant.TEST_ORDERLY_MESSAGE_TOPIC_NAME, "TagA || TagC || TagD");

            consumer.registerMessageListener(new MessageListenerOrderly() {

                Random random = new Random();

                @Override
                public ConsumeOrderlyStatus consumeMessage(List<MessageExt> msgs, ConsumeOrderlyContext context) {
                    context.setAutoCommit(true);
                    for (MessageExt msg : msgs) {
                        // 可以看到每个queue有唯一的consume线程来消费, 订单对每个queue(分区)有序
                        System.out.println("consumeThread=" + Thread.currentThread().getName() + " queueId=" + msg.getQueueId() + ", content:" + new String(msg.getBody()));
                    }

                    try {
                        //模拟业务逻辑处理中...
                        //TimeUnit.SECONDS.sleep(random.nextInt(10));
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    return ConsumeOrderlyStatus.SUCCESS;
                }
            });

            consumer.start();

            System.out.println("Consumer Started.");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 延时消息消费
     */
    public void consumeDelayMessage() {
        try {
            // 实例化消费者
            DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("ExampleConsumer");
            // 订阅Topics
            consumer.subscribe(RocketMQConstant.TEST_DELAY_MESSAGE_TOPIC_NAME, "*");
            // 注册消息监听者
            consumer.registerMessageListener(new MessageListenerConcurrently() {
                @Override
                public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> messages, ConsumeConcurrentlyContext context) {
                    for (MessageExt message : messages) {
                        // Print approximate delay time period
                        System.out.println("Receive message[msgId=" + message.getMsgId() + "] " + (System.currentTimeMillis() - message.getStoreTimestamp()) + "ms later");
                    }
                    return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
                }
            });
            // 启动消费者
            consumer.start();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    public static void main(String[] args) throws Exception {
        SimpleConsumer simpleConsumer = new SimpleConsumer();
//        simpleConsumer.initConsumer("SIMPLE_CONSUMER");
//		simpleConsumer.consumeMessage();

        simpleConsumer.initConsumer("ORDERLY_CONSUMER");
        simpleConsumer.consumeOrderlyMessage();
//
//        simpleConsumer.initConsumer("DELAY_CONSUMER");
//        simpleConsumer.consumeDelayMessage();
    }
}