package com.ft.demo.producer;

import com.ft.demo.business.dto.OrderStep;
import com.ft.demo.consitant.RocketMQConstant;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.CountDownLatch2;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.common.RemotingHelper;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class SimpleProducer {

	private DefaultMQProducer producer = null;

	public void initProducer(String producerGroup){
		// 实例化消息生产者Producer
		DefaultMQProducer defaultMQProducer = new DefaultMQProducer(producerGroup);
		// 设置NameServer的地址
		defaultMQProducer.setNamesrvAddr(RocketMQConstant.TEST_NAMESERVER);
		producer =  defaultMQProducer;
	}

	/**
	 * Producer端发送同步消息
	 * 这种可靠性同步地发送方式使用的比较广泛，比如：重要的消息通知，短信通知。
	 */
	public void sendSyncMessage(){
		try {
			// 启动Producer实例
			producer.start();
			for (int i = 0; i < 1; i++) {
				// 创建消息，并指定Topic，Tag和消息体
				Message msg = new Message(RocketMQConstant.TEST_SIMPLE_MESSAGE_TOPIC_NAME /* Topic */,
						"TagA" /* Tag */,
						("Hello RocketMQ " + i).getBytes(RemotingHelper.DEFAULT_CHARSET) /* Message body */
				);
				// 发送消息到一个Broker
				SendResult sendResult = producer.send(msg);
				// 通过sendResult返回消息是否成功送达
				System.out.printf("%s%n", sendResult);

				//发一条休眠2s 不然consumer打印太快
				//Thread.sleep(2000);
			}
			// 如果不再发送消息，关闭Producer实例。
			producer.shutdown();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * 单向发送消息
	 * 异步消息通常用在对响应时间敏感的业务场景，即发送端不能容忍长时间地等待Broker的响应。
	 */
	public void sendAsyncMessage(){
		try {
			// 启动Producer实例
			producer.start();
			producer.setRetryTimesWhenSendAsyncFailed(0);

			int messageCount = 100;
			// 根据消息数量实例化倒计时计算器
			final CountDownLatch2 countDownLatch = new CountDownLatch2(messageCount);
			for (int i = 0; i < messageCount; i++) {
				final int index = i;
				// 创建消息，并指定Topic，Tag和消息体
				Message msg = new Message(RocketMQConstant.TEST_SIMPLE_MESSAGE_TOPIC_NAME,
						"TagA",
						"OrderID188",
						"Hello world".getBytes(RemotingHelper.DEFAULT_CHARSET));
				// SendCallback接收异步返回结果的回调
				producer.send(msg, new SendCallback() {
					@Override
					public void onSuccess(SendResult sendResult) {
						System.out.printf("%-10d OK %s %n", index,
								sendResult.getMsgId());
					}
					@Override
					public void onException(Throwable e) {
						System.out.printf("%-10d Exception %s %n", index, e);
						e.printStackTrace();
					}
				});
				//发一条休眠2s 不然consumer打印太快
				////Thread.sleep(2000);
			}
			// 等待5s
			countDownLatch.await(5, TimeUnit.SECONDS);
			// 如果不再发送消息，关闭Producer实例。
			producer.shutdown();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * Producer端发送异步消息
	 * 这种方式主要用在不特别关心发送结果的场景，例如日志发送。
	 */
	public void sendOneWayMessage(){
		try {
			// 启动Producer实例
			producer.start();
			for (int i = 0; i < 100; i++) {
				// 创建消息，并指定Topic，Tag和消息体
				Message msg = new Message(RocketMQConstant.TEST_SIMPLE_MESSAGE_TOPIC_NAME /* Topic */,
						"TagA" /* Tag */,
						("Hello RocketMQ " + i).getBytes(RemotingHelper.DEFAULT_CHARSET) /* Message body */
				);
				// 发送单向消息，没有任何返回结果
				producer.sendOneway(msg);

				//发一条休眠2s 不然consumer打印太快
				//Thread.sleep(2000);
			}
			// 如果不再发送消息，关闭Producer实例。
			producer.shutdown();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * 顺序消息生产
	 */
	public void sendOrderlyMessage(){
		try {
			producer.start();
			String[] tags = new String[]{"TagA", "TagC", "TagD"};
			// 订单列表
			List<OrderStep> orderList = new OrderStep().buildOrders();
			Date date = new Date();
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			String dateStr = sdf.format(date);
			for (int i = 0; i < 10; i++) {
				// 加个时间前缀
				String body = dateStr + " Hello RocketMQ " + orderList.get(i);
				Message msg = new Message(RocketMQConstant.TEST_ORDERLY_MESSAGE_TOPIC_NAME, tags[i % tags.length], "KEY" + i, body.getBytes());

				SendResult sendResult = producer.send(msg, new MessageQueueSelector() {
					@Override
					public MessageQueue select(List<MessageQueue> mqs, Message msg, Object arg) {
						Long id = (Long) arg;  //根据订单id选择发送queue
						long index = id % mqs.size();
						return mqs.get((int) index);
					}
				}, orderList.get(i).getOrderId());//订单id

				System.out.println(String.format("SendResult status:%s, queueId:%d, body:%s",
						sendResult.getSendStatus(),
						sendResult.getMessageQueue().getQueueId(),
						body));
			}
			producer.shutdown();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * 顺序消息生产
	 */
	public void sendDelayMessage(){
		try {
			// 实例化一个生产者来产生延时消息
			DefaultMQProducer producer = new DefaultMQProducer("ExampleProducerGroup");

			producer.setNamesrvAddr("127.0.0.1:9876");
			// 启动生产者
			producer.start();
			int totalMessagesToSend = 10;
			for (int i = 0; i < totalMessagesToSend; i++) {
				Message message = new Message(RocketMQConstant.TEST_DELAY_MESSAGE_TOPIC_NAME, ("Hello scheduled message " + i).getBytes());
				// 设置延时等级3,这个消息将在10s之后发送(现在只支持固定的几个时间,详看delayTimeLevel)
				message.setDelayTimeLevel(1);
				// 发送消息
				final int index = i;
				producer.send(message, new SendCallback() {
					@Override
					public void onSuccess(SendResult sendResult) {
						System.out.printf("%-10d OK %s %n", index,
								sendResult.getMsgId());
					}
					@Override
					public void onException(Throwable e) {
						System.out.printf("%-10d Exception %s %n", index, e);
						e.printStackTrace();
					}
				});
			}
			// 关闭生产者  延时消息是异步的 这里提前关闭了  导致找不到topic
//			producer.shutdown();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) throws Exception {
		SimpleProducer simpleProducer = new SimpleProducer();

//		simpleProducer.initProducer("SIMPLE_PRODUCER");
//		simpleProducer.sendSyncMessage();

//		simpleProducer.initProducer("SIMPLE_PRODUCER");
//		simpleProducer.sendAsyncMessage();
//
//		simpleProducer.initProducer("SIMPLE_PRODUCER");
//		simpleProducer.sendOneWayMessage();
//
		simpleProducer.initProducer("ORDERLY_PRODUCER");
		simpleProducer.sendOrderlyMessage();
//
//		simpleProducer.initProducer("DELAY_PRODUCER");
//		simpleProducer.sendDelayMessage();
    }
}