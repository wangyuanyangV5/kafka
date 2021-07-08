package kafka.examples;

import java.util.Properties;
import java.util.UUID;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;


public class ProducerDemo {

	public static void main(String[] args) throws Exception {
		KafkaProducer<String, String> producer = createKafkaProducer();

		Order order = createOrder();
		ProducerRecord<String, String> record = new ProducerRecord<>(
				"order-topic", order.getOrderId()+"", order.toString());

		// 这是异步发送的模式
		long startTime = System.currentTimeMillis();

		producer.send(record, new Callback() {

			@Override
			public void onCompletion(RecordMetadata metadata, Exception exception) {
				if(exception == null) {
					// 消息发送成功
					System.out.println("消息发送成功"); // 10:00:01.301 // 10:00:01.304
				} else {
					// 消息发送失败，需要重新发送
				}
			}

		});

		long endTime = System.currentTimeMillis();
		if(endTime - startTime > 10) {
			// 你应该走一个监控和报警的过程，开发一些应用程序，系统
			// metric监控和报警，小米开源的open-falcon，挺好用的，监控metric，报警
			// 立马给程序员发短信，或者是发送钉钉，或者发邮件
		}

		Thread.sleep(10 * 1000);

		// 这是同步发送的模式
//		producer.send(record).get();
		// 你要一直等待人家后续一系列的步骤都做完，发送消息之后
		// 有了消息的回应返回给你，你这个方法才会退出来

		producer.close();
	}

	/**
	 * 创建一个测试用的订单
	 * @return
	 */
	private static  Order createOrder() {
		Order order = new Order();
		order.orderId=63988L;
		order.orderNo=UUID.randomUUID().toString();
		order.userId=1147L;
		order.productId=380L;
		order.purchaseCount=2L;
		order.productPrice=50.0;
		order.totalAmount=100.0;
		order._OPERATION_="PAY";
		return order;
	}

	private static class Order{
		private Long orderId;
		private String orderNo;
		private Long userId;
		private Long productId;
		private Long purchaseCount;
		private Double productPrice;
		private Double totalAmount;
		private String _OPERATION_;

		@Override
		public String toString() {
			return "Order{" +
					"orderId=" + orderId +
					", orderNo='" + orderNo + '\'' +
					", userId=" + userId +
					", productId=" + productId +
					", purchaseCount=" + purchaseCount +
					", productPrice=" + productPrice +
					", totalAmount=" + totalAmount +
					", _OPERATION_='" + _OPERATION_ + '\'' +
					'}';
		}

		public Long getOrderId() {
			return orderId;
		}

		public void setOrderId(Long orderId) {
			this.orderId = orderId;
		}

		public String getOrderNo() {
			return orderNo;
		}

		public void setOrderNo(String orderNo) {
			this.orderNo = orderNo;
		}

		public Long getUserId() {
			return userId;
		}

		public void setUserId(Long userId) {
			this.userId = userId;
		}

		public Long getProductId() {
			return productId;
		}

		public void setProductId(Long productId) {
			this.productId = productId;
		}

		public Long getPurchaseCount() {
			return purchaseCount;
		}

		public void setPurchaseCount(Long purchaseCount) {
			this.purchaseCount = purchaseCount;
		}

		public Double getProductPrice() {
			return productPrice;
		}

		public void setProductPrice(Double productPrice) {
			this.productPrice = productPrice;
		}

		public Double getTotalAmount() {
			return totalAmount;
		}

		public void setTotalAmount(Double totalAmount) {
			this.totalAmount = totalAmount;
		}

		public String get_OPERATION_() {
			return _OPERATION_;
		}

		public void set_OPERATION_(String _OPERATION_) {
			this._OPERATION_ = _OPERATION_;
		}
	}

	private static KafkaProducer<String, String> createKafkaProducer() {
		Properties props = new Properties();

		// 这里可以配置几台broker即可，他会自动从broker去拉取元数据进行缓存
		props.put("bootstrap.servers", "127.0.0.1:9092");
		// 这个就是负责把发送的key从字符串序列化为字节数组
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		// 这个就是负责把你发送的实际的message从字符串序列化为字节数组
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("buffer.memory", 67108864);
		props.put("batch.size", 131072); // 一般来说是要自己手动设置的，不是纯粹依靠默认值的，16kb
		props.put("linger.ms", 100); // 发送一条消息出去，100ms内还没有凑成一个batch发送，必须立即发送出去
		props.put("max.request.size", 10485760); // 这个是说你最多可以发送多大的一条消息出去
		props.put("acks", "1"); // follower有没有同步成功你就不管了
		props.put("retries", 10); // 这个重试，一般来说，给个3次~5次就足够了，可以cover住一般的异常场景
		props.put("retry.backoff.ms", 500); // 每次重试间隔100ms

		// 创建一个Producer实例：线程资源，跟各个broker建立socket连接资源
		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);

		return producer;
	}

}
