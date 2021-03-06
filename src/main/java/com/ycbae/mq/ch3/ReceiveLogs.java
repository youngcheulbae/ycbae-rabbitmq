package com.ycbae.mq.ch3;

import com.rabbitmq.client.*;

import java.io.IOException;

public class ReceiveLogs {
	private static final String EXCHANGE_NAME = "logs";

	public static void main(String[] argv) throws Exception {
		ConnectionFactory factory = new ConnectionFactory();
		// factory.setHost("localhost");
		// Connection connection = factory.newConnection();
		// Channel channel = connection.createChannel();
		Address master = new Address("10.113.238.58", 5672);
		Address slave = new Address("10.113.232.162", 5672);

		Address[] addrs = { master, slave };
		factory.setUsername("guest");
		factory.setPassword("guest");
		factory.setVirtualHost("test_host");

		final Connection connection = factory.newConnection(addrs);
		final Channel channel = connection.createChannel();

		channel.exchangeDeclare(EXCHANGE_NAME, "fanout");
		String queueName = channel.queueDeclare().getQueue();
		channel.queueBind(queueName, EXCHANGE_NAME, "");

		System.out.println(" [*] Waiting for messages. To exit press CTRL+C");

		Consumer consumer = new DefaultConsumer(channel) {
			@Override
			public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties,
					byte[] body) throws IOException {
				String message = new String(body, "UTF-8");
				System.out.println(" [x] Received '" + message + "'");
			}
		};
		channel.basicConsume(queueName, true, consumer);
	}
}
