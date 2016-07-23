package com.ycbae.mq.ch5;

import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Address;
import com.rabbitmq.client.Channel;

public class EmitLogTopic {

	private static final String EXCHANGE_NAME = "topic_logs";

	public static void main(String[] argv) {
		Connection connection = null;
		Channel channel = null;
		try {
			ConnectionFactory factory = new ConnectionFactory();
			// factory.setHost("localhost");

			// connection = factory.newConnection();
			// channel = connection.createChannel();

			Address master = new Address("10.113.238.58", 5672);
			Address slave = new Address("10.113.232.162", 5672);

			Address[] addrs = { master, slave };
			factory.setUsername("guest");
			factory.setPassword("guest");
			factory.setVirtualHost("test_host");

			connection = factory.newConnection(addrs);
			channel = connection.createChannel();

			channel.exchangeDeclare(EXCHANGE_NAME, "topic");

			String routingKey = getRouting(argv);
			String message = getMessage(argv);

			channel.basicPublish(EXCHANGE_NAME, routingKey, null, message.getBytes("UTF-8"));
			System.out.println(" [x] Sent '" + routingKey + "':'" + message + "'");

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (connection != null) {
				try {
					connection.close();
				} catch (Exception ignore) {
				}
			}
		}
	}

	private static String getRouting(String[] strings) {
		if (strings.length < 1)
			return "anonymous.info";
		return strings[0];
	}

	private static String getMessage(String[] strings) {
		if (strings.length < 2)
			return "Hello World!";
		return joinStrings(strings, " ", 1);
	}

	private static String joinStrings(String[] strings, String delimiter, int startIndex) {
		int length = strings.length;
		if (length == 0)
			return "";
		if (length < startIndex)
			return "";
		StringBuilder words = new StringBuilder(strings[startIndex]);
		for (int i = startIndex + 1; i < length; i++) {
			words.append(delimiter).append(strings[i]);
		}
		return words.toString();
	}
}
