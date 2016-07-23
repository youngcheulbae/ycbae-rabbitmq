package com.ycbae.mq.ch1;
import com.rabbitmq.client.*;

import java.io.IOException;

public class Recv {

  private final static String QUEUE_NAME = "hello";

  public static void main(String[] argv) throws Exception {
    ConnectionFactory factory = new ConnectionFactory();
    //factory.setHost("10.113.238.58");
    Address master = new Address("10.113.238.58", 5672);
    Address slave = new Address("10.113.232.162", 5672);
    
    Address[] addrs = {master, slave};
    factory.setUsername("guest");
    factory.setPassword("guest");
    
    Connection connection = factory.newConnection(addrs);
    
    //Connection connection = factory.newConnection();
    Channel channel = connection.createChannel();

    channel.queueDeclare(QUEUE_NAME, false, false, false, null);
    System.out.println(" [*] Waiting for messages. To exit press CTRL+C");

    Consumer consumer = new DefaultConsumer(channel) {
      @Override
      public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body)
          throws IOException {
        String message = new String(body, "UTF-8");
        System.out.println(" [x] Received '" + message + "'");
        try {
			Thread.sleep(5000L);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
      }
    };
    channel.basicConsume(QUEUE_NAME, true, consumer);
  }
}