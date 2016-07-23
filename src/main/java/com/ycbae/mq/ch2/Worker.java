package com.ycbae.mq.ch2;
import com.rabbitmq.client.*;

import java.io.IOException;

public class Worker {

  private static final String TASK_QUEUE_NAME = "hello";

  public static void main(String[] argv) throws Exception {
    ConnectionFactory factory = new ConnectionFactory();
/*    factory.setHost("localhost");
    final Connection connection = factory.newConnection();
    final Channel channel = connection.createChannel();

    channel.queueDeclare(TASK_QUEUE_NAME, true, false, false, null);
    System.out.println(" [*] Waiting for messages. To exit press CTRL+C");

    channel.basicQos(1);*/
    
    //factory.setHost("10.113.238.58");
    
    Address master = new Address("10.113.238.58", 5672);
    Address slave = new Address("10.113.232.162", 5672);
    
    Address[] addrs = {master, slave};
    factory.setUsername("guest");
    factory.setPassword("guest");
    factory.setVirtualHost("test_host");
    
    final Connection connection = factory.newConnection(addrs);
    final Channel channel = connection.createChannel();

    final Consumer consumer = new DefaultConsumer(channel) {
      @Override
      public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
        String message = new String(body, "UTF-8");

        System.out.println(" [x] Received '" + message + "'");
        try {
          doWork(message);
        } finally {
          System.out.println(" [x] Done");
          channel.basicAck(envelope.getDeliveryTag(), false);
        }
      }
    };
    channel.basicConsume(TASK_QUEUE_NAME, false, consumer);
  }

  private static void doWork(String task) {
    for (char ch : task.toCharArray()) {
      //if (ch == '.') {
        try {
          Thread.sleep(1000);
        } catch (InterruptedException _ignored) {
          Thread.currentThread().interrupt();
        }
      //}
    }
  }
}
