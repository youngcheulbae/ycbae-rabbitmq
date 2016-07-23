package com.ycbae.mq.ch1;
import com.rabbitmq.client.Address;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.MessageProperties;

public class Send {

  private final static String QUEUE_NAME = "hello";

  public static void main(String[] argv) throws Exception {
    ConnectionFactory factory = new ConnectionFactory();
    //factory.setHost("10.113.238.58");
    
    Address master = new Address("10.113.238.58", 5672);
    Address slave = new Address("10.113.232.162", 5672);
    
    Address[] addrs = {master, slave};
    factory.setUsername("guest");
    factory.setPassword("guest");
    factory.setVirtualHost("test_host");
    
    Connection connection = factory.newConnection(addrs);
    Channel channel = connection.createChannel();

    //channel.queueDeclare(QUEUE_NAME, false, false, false, null);
    channel.queueDeclare(QUEUE_NAME, true, false, false, null);
     
    String message = "Hello World!";
    for(int i=0; i < 10 ; i++) {
	    //channel.basicPublish("", QUEUE_NAME, null, message.getBytes("UTF-8"));
    	channel.basicPublish( "", QUEUE_NAME, MessageProperties.PERSISTENT_TEXT_PLAIN, message.getBytes("UTF-8"));
	    System.out.println(" [x] Sent '" + message + "'");
    }

    channel.close();
    connection.close();
  }
}