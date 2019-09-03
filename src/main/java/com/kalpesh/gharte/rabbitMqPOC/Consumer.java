package com.kalpesh.gharte.rabbitMqPOC;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.QueueingConsumer;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.TimeoutException;

public class Consumer
{
    public static void main(String args[]) throws NoSuchAlgorithmException, KeyManagementException, URISyntaxException, IOException, TimeoutException, InterruptedException {
        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setUri("amqp://guest:guest@localhost");
        connectionFactory.setConnectionTimeout(300000);

        Connection connection = connectionFactory.newConnection();
        Channel channel = connection.createChannel();
        channel.queueDeclare("my-queue",true,false,false,null);

        QueueingConsumer consumer = new QueueingConsumer(channel);
        channel.basicConsume("my-queue",false,consumer);

        while (true){
            QueueingConsumer.Delivery delivery = consumer.nextDelivery();

            if(delivery != null)
            {
                try{
                    String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
                    System.out.println("Message consumed: " + message);
                    channel.basicAck(delivery.getEnvelope().getDeliveryTag(),false);
                } catch (Exception e){
                    channel.basicReject(delivery.getEnvelope().getDeliveryTag(),true);
                }
            }
        }
    }
}
