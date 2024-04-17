import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DeliverCallback;
import com.rabbitmq.client.*;

public class ClientReader {

    private final static String EXCHANGE_NAME = "text_exchange";

    public static void main(String[] argv) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost"); // RabbitMQ server address

        try (Connection connection = factory.newConnection();
             Channel channel = connection.createChannel()) {

            channel.exchangeDeclare(EXCHANGE_NAME, "fanout");
            String queueName = channel.queueDeclare().getQueue();
            channel.queueBind(queueName, EXCHANGE_NAME, "");

            String request = "ReadLast";
            AMQP.BasicProperties props = new AMQP.BasicProperties.Builder()
                    .replyTo(queueName)
                    .build();

            channel.basicPublish(EXCHANGE_NAME, "", props, request.getBytes());
            System.out.println(" [x] Sent request: '" + request + "'");


            System.out.println(" [*] Waiting for responses. To exit press CTRL+C");

            DeliverCallback deliverCallback = (consumerTag, delivery) -> {
                String message = new String(delivery.getBody(), "UTF-8");
                System.out.println(" [x] Received: '" + message + "'");
            };
            channel.basicConsume(queueName, true, deliverCallback, consumerTag -> { });

            Thread.sleep(10000);       }
    }
}

