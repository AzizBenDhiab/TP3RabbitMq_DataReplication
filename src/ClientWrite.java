
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Channel;

public class ClientWrite {

    private final static String EXCHANGE_NAME = "text_exchange";

    public static void main(String[] argv) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost"); // RabbitMQ server address

        try (Connection connection = factory.newConnection();
             Channel channel = connection.createChannel()) {

            channel.exchangeDeclare(EXCHANGE_NAME, "fanout");
            String message;
if(argv.length >= 2)
{  message = argv[0]+ argv[1];
    channel.basicPublish(EXCHANGE_NAME, "", null, message.getBytes());
    System.out.println(" [x] Sent '" + message + "'");}
else {
    System.err.println("Insufficient arguments provided.");
    System.exit(1);
}

        }
    }
}
