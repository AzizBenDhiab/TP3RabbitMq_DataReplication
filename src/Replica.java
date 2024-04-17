
import com.rabbitmq.client.*;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class Replica {

    private final static String EXCHANGE_NAME = "text_exchange";

    public static void main(String[] argv) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");


        try (Connection connection = factory.newConnection();
             Channel channel = connection.createChannel()) {

            channel.exchangeDeclare(EXCHANGE_NAME, "fanout");
            String queueName = "";
            if (argv.length >= 1) {
                queueName = "queue" + argv[0];
            } else {
                System.err.println("Insufficient arguments provided.");
                System.exit(1);
            }

            channel.queueDeclare(queueName, false, false, false, null);
            channel.queueBind(queueName, EXCHANGE_NAME, "");

            System.out.println(" [*] Waiting for messages. To exit press CTRL+C");

            DeliverCallback deliverCallback = (consumerTag, delivery) -> {
                String message = new String(delivery.getBody(), "UTF-8");
                System.out.println(" [x] Received '" + message + "'");
                String filePath = "C:/Users/PAVILION/Desktop/TP3-RabbitMq/" + argv[0] + "/fichier.txt";
                if ("ReadLast".equals(message)) {
                    String lastLine = getLastLineOfFile(filePath);
                    if (lastLine != null) {
                        String routingKey = delivery.getProperties().getReplyTo();
                        channel.basicPublish("", routingKey, null, lastLine.getBytes(StandardCharsets.UTF_8));
                        System.out.println("Sent response message: '" + lastLine + "'");
                    }
                    } else {
                        insertLineIntoFile(filePath, message, argv);
                    }

                }
                ;
                channel.basicConsume(queueName, true, deliverCallback, consumerTag -> {
                });
                while (true) {
                    Thread.sleep(1000);
                }
            }

    }




    public static void insertLineIntoFile(String filePath, String lineToInsert , String argv[]) {
        try {
            File inputFile = new File(filePath);
            String temppath = "tempFile"+argv[0]+".txt";
            File tempFile = new File(temppath);

            BufferedReader reader = new BufferedReader(new FileReader(inputFile));
            BufferedWriter writer = new BufferedWriter(new FileWriter(tempFile));

            String currentLine;
            int lineNumberToInsert = getLineNumberFromLine(lineToInsert);

            List<String> lines = new ArrayList<>();

            // Read lines from the input file and add them to the list
            while ((currentLine = reader.readLine()) != null) {
                lines.add(currentLine);
            }

            // Add the new line to the list
            lines.add(lineToInsert);

            // Sort the lines based on their line numbers
            lines.sort(Comparator.comparingInt(Replica::getLineNumberFromLine));

            // Write the sorted lines to the temporary file
            for (String line : lines) {
                writer.write(line + System.lineSeparator());
            }

            writer.close();
            reader.close();

            // Replace the original file with the temporary file
            if (!inputFile.delete()) {
                System.err.println("Could not delete the original file.");
                return;
            }

            if (!tempFile.renameTo(inputFile)) {
                System.err.println("Could not rename the temp file.");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


        private static int getLineNumberFromLine(String line) {
            String[] parts = line.split(" ", 2);
            return Integer.parseInt(parts[0]);
        }

        public static String getLastLineOfFile(String filePath) {
            String lastLine = null;

            try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    lastLine = line;
                }
            } catch (IOException e) {
                e.printStackTrace();
            }

            return lastLine;
        }


    }


