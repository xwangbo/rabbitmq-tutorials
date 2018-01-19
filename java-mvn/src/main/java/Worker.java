import com.rabbitmq.client.*;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeoutException;


@Slf4j
public class Worker {

    public static void main(String[] args) {

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(NewTask.HOST);

        try {
            Connection connection = factory.newConnection();
            Channel channel = connection.createChannel();

            int prefetchCount = 1;
            channel.basicQos(prefetchCount);

            boolean durable = true;
            channel.queueDeclare(NewTask.QUEUE_NAME, durable, false, false, null);

            Consumer consumer = new DefaultConsumer(channel) {
                @Override
                public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                    String message = new String(body, StandardCharsets.UTF_8);
                    log.info("[x] Received '{}'", message);
                    try {
                        doWork(message);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    } finally {
                        log.info("[x] Done.");
                        channel.basicAck(envelope.getDeliveryTag(), false);
                    }
                }
            };

            boolean autoAck = false;
            channel.basicConsume(NewTask.QUEUE_NAME, autoAck, consumer);

        } catch (TimeoutException | IOException e) {
            e.printStackTrace();
        }
    }

    private static void doWork(String task) throws InterruptedException {
        for (char ch : task.toCharArray()) {
            if ('.' == ch) {
                Thread.sleep(1000L);
            }
        }
    }
}
