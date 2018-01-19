import com.rabbitmq.client.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.ArrayUtils;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeoutException;


@Slf4j
public class ReceiveLogsDirect {

    public static void main(String[] args) {

        if (ArrayUtils.isEmpty(args)) {
            log.error("Usage: ReceiveLogsDirect [info] [warning] [error]");
            System.exit(1);
        }

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(EmitLogDirect.HOST);

        try {
            Connection connection = factory.newConnection();
            Channel channel = connection.createChannel();

            channel.exchangeDeclare(EmitLogDirect.EXCHANGE_NAME, BuiltinExchangeType.DIRECT);

            String queueName = channel.queueDeclare().getQueue();

            for (String severity : args) {
                channel.queueBind(queueName, EmitLogDirect.EXCHANGE_NAME, severity);
            }

            log.info(" [*] Waiting for messages. To exit press CTRL+C");

            Consumer consumer = new DefaultConsumer(channel) {
                @Override
                public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                    String message = new String(body, StandardCharsets.UTF_8);
                    log.info("[x] Received [{}] : '{}'", envelope.getRoutingKey(), message);
                    try {
                        doWork(message);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    } finally {
                        log.info("[x] Done.");
                    }
                }
            };

            channel.basicConsume(queueName, true, consumer);

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
