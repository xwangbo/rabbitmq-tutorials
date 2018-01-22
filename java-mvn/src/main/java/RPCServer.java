import com.rabbitmq.client.*;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeoutException;

/**
 * Created by Seven7 on 2018/1/22.
 */
@Slf4j
public class RPCServer {

    public static final String RPC_QUEUE_NAME = "rpc_queue";
    public static final String HOST_NAME = "localhost";


    public static void main(String[] args) {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(HOST_NAME);

        try (final Connection connection = factory.newConnection()) {

            final Channel channel = connection.createChannel();

            channel.queueDeclare(RPC_QUEUE_NAME, false, false, false, null);
            channel.basicQos(1);

            log.info("[x] awaiting RPC requests...");

            Consumer consumer = new DefaultConsumer(channel) {
                @Override
                public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {

                    String requestQueueName = properties.getReplyTo();

                    AMQP.BasicProperties replyProps = new AMQP.BasicProperties.Builder()
                            .correlationId(properties.getCorrelationId()).build();

                    String response = "";
                    try {
                        String message = new String(body, StandardCharsets.UTF_8);
                        int n = Integer.valueOf(message);

                        response += String.valueOf(Fibonacci.fibonacci(n));
                        log.info("[.] fib({})={}", n, response);
                    } catch (NumberFormatException nfe) {
                        log.error("Wrong number.", nfe);
                    } finally {
                        channel.basicPublish("", requestQueueName, replyProps, response.getBytes(StandardCharsets.UTF_8));
                        channel.basicAck(envelope.getDeliveryTag(), false);

                        synchronized (this) {
                            this.notify();
                        }
                    }
                }
            };

            channel.basicConsume(RPC_QUEUE_NAME, false, consumer);

            while (true) {
                synchronized (consumer) {
                    try {
                        consumer.wait();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }

        } catch (TimeoutException | IOException e) {
            e.printStackTrace();
        }
    }

}
