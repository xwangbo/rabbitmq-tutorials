import com.rabbitmq.client.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeoutException;

/**
 * Created by Seven7 on 2018/1/22.
 */
public class RPCClient {

    private final Connection connection;
    private final Channel channel;
    private final String replyQueueName;

    public RPCClient() throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(RPCServer.HOST_NAME);

        this.connection = factory.newConnection();

        this.channel = this.connection.createChannel();
        this.replyQueueName = this.channel.queueDeclare().getQueue();
    }

    public String call(String message) throws IOException, InterruptedException {
        String correlationId = UUID.randomUUID().toString();

        String requestQueueName = RPCServer.RPC_QUEUE_NAME;

        AMQP.BasicProperties props = new AMQP.BasicProperties.Builder()
                .correlationId(correlationId)
                .replyTo(requestQueueName)
                .build();

        channel.basicPublish("", requestQueueName, props, message.getBytes(StandardCharsets.UTF_8));

        final BlockingQueue<String> response = new ArrayBlockingQueue<>(1);

        channel.basicConsume(replyQueueName, true, new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                if (correlationId.equals(properties.getCorrelationId())) {
                    response.offer(new String(body, StandardCharsets.UTF_8));
                }
            }
        });

        return response.take();
    }

    public void close() throws IOException {
        connection.close();
    }
}
