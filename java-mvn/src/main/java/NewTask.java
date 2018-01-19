import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import static org.apache.commons.lang3.StringUtils.SPACE;

@Slf4j
public class NewTask {

    public final static String QUEUE_NAME = "hello";
    public final static String HOST = "localhost";

    public static void main(String[] args) {
        log.info("Starting");
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(HOST);

        try (Connection connection = factory.newConnection()) {


            log.info("connected");

            Channel channel = connection.createChannel();
            channel.queueDeclare(QUEUE_NAME, false, false, false, null);

            String message = getMessage(args);

            channel.basicPublish("", QUEUE_NAME, null, message.getBytes());

            log.info("[x] sent '{}'", message);

            channel.close();
//            connection.close();
            log.info("disconnected");
        } catch (TimeoutException | IOException e) {
            e.printStackTrace();
        }

    }

    public static String getMessage(String[] strings) {
        if (ArrayUtils.isEmpty(strings)) {
            return "Hello World!";
        }

        return StringUtils.join(strings, SPACE);
    }
}
