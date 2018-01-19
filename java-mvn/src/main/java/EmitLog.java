import com.rabbitmq.client.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import static org.apache.commons.lang3.StringUtils.EMPTY;
import static org.apache.commons.lang3.StringUtils.SPACE;

@Slf4j
public class EmitLog {

    public final static String EXCHANGE_NAME = "logs";
    public final static String HOST = "localhost";

    public static void main(String[] args) {
        log.info("Starting");
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(HOST);

        try (Connection connection = factory.newConnection()) {


            log.info("connected");

            Channel channel = connection.createChannel();

            channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.FANOUT);

            String message = getMessage(args);

            channel.basicPublish(EXCHANGE_NAME, EMPTY, null, message.getBytes());

            log.info("[x] Sent '{}'", message);

            channel.close();
            //auto close with try resource
            //connection.close();
            log.info("disconnected");
        } catch (TimeoutException | IOException e) {
            e.printStackTrace();
        }

    }

    private static String getMessage(String[] strings) {
        if (ArrayUtils.isEmpty(strings)) {
            return "Hello World!";
        }

        return StringUtils.join(strings, SPACE);
    }
}
