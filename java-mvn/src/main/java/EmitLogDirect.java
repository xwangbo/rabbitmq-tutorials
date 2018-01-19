import com.rabbitmq.client.BuiltinExchangeType;
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
public class EmitLogDirect {

    public final static String EXCHANGE_NAME = "direct_logs";
    public final static String HOST = "localhost";

    public static void main(String[] args) {

        if (ArrayUtils.isEmpty(args)) {
            log.error("Usage: EmitLogDirect [info] [warning] [error]");
            System.exit(1);
        }

        log.info("Starting");
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(HOST);

        try (Connection connection = factory.newConnection()) {


            log.info("connected");

            Channel channel = connection.createChannel();

            channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.DIRECT);

            String message = getMessage(args),
                    severity = getSeverity(args);

            channel.basicPublish(EXCHANGE_NAME, severity, null, message.getBytes());

            log.info("[x] Sent [{}]:'{}'", severity, message);

            channel.close();
            //auto close with try resource
            //connection.close();
            log.info("disconnected");
        } catch (TimeoutException | IOException e) {
            e.printStackTrace();
        }

    }

    private static String getMessage(String[] strings) {
        if (strings.length < 2) {
            return "Hello World!";
        }
        return StringUtils.join(ArrayUtils.subarray(strings, 1, strings.length), SPACE);
    }

    private static String getSeverity(String[] strings) {
        if (ArrayUtils.isEmpty(strings)) {
            return "info";
        }

        return strings[0];
    }
}
