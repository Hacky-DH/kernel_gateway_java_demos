package org.dhcaky;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.http.HttpEntity;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.java_websocket.client.WebSocketClient;
import org.java_websocket.handshake.ServerHandshake;

import java.io.IOException;
import java.io.PrintStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLEncoder;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * WebSocket Client for Kernel Gateway
 *
 * @see @link http://github.com/TooTallNate/Java-WebSocket/wiki/Drafts
 */
public class WSClient extends WebSocketClient {
    private static final ObjectMapper mapper = new ObjectMapper();
    private PrintStream logger;
    private String messageID;
    private final CountDownLatch streamLatch = new CountDownLatch(1);

    WSClient(URI uri, PrintStream logger) {
        super(uri);
        this.logger = logger;
    }

    @Override
    public void onOpen(ServerHandshake serverHandshake) {
        if (logger != null)
            logger.printf("opened websocket connection %s status: %d\n",
                    this.getConnection().getRemoteSocketAddress().getHostName(),
                    serverHandshake.getHttpStatus());
    }

    @Override
    public void onMessage(String s) {
        if (logger != null)
            logger.println("received message " + s);
        try {
            JsonNode msg = mapper.readTree(s);
            String type = msg.path("msg_type").asText();
            if (type.equalsIgnoreCase("error")) {
                if (logger != null) {
                    logger.println("Error: web socket occurs an error " + s);
                }
            } else if (type.equalsIgnoreCase("stream")) {
                if (messageID.equalsIgnoreCase(msg.path("parent_header").path("msg_id").asText())) {
                    if (logger != null) {
                        logger.println("Result: " + msg.path("content").path("text").asText());
                    }
                    streamLatch.countDown();
                }
            }
            // skip other message type
        } catch (IOException e) {
            if (logger != null) {
                logger.println("Error: received message is not a JSON!");
                e.printStackTrace(logger);
            }
        }
    }

    public void await() throws InterruptedException {
        streamLatch.await();
    }

    public void await(long timeout, TimeUnit timeUnit) throws InterruptedException {
        streamLatch.await(timeout, timeUnit);
    }

    @Override
    public void onClose(int code, String reason, boolean remote) {
        if (logger != null)
            logger.printf("closed websocket connection %s code: %d, reason: %s\n",
                    this.getConnection().getRemoteSocketAddress().getHostName(),
                    code, reason);
    }

    @Override
    public void onError(Exception e) {
        if (logger != null) {
            logger.println("Error: web socket occurs an error");
            e.printStackTrace(logger);
        }
    }

    public void sendCode(String code) {
        ObjectNode root = mapper.createObjectNode();
        ObjectNode header = mapper.createObjectNode();
        header.put("username", "");
        header.put("version", "5.0");
        messageID = UUID.randomUUID().toString();
        header.put("msg_id", messageID);
        header.put("msg_type", "execute_request");
        root.set("header", header);
        root.putNull("parent_header");
        root.put("channel", "shell");
        ObjectNode content = mapper.createObjectNode();
        content.put("code", code);
        content.put("silent", false);
        content.put("store_history", false);
        content.putNull("user_expressions");
        content.put("allow_stdin", false);
        root.set("content", content);
        root.putNull("metadata");
        root.putNull("buffers");
        if (logger != null) {
            logger.println(root.toString());
        }
        super.send(root.toString());
    }

    public static String createKernel(String address, String kernelName, PrintStream logger) throws IOException {
        try (CloseableHttpClient httpClient = HttpClients.createDefault();) {
            HttpPost httpPost = new HttpPost(String.format("http://%s/api/kernels", address));
            ObjectNode node = mapper.createObjectNode();
            node.put("name", kernelName);
            StringEntity requestEntity = new StringEntity(node.toString(), ContentType.APPLICATION_JSON);
            httpPost.setEntity(requestEntity);
            ResponseHandler<JsonNode> responseHandler = response -> {
                int status = response.getStatusLine().getStatusCode();
                if (status >= 200 && status < 300) {
                    HttpEntity entity = response.getEntity();
                    if (entity == null) {
                        throw new ClientProtocolException("empty response");
                    }
                    return mapper.readTree(EntityUtils.toString(entity));
                } else {
                    throw new ClientProtocolException("Unexpected response status: " + status);
                }
            };
            JsonNode responseBody = httpClient.execute(httpPost, responseHandler);
            //responseBody keys: id name last_activity execution_state connections
            String id = responseBody.path("id").asText();
            logger.printf("Created kernel %s id = %s\n", kernelName, id);
            return id;
        }
    }

    public static class WSClientBuilder {
        public static String defaultAddress = "localhost:8888";
        public static String defaultKernelName = "python";
        private String address;
        private String kernelID;
        private String kernelName;
        private PrintStream logger;

        /**
         * @param address address of Kernel Gateway default is localhost:8888
         * @return this
         */
        public WSClientBuilder withAddress(String address) {
            this.address = address;
            return this;
        }

        /**
         * @param kernelName kernel name, default is python
         * @return this
         */
        public WSClientBuilder withKernelName(String kernelName) {
            this.kernelName = kernelName;
            return this;
        }

        /**
         * @param kernelID if kernel id is specified, client will use the given kernel
         *                 otherwise a new kernel will be created
         * @return this
         */
        public WSClientBuilder withKernelID(String kernelID) {
            this.kernelID = kernelID;
            return this;
        }

        /**
         * @param logger for printing some logs, can be null
         * @return this
         */
        public WSClientBuilder withLogger(PrintStream logger) {
            this.logger = logger;
            return this;
        }

        public WSClient build() throws IOException {
            if (address == null || address.length() == 0) {
                address = defaultAddress;
            } else if (!address.contains(":")) {
                address += ":8888";
            }
            if (kernelName == null || kernelName.length() == 0) {
                kernelName = defaultKernelName;
            }
            if (kernelID == null || kernelID.length() == 0) {
                kernelID = createKernel(address, kernelName, logger);
            }
            URI uri;
            try {
                uri = new URI(String.format("ws://%s/api/kernels/%s/channels", address,
                        URLEncoder.encode(kernelID, "UTF-8")));
            } catch (URISyntaxException e) {
                throw new IOException((e));
            }
            return new WSClient(uri, logger);
        }

        public static void main(String[] args) throws Exception {
            WSClient c = null;
            try {
                c = new WSClient.WSClientBuilder()
                        .withLogger(System.out)
                        .withAddress(null)
                        .withKernelName(null)
                        .withKernelID(null)
                        .build();
                if (c.connectBlocking(1, TimeUnit.MINUTES)) {
                    c.sendCode("print('Hello gateway')");
                    c.await(5, TimeUnit.MINUTES);
                }
            } finally {
                if (c != null)
                    c.closeBlocking();
            }
        }
    }
}
