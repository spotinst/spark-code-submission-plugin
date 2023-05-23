package com.netapp.spark;

import org.apache.spark.SparkContext;
import org.apache.spark.api.plugin.PluginContext;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.http.HttpClient;
import java.net.http.WebSocket;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.TimerTask;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class SparkConnectWebsocketTranscodeDriverPlugin implements org.apache.spark.api.plugin.DriverPlugin {
    static Logger logger = LoggerFactory.getLogger(SparkConnectWebsocketTranscodeDriverPlugin.class);
    ExecutorService transcodeThreads;
    int port = -1;
    String urlstr;
    Map<String,String> headers;
    WebSocket   webSocket;

    public SparkConnectWebsocketTranscodeDriverPlugin(int port, String url, String header) {
        this();
        this.port = port;
        this.urlstr = url;
        this.headers = initHeaders(header);
    }

    public SparkConnectWebsocketTranscodeDriverPlugin() {
        transcodeThreads = Executors.newFixedThreadPool(10);
    }

    WebSocket getWebSocket(WritableByteChannel channel) {
        var wsListener = new SparkCodeSubmissionWebSocketListener();
        wsListener.setChannel(channel);

        var client = HttpClient.newHttpClient();
        var webSocketBuilder = client.newWebSocketBuilder();
        for (var h : headers.entrySet()) {
            webSocketBuilder = webSocketBuilder.header(h.getKey(), h.getValue());
        }
        return webSocketBuilder.buildAsync(java.net.URI.create(urlstr), wsListener).join();
    }

    void startTranscodeServer() {
        logger.info("Starting code submission server");
        transcodeThreads.submit(() -> {
            try (var serverSocket = new ServerSocket(port)) {
                var running = true;
                while (running) {
                    var socket = serverSocket.accept();
                    transcodeThreads.submit(() -> {
                        try (socket) {
                            var bb = new byte[1024*1024];
                            var output = socket.getOutputStream();
                            var input = socket.getInputStream();
                            var channel = Channels.newChannel(output);

                            var webSocket = getWebSocket(channel);
                            var timerTask = new TimerTask() {
                                @Override
                                public void run() {
                                    logger.info("sending ping");
                                    webSocket.sendPing(ByteBuffer.wrap("ping".getBytes()));
                                }
                            };
                            var timer = new java.util.Timer();
                            timer.schedule(timerTask, 5000, 5000);
                            while (true) {
                                var available = Math.max(input.available(), 1);
                                var read = input.read(bb, 0, Math.min(available, bb.length));
                                if (read == -1) {
                                    break;
                                } else {
                                    /*if (webSocket.isInputClosed() || webSocket.isOutputClosed()) {
                                        webSocket.sendClose(200, "Spark Connect closed");
                                        webSocket = getWebSocket(channel);
                                    }*/
                                    webSocket.sendBinary(ByteBuffer.wrap(bb, 0, read), true);
                                }
                            }
                            webSocket.sendText("loft", true);
                            webSocket.sendClose(200, "Spark Connect closed");
                            timer.cancel();
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    });
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }

    Map<String,String> initHeaders(String header) {
        var headers = new HashMap<String,String>();
        var hsplit = header.split(",");
        for (var h : hsplit) {
            var i = h.indexOf('=');
            if (i != -1) headers.put(h.substring(0,i), h.substring(i+1));
        }
        return headers;
    }

    private void fixContext(Path pysparkPath) {
        if (Files.exists(pysparkPath)) {
            try {
                var oldStatement = "raise RuntimeError(\n" +
                        "                                \"Cannot start a remote Spark session because there \"\n" +
                        "                                \"is a regular Spark session already running.\"\n" +
                        "                            )";
                var pyspark = Files.readString(pysparkPath);
                if (pyspark.contains(oldStatement)) {
                    pyspark = pyspark.replace(oldStatement, "url = opts.get(\"spark.remote\", os.environ.get(\"SPARK_REMOTE\"))\n" +
                            "\n" +
                            "                            if url.startswith(\"local\"):\n" +
                            "                                os.environ[\"SPARK_LOCAL_REMOTE\"] = \"1\"\n" +
                            "                                RemoteSparkSession._start_connect_server(url, opts)\n" +
                            "                                url = \"sc://localhost\"\n" +
                            "\n" +
                            "                            os.environ[\"SPARK_REMOTE\"] = url\n" +
                            "                            opts[\"spark.remote\"] = url\n" +
                            "                            return RemoteSparkSession.builder.config(map=opts).getOrCreate()");
                    Files.writeString(pysparkPath, pyspark);
                    logger.info("Pyspark initialize context altered");
                }
            } catch (IOException e) {
                logger.error("Failed to alter pyspark initialize context", e);
            }
        }
    }

    private void alterPysparkRemoteSession() {
        var sparkHome = System.getenv("SPARK_HOME");
        if (sparkHome != null) {
            var pysparkPath = Path.of(sparkHome, "python", "pyspark", "sql", "session.py");
            fixContext(pysparkPath);
        }
        var pysparkPython = System.getenv("PYSPARK_PYTHON");
        var cmd = pysparkPython != null ? pysparkPython : "python3";
        var processBuilder = new ProcessBuilder(cmd, "-c", "import pyspark, os; print(os.path.dirname(pyspark.__file__))");
        try {
            var process = processBuilder.start();
            var path = new String(process.getInputStream().readAllBytes());
            var pysparkPath = Path.of(path.trim(), "sql", "session.py");
            fixContext(pysparkPath);
        } catch (IOException e) {
            logger.info("Failed to alter pyspark initialize context info", e);
        }
    }

    @Override
    public Map<String,String> init(SparkContext sc, PluginContext myContext) {
        if (port == -1) {
            port = Integer.parseInt(sc.getConf().get("spark.code.submission.port", "15002"));
        }
        if (urlstr == null) {
            urlstr = sc.getConf().get("spark.code.submission.url", "ws://localhost:9000");
        }
        if (headers == null) {
            headers = initHeaders(sc.getConf().get("spark.code.submission.headers", ""));
        }
        startTranscodeServer();
        return Collections.emptyMap();
    }

    @Override
    public void shutdown() {
        transcodeThreads.shutdown();
    }

    public static void main(String[] args) {
        int port = Integer.parseInt(args[0]);
        var url = args[1];
        var auth = args.length > 2 ? args[2] : "";
        var plugin = new SparkConnectWebsocketTranscodeDriverPlugin(port, url, auth);
        plugin.startTranscodeServer();
    }
}
