package com.netapp.spark;

import org.apache.spark.SparkContext;
import org.apache.spark.api.plugin.PluginContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.SocketException;
import java.net.http.HttpClient;
import java.net.http.WebSocket;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

public class SparkConnectWebsocketTranscodeDriverPlugin implements org.apache.spark.api.plugin.DriverPlugin {
    static Logger logger = LoggerFactory.getLogger(SparkConnectWebsocketTranscodeDriverPlugin.class);
    ExecutorService transcodeThreads;
    List<Integer> ports = Collections.emptyList();
    String urlstr;
    Map<String,String> headers;

    public SparkConnectWebsocketTranscodeDriverPlugin(List<Integer> ports, String url, String header) {
        this();
        this.ports = ports;
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

    void servePort(int port) {
        System.err.println("Starting server on port " + port);
        try (var serverSocket = new ServerSocket(port)) {
            var running = true;
            while (running) {
                System.err.println("Waiting for connection on port " + port);
                var socket = serverSocket.accept();
                System.err.println("Got connection on port " + port);
                transcodeThreads.submit(() -> {
                    try (socket) {
                        var bb = ByteBuffer.allocate(1024 * 1024);
                        bb.putInt(port);
                        int offset = 4;
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
                        var bba = bb.array();
                        while (true) {
                            var available = Math.max(input.available(), 1);
                            var read = input.read(bba, offset, Math.min(available, bba.length - offset));
                            if (read == -1) {
                                break;
                            } else {
                                        /*if (webSocket.isInputClosed() || webSocket.isOutputClosed()) {
                                            webSocket.sendClose(200, "Spark Connect closed");
                                            webSocket = getWebSocket(channel);
                                        }*/
                                webSocket.sendBinary(ByteBuffer.wrap(bba, 0, read+offset), true);
                            }
                            offset = 0;
                        }
                        webSocket.sendText("loft", true);
                        webSocket.sendClose(200, "Spark Connect closed");
                        timer.cancel();
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                });
            }
            System.err.println("Server on port " + port + " closed");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    void startTranscodeServers() {
        logger.info("Starting code submission server");
        for (int port : ports) {
            transcodeThreads.submit(() -> servePort(port));

            if (port == 10000) {
                try (var connection = DriverManager.getConnection("jdbc:hive2://localhost:10000"); var statement = connection.createStatement();) {
                    var resultSet = statement.executeQuery("SELECT * FROM global_temp.spark_connect_info");
                    while (resultSet.next()) {
                        var type = resultSet.getString(1);
                        var langport = resultSet.getInt(2)+10;
                        var secret = resultSet.getString(3);
                        if (type.equals("py4j")) {
                            System.err.println("export PYSPARK_PIN_THREAD=true");
                            System.err.println("export PYSPARK_GATEWAY_PORT=" + langport);
                            System.err.println("export PYSPARK_GATEWAY_SECRET=" + secret);
                            transcodeThreads.submit(() -> servePort(langport));
                            /*transcodeThreads.submit(() -> {
                                try (var datagramSocket = new java.net.DatagramSocket(langport)) {
                                    datagramSocket.setReuseAddress(true);
                                    var dpacket = new java.net.DatagramPacket(new byte[1024], 1024);
                                    datagramSocket.receive(dpacket);
                                    String received = new String(dpacket.getData(), 0, dpacket.getLength());
                                    System.out.println("Quote of the Moment: " + received);
                                } catch (IOException e) {
                                    throw new RuntimeException(e);
                                }
                            });*/
                        } else if(type.equals("rbackend")) {
                            System.err.println("export EXISTING_SPARKR_BACKEND_PORT=" + langport);
                            System.err.println("export SPARKR_BACKEND_AUTH_SECRET=" + secret);
                            transcodeThreads.submit(() -> servePort(langport));
                        }
                    }
                } catch (SQLException e) {
                    logger.error("Error getting spark connect info", e);
                }
            }
        }
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

    @Override
    public Map<String,String> init(SparkContext sc, PluginContext myContext) {
        if (ports.size() == 0) {
            ports = Arrays.stream(sc.getConf().get("spark.code.submission.ports", "15002").split(";")).mapToInt(Integer::parseInt).boxed().collect(Collectors.toList());
        }
        if (urlstr == null) {
            urlstr = sc.getConf().get("spark.code.submission.url", "ws://localhost:9000");
        }
        if (headers == null) {
            headers = initHeaders(sc.getConf().get("spark.code.submission.headers", ""));
        }
        startTranscodeServers();
        return Collections.emptyMap();
    }

    @Override
    public void shutdown() {
        transcodeThreads.shutdown();
    }

    public static void main(String[] args) {
        var ports = Arrays.stream(args[0].split(";")).mapToInt(Integer::parseInt).boxed().collect(Collectors.toList());
        var url = args[1];
        var auth = args.length > 2 ? args[2] : "";
        var plugin = new SparkConnectWebsocketTranscodeDriverPlugin(ports, url, auth);
        plugin.startTranscodeServers();
    }
}
