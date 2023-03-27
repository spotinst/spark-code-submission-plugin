package com.netapp.spark;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.undertow.Undertow;
import io.undertow.server.handlers.BlockingHandler;
import org.apache.spark.SparkContext;
import org.apache.spark.api.plugin.PluginContext;
import org.apache.spark.api.python.Py4JServer;
import org.apache.spark.api.r.RAuthHelper;
import org.apache.spark.api.r.RBackend;
import org.apache.spark.sql.SQLContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import javax.tools.JavaCompiler;
import javax.tools.ToolProvider;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class SparkCodeSubmissionDriverPlugin implements org.apache.spark.api.plugin.DriverPlugin {
    static Logger logger = LoggerFactory.getLogger(SparkCodeSubmissionDriverPlugin.class);
    Undertow codeSubmissionServer;
    int port;
    ExecutorService virtualThreads;
    Py4JServer py4jServer;
    RBackend rBackend;
    int rbackendPort;
    String rbackendSecret;
    JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();

    public SparkCodeSubmissionDriverPlugin() {
        this(8080);
    }

    public SparkCodeSubmissionDriverPlugin(int port) {
        this.port = port;
        virtualThreads = Executors.newVirtualThreadPerTaskExecutor();
    }

    public int getPort() {
        return port;
    }

    private void initPy4JServer(SparkContext sc) {
        py4jServer = new Py4JServer(sc.conf());
        py4jServer.start();
    }

    private void initRBackend() {
        rBackend = new RBackend();
        Tuple2<Object, RAuthHelper> tuple = rBackend.init();
        rbackendPort = (Integer) tuple._1;
        rbackendSecret = tuple._2.secret();

        new Thread(() -> rBackend.run()).start();
    }

    private void runPython(String query) throws IOException, InterruptedException {
        var pysparkPython = System.getenv("PYSPARK_PYTHON");
        var cmd = pysparkPython != null ? pysparkPython : "python3";
        var     pb  = new ProcessBuilder(cmd);
        pb.redirectError(ProcessBuilder.Redirect.INHERIT);
        pb.redirectOutput(ProcessBuilder.Redirect.INHERIT);
        var env = pb.environment();

        var port = py4jServer.getListeningPort();
        var secret = py4jServer.secret();

        env.put("PYSPARK_GATEWAY_PORT", Integer.toString(port));
        env.put("PYSPARK_GATEWAY_SECRET", secret);
        env.put("PYSPARK_PIN_THREAD", "true");

        var pythonProcess = pb.start();
        pythonProcess.getOutputStream().write(query.getBytes());
        pythonProcess.waitFor();
    }

    private void runRscript(String query) throws IOException, InterruptedException {
        var pb  = new ProcessBuilder("Rscript");
        pb.redirectError(ProcessBuilder.Redirect.INHERIT);
        pb.redirectOutput(ProcessBuilder.Redirect.INHERIT);
        var env = pb.environment();

        env.put("EXISTING_SPARKR_BACKEND_PORT", Integer.toString(rbackendPort));
        env.put("SPARKR_BACKEND_AUTH_SECRET", rbackendSecret);

        var RProcess = pb.start();
        RProcess.getOutputStream().write(query.getBytes());
        RProcess.waitFor();
    }

    public void submitCode(SQLContext sqlContext, CodeSubmission codeSubmission) throws IOException, ClassNotFoundException, NoSuchMethodException {
        switch (codeSubmission.type()) {
            case SQL -> {
                System.err.println("heyhyhyho");
                var sqlCode = codeSubmission.code();
                virtualThreads.submit(() -> sqlContext.sql(sqlCode)
                        .write()
                        .format(codeSubmission.resultFormat())
                        .save(codeSubmission.resultsPath()));
            }
            case PYTHON -> {
                var pythonCode = codeSubmission.code();
                virtualThreads.submit(() -> {
                    try {
                        runPython(pythonCode);
                    } catch (IOException | InterruptedException e) {
                        logger.error("Python Execution failed", e);
                    }
                });
            }
            case R -> {
                var rCode = codeSubmission.code();
                virtualThreads.submit(() -> {
                    try {
                        runRscript(rCode);
                    } catch (IOException | InterruptedException e) {
                        logger.error("R Execution failed: ", e);
                    }
                });
            }
            case JAVA -> {
                var javaCode = codeSubmission.code();
                var classPath = Path.of(codeSubmission.className()+".java");
                Files.writeString(classPath, javaCode);
                int exitcode = compiler.run(System.in, System.out, System.err, classPath.getFileName().toString());
                if (exitcode != 0) {
                    logger.error("Java Compilation failed: "+exitcode);
                } else {
                    var submissionClass = this.getClass().getClassLoader().loadClass(codeSubmission.className());
                    var mainMethod = submissionClass.getMethod("main", String[].class);
                    virtualThreads.submit(() -> {
                        try {
                            mainMethod.invoke(null, (Object) new String[]{});
                        } catch (IllegalAccessException | InvocationTargetException e) {
                            logger.error("Java Execution failed: ", e);
                        }
                    });
                }
            }
            default -> {
                System.err.println("MUMUMUMUMUMU");
                logger.error("Unknown code type: "+codeSubmission.type());
            }
        }
    }

    @Override
    public Map<String,String> init(SparkContext sc, PluginContext myContext) {
        initPy4JServer(sc);
        initRBackend();

        var mapper = new ObjectMapper();
        var sqlContext = new org.apache.spark.sql.SQLContext(sc);

        codeSubmissionServer = Undertow.builder()
                .addHttpListener(port, "0.0.0.0")
                .setHandler(new BlockingHandler(exchange -> {
                    var codeSubmission = mapper.readValue(exchange.getInputStream(), CodeSubmission.class);
                    submitCode(sqlContext, codeSubmission);
                    exchange.getResponseSender().send(codeSubmission.type() + " code submitted");
                }))
                .build();
        codeSubmissionServer.start();

        return Map.of();
    }

    @Override
    public void shutdown() {
        if (codeSubmissionServer!=null) codeSubmissionServer.stop();
        if (py4jServer!=null) py4jServer.shutdown();
        if (rBackend!=null) rBackend.close();
        try {
            waitForVirtualThreads();
        } catch (InterruptedException e) {
            logger.debug("Interrupted while waiting for virtual threads to finish", e);
        }
        virtualThreads.shutdown();
    }

    public void waitForVirtualThreads() throws InterruptedException {
        virtualThreads.awaitTermination(10, TimeUnit.SECONDS);
    }

    public static void main(String[] args) {
        var sparkCodeSubmissionPlugin = new SparkCodeSubmissionPlugin();
        var sparkContext = new SparkContext("local", "test");
        sparkCodeSubmissionPlugin.driverPlugin().init(sparkContext, null);
    }
}
