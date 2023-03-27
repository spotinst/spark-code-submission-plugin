package spark;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.netapp.spark.CodeSubmission;
import com.netapp.spark.CodeSubmissionType;
import com.netapp.spark.SparkCodeSubmissionDriverPlugin;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.*;

import java.io.IOException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;

public class SparkCodeSubmissionDriverPluginTest {
    static SparkSession spark;
    static ObjectMapper mapper = new ObjectMapper();
    static SparkCodeSubmissionDriverPlugin sparkCodeSubmissionDriverPlugin;

    @BeforeAll
    public static void setup() {
        spark = SparkSession.builder().master("local").getOrCreate();
        spark.sql("select random()").count();
        sparkCodeSubmissionDriverPlugin = new SparkCodeSubmissionDriverPlugin(58979);
        sparkCodeSubmissionDriverPlugin.init(spark.sparkContext(), null);
    }

    private void checkResultsFile() throws IOException {
        var resultsFile = Path.of("test.csv");
        Assertions.assertTrue(Files.exists(resultsFile));
        try (var stream = Files.walk(resultsFile)) {
            var csvPath = stream.filter(Files::isRegularFile).filter(path -> path.getFileName().toString().endsWith("csv")).findFirst().get();
            var result = Files.readString(csvPath).trim();
            try {
                Double.parseDouble(result);
            } catch (NumberFormatException e) {
                Assertions.fail("Result is not a number: "+result);
            }
        }
        try (var resdir = Files.walk(resultsFile)) {
            resdir.sorted(Comparator.reverseOrder()).forEach(path -> {
                try {
                    Files.delete(path);
                } catch (IOException e) {
                    // Don't care
                }
            });
        }
    }

    @Test
    public void testSparkSQLSubmissionDriverPlugin() throws IOException, ClassNotFoundException, NoSuchMethodException, InterruptedException {
        var codeSubmission = new CodeSubmission(CodeSubmissionType.SQL, "select random()", "", "", "csv", "test.csv");
        sparkCodeSubmissionDriverPlugin.submitCode(spark.sqlContext(), codeSubmission);
        sparkCodeSubmissionDriverPlugin.waitForVirtualThreads();
        checkResultsFile();
    }

    @Test
    public void testSparkSQLSubmissionToServer() throws IOException, InterruptedException {
        var codeSubmission = new CodeSubmission(CodeSubmissionType.SQL, "select random()", "", "", "csv", "test.csv");
        var codeSubmissionJSON = mapper.writeValueAsString(codeSubmission);
        var client = HttpClient.newHttpClient();
        var request = HttpRequest.newBuilder()
                .uri(java.net.URI.create("http://localhost:"+sparkCodeSubmissionDriverPlugin.getPort()))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(codeSubmissionJSON))
                .build();
        var response = client.send(request, java.net.http.HttpResponse.BodyHandlers.ofString());
        Assertions.assertEquals(200, response.statusCode());
        Assertions.assertEquals("SQL code submitted", response.body());
        sparkCodeSubmissionDriverPlugin.waitForVirtualThreads();
        checkResultsFile();
    }

    @Test
    @Disabled
    public void testSparkJavaSubmissionToServer() {
        sparkCodeSubmissionDriverPlugin.init(spark.sparkContext(), null);
        var codeSubmission = new CodeSubmission(CodeSubmissionType.JAVA, "public class TestClass { public static String testMethod() { return \"Hello World!\"; } }", "TestClass", "", "", "");
        sparkCodeSubmissionDriverPlugin.shutdown();
    }

    @AfterAll
    public static void teardown() {
        sparkCodeSubmissionDriverPlugin.shutdown();
        spark.stop();
    }
}
