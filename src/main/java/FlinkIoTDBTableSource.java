import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.iotdb.isession.ITableSession;
import org.apache.iotdb.session.TableSessionBuilder;
import org.apache.iotdb.isession.SessionDataSet;

public class FlinkIoTDBTableSource {
    public static void main(String[] args) throws Exception {
        // 1. Set up Flink Environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        System.out.println(">>> Starting Flink Job...");

        // 2. Add our Custom Source (This is the GSoC part!)
        env.addSource(new SourceFunction<String>() {
            volatile boolean isRunning = true;

            @Override
            public void run(SourceContext<String> ctx) throws Exception {
                // CRITICAL: We must use TableSessionBuilder here.
                // The standard 'new Session()' defaults to Tree Mode and fails
                // to parse SQL commands like 'CREATE TABLE'.
                try (ITableSession session = new TableSessionBuilder()
                        .nodeUrls(java.util.Collections.singletonList("127.0.0.1:6667"))
                        .username("root")
                        .password("root")
                        .build()) {

                    session.executeNonQueryStatement("USE factory_db");

                    // B. Query the Table
                    SessionDataSet dataSet = session.executeQueryStatement("SELECT * FROM machines");

                    // C. Stream the data into Flink
                    while (dataSet.hasNext() && isRunning) {
                        String row = dataSet.next().toString();
                        ctx.collect("FLINK READ: " + row); // Handing data to Flink
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

            @Override
            public void cancel() {
                isRunning = false;
            }
        }).print(); // 3. Print the output to the console

        // 4. Execute the Job
        env.execute("Table Mode Demo");
    }
}